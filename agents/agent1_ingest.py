"""
Agent 1: Document Ingestion
============================
Parses BMD NTCS CSV exports (FIBU, WAWI) into structured database records.

Handles:
- Encoding detection (UTF-8 / Windows-1252)
- Semicolon delimiters
- Austrian date formats (DD.MM.YYYY)
- Decimal comma (1.234,56 -> 1234.56)
- File hash deduplication (won't process the same file twice)
- Malformed row quarantine
"""
import os
import hashlib
import re
from datetime import datetime
from pathlib import Path

import chardet
import pandas as pd
from rich.console import Console
from rich.table import Table as RichTable

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import (
    get_session, init_db, log_audit,
    Company, Transaction, Supplier, FileIngestionLog,
)

console = Console()

# ─── BMD FIBU Field Mapping ───
# BMD NTCS FIBU04 export: semicolon-delimited, variable encoding
# Field names can vary between BMD versions — we map common variants
FIBU_FIELD_VARIANTS = {
    "date": ["Buchungsdatum", "BuchDatum", "Datum", "Belegdatum"],
    "account": ["Konto", "KontoNr", "Konto-Nr", "Kontonummer"],
    "counter_account": ["Gegenkonto", "GegenKto", "Gegen-Konto", "GegenkontoNr"],
    "amount": ["Betrag", "Betrag EUR", "BetragEUR", "Nettobetrag"],
    "vat_code": ["Steuercode", "StCode", "USt-Code", "Steuerschluessel", "SteuerSchluessel"],
    "cost_center": ["Kostenstelle", "KSt", "KST", "Kostenst"],
    "document_ref": ["Belegnummer", "BelegNr", "Beleg-Nr", "BelNr"],
    "booking_text": ["Buchungstext", "Text", "BuchText", "Bezeichnung"],
    "document_date": ["Belegdatum", "BelDatum", "Beleg-Datum"],
}

# BMD WAWI field mapping for supplier data
WAWI_FIELD_VARIANTS = {
    "supplier_id": ["LieferantenNr", "Lieferant-Nr", "LfNr", "KreditorNr"],
    "name": ["Name", "Firmenname", "Firma", "Name1", "Bezeichnung"],
    "uid": ["UID", "UID-Nr", "UIDNr", "ATU", "Steuernummer"],
    "country": ["Land", "Laendercode", "LandCode", "ISO-Land"],
    "amount": ["Betrag", "Umsatz", "Jahresumsatz", "Gesamtbetrag"],
}


def detect_encoding(file_path: str) -> str:
    """Detect file encoding. Try UTF-8 first, fall back to Windows-1252."""
    with open(file_path, "rb") as f:
        raw = f.read(10000)  # Read first 10KB for detection

    # Try UTF-8 first
    try:
        raw.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        pass

    # Use chardet as fallback
    result = chardet.detect(raw)
    detected = result.get("encoding", "windows-1252")

    # Map common chardet results to standard names
    if detected and detected.lower() in ("iso-8859-1", "latin-1", "ascii"):
        return "windows-1252"

    return detected or "windows-1252"


def parse_austrian_date(date_str: str) -> str:
    """Convert DD.MM.YYYY to ISO YYYY-MM-DD format."""
    if not date_str or pd.isna(date_str):
        return None
    date_str = str(date_str).strip()

    # Try DD.MM.YYYY
    match = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", date_str)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    # Try DD.MM.YY
    match = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{2})", date_str)
    if match:
        day, month, year = match.groups()
        full_year = f"20{year}" if int(year) < 50 else f"19{year}"
        return f"{full_year}-{month.zfill(2)}-{day.zfill(2)}"

    # Already ISO?
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if match:
        return date_str

    return None


def parse_austrian_number(num_str) -> float:
    """Convert Austrian number format (1.234,56) to float (1234.56)."""
    if num_str is None or (isinstance(num_str, float) and pd.isna(num_str)):
        return 0.0
    if isinstance(num_str, (int, float)):
        return float(num_str)

    s = str(num_str).strip()
    if not s:
        return 0.0

    # Remove thousands separators (dots) and convert decimal comma to dot
    # Pattern: if there's both dot and comma, dot is thousands, comma is decimal
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return 0.0


def find_column(df_columns: list, field_variants: list) -> str:
    """Find matching column name from a list of variants."""
    df_cols_lower = {c.lower().strip(): c for c in df_columns}
    for variant in field_variants:
        if variant.lower() in df_cols_lower:
            return df_cols_lower[variant.lower()]
    return None


def compute_file_hash(file_path: str) -> str:
    """SHA-256 hash of file contents for deduplication."""
    sha = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def detect_file_type(df: pd.DataFrame) -> str:
    """Detect whether this is a FIBU or WAWI export based on columns."""
    cols_lower = [c.lower() for c in df.columns]

    fibu_indicators = ["konto", "gegenkonto", "buchungsdatum", "betrag", "belegnummer"]
    wawi_indicators = ["lieferantennr", "firmenname", "uid", "umsatz", "kreditor"]

    fibu_score = sum(1 for ind in fibu_indicators if any(ind in c for c in cols_lower))
    wawi_score = sum(1 for ind in wawi_indicators if any(ind in c for c in cols_lower))

    if fibu_score >= 2:
        return "FIBU"
    elif wawi_score >= 2:
        return "WAWI"
    else:
        return "UNKNOWN"


def ingest_fibu(df: pd.DataFrame, company_id: int, file_name: str, session) -> dict:
    """Parse FIBU CSV data into Transaction records."""
    cols = df.columns.tolist()

    # Map columns
    col_date = find_column(cols, FIBU_FIELD_VARIANTS["date"])
    col_account = find_column(cols, FIBU_FIELD_VARIANTS["account"])
    col_counter = find_column(cols, FIBU_FIELD_VARIANTS["counter_account"])
    col_amount = find_column(cols, FIBU_FIELD_VARIANTS["amount"])
    col_vat = find_column(cols, FIBU_FIELD_VARIANTS["vat_code"])
    col_cost = find_column(cols, FIBU_FIELD_VARIANTS["cost_center"])
    col_docref = find_column(cols, FIBU_FIELD_VARIANTS["document_ref"])
    col_text = find_column(cols, FIBU_FIELD_VARIANTS["booking_text"])

    if not col_account:
        return {"error": "Could not find account number column", "rows_valid": 0}
    if not col_amount:
        return {"error": "Could not find amount column", "rows_valid": 0}

    console.print(f"  [dim]Column mapping: date={col_date}, account={col_account}, "
                  f"amount={col_amount}, counter={col_counter}[/dim]")

    rows_valid = 0
    rows_quarantined = 0
    quarantine_reasons = []

    for idx, row in df.iterrows():
        try:
            account = str(row.get(col_account, "")).strip()
            amount_raw = row.get(col_amount)
            amount = parse_austrian_number(amount_raw)

            if not account:
                rows_quarantined += 1
                quarantine_reasons.append(f"Row {idx+2}: missing account number")
                continue

            date_val = parse_austrian_date(row.get(col_date)) if col_date else None

            tx = Transaction(
                company_id=company_id,
                date=date_val or "1900-01-01",
                account_number=account,
                counter_account=str(row.get(col_counter, "")).strip() if col_counter else None,
                amount_eur=amount,
                vat_code=str(row.get(col_vat, "")).strip() if col_vat else None,
                cost_center=str(row.get(col_cost, "")).strip() if col_cost else None,
                document_ref=str(row.get(col_docref, "")).strip() if col_docref else None,
                booking_text=str(row.get(col_text, "")).strip() if col_text else None,
                source_file=file_name,
                row_number=idx + 2,  # +2 for header row and 0-indexing
            )
            session.add(tx)
            rows_valid += 1

        except Exception as e:
            rows_quarantined += 1
            quarantine_reasons.append(f"Row {idx+2}: {str(e)}")

    session.commit()
    return {
        "rows_valid": rows_valid,
        "rows_quarantined": rows_quarantined,
        "quarantine_reasons": quarantine_reasons[:20],  # Cap at 20
    }


def ingest_wawi(df: pd.DataFrame, company_id: int, file_name: str, session) -> dict:
    """Parse WAWI CSV data into Supplier records."""
    cols = df.columns.tolist()

    col_name = find_column(cols, WAWI_FIELD_VARIANTS["name"])
    col_uid = find_column(cols, WAWI_FIELD_VARIANTS["uid"])
    col_country = find_column(cols, WAWI_FIELD_VARIANTS["country"])
    col_amount = find_column(cols, WAWI_FIELD_VARIANTS["amount"])

    if not col_name:
        return {"error": "Could not find supplier name column", "rows_valid": 0}

    rows_valid = 0
    rows_quarantined = 0

    for idx, row in df.iterrows():
        try:
            name = str(row.get(col_name, "")).strip()
            if not name or name == "nan":
                rows_quarantined += 1
                continue

            supplier = Supplier(
                company_id=company_id,
                name=name,
                uid_vat=str(row.get(col_uid, "")).strip() if col_uid else None,
                country=str(row.get(col_country, "")).strip() if col_country else None,
                spend_eur_annual=parse_austrian_number(row.get(col_amount)) if col_amount else None,
            )
            session.add(supplier)
            rows_valid += 1

        except Exception as e:
            rows_quarantined += 1

    session.commit()
    return {"rows_valid": rows_valid, "rows_quarantined": rows_quarantined}


def ingest_file(file_path: str, company_id: int) -> dict:
    """
    Main ingestion function. Call this with a CSV file path and a company ID.
    Returns a summary dict with rows parsed, valid, quarantined.
    """
    session = get_session()
    file_name = os.path.basename(file_path)

    console.print(f"\n[bold blue]Agent 1: Document Ingestion[/bold blue]")
    console.print(f"  File: {file_name}")

    # Step 1: Check for duplicate
    file_hash = compute_file_hash(file_path)
    existing = session.query(FileIngestionLog).filter_by(file_hash=file_hash).first()
    if existing:
        console.print(f"  [yellow]SKIP: File already ingested on {existing.ingested_at}[/yellow]")
        session.close()
        return {"status": "duplicate", "original_ingestion": str(existing.ingested_at)}

    # Step 2: Detect encoding
    encoding = detect_encoding(file_path)
    console.print(f"  Encoding: {encoding}")

    # Step 3: Read CSV with detected encoding
    try:
        # Try semicolon first (BMD standard), then comma, then tab
        for sep in [";", ",", "\t"]:
            try:
                df = pd.read_csv(file_path, encoding=encoding, sep=sep, dtype=str)
                if len(df.columns) > 2:
                    console.print(f"  Delimiter: '{sep}' ({len(df.columns)} columns, {len(df)} rows)")
                    break
            except Exception:
                continue
        else:
            raise ValueError("Could not parse CSV with any common delimiter")
    except Exception as e:
        # Quarantine the file
        quarantine_path = os.path.join(
            os.path.dirname(os.path.dirname(file_path)), "data", "quarantine", file_name
        )
        os.makedirs(os.path.dirname(quarantine_path), exist_ok=True)

        log = FileIngestionLog(
            file_name=file_name, file_hash=file_hash,
            encoding_detected=encoding, rows_parsed=0, rows_valid=0,
            rows_quarantined=0, status="failed", error_message=str(e),
        )
        session.add(log)
        session.commit()
        console.print(f"  [red]FAILED: {e}[/red]")
        session.close()
        return {"status": "failed", "error": str(e)}

    # Step 4: Detect file type
    file_type = detect_file_type(df)
    console.print(f"  Detected type: {file_type}")

    # Step 5: Parse based on type
    if file_type == "FIBU":
        result = ingest_fibu(df, company_id, file_name, session)
    elif file_type == "WAWI":
        result = ingest_wawi(df, company_id, file_name, session)
    else:
        # Try FIBU parsing as default
        console.print("  [yellow]Unknown type, attempting FIBU parsing...[/yellow]")
        result = ingest_fibu(df, company_id, file_name, session)

    # Step 6: Log ingestion
    log = FileIngestionLog(
        file_name=file_name,
        file_hash=file_hash,
        file_size=os.path.getsize(file_path),
        encoding_detected=encoding,
        rows_parsed=len(df),
        rows_valid=result.get("rows_valid", 0),
        rows_quarantined=result.get("rows_quarantined", 0),
        status="success" if result.get("rows_valid", 0) > 0 else "failed",
        error_message=result.get("error"),
    )
    session.add(log)
    log_audit(session, "file_ingested", "file", file_name,
              f"type={file_type}, valid={result.get('rows_valid', 0)}, quarantined={result.get('rows_quarantined', 0)}")
    session.commit()

    # Print summary
    console.print(f"  [green]Valid rows: {result.get('rows_valid', 0)}[/green]")
    if result.get("rows_quarantined", 0) > 0:
        console.print(f"  [yellow]Quarantined rows: {result['rows_quarantined']}[/yellow]")

    session.close()
    return {
        "status": "success",
        "file_type": file_type,
        "encoding": encoding,
        "rows_total": len(df),
        **result,
    }


def ingest_all_from_inbox(company_id: int) -> list:
    """Scan the data/inbox folder and ingest all CSV files."""
    inbox = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "inbox")
    results = []

    csv_files = sorted(Path(inbox).glob("*.csv")) + sorted(Path(inbox).glob("*.CSV"))
    if not csv_files:
        console.print("[yellow]No CSV files found in data/inbox/[/yellow]")
        return results

    for csv_file in csv_files:
        result = ingest_file(str(csv_file), company_id)
        results.append({"file": csv_file.name, **result})

        # Move processed file
        if result.get("status") == "success":
            processed_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "processed")
            os.makedirs(processed_dir, exist_ok=True)
            dest = os.path.join(processed_dir, csv_file.name)
            os.rename(str(csv_file), dest)
            console.print(f"  [dim]Moved to data/processed/[/dim]")

    return results


if __name__ == "__main__":
    init_db()
    # Quick test: create a dummy company and ingest from inbox
    session = get_session()
    company = session.query(Company).first()
    if not company:
        company = Company(name="Test Company", uid_vat="ATU12345678")
        session.add(company)
        session.commit()
    session.close()

    results = ingest_all_from_inbox(company.id)
    console.print(f"\n[bold]Ingestion complete: {len(results)} files processed[/bold]")
