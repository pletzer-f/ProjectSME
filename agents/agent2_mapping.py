"""
Agent 2: Account Mapping
=========================
Takes unique accounts from parsed transactions, sends them to Claude API
for ESG classification, then presents results for human confirmation.

The mapping library compounds: client 1 is hardest, client 50 is mostly auto-matched.
"""
import os
import json
from datetime import datetime

from rich.console import Console
from rich.table import Table as RichTable
from rich.prompt import Prompt, Confirm

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_session, log_audit, AccountMapping, Transaction, Company

console = Console()

# Valid ESG categories
ESG_CATEGORIES = [
    "energy_electricity",  # Electricity (Scope 2)
    "energy_gas",          # Natural gas (Scope 1 or 2)
    "fuel",                # Diesel, petrol, heating oil (Scope 1)
    "water",               # Water consumption
    "waste",               # Waste disposal and recycling
    "logistics",           # Freight, transport, shipping
    "materials",           # Raw materials, office supplies
    "travel",              # Business travel
    "hr_payroll",          # Wages, salaries, social contributions
    "rent_facilities",     # Rent, building costs
    "insurance",           # Insurance premiums
    "professional_fees",   # Legal, consulting, audit fees
    "revenue",             # Sales revenue (not ESG-relevant for emissions)
    "financial",           # Interest, bank charges, financial instruments
    "tax",                 # Tax payments
    "depreciation",        # Depreciation and amortisation
    "other",               # Cannot be classified into above categories
]


def get_unmapped_accounts(company_id: int) -> list:
    """Find accounts in transactions that don't have mappings yet."""
    session = get_session()

    # Get all unique accounts from transactions
    tx_accounts = (
        session.query(Transaction.account_number)
        .filter(Transaction.company_id == company_id)
        .distinct()
        .all()
    )
    tx_account_numbers = {a[0] for a in tx_accounts}

    # Get already mapped accounts
    mapped = (
        session.query(AccountMapping.account_number)
        .filter(AccountMapping.company_id == company_id)
        .all()
    )
    mapped_numbers = {m[0] for m in mapped}

    unmapped = tx_account_numbers - mapped_numbers
    session.close()
    return sorted(list(unmapped))


def get_account_context(company_id: int, account_number: str, limit: int = 10) -> list:
    """Get recent transactions for an account to provide context for classification."""
    session = get_session()
    txs = (
        session.query(Transaction)
        .filter(
            Transaction.company_id == company_id,
            Transaction.account_number == account_number,
        )
        .order_by(Transaction.date.desc())
        .limit(limit)
        .all()
    )

    context = []
    for tx in txs:
        context.append({
            "date": tx.date,
            "amount": tx.amount_eur,
            "booking_text": tx.booking_text or "",
            "counter_account": tx.counter_account or "",
        })

    # Also get total spend for this account
    from sqlalchemy import func
    total = (
        session.query(func.sum(Transaction.amount_eur))
        .filter(
            Transaction.company_id == company_id,
            Transaction.account_number == account_number,
        )
        .scalar()
    )

    session.close()
    return context, total or 0.0


def check_mapping_library(account_name: str) -> dict:
    """
    Check if this account name has been seen and confirmed in other companies.
    This is how the mapping library compounds across clients.
    """
    session = get_session()
    # Look for confirmed mappings with similar account names
    similar = (
        session.query(AccountMapping)
        .filter(
            AccountMapping.account_name == account_name,
            AccountMapping.confirmed_by.isnot(None),  # Only confirmed mappings
        )
        .all()
    )
    session.close()

    if similar:
        # Return the most common category
        categories = [m.esg_category for m in similar]
        most_common = max(set(categories), key=categories.count)
        confidence = categories.count(most_common) / len(categories)
        return {
            "category": most_common,
            "confidence": confidence,
            "source": "mapping_library",
            "matches": len(similar),
        }
    return None


def classify_with_claude(account_number: str, account_name: str,
                         transactions: list, total_spend: float) -> dict:
    """
    Send account info to Claude API for ESG classification.
    Returns: {"category": str, "confidence": float, "reasoning": str}
    """
    try:
        from anthropic import Anthropic
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"), override=True)

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key or api_key.startswith("sk-ant-api03-YOUR"):
            return {
                "category": "other",
                "confidence": 0.0,
                "reasoning": "NO API KEY - Set ANTHROPIC_API_KEY in .env file",
                "source": "no_api_key",
            }

        client = Anthropic(api_key=api_key)

        # Build transaction context string
        tx_context = ""
        for tx in transactions[:10]:
            tx_context += f"  - {tx['date']}: EUR {tx['amount']:.2f} | {tx['booking_text']}\n"

        prompt = f"""You are classifying Austrian accounting accounts (Kontenplan) into ESG categories for CSRD/ESRS compliance reporting.

Account Number: {account_number}
Account Name: {account_name or 'Unknown'}
Total Annual Spend: EUR {total_spend:,.2f}

Recent transactions from this account:
{tx_context if tx_context else '  No transaction data available'}

Classify this account into exactly ONE of these categories:
- energy_electricity: Electricity costs (Strom)
- energy_gas: Natural gas costs (Erdgas, Gas)
- fuel: Diesel, petrol, heating oil (Treibstoff, Heizoel)
- water: Water consumption (Wasser)
- waste: Waste disposal, recycling (Abfall, Entsorgung, Muell)
- logistics: Freight, transport, shipping (Fracht, Transport, Versand)
- materials: Raw materials, office supplies (Material, Rohstoffe, Buero)
- travel: Business travel (Reise, Dienstreise, Flug)
- hr_payroll: Wages, salaries, social costs (Loehne, Gehaelter, Sozialaufwand)
- rent_facilities: Rent, building costs (Miete, Gebaeude, Betriebskosten)
- insurance: Insurance (Versicherung)
- professional_fees: Legal, consulting, audit (Rechtsberatung, Beratung, Pruefung)
- revenue: Sales revenue (Umsatz, Erloes) - not relevant for emissions
- financial: Bank charges, interest (Zinsen, Bankspesen, Finanzaufwand)
- tax: Tax payments (Steuer, KoeSt, USt)
- depreciation: Depreciation (Abschreibung, AfA)
- other: Cannot determine from available information

Respond with ONLY a JSON object (no markdown, no backticks):
{{"category": "the_category", "confidence": 0.85, "reasoning": "Brief explanation in German or English"}}"""

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )

        # Parse response
        text = response.content[0].text.strip()
        # Handle potential markdown code blocks
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        result = json.loads(text)
        result["source"] = "claude_api"
        return result

    except json.JSONDecodeError as e:
        return {
            "category": "other",
            "confidence": 0.0,
            "reasoning": f"Failed to parse Claude response: {text[:200]}",
            "source": "claude_api_error",
        }
    except Exception as e:
        return {
            "category": "other",
            "confidence": 0.0,
            "reasoning": f"API error: {str(e)}",
            "source": "error",
        }


def map_accounts_interactive(company_id: int) -> dict:
    """
    Main mapping function. Classifies unmapped accounts and asks for human confirmation.
    """
    console.print(f"\n[bold blue]Agent 2: Account Mapping[/bold blue]")

    session = get_session()
    company = session.query(Company).get(company_id)
    if not company:
        console.print("[red]Company not found[/red]")
        session.close()
        return {"error": "Company not found"}
    session.close()

    unmapped = get_unmapped_accounts(company_id)
    if not unmapped:
        console.print("  [green]All accounts are already mapped![/green]")
        return {"status": "complete", "mapped": 0}

    console.print(f"  Found [bold]{len(unmapped)}[/bold] unmapped accounts")
    console.print()

    mapped_count = 0
    auto_count = 0

    for account_number in unmapped:
        transactions, total_spend = get_account_context(company_id, account_number)

        # Get account name from transactions
        session = get_session()
        # Account name might be in the booking text or we derive it from the number
        account_name = account_number  # Default
        if transactions and transactions[0].get("booking_text"):
            account_name = f"{account_number}"
        session.close()

        # Step 1: Check mapping library (from other clients)
        library_match = check_mapping_library(account_name)

        # Step 2: If no library match, use Claude API
        if library_match and library_match["confidence"] >= 0.9:
            suggestion = library_match
            console.print(f"  [dim]Account {account_number}: auto-matched from library "
                          f"-> {suggestion['category']} ({suggestion['matches']} previous matches)[/dim]")
        else:
            suggestion = classify_with_claude(account_number, account_name, transactions, total_spend)

        # Step 3: Display suggestion and ask for confirmation
        table = RichTable(title=f"Account: {account_number}", show_header=False, box=None)
        table.add_row("Total Spend", f"EUR {total_spend:,.2f}")
        table.add_row("Suggested Category", f"[bold]{suggestion['category']}[/bold]")
        table.add_row("Confidence", f"{suggestion.get('confidence', 0):.0%}")
        table.add_row("Reasoning", suggestion.get("reasoning", "N/A"))
        if transactions:
            sample_texts = [t["booking_text"] for t in transactions[:3] if t.get("booking_text")]
            table.add_row("Sample Bookings", " | ".join(sample_texts) if sample_texts else "N/A")
        console.print(table)

        # Auto-accept high-confidence suggestions
        if suggestion.get("confidence", 0) >= 0.9 and suggestion.get("source") != "no_api_key":
            console.print(f"  [green]Auto-accepted (confidence >= 90%)[/green]")
            confirmed_category = suggestion["category"]
            confirmed_by = "auto_high_confidence"
            auto_count += 1
        else:
            # Ask human
            console.print(f"\n  Categories: {', '.join(ESG_CATEGORIES)}")
            choice = Prompt.ask(
                "  Confirm category (Enter to accept, or type new category)",
                default=suggestion["category"],
            )
            if choice in ESG_CATEGORIES:
                confirmed_category = choice
            else:
                console.print(f"  [yellow]Invalid category '{choice}', using suggestion[/yellow]")
                confirmed_category = suggestion["category"]
            confirmed_by = "human"

        # Step 4: Save mapping
        session = get_session()
        mapping = AccountMapping(
            company_id=company_id,
            account_number=account_number,
            account_name=account_name,
            esg_category=confirmed_category,
            confidence_score=suggestion.get("confidence", 0),
            confirmed_by=confirmed_by,
            confirmed_at=datetime.utcnow(),
            source=suggestion.get("source", "unknown"),
        )
        session.add(mapping)
        log_audit(session, "account_mapped", "account_mapping", account_number,
                  f"category={confirmed_category}, confidence={suggestion.get('confidence', 0):.2f}, source={suggestion.get('source')}")
        session.commit()
        session.close()

        mapped_count += 1
        console.print()

    console.print(f"  [bold green]Mapping complete: {mapped_count} accounts mapped "
                  f"({auto_count} auto-accepted, {mapped_count - auto_count} human-confirmed)[/bold green]")

    return {
        "status": "complete",
        "mapped": mapped_count,
        "auto_accepted": auto_count,
        "human_confirmed": mapped_count - auto_count,
    }


def map_accounts_auto(company_id: int) -> dict:
    """
    Non-interactive version: classify all accounts with Claude, auto-accept
    all high-confidence (>=80%), mark low-confidence for later review.
    Use this for batch processing.
    """
    console.print(f"\n[bold blue]Agent 2: Account Mapping (Auto Mode)[/bold blue]")

    unmapped = get_unmapped_accounts(company_id)
    if not unmapped:
        console.print("  [green]All accounts already mapped![/green]")
        return {"status": "complete", "mapped": 0}

    console.print(f"  Found [bold]{len(unmapped)}[/bold] unmapped accounts")

    mapped_count = 0
    needs_review = []

    for account_number in unmapped:
        transactions, total_spend = get_account_context(company_id, account_number)
        account_name = account_number

        # Check library first, then Claude
        library_match = check_mapping_library(account_name)
        if library_match and library_match["confidence"] >= 0.8:
            suggestion = library_match
        else:
            suggestion = classify_with_claude(account_number, account_name, transactions, total_spend)

        # Save mapping
        session = get_session()
        confirmed_by = "auto" if suggestion.get("confidence", 0) >= 0.8 else "needs_review"

        if suggestion.get("confidence", 0) < 0.8:
            needs_review.append({
                "account": account_number,
                "suggested": suggestion["category"],
                "confidence": suggestion.get("confidence", 0),
                "reasoning": suggestion.get("reasoning", ""),
            })

        mapping = AccountMapping(
            company_id=company_id,
            account_number=account_number,
            account_name=account_name,
            esg_category=suggestion["category"],
            confidence_score=suggestion.get("confidence", 0),
            confirmed_by=confirmed_by,
            confirmed_at=datetime.utcnow() if confirmed_by == "auto" else None,
            source=suggestion.get("source", "unknown"),
        )
        session.add(mapping)
        session.commit()
        session.close()

        mapped_count += 1
        status = "[green]OK[/green]" if confirmed_by == "auto" else "[yellow]REVIEW[/yellow]"
        console.print(f"  {account_number} -> {suggestion['category']} "
                      f"({suggestion.get('confidence', 0):.0%}) {status}")

    console.print(f"\n  [bold]Mapped: {mapped_count} | Needs review: {len(needs_review)}[/bold]")
    return {
        "status": "complete",
        "mapped": mapped_count,
        "needs_review": needs_review,
    }


if __name__ == "__main__":
    from db import init_db
    init_db()

    session = get_session()
    company = session.query(Company).first()
    session.close()

    if company:
        map_accounts_interactive(company.id)
    else:
        console.print("[red]No company found. Run agent1_ingest.py first.[/red]")
