#!/usr/bin/env python3
"""
SME Data Intelligence Pipeline
================================
One command runs the entire pipeline:
    python run_pipeline.py

What it does:
1. Ingests all CSV files from data/inbox/
2. Maps accounts to ESG categories (via Claude API or manual)
3. Calculates Scope 1/2/3 emissions (deterministic, no AI)
4. Generates an ESRS E1 gap report as a Word document

Output: output/ folder contains the generated report.

Setup:
  pip install -r requirements.txt
  cp .env.example .env
  # Edit .env with your Anthropic API key
  # Drop BMD CSV files into data/inbox/
  python run_pipeline.py
"""
import os
import sys

# Ensure imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

from db import init_db, get_session, Company
from agents.agent1_ingest import ingest_all_from_inbox
from agents.agent2_mapping import map_accounts_interactive, map_accounts_auto
from agents.agent3_emissions import calculate_emissions
from agents.agent4_report import generate_report

console = Console()


def get_or_create_company() -> int:
    """Get existing company or create a new one."""
    session = get_session()
    companies = session.query(Company).all()

    if companies:
        if len(companies) == 1:
            console.print(f"  Using company: [bold]{companies[0].name}[/bold] (ID: {companies[0].id})")
            cid = companies[0].id
            session.close()
            return cid

        console.print("\n  Existing companies:")
        for c in companies:
            console.print(f"    [{c.id}] {c.name} ({c.uid_vat or 'no UID'})")
        choice = Prompt.ask("  Select company ID or 'new' for a new company", default=str(companies[0].id))

        if choice.lower() == "new":
            session.close()
            return create_company()

        try:
            cid = int(choice)
            session.close()
            return cid
        except ValueError:
            session.close()
            return companies[0].id
    else:
        session.close()
        return create_company()


def create_company() -> int:
    """Create a new company interactively."""
    console.print("\n  [bold]Create a new company:[/bold]")
    name = Prompt.ask("  Company name", default="Test Company")
    uid = Prompt.ask("  UID/VAT number (e.g. ATU12345678)", default="")
    nace = Prompt.ask("  NACE code (e.g. C16 for wood products)", default="")
    employees = Prompt.ask("  Number of employees", default="0")

    session = get_session()
    company = Company(
        name=name,
        uid_vat=uid or None,
        nace_code=nace or None,
        size_employees=int(employees) if employees else None,
        status="active",
    )
    session.add(company)
    session.commit()
    cid = company.id
    console.print(f"  [green]Created company: {name} (ID: {cid})[/green]")
    session.close()
    return cid


def main():
    console.print(Panel.fit(
        "[bold blue]SME Data Intelligence Pipeline[/bold blue]\n"
        "[dim]CSV \u2192 Parse \u2192 Map Accounts \u2192 Calculate Emissions \u2192 Generate Report[/dim]",
        border_style="blue",
    ))

    # Initialize database
    console.print("\n[bold]Step 0: Initializing database...[/bold]")
    init_db()
    console.print("  [green]Database ready[/green]")

    # Get or create company
    console.print("\n[bold]Step 1: Company setup[/bold]")
    company_id = get_or_create_company()

    # Check for files
    inbox = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "inbox")
    csv_files = [f for f in os.listdir(inbox) if f.lower().endswith(".csv")] if os.path.exists(inbox) else []

    if csv_files:
        console.print(f"\n[bold]Step 2: Ingesting {len(csv_files)} CSV file(s)...[/bold]")
        ingest_results = ingest_all_from_inbox(company_id)
    else:
        console.print(f"\n[bold]Step 2: No CSV files in data/inbox/[/bold]")
        console.print("  [yellow]Drop your BMD CSV exports into pipeline/data/inbox/ and re-run.[/yellow]")
        console.print("  [dim]A sample file has been provided for testing: data/inbox/sample_fibu.csv[/dim]")

        # Check if there's existing data
        session = get_session()
        from db import Transaction
        tx_count = session.query(Transaction).filter(Transaction.company_id == company_id).count()
        session.close()

        if tx_count == 0:
            console.print("  [red]No data to process. Exiting.[/red]")
            return

        console.print(f"  [dim]Using {tx_count} existing transactions from previous ingestion.[/dim]")

    # Account mapping
    console.print(f"\n[bold]Step 3: Account Mapping[/bold]")
    mode = Prompt.ask(
        "  Mapping mode",
        choices=["interactive", "auto", "skip"],
        default="auto",
    )

    if mode == "interactive":
        mapping_result = map_accounts_interactive(company_id)
    elif mode == "auto":
        mapping_result = map_accounts_auto(company_id)
    else:
        console.print("  [dim]Skipping mapping (using existing mappings)[/dim]")

    # Emissions calculation
    console.print(f"\n[bold]Step 4: Emissions Calculation[/bold]")
    emissions_result = calculate_emissions(company_id)

    # Report generation
    console.print(f"\n[bold]Step 5: Report Generation[/bold]")
    report_path = generate_report(company_id)

    # Summary
    console.print(Panel.fit(
        f"[bold green]Pipeline Complete![/bold green]\n\n"
        f"Report saved to:\n[bold]{report_path}[/bold]\n\n"
        f"[dim]Open the .docx file to review the ESRS E1 gap report.\n"
        f"Remember: This is a DRAFT that requires human review.[/dim]",
        border_style="green",
    ))


if __name__ == "__main__":
    main()
