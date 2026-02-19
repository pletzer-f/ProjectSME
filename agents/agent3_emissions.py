"""
Agent 3: Emissions Calculation Engine
=======================================
DETERMINISTIC ONLY. No LLM. Every calculation is:
    quantity x emission_factor = tCO2e

Every number is fully traceable: input values, factor used, factor source,
factor vintage, formula applied, calculation timestamp.

Austrian emission factors from Umweltbundesamt.
"""
import os
from datetime import datetime
from collections import defaultdict

from rich.console import Console
from rich.table import Table as RichTable

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_session, log_audit, AccountMapping, Transaction, EmissionRecord, Company

console = Console()

# ═══════════════════════════════════════════════════════════════
# EMISSION FACTORS — Austrian Umweltbundesamt (UBA)
# Source: Umweltbundesamt Austria, Emissionsfaktoren
# All factors in kg CO2e per unit
# ═══════════════════════════════════════════════════════════════
EMISSION_FACTORS = {
    # ─── SCOPE 2: Purchased Energy ───
    "electricity_austria": {
        "factor": 0.071,       # kg CO2e per kWh
        "unit": "kWh",
        "source": "Umweltbundesamt Austria - Stromkennzeichnung",
        "vintage": 2024,
        "scope": 2,
        "notes": "Austrian grid average including renewables (~78% renewable share)",
    },
    "electricity_eu_average": {
        "factor": 0.233,
        "unit": "kWh",
        "source": "European Environment Agency",
        "vintage": 2023,
        "scope": 2,
        "notes": "EU-27 average for comparison",
    },
    "district_heating_austria": {
        "factor": 0.124,
        "unit": "kWh",
        "source": "Umweltbundesamt Austria - Fernwaerme",
        "vintage": 2024,
        "scope": 2,
        "notes": "Austrian district heating average",
    },
    "natural_gas_scope2": {
        "factor": 0.201,
        "unit": "kWh",
        "source": "Umweltbundesamt Austria - Erdgas",
        "vintage": 2024,
        "scope": 2,
        "notes": "Natural gas for heating (if purchased as energy, Scope 2)",
    },

    # ─── SCOPE 1: Direct Combustion ───
    "natural_gas_scope1": {
        "factor": 0.201,
        "unit": "kWh",
        "source": "Umweltbundesamt Austria - Erdgas",
        "vintage": 2024,
        "scope": 1,
        "notes": "Natural gas burned on-site (boilers, furnaces)",
    },
    "heating_oil": {
        "factor": 0.266,
        "unit": "kWh",
        "source": "Umweltbundesamt Austria - Heizoel Extra Leicht",
        "vintage": 2024,
        "scope": 1,
        "notes": "Heating oil (Heizoel EL), ~10 kWh per litre",
    },
    "diesel": {
        "factor": 2.64,
        "unit": "litre",
        "source": "Umweltbundesamt Austria - Diesel",
        "vintage": 2024,
        "scope": 1,
        "notes": "Diesel fuel for company vehicles/machinery",
    },
    "petrol": {
        "factor": 2.37,
        "unit": "litre",
        "source": "Umweltbundesamt Austria - Benzin",
        "vintage": 2024,
        "scope": 1,
        "notes": "Petrol fuel for company vehicles",
    },

    # ─── SCOPE 3: Upstream ───
    "business_travel_flight_short": {
        "factor": 0.255,
        "unit": "passenger-km",
        "source": "Umweltbundesamt Austria / DEFRA 2024",
        "vintage": 2024,
        "scope": 3,
        "notes": "Short-haul flights (<1500km), economy class",
    },
    "business_travel_flight_long": {
        "factor": 0.195,
        "unit": "passenger-km",
        "source": "Umweltbundesamt Austria / DEFRA 2024",
        "vintage": 2024,
        "scope": 3,
        "notes": "Long-haul flights (>1500km), economy class",
    },
    "business_travel_train": {
        "factor": 0.006,
        "unit": "passenger-km",
        "source": "OeBB Umweltbilanz",
        "vintage": 2024,
        "scope": 3,
        "notes": "Austrian rail (OeBB), very low due to 100% renewable electricity",
    },
    "freight_road": {
        "factor": 0.062,
        "unit": "tonne-km",
        "source": "Umweltbundesamt Austria - Strassengueterverkehr",
        "vintage": 2024,
        "scope": 3,
        "notes": "Road freight, average truck",
    },
}

# ─── Price-to-quantity conversion factors (Austrian averages) ───
# Used to estimate physical quantities from EUR spend
PRICE_CONVERSIONS = {
    "energy_electricity": {
        "eur_per_unit": 0.22,   # EUR per kWh (Austrian SME electricity price ~0.22 EUR/kWh)
        "unit": "kWh",
        "factor_key": "electricity_austria",
    },
    "energy_gas": {
        "eur_per_unit": 0.08,   # EUR per kWh (Austrian natural gas ~0.08 EUR/kWh)
        "unit": "kWh",
        "factor_key": "natural_gas_scope1",
    },
    "fuel": {
        "eur_per_unit": 1.50,   # EUR per litre (Austrian diesel ~1.50 EUR/l)
        "unit": "litre",
        "factor_key": "diesel",
    },
    "logistics": {
        "eur_per_unit": 0.15,   # EUR per tonne-km (rough estimate)
        "unit": "tonne-km",
        "factor_key": "freight_road",
    },
}


def calculate_emissions(company_id: int, period: str = None) -> dict:
    """
    Main emissions calculation function.

    For each mapped ESG category with a price-to-quantity conversion:
    1. Sum total EUR spend from transactions
    2. Convert EUR to physical quantity (kWh, litres, etc.)
    3. Apply emission factor: quantity x factor = kg CO2e
    4. Store result with full traceability

    Args:
        company_id: The company to calculate for
        period: Optional period filter (e.g. "2025"). If None, uses all data.

    Returns:
        Summary dict with scope 1, 2, 3 totals
    """
    console.print(f"\n[bold blue]Agent 3: Emissions Calculation Engine[/bold blue]")
    console.print(f"  [dim]All calculations are deterministic. No LLM used.[/dim]")

    session = get_session()

    company = session.get(Company, company_id)
    if not company:
        console.print("[red]Company not found[/red]")
        session.close()
        return {"error": "Company not found"}

    # Store company name before we might close the session
    company_name = company.name

    # Get all mappings for this company
    mappings = (
        session.query(AccountMapping)
        .filter(AccountMapping.company_id == company_id)
        .all()
    )
    if not mappings:
        console.print("  [yellow]No account mappings found. Run Agent 2 first.[/yellow]")
        session.close()
        return {"error": "No mappings"}

    # Build account -> category lookup
    account_category = {m.account_number: m.esg_category for m in mappings}

    # Get transactions and sum by ESG category
    query = session.query(Transaction).filter(Transaction.company_id == company_id)
    if period:
        query = query.filter(Transaction.date.like(f"{period}%"))
    transactions = query.all()

    if not transactions:
        console.print("  [yellow]No transactions found.[/yellow]")
        session.close()
        return {"error": "No transactions"}

    # Determine period from data if not specified
    if not period:
        dates = [t.date for t in transactions if t.date and t.date != "1900-01-01"]
        if dates:
            years = set(d[:4] for d in dates if len(d) >= 4)
            period = "-".join(sorted(years)) if len(years) > 1 else list(years)[0]
        else:
            period = "unknown"

    console.print(f"  Period: {period}")
    console.print(f"  Transactions: {len(transactions)}")
    console.print(f"  Mapped accounts: {len(account_category)}")

    # Sum spend by ESG category
    category_spend = defaultdict(float)
    for tx in transactions:
        cat = account_category.get(tx.account_number, "other")
        if tx.amount_eur and tx.amount_eur > 0:  # Only expenses (positive amounts)
            category_spend[cat] += abs(tx.amount_eur)

    # Clear previous emissions for this period
    session.query(EmissionRecord).filter(
        EmissionRecord.company_id == company_id,
        EmissionRecord.period == period,
    ).delete()
    session.commit()

    # Calculate emissions for each category with a conversion factor
    results = []
    scope_totals = {1: 0.0, 2: 0.0, 3: 0.0}

    for category, conversion in PRICE_CONVERSIONS.items():
        spend = category_spend.get(category, 0.0)
        if spend <= 0:
            continue

        factor_info = EMISSION_FACTORS[conversion["factor_key"]]

        # Step 1: Convert EUR to physical quantity
        quantity = spend / conversion["eur_per_unit"]

        # Step 2: Calculate emissions (DETERMINISTIC)
        emissions_kg = quantity * factor_info["factor"]
        emissions_tco2e = emissions_kg / 1000.0  # Convert kg to tonnes

        # Step 3: Store with full traceability
        record = EmissionRecord(
            company_id=company_id,
            period=period,
            scope=factor_info["scope"],
            category=category,
            value_tco2e=round(emissions_tco2e, 4),
            quantity=round(quantity, 2),
            unit=conversion["unit"],
            emission_factor_used=factor_info["factor"],
            factor_source=factor_info["source"],
            factor_vintage=factor_info["vintage"],
            calculation_method=(
                f"spend_eur={spend:.2f} / price_per_{conversion['unit']}="
                f"{conversion['eur_per_unit']} = {quantity:.2f} {conversion['unit']} "
                f"x factor={factor_info['factor']} {factor_info['unit']} = "
                f"{emissions_kg:.2f} kg CO2e = {emissions_tco2e:.4f} tCO2e"
            ),
        )
        session.add(record)

        scope_totals[factor_info["scope"]] += emissions_tco2e

        results.append({
            "category": category,
            "scope": factor_info["scope"],
            "spend_eur": spend,
            "quantity": quantity,
            "unit": conversion["unit"],
            "factor": factor_info["factor"],
            "factor_source": factor_info["source"],
            "emissions_tco2e": emissions_tco2e,
        })

    session.commit()
    log_audit(
        session, "emissions_calculated", "company", company_id,
        f"period={period}, scope1={scope_totals[1]:.2f}, scope2={scope_totals[2]:.2f}, "
        f"scope3={scope_totals[3]:.2f} tCO2e"
    )
    session.close()

    # Print results table
    table = RichTable(title=f"Emissions Summary \u2014 {company_name} \u2014 {period}")
    table.add_column("Category", style="bold")
    table.add_column("Scope", justify="center")
    table.add_column("Spend (EUR)", justify="right")
    table.add_column("Quantity", justify="right")
    table.add_column("Unit")
    table.add_column("Factor")
    table.add_column("tCO2e", justify="right", style="bold")

    for r in results:
        table.add_row(
            r["category"],
            str(r["scope"]),
            f"{r['spend_eur']:,.2f}",
            f"{r['quantity']:,.1f}",
            r["unit"],
            f"{r['factor']}",
            f"{r['emissions_tco2e']:,.4f}",
        )

    # Totals row
    total = sum(scope_totals.values())
    table.add_section()
    table.add_row(
        "[bold]TOTAL[/bold]", "", "", "", "", "",
        f"[bold]{total:,.4f}[/bold]",
    )
    table.add_row("  Scope 1 (Direct)", "1", "", "", "", "", f"{scope_totals[1]:,.4f}")
    table.add_row("  Scope 2 (Energy)", "2", "", "", "", "", f"{scope_totals[2]:,.4f}")
    table.add_row("  Scope 3 (Upstream)", "3", "", "", "", "", f"{scope_totals[3]:,.4f}")

    console.print(table)

    # Print important caveats
    console.print()
    console.print("  [yellow]IMPORTANT CAVEATS:[/yellow]")
    console.print("  [dim]- Quantities estimated from EUR spend using average Austrian prices[/dim]")
    console.print("  [dim]- For auditable reports, replace with actual consumption data (kWh from utility bills)[/dim]")
    console.print("  [dim]- Scope 3 is incomplete (only logistics estimated, not full supply chain)[/dim]")
    console.print("  [dim]- All factors from Umweltbundesamt Austria, vintage 2024[/dim]")

    return {
        "status": "complete",
        "period": period,
        "scope_1_tco2e": round(scope_totals[1], 4),
        "scope_2_tco2e": round(scope_totals[2], 4),
        "scope_3_tco2e": round(scope_totals[3], 4),
        "total_tco2e": round(total, 4),
        "details": results,
        "category_spend": dict(category_spend),
    }


if __name__ == "__main__":
    from db import init_db
    init_db()

    session = get_session()
    company = session.query(Company).first()
    session.close()

    if company:
        calculate_emissions(company.id)
    else:
        console.print("[red]No company found. Run agents 1 and 2 first.[/red]")
