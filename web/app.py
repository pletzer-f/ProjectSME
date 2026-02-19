#!/usr/bin/env python3
"""
SME Data Intelligence — Web Dashboard
========================================
Flask app that reads the SQLite database and presents:
- Company overview
- Emissions breakdown with charts
- ESRS E1 gap analysis
- Account mappings
- Audit trail
- Pipeline runner (trigger from browser)

Usage:
    python3 web/app.py
    Then open http://localhost:5001
"""
import os
import sys
import json
from datetime import datetime
from collections import defaultdict

# Ensure pipeline root is importable
PIPELINE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PIPELINE_DIR)

from flask import Flask, render_template, jsonify, request, send_file
from db import init_db, get_session, Company, AccountMapping, Transaction, EmissionRecord, EsrsDisclosure, ReportVersion, AuditLog, FileIngestionLog

app = Flask(__name__)


def get_companies():
    """Get all companies from DB."""
    session = get_session()
    companies = session.query(Company).all()
    result = []
    for c in companies:
        result.append({
            "id": c.id,
            "name": c.name,
            "uid_vat": c.uid_vat,
            "nace_code": c.nace_code,
            "size_employees": c.size_employees,
            "status": c.status,
        })
    session.close()
    return result


def get_company_data(company_id):
    """Get full dashboard data for a company."""
    session = get_session()

    company = session.get(Company, company_id)
    if not company:
        session.close()
        return None

    company_info = {
        "id": company.id,
        "name": company.name,
        "uid_vat": company.uid_vat,
        "nace_code": company.nace_code,
        "size_employees": company.size_employees,
    }

    # Transactions summary
    transactions = session.query(Transaction).filter(
        Transaction.company_id == company_id
    ).all()

    tx_count = len(transactions)
    total_expenses = sum(t.amount_eur for t in transactions if t.amount_eur and t.amount_eur > 0)
    total_revenue = abs(sum(t.amount_eur for t in transactions if t.amount_eur and t.amount_eur < 0))

    # Date range
    dates = [t.date for t in transactions if t.date and t.date != "1900-01-01"]
    date_range = ""
    if dates:
        date_range = f"{min(dates)} — {max(dates)}"

    # Emissions
    emissions = session.query(EmissionRecord).filter(
        EmissionRecord.company_id == company_id
    ).all()

    scope_totals = {1: 0.0, 2: 0.0, 3: 0.0}
    emission_details = []
    category_emissions = defaultdict(float)
    for e in emissions:
        scope_totals[e.scope] += e.value_tco2e
        category_emissions[e.category] += e.value_tco2e
        emission_details.append({
            "scope": e.scope,
            "category": e.category,
            "value_tco2e": round(e.value_tco2e, 4),
            "quantity": round(e.quantity, 1) if e.quantity else 0,
            "unit": e.unit,
            "emission_factor_used": e.emission_factor_used,
            "factor_source": e.factor_source,
            "calculation_method": e.calculation_method,
        })

    total_emissions = sum(scope_totals.values())
    period = emissions[0].period if emissions else "N/A"

    # Account mappings
    mappings = session.query(AccountMapping).filter(
        AccountMapping.company_id == company_id
    ).all()

    mapping_list = []
    category_spend = defaultdict(float)
    for m in mappings:
        # Calculate total spend for this account
        spend = sum(
            abs(t.amount_eur) for t in transactions
            if t.account_number == m.account_number and t.amount_eur and t.amount_eur > 0
        )
        mapping_list.append({
            "account_number": m.account_number,
            "account_name": m.account_name,
            "esg_category": m.esg_category,
            "confidence_score": m.confidence_score,
            "source": m.source,
            "spend_eur": round(spend, 2),
        })
        category_spend[m.esg_category] += spend

    # ESRS Gap Analysis
    disclosures = session.query(EsrsDisclosure).filter(
        EsrsDisclosure.company_id == company_id
    ).order_by(EsrsDisclosure.standard_ref).all()

    gap_analysis = []
    gap_counts = {"met": 0, "partial": 0, "gap": 0}
    for d in disclosures:
        gap_analysis.append({
            "ref": d.standard_ref,
            "title": d.disclosure_title,
            "status": d.status,
            "data_available": d.data_available,
            "gap_notes": d.gap_notes,
        })
        gap_counts[d.status] = gap_counts.get(d.status, 0) + 1

    # Reports
    reports = session.query(ReportVersion).filter(
        ReportVersion.company_id == company_id
    ).order_by(ReportVersion.generated_at.desc()).all()

    report_list = []
    for r in reports:
        report_list.append({
            "id": r.id,
            "report_type": r.report_type,
            "version": r.version_number,
            "generated_at": r.generated_at.isoformat() if r.generated_at else "",
            "file_path": r.file_path,
            "status": r.status,
        })

    # Audit log (last 50)
    audits = session.query(AuditLog).order_by(
        AuditLog.timestamp.desc()
    ).limit(50).all()

    audit_list = []
    for a in audits:
        audit_list.append({
            "timestamp": a.timestamp.isoformat() if a.timestamp else "",
            "action": a.action,
            "entity_type": a.resource_type,
            "details": a.details,
        })

    # Monthly spend trend (for chart)
    monthly_spend = defaultdict(float)
    monthly_revenue = defaultdict(float)
    for t in transactions:
        if t.date and len(t.date) >= 7:
            month_key = t.date[:7]  # "2024-01"
            if t.amount_eur and t.amount_eur > 0:
                monthly_spend[month_key] += t.amount_eur
            elif t.amount_eur and t.amount_eur < 0:
                monthly_revenue[month_key] += abs(t.amount_eur)

    session.close()

    return {
        "company": company_info,
        "period": period,
        "tx_count": tx_count,
        "total_expenses": round(total_expenses, 2),
        "total_revenue": round(total_revenue, 2),
        "date_range": date_range,
        "emissions": {
            "total": round(total_emissions, 2),
            "scope_1": round(scope_totals[1], 2),
            "scope_2": round(scope_totals[2], 2),
            "scope_3": round(scope_totals[3], 2),
            "details": emission_details,
            "by_category": {k: round(v, 4) for k, v in category_emissions.items()},
        },
        "mappings": sorted(mapping_list, key=lambda x: x["account_number"]),
        "category_spend": {k: round(v, 2) for k, v in sorted(category_spend.items())},
        "gap_analysis": gap_analysis,
        "gap_counts": gap_counts,
        "reports": report_list,
        "audit_log": audit_list,
        "monthly_spend": dict(sorted(monthly_spend.items())),
        "monthly_revenue": dict(sorted(monthly_revenue.items())),
    }


# ─── Routes ───

@app.route("/")
def index():
    """Main dashboard."""
    init_db()
    companies = get_companies()
    # Default to first company
    company_id = request.args.get("company_id", type=int)
    if not company_id and companies:
        company_id = companies[0]["id"]

    data = get_company_data(company_id) if company_id else None
    return render_template("dashboard.html", companies=companies, data=data, active_company_id=company_id)


@app.route("/api/company/<int:company_id>")
def api_company(company_id):
    """JSON API for company data."""
    data = get_company_data(company_id)
    if not data:
        return jsonify({"error": "Company not found"}), 404
    return jsonify(data)


@app.route("/api/run-pipeline", methods=["POST"])
def api_run_pipeline():
    """Trigger pipeline run (for future use)."""
    return jsonify({"status": "not_implemented", "message": "Use the CLI: python3 run_pipeline.py"})


@app.route("/download-report/<int:report_id>")
def download_report(report_id):
    """Download a generated report."""
    session = get_session()
    report = session.get(ReportVersion, report_id)
    if not report or not report.file_path:
        session.close()
        return "Report not found", 404
    filepath = report.file_path
    session.close()
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True)
    return "File not found on disk", 404


if __name__ == "__main__":
    print("\n  SME Data Intelligence Dashboard")
    print("  Open: http://localhost:5001\n")
    app.run(debug=True, port=5001)
