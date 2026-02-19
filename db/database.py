"""
Database layer using SQLite for simplicity.
No PostgreSQL setup needed — just run and it works.
Upgrade to PostgreSQL when you have a developer.
"""
import os
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, String, Integer, Float, Boolean,
    DateTime, Text, ForeignKey, UniqueConstraint, CheckConstraint,
    event
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "db", "sme_intelligence.db")

engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base()


# Enable foreign keys for SQLite
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


class Company(Base):
    __tablename__ = "companies"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    uid_vat = Column(String, unique=True)
    nace_code = Column(String)
    size_employees = Column(Integer)
    bmd_client_id = Column(String)
    onboarding_date = Column(DateTime, default=datetime.utcnow)
    status = Column(String, default="onboarding")

    transactions = relationship("Transaction", back_populates="company")
    account_mappings = relationship("AccountMapping", back_populates="company")
    emissions = relationship("EmissionRecord", back_populates="company")
    suppliers = relationship("Supplier", back_populates="company")
    disclosures = relationship("EsrsDisclosure", back_populates="company")
    reports = relationship("ReportVersion", back_populates="company")


class AccountMapping(Base):
    __tablename__ = "account_mappings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    account_number = Column(String, nullable=False)
    account_name = Column(String)
    esg_category = Column(String, nullable=False)
    confidence_score = Column(Float)
    confirmed_by = Column(String)
    confirmed_at = Column(DateTime)
    source = Column(String, default="auto")

    company = relationship("Company", back_populates="account_mappings")
    __table_args__ = (
        UniqueConstraint("company_id", "account_number", name="uq_company_account"),
    )


class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    date = Column(String, nullable=False)  # ISO format YYYY-MM-DD
    account_number = Column(String, nullable=False)
    counter_account = Column(String)
    amount_eur = Column(Float, nullable=False)
    vat_code = Column(String)
    cost_center = Column(String)
    document_ref = Column(String)
    booking_text = Column(String)
    source_file = Column(String, nullable=False)
    row_number = Column(Integer)
    ingested_at = Column(DateTime, default=datetime.utcnow)

    company = relationship("Company", back_populates="transactions")


class EmissionRecord(Base):
    __tablename__ = "emissions_records"
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    period = Column(String, nullable=False)  # e.g. "2025-Q1"
    scope = Column(Integer, nullable=False)  # 1, 2, or 3
    category = Column(String)
    value_tco2e = Column(Float, nullable=False)
    quantity = Column(Float)
    unit = Column(String)
    emission_factor_used = Column(Float)
    factor_source = Column(String, nullable=False)
    factor_vintage = Column(Integer)
    calculation_method = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    company = relationship("Company", back_populates="emissions")


class Supplier(Base):
    __tablename__ = "suppliers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    uid_vat = Column(String)
    name = Column(String, nullable=False)
    country = Column(String)
    spend_eur_annual = Column(Float)
    outreach_status = Column(String, default="pending")
    response_date = Column(DateTime)

    company = relationship("Company", back_populates="suppliers")


class EsrsDisclosure(Base):
    __tablename__ = "esrs_disclosures"
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    standard_ref = Column(String, nullable=False)
    disclosure_title = Column(String)
    status = Column(String, default="gap")  # met / partial / gap
    data_available = Column(Boolean, default=False)
    gap_notes = Column(Text)
    last_assessed_at = Column(DateTime)

    company = relationship("Company", back_populates="disclosures")


class ReportVersion(Base):
    __tablename__ = "report_versions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    report_type = Column(String, nullable=False)
    version_number = Column(Integer, nullable=False)
    generated_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String, default="draft")
    reviewed_by = Column(String)
    review_notes = Column(Text)
    file_path = Column(String)

    company = relationship("Company", back_populates="reports")


class FileIngestionLog(Base):
    __tablename__ = "file_ingestion_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(String, nullable=False)
    file_hash = Column(String, nullable=False, unique=True)
    file_size = Column(Integer)
    encoding_detected = Column(String)
    rows_parsed = Column(Integer)
    rows_valid = Column(Integer)
    rows_quarantined = Column(Integer)
    ingested_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String, default="success")  # success / partial / failed
    error_message = Column(Text)


class AuditLog(Base):
    __tablename__ = "audit_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    action = Column(String, nullable=False)
    resource_type = Column(String)
    resource_id = Column(String)
    details = Column(Text)
    user = Column(String, default="system")


def init_db():
    """Create all tables. Safe to call multiple times."""
    Base.metadata.create_all(engine)
    return engine


def get_session():
    """Get a new database session."""
    return Session()


def log_audit(session, action, resource_type=None, resource_id=None, details=None):
    """Write an audit log entry."""
    entry = AuditLog(
        action=action,
        resource_type=resource_type,
        resource_id=str(resource_id) if resource_id else None,
        details=details,
    )
    session.add(entry)
    session.commit()
