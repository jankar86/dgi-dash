from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship
import enum

Base = declarative_base()


class TxnType(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"
    CAPITAL_GAIN_DISTRIBUTION = "CAPITAL_GAIN_DISTRIBUTION"
    REINVESTMENT = "REINVESTMENT"


class Account(Base):
    __tablename__ = "accounts"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    display_name = Column(String(128))
    institution = Column(String(32))
    account_last4 = Column(String(4))
    account_group = Column(String(32))
    tax_treatment = Column(String(32))
    is_active = Column(Boolean, nullable=False, default=True)
    notes = Column(String(255))

    transactions = relationship("Transaction", back_populates="account")


class Security(Base):
    __tablename__ = "securities"
    id = Column(Integer, primary_key=True)
    ticker = Column(String, unique=True)

    transactions = relationship("Transaction", back_populates="security")


class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True)

    account_id = Column(Integer, ForeignKey("accounts.id"))
    security_id = Column(Integer, ForeignKey("securities.id"))
    txn_type = Column(Enum(TxnType, native_enum=False, length=32), nullable=False)
    date = Column(Date, nullable=False)
    quantity = Column(Numeric(20, 6), nullable=False, default=0)
    price = Column(Numeric(20, 6), nullable=False, default=0)
    amount = Column(Numeric(20, 2), nullable=False, default=0)
    is_qualified = Column(Boolean, nullable=False, default=False)
    allocation_status = Column(String(32), nullable=False, default="ALLOCATED")
    source_system = Column(String(32))
    source_file = Column(String(255))
    source_row_hash = Column(String(64), unique=True)
    raw_action = Column(String(128))
    reporting_tag = Column(String(64))
    annotation_note = Column(String(255))
    imported_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    account = relationship("Account", back_populates="transactions")
    security = relationship("Security", back_populates="transactions")

    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "security_id",
            "txn_type",
            "date",
            "amount",
            name="uix_txn_unique",
        ),
        Index("idx_transactions_date", "date"),
        Index("idx_transactions_account_date", "account_id", "date"),
        Index("idx_transactions_security_date", "security_id", "date"),
        Index("idx_transactions_type_date", "txn_type", "date"),
    )
