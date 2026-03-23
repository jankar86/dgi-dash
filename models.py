from sqlalchemy import Column, Integer, Float, String, Date, ForeignKey, Enum, UniqueConstraint, Boolean
from sqlalchemy.orm import declarative_base, relationship
import enum

Base = declarative_base()

class TxnType(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"

class Account(Base):
    __tablename__ = 'accounts'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    transactions = relationship("Transaction", back_populates="account")

class Security(Base):
    __tablename__ = 'securities'
    id = Column(Integer, primary_key=True)
    ticker = Column(String, unique=True)

    transactions = relationship("Transaction", back_populates="security")


class Transaction(Base):
    __tablename__ = 'transactions'
    id = Column(Integer, primary_key=True)

    account_id = Column(Integer, ForeignKey('accounts.id'))
    security_id = Column(Integer, ForeignKey('securities.id'))
    txn_type = Column(Enum(TxnType))
    date = Column(Date)
    quantity = Column(Float)
    price = Column(Float)
    amount = Column(Float)
    is_qualified = Column(Boolean, default=False)  # ✅ New field

    account = relationship("Account", back_populates="transactions")
    security = relationship("Security", back_populates="transactions")

    __table_args__ = (
        UniqueConstraint('account_id', 'security_id', 'txn_type', 'date', 'amount', name='uix_txn_unique'),
    )

