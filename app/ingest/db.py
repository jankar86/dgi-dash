from sqlalchemy import create_engine, Column, Integer, String, Date, Float, Text, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Account(Base):
    __tablename__ = 'accounts'
    account_id = Column(Integer, primary_key=True)
    account_number = Column(String, unique=True)

class Dividend(Base):
    __tablename__ = 'dividends'
    id = Column(Integer, primary_key=True)
    transaction_date = Column(Date)
    transaction_type = Column(String)
    security_type = Column(String)
    symbol = Column(String)
    quantity = Column(Float)
    amount = Column(Float)
    price = Column(Float)
    commission = Column(Float)
    description = Column(Text)
    account_id = Column(Integer, ForeignKey('accounts.account_id'))
    __table_args__ = (UniqueConstraint('transaction_date', 'transaction_type', 'symbol', 'amount', 'account_id'),)

def create_database(db_url):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return engine

def get_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()
