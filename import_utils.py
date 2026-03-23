from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Account, Security, Transaction, TxnType


DEFAULT_DB_URL = "sqlite:///dividends.db"


def create_session(db_url=DEFAULT_DB_URL):
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    return Session()


def _resolve_txn_type(txn_type):
    if isinstance(txn_type, TxnType):
        return txn_type
    return TxnType[str(txn_type).upper()]


def _get_or_create_account(session, account_name):
    account = session.query(Account).filter_by(name=account_name).first()
    if not account:
        account = Account(name=account_name)
        session.add(account)
    return account


def _get_or_create_security(session, symbol):
    security = session.query(Security).filter_by(ticker=symbol).first()
    if not security:
        security = Security(ticker=symbol)
        session.add(security)
    return security


def upsert_transaction(
    session,
    *,
    account_name,
    symbol,
    txn_type,
    date,
    quantity,
    price,
    amount,
    is_qualified=False,
    allocation_status="ALLOCATED",
):
    account = _get_or_create_account(session, account_name)
    security = _get_or_create_security(session, symbol)
    session.flush()

    resolved_txn_type = _resolve_txn_type(txn_type)
    exists = session.query(Transaction).filter_by(
        account_id=account.id,
        security_id=security.id,
        txn_type=resolved_txn_type,
        date=date,
        amount=amount,
    ).first()

    if exists:
        return False

    session.add(
        Transaction(
            account=account,
            security=security,
            txn_type=resolved_txn_type,
            date=date,
            quantity=quantity,
            price=price,
            amount=amount,
            is_qualified=is_qualified,
            allocation_status=allocation_status,
        )
    )
    return True
