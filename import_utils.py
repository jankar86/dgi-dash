import hashlib
from datetime import datetime

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


def _build_source_row_hash(
    *,
    source_system,
    source_file,
    account_name,
    symbol,
    txn_type,
    date,
    amount,
    quantity,
    price,
    raw_action,
):
    payload = "|".join(
        str(v) for v in (
            account_name or "",
            symbol or "",
            txn_type or "",
            date or "",
            amount if amount is not None else "",
            quantity if quantity is not None else "",
            price if price is not None else "",
            raw_action or "",
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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
    source_system=None,
    source_file=None,
    source_row_hash=None,
    raw_action=None,
    imported_at=None,
):
    account = _get_or_create_account(session, account_name)
    security = _get_or_create_security(session, symbol)
    session.flush()

    resolved_txn_type = _resolve_txn_type(txn_type)
    if source_row_hash is None:
        source_row_hash = _build_source_row_hash(
            source_system=source_system,
            source_file=source_file,
            account_name=account_name,
            symbol=symbol,
            txn_type=resolved_txn_type.value,
            date=date,
            amount=amount,
            quantity=quantity,
            price=price,
            raw_action=raw_action,
        )

    if source_row_hash:
        exists_by_hash = session.query(Transaction).filter_by(source_row_hash=source_row_hash).first()
        if exists_by_hash:
            return False

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
            source_system=source_system,
            source_file=source_file,
            source_row_hash=source_row_hash,
            raw_action=raw_action,
            imported_at=imported_at or datetime.utcnow(),
        )
    )
    return True
