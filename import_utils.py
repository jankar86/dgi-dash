import hashlib
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Account, Security, Transaction, TxnType


DEFAULT_DB_URL = "sqlite:///dividends.db"
IMPORT_GUARD_LOG = Path("logs/import_guard.log")


def create_session(db_url=DEFAULT_DB_URL):
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    return Session()


def _resolve_txn_type(txn_type):
    if isinstance(txn_type, TxnType):
        return txn_type
    return TxnType[str(txn_type).upper()]


def _contains_unknown_placeholder(value):
    if value is None:
        return True
    normalized = str(value).strip().upper()
    return normalized == "" or "UNKNOWN" in normalized


def _log_import_guard_failure(*, reason, account_name, symbol, txn_type, date, amount, source_system, source_file, raw_action):
    IMPORT_GUARD_LOG.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    with IMPORT_GUARD_LOG.open("a", encoding="utf-8") as f:
        f.write(
            " | ".join(
                [
                    f"timestamp={timestamp}Z",
                    "event=import_guard_failure",
                    f"reason={reason}",
                    f"account={account_name!r}",
                    f"symbol={symbol!r}",
                    f"txn_type={txn_type!r}",
                    f"date={date!r}",
                    f"amount={amount!r}",
                    f"source_system={source_system!r}",
                    f"source_file={source_file!r}",
                    f"raw_action={raw_action!r}",
                ]
            )
        )
        f.write("\n")


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
    if _contains_unknown_placeholder(account_name):
        _log_import_guard_failure(
            reason="unknown_account",
            account_name=account_name,
            symbol=symbol,
            txn_type=txn_type,
            date=date,
            amount=amount,
            source_system=source_system,
            source_file=source_file,
            raw_action=raw_action,
        )
        raise ValueError(f"Refusing to import transaction with unknown account: {account_name!r}")
    if _contains_unknown_placeholder(symbol):
        _log_import_guard_failure(
            reason="unknown_symbol",
            account_name=account_name,
            symbol=symbol,
            txn_type=txn_type,
            date=date,
            amount=amount,
            source_system=source_system,
            source_file=source_file,
            raw_action=raw_action,
        )
        raise ValueError(f"Refusing to import transaction with unknown symbol: {symbol!r}")

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
