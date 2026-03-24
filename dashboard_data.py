from datetime import date, datetime

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from models import Account, Security, Transaction, TxnType


DEFAULT_DB_URL = "sqlite:///dividends.db"


def _session(db_url=DEFAULT_DB_URL):
    engine = create_engine(db_url)
    return sessionmaker(bind=engine)()


def _parse_date(value):
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def get_dashboard_data(db_url=DEFAULT_DB_URL, year=None):
    session = _session(db_url=db_url)
    try:
        max_date = session.query(func.max(Transaction.date)).scalar()
        current_year = int(year) if year else (max_date.year if max_date else date.today().year)

        total_transactions = session.query(func.count(Transaction.id)).scalar() or 0
        total_accounts = session.query(func.count(Account.id)).scalar() or 0
        total_securities = session.query(func.count(Security.id)).scalar() or 0
        available_years = [
            int(row[0])
            for row in session.query(func.strftime("%Y", Transaction.date))
            .filter(Transaction.date.isnot(None))
            .distinct()
            .order_by(func.strftime("%Y", Transaction.date).desc())
            .all()
            if row[0]
        ]

        ytd_dividend_total = (
            session.query(func.coalesce(func.sum(Transaction.amount), 0))
            .filter(
                Transaction.txn_type == TxnType.DIVIDEND,
                func.strftime("%Y", Transaction.date) == str(current_year),
            )
            .scalar()
            or 0
        )

        ytd_reinvestment_total = (
            session.query(func.coalesce(func.sum(Transaction.amount), 0))
            .filter(
                Transaction.txn_type == TxnType.REINVESTMENT,
                func.strftime("%Y", Transaction.date) == str(current_year),
            )
            .scalar()
            or 0
        )

        ytd_interest_total = (
            session.query(func.coalesce(func.sum(Transaction.amount), 0))
            .join(Security, Security.id == Transaction.security_id)
            .filter(
                Transaction.txn_type == TxnType.DIVIDEND,
                Security.ticker == "INTEREST",
                func.strftime("%Y", Transaction.date) == str(current_year),
            )
            .scalar()
            or 0
        )

        monthly_income = (
            session.query(
                func.strftime("%Y-%m", Transaction.date).label("month"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total"),
            )
            .filter(Transaction.txn_type == TxnType.DIVIDEND)
            .filter(func.strftime("%Y", Transaction.date) == str(current_year))
            .group_by("month")
            .order_by("month")
            .all()
        )
        monthly_income = [
            {"month": row.month, "total": float(row.total)}
            for row in monthly_income
        ]

        account_totals = (
            session.query(
                Account.name.label("account"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total"),
                func.max(Transaction.date).label("last_date"),
                func.count(Transaction.id).label("txn_count"),
            )
            .join(Transaction, Transaction.account_id == Account.id)
            .filter(Transaction.txn_type == TxnType.DIVIDEND)
            .filter(func.strftime("%Y", Transaction.date) == str(current_year))
            .group_by(Account.name)
            .order_by(func.sum(Transaction.amount).desc(), Account.name)
            .all()
        )
        account_totals = [
            {
                "account": row.account,
                "total": float(row.total),
                "last_date": row.last_date,
                "txn_count": int(row.txn_count),
            }
            for row in account_totals
        ]

        top_securities = (
            session.query(
                Security.ticker.label("security"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total"),
                func.count(Transaction.id).label("txn_count"),
                func.max(Transaction.date).label("last_date"),
            )
            .join(Transaction, Transaction.security_id == Security.id)
            .filter(
                Transaction.txn_type == TxnType.DIVIDEND,
                Security.ticker != "INTEREST",
            )
            .filter(func.strftime("%Y", Transaction.date) == str(current_year))
            .group_by(Security.ticker)
            .order_by(func.sum(Transaction.amount).desc(), Security.ticker)
            .limit(15)
            .all()
        )
        top_securities = [
            {
                "security": row.security,
                "total": float(row.total),
                "txn_count": int(row.txn_count),
                "last_date": row.last_date,
            }
            for row in top_securities
        ]

        recent_transactions = get_transactions(
            db_url=db_url,
            year=current_year,
            limit=20,
        )["rows"]
        import_status = get_import_status(db_url=db_url)

        return {
            "as_of": max_date,
            "current_year": current_year,
            "available_years": available_years,
            "totals": {
                "transactions": int(total_transactions),
                "accounts": int(total_accounts),
                "securities": int(total_securities),
                "ytd_dividend_total": float(ytd_dividend_total),
                "ytd_reinvestment_total": float(ytd_reinvestment_total),
                "ytd_interest_total": float(ytd_interest_total),
            },
            "monthly_income": monthly_income,
            "account_totals": account_totals,
            "top_securities": top_securities,
            "recent_transactions": recent_transactions,
            "import_status": import_status,
        }
    finally:
        session.close()


def get_transactions(
    *,
    db_url=DEFAULT_DB_URL,
    account=None,
    security=None,
    txn_type=None,
    year=None,
    limit=200,
    sort="date",
    direction="desc",
):
    session = _session(db_url=db_url)
    try:
        query = (
            session.query(
                Transaction.id,
                Transaction.date,
                Security.ticker.label("security"),
                Account.name.label("account"),
                Transaction.txn_type,
                Transaction.amount,
                Transaction.is_qualified,
                Transaction.source_system,
                Transaction.source_file,
                Transaction.raw_action,
            )
            .join(Account, Account.id == Transaction.account_id)
            .join(Security, Security.id == Transaction.security_id)
        )

        if account:
            query = query.filter(Account.name == account)
        if security:
            query = query.filter(Security.ticker == str(security).upper())
        if txn_type:
            query = query.filter(Transaction.txn_type == TxnType[str(txn_type).upper()])
        if year:
            query = query.filter(func.strftime("%Y", Transaction.date) == str(year))

        sort_columns = {
            "date": Transaction.date,
            "security": Security.ticker,
            "account": Account.name,
            "txn_type": Transaction.txn_type,
            "amount": Transaction.amount,
            "qualified": Transaction.is_qualified,
            "source_system": Transaction.source_system,
            "raw_action": Transaction.raw_action,
            "source_file": Transaction.source_file,
        }
        sort_key = sort if sort in sort_columns else "date"
        sort_column = sort_columns[sort_key]
        if str(direction).lower() == "asc":
            query = query.order_by(sort_column.asc(), Transaction.id.asc())
            direction = "asc"
        else:
            query = query.order_by(sort_column.desc(), Transaction.id.desc())
            direction = "desc"

        rows = query.limit(int(limit)).all()

        accounts = [row[0] for row in session.query(Account.name).order_by(Account.name).all()]
        securities = [row[0] for row in session.query(Security.ticker).order_by(Security.ticker).all()]
        years = [
            row[0]
            for row in session.query(func.strftime("%Y", Transaction.date))
            .filter(Transaction.date.isnot(None))
            .distinct()
            .order_by(func.strftime("%Y", Transaction.date).desc())
            .all()
        ]

        return {
            "rows": [
                {
                    "id": row.id,
                    "date": row.date,
                    "security": row.security,
                    "account": row.account,
                    "txn_type": row.txn_type.value if hasattr(row.txn_type, "value") else str(row.txn_type),
                    "amount": float(row.amount),
                    "is_qualified": bool(row.is_qualified),
                    "source_system": row.source_system,
                    "source_file": row.source_file,
                    "raw_action": row.raw_action,
                }
                for row in rows
            ],
            "filters": {
                "account": account or "",
                "security": str(security).upper() if security else "",
                "txn_type": txn_type or "",
                "year": str(year) if year else "",
                "limit": int(limit),
                "sort": sort_key,
                "direction": direction,
            },
            "options": {
                "accounts": accounts,
                "securities": securities,
                "years": [str(y) for y in years if y],
                "txn_types": [member.value for member in TxnType],
                "sort_fields": list(sort_columns.keys()),
            },
        }
    finally:
        session.close()


def get_import_status(db_url=DEFAULT_DB_URL):
    session = _session(db_url=db_url)
    try:
        latest_imported_at = session.query(func.max(Transaction.imported_at)).scalar()
        latest_txn_date = session.query(func.max(Transaction.date)).scalar()

        source_rows = (
            session.query(
                Transaction.source_system,
                func.coalesce(func.max(Transaction.imported_at), None).label("last_imported_at"),
                func.coalesce(func.max(Transaction.date), None).label("last_txn_date"),
                func.count(Transaction.id).label("txn_count"),
                func.count(func.distinct(Transaction.source_file)).label("file_count"),
            )
            .filter(Transaction.source_system.isnot(None))
            .group_by(Transaction.source_system)
            .order_by(func.max(Transaction.imported_at).desc(), Transaction.source_system)
            .all()
        )

        coverage_rows = (
            session.query(
                Account.name.label("account"),
                func.min(Transaction.date).label("first_date"),
                func.max(Transaction.date).label("last_date"),
                func.count(Transaction.id).label("txn_count"),
            )
            .join(Transaction, Transaction.account_id == Account.id)
            .group_by(Account.name)
            .order_by(func.max(Transaction.date).asc(), Account.name)
            .all()
        )

        today = latest_txn_date or date.today()
        coverage = []
        for row in coverage_rows:
            days_since_last = (today - row.last_date).days if row.last_date else None
            coverage.append(
                {
                    "account": row.account,
                    "first_date": row.first_date,
                    "last_date": row.last_date,
                    "txn_count": int(row.txn_count),
                    "days_since_last": days_since_last,
                }
            )

        return {
            "latest_imported_at": latest_imported_at,
            "latest_txn_date": latest_txn_date,
            "sources": [
                {
                    "source_system": row.source_system or "",
                    "last_imported_at": row.last_imported_at,
                    "last_txn_date": row.last_txn_date,
                    "txn_count": int(row.txn_count),
                    "file_count": int(row.file_count),
                }
                for row in source_rows
            ],
            "coverage": coverage,
        }
    finally:
        session.close()


def get_account_detail(account_name, db_url=DEFAULT_DB_URL):
    session = _session(db_url=db_url)
    try:
        account = session.query(Account).filter(Account.name == account_name).first()
        if not account:
            return None

        summary = (
            session.query(
                func.count(Transaction.id).label("txn_count"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total_amount"),
                func.min(Transaction.date).label("first_date"),
                func.max(Transaction.date).label("last_date"),
            )
            .filter(Transaction.account_id == account.id)
            .one()
        )

        by_type_rows = (
            session.query(
                Transaction.txn_type,
                func.count(Transaction.id).label("txn_count"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total_amount"),
            )
            .filter(Transaction.account_id == account.id)
            .group_by(Transaction.txn_type)
            .order_by(Transaction.txn_type)
            .all()
        )

        top_securities = (
            session.query(
                Security.ticker.label("security"),
                func.count(Transaction.id).label("txn_count"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total_amount"),
                func.max(Transaction.date).label("last_date"),
            )
            .join(Transaction, Transaction.security_id == Security.id)
            .filter(Transaction.account_id == account.id)
            .group_by(Security.ticker)
            .order_by(func.sum(Transaction.amount).desc(), Security.ticker)
            .limit(20)
            .all()
        )

        recent_transactions = get_transactions(
            db_url=db_url,
            account=account.name,
            limit=50,
        )["rows"]

        return {
            "account": account.name,
            "summary": {
                "txn_count": int(summary.txn_count or 0),
                "total_amount": float(summary.total_amount or 0),
                "first_date": summary.first_date,
                "last_date": summary.last_date,
            },
            "by_type": [
                {
                    "txn_type": row.txn_type.value if hasattr(row.txn_type, "value") else str(row.txn_type),
                    "txn_count": int(row.txn_count),
                    "total_amount": float(row.total_amount),
                }
                for row in by_type_rows
            ],
            "top_securities": [
                {
                    "security": row.security,
                    "txn_count": int(row.txn_count),
                    "total_amount": float(row.total_amount),
                    "last_date": row.last_date,
                }
                for row in top_securities
            ],
            "recent_transactions": recent_transactions,
        }
    finally:
        session.close()


def get_security_detail(security_ticker, db_url=DEFAULT_DB_URL):
    normalized = str(security_ticker).upper()
    session = _session(db_url=db_url)
    try:
        security = session.query(Security).filter(Security.ticker == normalized).first()
        if not security:
            return None

        summary = (
            session.query(
                func.count(Transaction.id).label("txn_count"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total_amount"),
                func.min(Transaction.date).label("first_date"),
                func.max(Transaction.date).label("last_date"),
            )
            .filter(Transaction.security_id == security.id)
            .one()
        )

        by_account_rows = (
            session.query(
                Account.name.label("account"),
                func.count(Transaction.id).label("txn_count"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total_amount"),
                func.max(Transaction.date).label("last_date"),
            )
            .join(Transaction, Transaction.account_id == Account.id)
            .filter(Transaction.security_id == security.id)
            .group_by(Account.name)
            .order_by(func.sum(Transaction.amount).desc(), Account.name)
            .all()
        )

        yearly_rows = (
            session.query(
                func.strftime("%Y", Transaction.date).label("year"),
                func.count(Transaction.id).label("txn_count"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total_amount"),
            )
            .filter(Transaction.security_id == security.id)
            .group_by("year")
            .order_by("year")
            .all()
        )

        recent_transactions = get_transactions(
            db_url=db_url,
            security=security.ticker,
            limit=50,
        )["rows"]

        return {
            "security": security.ticker,
            "summary": {
                "txn_count": int(summary.txn_count or 0),
                "total_amount": float(summary.total_amount or 0),
                "first_date": summary.first_date,
                "last_date": summary.last_date,
            },
            "by_account": [
                {
                    "account": row.account,
                    "txn_count": int(row.txn_count),
                    "total_amount": float(row.total_amount),
                    "last_date": row.last_date,
                }
                for row in by_account_rows
            ],
            "by_year": [
                {
                    "year": row.year,
                    "txn_count": int(row.txn_count),
                    "total_amount": float(row.total_amount),
                }
                for row in yearly_rows
                if row.year
            ],
            "recent_transactions": recent_transactions,
        }
    finally:
        session.close()
