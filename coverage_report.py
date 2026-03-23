from datetime import date, datetime, timedelta

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from models import Account, Transaction


def _parse_as_of(as_of):
    if as_of is None:
        return date.today()
    if isinstance(as_of, date):
        return as_of
    return datetime.strptime(as_of, "%Y-%m-%d").date()


def get_account_coverage(as_of=None, db_url="sqlite:///dividends.db"):
    as_of_date = _parse_as_of(as_of)
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    rows = (
        session.query(
            Account.name.label("account"),
            func.min(Transaction.date).label("first_date"),
            func.max(Transaction.date).label("last_date"),
            func.count(Transaction.id).label("txn_count"),
        )
        .join(Transaction, Transaction.account_id == Account.id)
        .group_by(Account.name)
        .order_by(Account.name)
        .all()
    )

    report = []
    for row in rows:
        days_since_last = (as_of_date - row.last_date).days if row.last_date else None
        next_needed_start = row.last_date + timedelta(days=1) if row.last_date else None
        report.append(
            {
                "account": row.account,
                "first_date": row.first_date,
                "last_date": row.last_date,
                "next_needed_start": next_needed_start,
                "days_since_last": days_since_last,
                "txn_count": row.txn_count,
            }
        )
    return report


def print_account_coverage(as_of=None, db_url="sqlite:///dividends.db"):
    as_of_date = _parse_as_of(as_of)
    report = get_account_coverage(as_of=as_of_date, db_url=db_url)

    print(f"as_of={as_of_date}")
    if not report:
        print("no_data=true")
        return

    print("account|first_date|last_date|next_needed_start|days_since_last|txn_count")
    for row in report:
        print(
            f"{row['account']}|{row['first_date']}|{row['last_date']}|"
            f"{row['next_needed_start']}|{row['days_since_last']}|{row['txn_count']}"
        )
