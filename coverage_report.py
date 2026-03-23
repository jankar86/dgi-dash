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


def _ym_iter(start_y, start_m, end_y, end_m):
    y, m = start_y, start_m
    while (y, m) <= (end_y, end_m):
        yield y, m
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1


def get_monthly_gap_analysis(as_of=None, db_url="sqlite:///dividends.db"):
    as_of_date = _parse_as_of(as_of)
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    rows = (
        session.query(Account.name, Transaction.date)
        .join(Transaction, Transaction.account_id == Account.id)
        .filter(Transaction.date.isnot(None))
        .order_by(Account.name, Transaction.date)
        .all()
    )

    by_account = {}
    for account, txn_date in rows:
        by_account.setdefault(account, []).append(txn_date)

    report = []
    for account in sorted(by_account):
        dates = by_account[account]
        first_date = dates[0]
        last_date = dates[-1]
        present_months = {(d.year, d.month) for d in dates}

        expected_months = set(
            _ym_iter(first_date.year, first_date.month, last_date.year, last_date.month)
        )
        missing_internal = sorted(expected_months - present_months)

        if (last_date.year, last_date.month) < (as_of_date.year, as_of_date.month):
            trailing = list(
                _ym_iter(last_date.year, last_date.month, as_of_date.year, as_of_date.month)
            )[1:]
        else:
            trailing = []

        report.append(
            {
                "account": account,
                "first_date": first_date,
                "last_date": last_date,
                "missing_internal_months": [f"{y:04d}-{m:02d}" for y, m in missing_internal],
                "trailing_gap_months": [f"{y:04d}-{m:02d}" for y, m in trailing],
            }
        )
    return report


def print_monthly_gap_analysis(as_of=None, db_url="sqlite:///dividends.db"):
    as_of_date = _parse_as_of(as_of)
    report = get_monthly_gap_analysis(as_of=as_of_date, db_url=db_url)

    print(f"as_of={as_of_date}")
    if not report:
        print("no_data=true")
        return

    print(
        "account|first_date|last_date|internal_missing_month_count|"
        "trailing_gap_month_count|first_5_internal_missing"
    )
    for row in report:
        first5 = ",".join(row["missing_internal_months"][:5])
        print(
            f"{row['account']}|{row['first_date']}|{row['last_date']}|"
            f"{len(row['missing_internal_months'])}|{len(row['trailing_gap_months'])}|{first5}"
        )

    print("")
    print("details")
    for row in report:
        if row["missing_internal_months"]:
            internal = ", ".join(row["missing_internal_months"])
            print(f"{row['account']}: missing_between_first_last={internal}")
        else:
            print(f"{row['account']}: missing_between_first_last=none")

        if row["trailing_gap_months"]:
            trailing = ", ".join(row["trailing_gap_months"])
            print(f"{row['account']}: missing_after_last_to_as_of={trailing}")
        else:
            print(f"{row['account']}: missing_after_last_to_as_of=none")
