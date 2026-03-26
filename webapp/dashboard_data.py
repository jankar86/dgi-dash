from datetime import date, datetime

from sqlalchemy import Integer, create_engine, func
from sqlalchemy.orm import sessionmaker

from db.models import Account, Security, Transaction, TxnType


DEFAULT_DB_URL = "sqlite:///dividends.db"
MONTH_LABELS = ["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"]
INCOME_TXN_TYPES = (TxnType.DIVIDEND, TxnType.CAPITAL_GAIN_DISTRIBUTION)


def _session(db_url=DEFAULT_DB_URL):
    engine = create_engine(db_url)
    return sessionmaker(bind=engine)()


def _parse_date(value):
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _pct_change(current, prior):
    if current is None or prior in (None, 0):
        return None
    return ((current - prior) / prior) * 100.0


def _round_money(value):
    if value is None:
        return None
    return round(float(value), 2)


def _income_type_values(mode):
    normalized = (mode or "dividends").lower()
    if normalized == "combined":
        return [txn.value for txn in INCOME_TXN_TYPES]
    if normalized == "dividends":
        return [TxnType.DIVIDEND.value]
    if normalized == "interest":
        return [TxnType.DIVIDEND.value]
    return [TxnType.DIVIDEND.value]


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
            .filter(Transaction.txn_type.in_(INCOME_TXN_TYPES))
            .filter(Security.ticker != "INTEREST")
            .join(Security, Security.id == Transaction.security_id)
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
                Account.display_name.label("display_name"),
                Account.institution.label("institution"),
                Account.tax_treatment.label("tax_treatment"),
                Account.account_group.label("account_group"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total"),
                func.max(Transaction.date).label("last_date"),
                func.count(Transaction.id).label("txn_count"),
            )
            .join(Transaction, Transaction.account_id == Account.id)
            .filter(Transaction.txn_type == TxnType.DIVIDEND)
            .filter(func.strftime("%Y", Transaction.date) == str(current_year))
            .group_by(
                Account.name,
                Account.display_name,
                Account.institution,
                Account.tax_treatment,
                Account.account_group,
            )
            .order_by(func.sum(Transaction.amount).desc(), Account.name)
            .all()
        )
        account_totals = [
            {
                "account": row.account,
                "display_name": row.display_name,
                "institution": row.institution,
                "tax_treatment": row.tax_treatment,
                "account_group": row.account_group,
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
        expected_remainder = get_expected_remainder(db_url=db_url)

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
            "expected_remainder": expected_remainder,
        }
    finally:
        session.close()


def get_expected_remainder(db_url=DEFAULT_DB_URL, as_of_date=None, lookback_years=3):
    session = _session(db_url=db_url)
    try:
        as_of = _parse_date(as_of_date) if as_of_date else date.today()
        comparison_years = [str(y) for y in range(as_of.year - lookback_years, as_of.year)]
        if not comparison_years:
            return {"as_of": as_of, "rows": [], "total_estimate": 0.0}

        rows = (
            session.query(
                func.strftime("%Y", Transaction.date).label("year"),
                func.strftime("%d", Transaction.date).label("day"),
                Security.ticker.label("security"),
                Account.name.label("account"),
                func.sum(Transaction.amount).label("amount"),
            )
            .join(Account, Account.id == Transaction.account_id)
            .join(Security, Security.id == Transaction.security_id)
            .filter(
                Transaction.txn_type == TxnType.DIVIDEND,
                Security.ticker != "INTEREST",
                func.strftime("%m", Transaction.date) == f"{as_of.month:02d}",
                func.strftime("%Y", Transaction.date).in_(comparison_years),
                func.cast(func.strftime("%d", Transaction.date), Integer) > as_of.day,
            )
            .group_by("year", "day", "security", "account")
            .order_by("day", "security", "account", "year")
            .all()
        )

        grouped = {}
        for row in rows:
            key = (int(row.day), row.security, row.account)
            grouped.setdefault(key, {})[int(row.year)] = _round_money(row.amount)

        expected_rows = []
        for (day, security, account), yearly_amounts in sorted(grouped.items()):
            amounts = [amount for _, amount in sorted(yearly_amounts.items())]
            observed_years = sorted(yearly_amounts)
            last_observed_year = observed_years[-1] if observed_years else None
            previous_observed_year = observed_years[-2] if len(observed_years) >= 2 else None
            last_observed_amount = yearly_amounts.get(last_observed_year) if last_observed_year else None
            previous_observed_amount = (
                yearly_amounts.get(previous_observed_year) if previous_observed_year else None
            )
            expected_rows.append(
                {
                    "day": day,
                    "security": security,
                    "account": account,
                    "expected_amount": _round_money(sum(amounts) / len(amounts)),
                    "years_seen": len(amounts),
                    "last_observed_year": last_observed_year,
                    "last_observed_amount": last_observed_amount,
                    "previous_observed_year": previous_observed_year,
                    "previous_observed_amount": previous_observed_amount,
                    "trend_pct": _pct_change(
                        last_observed_amount,
                        previous_observed_amount,
                    ),
                }
            )

        total_estimate = _round_money(sum(row["expected_amount"] for row in expected_rows))
        return {
            "as_of": as_of,
            "rows": expected_rows,
            "total_estimate": total_estimate,
            "comparison_years": comparison_years,
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
                Account.display_name.label("account_display_name"),
                Transaction.txn_type,
                Transaction.amount,
                Transaction.is_qualified,
                Transaction.source_system,
                Transaction.source_file,
                Transaction.raw_action,
                Transaction.reporting_tag,
                Transaction.annotation_note,
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
                    "account_display_name": row.account_display_name,
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


def get_accounts_directory(db_url=DEFAULT_DB_URL):
    session = _session(db_url=db_url)
    try:
        rows = (
            session.query(
                Account.name.label("account"),
                Account.display_name.label("display_name"),
                Account.institution.label("institution"),
                Account.tax_treatment.label("tax_treatment"),
                Account.account_group.label("account_group"),
                Account.is_active.label("is_active"),
                func.count(Transaction.id).label("txn_count"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total_amount"),
                func.max(Transaction.date).label("last_date"),
            )
            .outerjoin(Transaction, Transaction.account_id == Account.id)
            .group_by(
                Account.name,
                Account.display_name,
                Account.institution,
                Account.tax_treatment,
                Account.account_group,
                Account.is_active,
            )
            .order_by(Account.is_active.desc(), Account.name.asc())
            .all()
        )

        return [
            {
                "account": row.account,
                "display_name": row.display_name,
                "institution": row.institution,
                "tax_treatment": row.tax_treatment,
                "account_group": row.account_group,
                "is_active": bool(row.is_active),
                "txn_count": int(row.txn_count or 0),
                "total_amount": float(row.total_amount or 0),
                "last_date": row.last_date,
            }
            for row in rows
        ]
    finally:
        session.close()


def get_monthly_income_report(
    *,
    db_url=DEFAULT_DB_URL,
    income_mode="dividends",
    tax_treatment=None,
):
    session = _session(db_url=db_url)
    try:
        query = (
            session.query(
                Transaction.date.label("date"),
                Transaction.amount.label("amount"),
                Account.tax_treatment.label("tax_treatment"),
                Security.ticker.label("security"),
                Transaction.txn_type.label("txn_type"),
            )
            .join(Account, Account.id == Transaction.account_id)
            .join(Security, Security.id == Transaction.security_id)
            .filter(Transaction.date.isnot(None))
        )

        mode = (income_mode or "dividends").lower()
        if mode == "dividends":
            query = query.filter(Transaction.txn_type == TxnType.DIVIDEND)
            query = query.filter(Security.ticker != "INTEREST")
        elif mode == "interest":
            query = query.filter(Transaction.txn_type == TxnType.DIVIDEND)
            query = query.filter(Security.ticker == "INTEREST")
        else:
            mode = "combined"
            query = query.filter(Transaction.txn_type.in_(INCOME_TXN_TYPES))

        if tax_treatment:
            query = query.filter(Account.tax_treatment == tax_treatment)

        rows = query.order_by(Transaction.date.asc()).all()
        if not rows:
            return {
                "income_mode": mode,
                "tax_treatment": tax_treatment or "",
                "available_tax_treatments": _get_tax_treatments(session),
                "years": [],
                "month_labels": MONTH_LABELS,
                "monthly_matrix": [],
                "annual_totals": {},
                "avg_monthly": {},
                "annual_yoy": {},
                "rolling_three_month": [],
            }

        monthly_totals = {}
        for row in rows:
            year = row.date.year
            month = row.date.month
            monthly_totals[(year, month)] = monthly_totals.get((year, month), 0.0) + float(row.amount or 0)

        years = sorted({year for year, _month in monthly_totals})

        monthly_matrix = []
        for month in range(1, 13):
            month_entry = {"month": month, "label": MONTH_LABELS[month - 1], "values": {}}
            for year in years:
                month_entry["values"][year] = monthly_totals.get((year, month), 0.0)
            monthly_matrix.append(month_entry)

        annual_totals = {
            year: sum(monthly_totals.get((year, month), 0.0) for month in range(1, 13))
            for year in years
        }
        avg_monthly = {
            year: annual_totals[year] / 12.0
            for year in years
        }

        annual_yoy = {}
        previous_year = None
        for year in years:
            if previous_year is None or annual_totals.get(previous_year, 0) == 0:
                annual_yoy[year] = None
            else:
                annual_yoy[year] = (annual_totals[year] - annual_totals[previous_year]) / annual_totals[previous_year]
            previous_year = year

        chronological = [(year, month, monthly_totals.get((year, month), 0.0)) for year in years for month in range(1, 13)]
        rolling_map = {}
        for idx, (year, month, _value) in enumerate(chronological):
            window = [entry[2] for entry in chronological[max(0, idx - 2): idx + 1]]
            rolling_map[(year, month)] = sum(window) / len(window) if window else 0.0

        rolling_three_month = []
        for month in range(1, 13):
            row = {"month": month, "label": MONTH_LABELS[month - 1], "values": {}}
            for year in years:
                row["values"][year] = rolling_map.get((year, month), 0.0)
            rolling_three_month.append(row)

        return {
            "income_mode": mode,
            "tax_treatment": tax_treatment or "",
            "available_tax_treatments": _get_tax_treatments(session),
            "years": years,
            "month_labels": MONTH_LABELS,
            "monthly_matrix": monthly_matrix,
            "annual_totals": annual_totals,
            "avg_monthly": avg_monthly,
            "annual_yoy": annual_yoy,
            "rolling_three_month": rolling_three_month,
        }
    finally:
        session.close()


def _get_tax_treatments(session):
    return [
        row[0]
        for row in session.query(Account.tax_treatment)
        .filter(Account.tax_treatment.isnot(None))
        .distinct()
        .order_by(Account.tax_treatment.asc())
        .all()
        if row[0]
    ]


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
            "metadata": {
                "display_name": account.display_name,
                "institution": account.institution,
                "account_last4": account.account_last4,
                "account_group": account.account_group,
                "tax_treatment": account.tax_treatment,
                "is_active": bool(account.is_active),
                "notes": account.notes,
            },
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
