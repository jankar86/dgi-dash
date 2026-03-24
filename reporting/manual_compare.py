from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.utils import create_session, upsert_transaction
from db.models import Security, Transaction, TxnType


DEFAULT_DB_URL = "sqlite:///dividends.db"
DEFAULT_MANUAL_INTEREST_ACCOUNT = "MANUAL-INTEREST"


def _read_table(path):
    suffix = Path(path).suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        try:
            return pd.read_excel(path)
        except ImportError as exc:
            raise RuntimeError(
                "Excel support requires openpyxl in the local venv. "
                "Install it or export the workbook as CSV."
            ) from exc
    return pd.read_csv(path)


def _normalize_column_name(name):
    return str(name).strip().lower().replace("_", " ")


def _pick_column(columns, *candidates):
    normalized = {_normalize_column_name(col): col for col in columns}
    for candidate in candidates:
        match = normalized.get(candidate)
        if match:
            return match
    for candidate in candidates:
        for normalized_name, original in normalized.items():
            if candidate in normalized_name:
                return original
    return None


def _normalize_security(value):
    if value is None:
        return ""
    return str(value).strip().upper()


def _is_interest_security(value):
    security = _normalize_security(value)
    return security == "INTEREST" or "INTEREST" in security


def _coerce_date(value):
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def load_manual_ledger(path):
    df = _read_table(path)
    date_col = _pick_column(df.columns, "date", "transaction date")
    security_col = _pick_column(df.columns, "security", "symbol", "ticker")
    amount_col = _pick_column(df.columns, "amount", "net amount", "value")

    missing = [
        label
        for label, col in (
            ("date", date_col),
            ("security", security_col),
            ("amount", amount_col),
        )
        if col is None
    ]
    if missing:
        raise ValueError(
            "Manual ledger is missing required columns: "
            + ", ".join(missing)
        )

    manual = pd.DataFrame(
        {
            "manual_row_number": range(2, len(df) + 2),
            "date": df[date_col].map(_coerce_date),
            "security": df[security_col].map(_normalize_security),
            "amount": pd.to_numeric(df[amount_col], errors="coerce"),
        }
    )
    manual["source_file"] = str(path)

    interest_rows = manual[manual["security"].map(_is_interest_security)].copy()
    manual = manual[~manual["security"].map(_is_interest_security)].copy()
    manual = manual[manual["date"].notna() & manual["security"].ne("") & manual["amount"].notna()].copy()
    manual["amount"] = manual["amount"].astype(float)

    return manual.reset_index(drop=True), interest_rows.reset_index(drop=True)


def load_db_transactions(db_url=DEFAULT_DB_URL):
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        rows = (
            session.query(
                Transaction.id,
                Transaction.date,
                Transaction.amount,
                Transaction.txn_type,
                Transaction.source_file,
                Security.ticker,
            )
            .join(Security, Security.id == Transaction.security_id)
            .filter(Transaction.date.isnot(None))
            .all()
        )
    finally:
        session.close()

    db = pd.DataFrame(
        [
            {
                "transaction_id": row.id,
                "date": row.date,
                "security": _normalize_security(row.ticker),
                "amount": float(row.amount),
                "txn_type": str(row.txn_type.value if hasattr(row.txn_type, "value") else row.txn_type),
                "source_file": row.source_file,
            }
            for row in rows
        ]
    )
    if db.empty:
        return db
    return db.sort_values(["security", "date", "amount", "transaction_id"]).reset_index(drop=True)


def compare_manual_ledger(path, db_url=DEFAULT_DB_URL, date_tolerance_days=1, amount_tolerance=0.05):
    manual, interest_rows = load_manual_ledger(path)
    db = load_db_transactions(db_url=db_url)

    if db.empty:
        raise ValueError("Database has no transactions to compare.")

    unmatched_db_ids = set(db["transaction_id"].tolist())
    matches = []
    unmatched_manual = []

    grouped_db = {security: group.copy() for security, group in db.groupby("security", sort=False)}

    for row in manual.to_dict("records"):
        candidates = grouped_db.get(row["security"])
        if candidates is None or candidates.empty:
            unmatched_manual.append({**row, "reason": "security_missing_in_db"})
            continue

        candidate_rows = candidates[candidates["transaction_id"].isin(unmatched_db_ids)].copy()
        if candidate_rows.empty:
            unmatched_manual.append({**row, "reason": "security_present_but_no_unused_rows"})
            continue

        candidate_rows["date_diff_days"] = candidate_rows["date"].map(
            lambda db_date: abs((db_date - row["date"]).days)
        )
        candidate_rows["amount_diff"] = candidate_rows["amount"].map(
            lambda db_amount: abs(db_amount - row["amount"])
        )
        candidate_rows = candidate_rows[
            (candidate_rows["date_diff_days"] <= date_tolerance_days)
            & (candidate_rows["amount_diff"] <= amount_tolerance)
        ]
        if candidate_rows.empty:
            unmatched_manual.append({**row, "reason": "no_close_match"})
            continue

        best = candidate_rows.sort_values(
            ["date_diff_days", "amount_diff", "transaction_id"]
        ).iloc[0]
        unmatched_db_ids.remove(int(best["transaction_id"]))
        matches.append(
            {
                **row,
                "matched_transaction_id": int(best["transaction_id"]),
                "matched_date": best["date"],
                "matched_amount": float(best["amount"]),
                "matched_txn_type": best["txn_type"],
                "matched_source_file": best["source_file"],
                "date_diff_days": int(best["date_diff_days"]),
                "amount_diff": float(best["amount_diff"]),
            }
        )

    unmatched_db = db[db["transaction_id"].isin(unmatched_db_ids)].copy()
    unmatched_db["reason"] = "not_matched_by_manual_ledger"

    matched_df = pd.DataFrame(matches)
    unmatched_manual_df = pd.DataFrame(unmatched_manual)
    interest_df = pd.DataFrame(interest_rows)

    summary = {
        "manual_rows_total": int(len(manual) + len(interest_rows)),
        "manual_rows_compared": int(len(manual)),
        "manual_interest_rows_ignored": int(len(interest_rows)),
        "manual_rows_matched": int(len(matched_df)),
        "manual_rows_unmatched": int(len(unmatched_manual_df)),
        "db_rows_total": int(len(db)),
        "db_rows_unmatched": int(len(unmatched_db)),
        "match_rate_manual": (float(len(matched_df)) / len(manual)) if len(manual) else 0.0,
        "date_tolerance_days": int(date_tolerance_days),
        "amount_tolerance": float(amount_tolerance),
    }
    if not matched_df.empty:
        summary["avg_date_diff_days"] = float(matched_df["date_diff_days"].mean())
        summary["avg_amount_diff"] = float(matched_df["amount_diff"].mean())
    else:
        summary["avg_date_diff_days"] = None
        summary["avg_amount_diff"] = None

    return summary, matched_df, unmatched_manual_df, unmatched_db, interest_df


def write_manual_compare_report(
    path,
    db_url=DEFAULT_DB_URL,
    date_tolerance_days=1,
    amount_tolerance=0.05,
    output_dir="reports",
):
    summary, matched_df, unmatched_manual_df, unmatched_db_df, interest_df = compare_manual_ledger(
        path,
        db_url=db_url,
        date_tolerance_days=date_tolerance_days,
        amount_tolerance=amount_tolerance,
    )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    stem = Path(path).stem

    pd.DataFrame([summary]).to_csv(output_path / f"{stem}_manual_compare_summary.csv", index=False)
    matched_df.to_csv(output_path / f"{stem}_manual_compare_matches.csv", index=False)
    unmatched_manual_df.to_csv(
        output_path / f"{stem}_manual_compare_manual_unmatched.csv", index=False
    )
    unmatched_db_df.to_csv(output_path / f"{stem}_manual_compare_db_unmatched.csv", index=False)
    interest_df.to_csv(output_path / f"{stem}_manual_compare_interest_ignored.csv", index=False)

    return summary, output_path


def print_manual_compare_report(
    path,
    db_url=DEFAULT_DB_URL,
    date_tolerance_days=1,
    amount_tolerance=0.05,
    output_dir="reports",
):
    summary, output_path = write_manual_compare_report(
        path,
        db_url=db_url,
        date_tolerance_days=date_tolerance_days,
        amount_tolerance=amount_tolerance,
        output_dir=output_dir,
    )
    print(f"manual_file={path}")
    print(f"report_dir={output_path}")
    for key, value in summary.items():
        print(f"{key}={value}")


def import_manual_interest(
    path,
    *,
    db_url=DEFAULT_DB_URL,
    account_name=DEFAULT_MANUAL_INTEREST_ACCOUNT,
):
    _manual_rows, interest_rows = load_manual_ledger(path)
    if interest_rows.empty:
        print(f"manual_interest_file={path}")
        print("interest_rows_found=0")
        print("interest_rows_imported=0")
        print("interest_rows_skipped=0")
        return 0, 0

    session = create_session(db_url=db_url)
    imported_count = 0
    skipped_count = 0

    try:
        for row in interest_rows.to_dict("records"):
            inserted = upsert_transaction(
                session,
                account_name=account_name,
                symbol=row["security"],
                txn_type=TxnType.DIVIDEND,
                date=row["date"],
                quantity=0,
                price=0,
                amount=row["amount"],
                is_qualified=False,
                allocation_status="ALLOCATED",
                source_system="manual_interest",
                source_file=str(path),
                raw_action="INTEREST",
            )
            if inserted:
                imported_count += 1
            else:
                skipped_count += 1

        session.commit()
    finally:
        session.close()

    print(f"manual_interest_file={path}")
    print(f"manual_interest_account={account_name}")
    print(f"interest_rows_found={len(interest_rows)}")
    print(f"interest_rows_imported={imported_count}")
    print(f"interest_rows_skipped={skipped_count}")
    return imported_count, skipped_count
