import os
import re

import pandas as pd

from db.models import Account, TxnType
from db.utils import (
    create_session,
    drop_unknown_placeholder_rows,
    drop_zero_amount_rows,
    upsert_transaction,
)

session = create_session()


def is_fidelity_format(df):
    cols = set(df.columns)
    old_expected = {
        "run date", "account", "action", "symbol", "description", "type",
        "quantity", "price ($)", "commission ($)", "fees ($)",
        "accrued interest ($)", "amount ($)", "settlement date",
    }
    new_expected = {
        "run date", "action", "symbol", "description", "type", "price ($)",
        "quantity", "commission ($)", "fees ($)", "accrued interest ($)",
        "amount ($)", "cash balance ($)", "settlement date",
    }
    return old_expected.issubset(cols) or new_expected.issubset(cols)


def _infer_account_from_filename(filepath):
    filename = os.path.basename(filepath)
    match = re.search(r"(\d{4,})-fidelity", filename.lower())
    if not match:
        raise ValueError(
            "New-format Fidelity CSV filenames must include 4+ account digits "
            "immediately before '-fidelity', for example '4217-fidelity-2025.csv'. "
            f"Received: {filename}"
        )

    suffix = match.group(1)
    candidates = (
        session.query(Account)
        .filter(Account.name.like(f"FIDELITY-%{suffix}"))
        .all()
    )
    if len(candidates) == 1:
        return candidates[0].name
    return f"FIDELITY-{suffix}"


def load_fidelity_csv(filepath):
    df = pd.read_csv(filepath, dtype=str, encoding="utf-8-sig", skip_blank_lines=True)
    df.columns = [c.replace("\ufeff", "").strip().lower() for c in df.columns]

    if not is_fidelity_format(df):
        print(f"❌ Skipped: {filepath} (not recognized as Fidelity format)")
        return None

    df = df[df["action"].notna() & df["symbol"].notna() & df["amount ($)"].notna()]
    df["action"] = df["action"].astype(str).str.upper().str.strip()
    df = df[df["action"].str.upper().str.contains("DIVIDEND RECEIVED|REINVESTMENT")]

    if df.empty:
        print(f"ℹ️ No dividend transactions found in {filepath}")
        return None

    df["date"] = pd.to_datetime(df["run date"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount ($)"].str.replace(",", "").str.strip(), errors="coerce").fillna(0)
    df["quantity"] = pd.to_numeric(df["quantity"].str.strip(), errors="coerce").fillna(0)
    df["price"] = pd.to_numeric(df["price ($)"].str.strip(), errors="coerce").fillna(0)
    df["symbol"] = df["symbol"].str.strip().fillna("UNKNOWN")
    df["is_qualified"] = False

    if "account" in df.columns:
        df["account"] = "FIDELITY-" + df["account"].str.extract(r"(\d+)", expand=False).fillna("UNKNOWN")
    else:
        df["account"] = _infer_account_from_filename(filepath)

    df["type"] = "DIVIDEND"
    df.loc[df["action"].str.contains("REINVESTMENT", na=False), "type"] = "REINVESTMENT"
    df["source_system"] = "fidelity_csv"
    df["source_file"] = filepath
    df["raw_action"] = df["action"]

    df = drop_unknown_placeholder_rows(
        df,
        column_name="symbol",
        source_name="fidelity",
        filepath=filepath,
        field_label="symbol",
    )
    df = drop_unknown_placeholder_rows(
        df,
        column_name="account",
        source_name="fidelity",
        filepath=filepath,
        field_label="account",
    )
    return drop_zero_amount_rows(df, source_name="fidelity", filepath=filepath)


def import_transactions(df):
    imported_count = 0
    skipped_count = 0

    for _, row in df.iterrows():
        inserted = upsert_transaction(
            session,
            account_name=row["account"],
            symbol=row["symbol"],
            txn_type=TxnType[str(row["type"]).upper()],
            date=row["date"],
            quantity=row["quantity"],
            price=row["price"],
            amount=row["amount"],
            is_qualified=row["is_qualified"],
            allocation_status="ALLOCATED",
            source_system=row.get("source_system"),
            source_file=row.get("source_file"),
            raw_action=row.get("raw_action"),
        )
        if inserted:
            imported_count += 1
        else:
            skipped_count += 1

    session.commit()
    print(f"✅ Imported: {imported_count} | ⏭️ Skipped (duplicate): {skipped_count}")
    return imported_count, skipped_count
