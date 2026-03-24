import os
import re
import pandas as pd
from models import TxnType
from import_utils import (
    create_session,
    drop_unknown_placeholder_rows,
    drop_zero_amount_rows,
    upsert_transaction,
)

session = create_session()

DATA_DIR = "data/etrade"

def _format_account_label(account_number):
    cleaned = str(account_number).strip()
    digits = "".join(ch for ch in cleaned if ch.isdigit())
    if digits and len(digits) == 4:
        return f"ETRADE-#####{digits}"
    if digits:
        return f"ETRADE-{digits}"
    return f"ETRADE-{cleaned}"


def _parse_etrade_legacy(lines, filepath):
    account_line = next((line for line in lines if line.startswith("For Account:")), None)
    if not account_line:
        return None, None

    try:
        account_number = account_line.split(",")[1].strip()
    except IndexError:
        return None, None

    csv_start_index = next(
        (i for i, line in enumerate(lines) if line.strip().startswith("TransactionDate")),
        None,
    )
    if csv_start_index is None:
        return None, None

    df = pd.read_csv(filepath, skiprows=csv_start_index)
    df.columns = [c.strip().lower() for c in df.columns]
    if "transactiontype" not in df.columns:
        return None, None

    df["transactiontype"] = df["transactiontype"].astype(str).str.upper().str.strip()
    df = df[df["transactiontype"].isin(["DIVIDEND", "QUALIFIED DIVIDEND"])]
    df["is_qualified"] = df["transactiontype"] == "QUALIFIED DIVIDEND"

    if df.empty:
        return account_number, None

    df["date"] = pd.to_datetime(df["transactiondate"], format="%m/%d/%y", errors="coerce")
    df["symbol"] = df["symbol"].fillna("UNKNOWN").astype(str).str.strip()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["account"] = _format_account_label(account_number)
    df["type"] = "DIVIDEND"
    if "description" in df.columns:
        reinvest_mask = df["description"].astype(str).str.upper().str.contains("REINVEST", na=False)
        df.loc[reinvest_mask, "type"] = "REINVESTMENT"
    df["source_system"] = "etrade_csv"
    df["source_file"] = filepath
    df["raw_action"] = df["transactiontype"]
    df = drop_unknown_placeholder_rows(
        df,
        column_name="symbol",
        source_name="etrade",
        filepath=filepath,
        field_label="symbol",
    )
    df = drop_zero_amount_rows(df, source_name="etrade", filepath=filepath)
    return account_number, df


def _parse_etrade_new(lines, filepath):
    header_idx = next(
        (i for i, line in enumerate(lines) if line.strip().startswith("Activity/Trade Date")),
        None,
    )
    if header_idx is None:
        return None, None

    account_line = next((line for line in lines if "Account Activity for" in line), "")
    match = re.search(r"Index\s*-\s*(\d+)", account_line)
    if not match:
        match = re.search(r"Account Activity for .*?-\s*(\d+)\s+from", account_line)
    if not match:
        return None, None
    account_number = match.group(1)

    df = pd.read_csv(filepath, skiprows=header_idx)
    df.columns = [c.replace("\ufeff", "").strip().lower() for c in df.columns]
    if "activity type" not in df.columns:
        return None, None

    df["activity type"] = df["activity type"].astype(str).str.upper().str.strip()
    df = df[df["activity type"].isin(["DIVIDEND", "QUALIFIED DIVIDEND"])]
    df["is_qualified"] = df["activity type"] == "QUALIFIED DIVIDEND"

    if df.empty:
        return account_number, None

    date_col = "transaction date" if "transaction date" in df.columns else "activity/trade date"
    df["date"] = pd.to_datetime(df[date_col], format="%m/%d/%y", errors="coerce")
    df["symbol"] = df["symbol"].fillna("UNKNOWN").astype(str).str.strip()
    df["amount"] = pd.to_numeric(
        df["amount $"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    ).fillna(0)
    df["quantity"] = pd.to_numeric(
        df.get("quantity #", 0).astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    ).fillna(0)
    df["price"] = pd.to_numeric(
        df.get("price $", 0).astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    ).fillna(0)
    df["account"] = _format_account_label(account_number)
    df["type"] = "DIVIDEND"
    if "description" in df.columns:
        reinvest_mask = df["description"].astype(str).str.upper().str.contains("REINVEST", na=False)
        df.loc[reinvest_mask, "type"] = "REINVESTMENT"
    df["source_system"] = "etrade_csv"
    df["source_file"] = filepath
    df["raw_action"] = df["activity type"]
    df = drop_unknown_placeholder_rows(
        df,
        column_name="symbol",
        source_name="etrade",
        filepath=filepath,
        field_label="symbol",
    )
    df = drop_zero_amount_rows(df, source_name="etrade", filepath=filepath)
    return account_number, df


def detect_etrade_account_and_data(filepath):
    with open(filepath, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    account_number, df = _parse_etrade_legacy(lines, filepath)
    if account_number is not None:
        return account_number, df

    return _parse_etrade_new(lines, filepath)


def import_transactions(df):
    imported_count = 0
    skipped_count = 0

    for _, row in df.iterrows():
        inserted = upsert_transaction(
            session,
            account_name=row['account'],
            symbol=row['symbol'],
            txn_type=TxnType[str(row['type']).upper()],
            date=row['date'],
            quantity=row['quantity'],
            price=row['price'],
            amount=row['amount'],
            is_qualified=row['is_qualified'],
            allocation_status='ALLOCATED',
            source_system=row.get('source_system'),
            source_file=row.get('source_file'),
            raw_action=row.get('raw_action'),
        )
        if inserted:
            imported_count += 1
        else:
            skipped_count += 1

    session.commit()
    print(f"✅ Imported: {imported_count} | ⏭️ Skipped (duplicate): {skipped_count} for account {df.iloc[0]['account']}")
    return imported_count, skipped_count


def process_etrade_file(filepath):
    print(f"Processing {filepath}...")
    account_number, df = detect_etrade_account_and_data(filepath)
    if account_number and df is not None:
        return import_transactions(df)

    print(f"Skipped: {filepath} (not valid E*TRADE or no dividend data)")
    return 0, 0


def process_all_etrade_files(data_dir=DATA_DIR):
    total_imported = 0
    total_skipped = 0
    for root, _, files in os.walk(data_dir):
        for file in sorted(files):
            if file.endswith(".csv"):
                filepath = os.path.join(root, file)
                imported_count, skipped_count = process_etrade_file(filepath)
                total_imported += imported_count
                total_skipped += skipped_count
    return total_imported, total_skipped

if __name__ == "__main__":
    from workflows import run_etrade_import

    run_etrade_import()
