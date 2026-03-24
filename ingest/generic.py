# import_csv.py
from datetime import datetime

import pandas as pd

from db.models import TxnType
from db.utils import create_session, upsert_transaction

session = create_session()


def load_csv(filepath):
    df = pd.read_csv(filepath)
    df.columns = [col.strip().lower() for col in df.columns]
    df["source_system"] = "generic_csv"
    df["source_file"] = filepath
    if "raw_action" not in df.columns:
        df["raw_action"] = ""
    return df


def import_transactions(df):
    imported_count = 0
    skipped_count = 0
    for _, row in df.iterrows():
        txn_type = TxnType[row["type"].upper()]
        date = datetime.strptime(row["date"], "%Y-%m-%d").date()
        inserted = upsert_transaction(
            session,
            account_name=row["account"],
            symbol=row["symbol"],
            txn_type=txn_type,
            date=date,
            quantity=row.get("quantity", 0),
            price=row.get("price", 0),
            amount=row.get("amount", 0),
            is_qualified=row.get("is_qualified", False),
            allocation_status=row.get("allocation_status", "ALLOCATED"),
            source_system=row.get("source_system"),
            source_file=row.get("source_file"),
            raw_action=row.get("raw_action"),
        )
        if inserted:
            imported_count += 1
        else:
            skipped_count += 1

    session.commit()
    print("Transactions imported.")
    return imported_count, skipped_count
