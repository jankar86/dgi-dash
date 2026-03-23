# import_csv.py
import pandas as pd
from models import TxnType
from datetime import datetime
from import_utils import create_session, upsert_transaction

session = create_session()

def load_csv(filepath):
    df = pd.read_csv(filepath)
    df.columns = [col.strip().lower() for col in df.columns]
    return df

def import_transactions(df):
    imported_count = 0
    skipped_count = 0
    for _, row in df.iterrows():
        txn_type = TxnType[row['type'].upper()]
        date = datetime.strptime(row['date'], "%Y-%m-%d").date()
        inserted = upsert_transaction(
            session,
            account_name=row['account'],
            symbol=row['symbol'],
            txn_type=txn_type,
            date=date,
            quantity=row.get('quantity', 0),
            price=row.get('price', 0),
            amount=row.get('amount', 0),
            is_qualified=row.get('is_qualified', False),
        )
        if inserted:
            imported_count += 1
        else:
            skipped_count += 1

    session.commit()
    print("Transactions imported.")
    return imported_count, skipped_count

if __name__ == "__main__":
    from workflows import run_generic_import

    run_generic_import()
