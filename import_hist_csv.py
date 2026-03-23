import pandas as pd
from models import TxnType
from import_utils import create_session, upsert_transaction

session = create_session()

def load_custom_historical(filepath):
    df = pd.read_csv(filepath, dtype=str, skiprows=1)  # Skip "HistoricalData," line
    df.columns = [c.strip().lower() for c in df.columns]

    # Filter only 'dividend' rows
    df = df[df['transaction_type'].str.lower() == 'dividend']

    # Normalize columns
    df['date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'].str.replace(',', '').str.strip(), errors='coerce').fillna(0)
    df['quantity'] = pd.to_numeric(df.get('quantity', 0), errors='coerce').fillna(0)
    df['price'] = pd.to_numeric(df.get('price', 0), errors='coerce').fillna(0)
    df['symbol'] = df['symbol'].str.strip().fillna('UNKNOWN')
    df['is_qualified'] = False
    df['account'] = 'UNALLOCATED-LEGACY'
    df['allocation_status'] = 'UNALLOCATED'

    return df

def import_transactions(df):
    imported_count = 0
    skipped_count = 0

    for _, row in df.iterrows():
        inserted = upsert_transaction(
            session,
            account_name=row['account'],
            symbol=row['symbol'],
            txn_type=TxnType.DIVIDEND,
            date=row['date'],
            quantity=row['quantity'],
            price=row['price'],
            amount=row['amount'],
            is_qualified=row['is_qualified'],
            allocation_status=row.get('allocation_status', 'UNALLOCATED'),
        )
        if inserted:
            imported_count += 1
        else:
            skipped_count += 1

    session.commit()
    print(f"✅ Imported: {imported_count} | ⏭️ Skipped (duplicate): {skipped_count}")
    return imported_count, skipped_count

if __name__ == "__main__":
    from workflows import run_historical_import

    run_historical_import()
