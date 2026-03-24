import pandas as pd
from models import TxnType
from import_utils import (
    create_session,
    drop_unknown_placeholder_rows,
    drop_zero_amount_rows,
    upsert_transaction,
)

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
    df['symbol'] = df['symbol'].str.strip().str.upper().fillna('UNKNOWN')
    df['is_qualified'] = False
    df['account'] = 'UNALLOCATED-LEGACY'
    df['allocation_status'] = 'UNALLOCATED'
    df['source_system'] = 'historical_csv'
    df['source_file'] = filepath
    df['raw_action'] = df.get('transaction_type', '')

    df = drop_unknown_placeholder_rows(
        df,
        column_name="symbol",
        source_name="historical",
        filepath=filepath,
        field_label="symbol",
    )
    return drop_zero_amount_rows(df, source_name="historical", filepath=filepath)

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
            source_system=row.get('source_system'),
            source_file=row.get('source_file'),
            raw_action=row.get('raw_action'),
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
