import os
import pandas as pd
from models import TxnType
from import_utils import create_session, upsert_transaction

session = create_session()

DATA_DIR = "data/etrade"

def detect_etrade_account_and_data(filepath):
    with open(filepath, 'r') as f:
        lines = f.readlines()

    account_line = next((line for line in lines if line.startswith("For Account:")), None)
    if not account_line:
        return None, None

    # Extract account number
    try:
        account_number = account_line.split(",")[1].strip()
    except IndexError:
        return None, None

    # Find where CSV header starts
    csv_start_index = next((i for i, line in enumerate(lines) if line.strip().startswith("TransactionDate")), None)
    if csv_start_index is None:
        return None, None

    df = pd.read_csv(filepath, skiprows=csv_start_index)
    df.columns = [c.strip().lower() for c in df.columns]

    # Filter for dividends
    df['transactiontype'] = df['transactiontype'].str.upper().str.strip()
    df = df[df['transactiontype'].isin(['DIVIDEND', 'QUALIFIED DIVIDEND'])]
    df['is_qualified'] = df['transactiontype'] == 'QUALIFIED DIVIDEND'

    if df.empty:
        return account_number, None

    # Clean columns
    df['date'] = pd.to_datetime(df['transactiondate'], format='%m/%d/%y')
    df['symbol'] = df['symbol'].str.strip().fillna('UNKNOWN')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
    df['account'] = f"ETRADE-{account_number}"
    df['type'] = 'DIVIDEND'

    return account_number, df


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
        )
        if inserted:
            imported_count += 1
        else:
            skipped_count += 1

    session.commit()
    print(f"✅ Imported: {imported_count} | ⏭️ Skipped (duplicate): {skipped_count} for account {df.iloc[0]['account']}")


def process_all_etrade_files():
    for root, _, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith(".csv"):
                filepath = os.path.join(root, file)
                print(f"Processing {filepath}...")
                account_number, df = detect_etrade_account_and_data(filepath)
                if account_number and df is not None:
                    import_transactions(df)
                else:
                    print(f"Skipped: {filepath} (not valid E*TRADE or no dividend data)")

if __name__ == "__main__":
    process_all_etrade_files()
