# import_fidelity_csv.py

import pandas as pd
from models import TxnType
from import_utils import create_session, upsert_transaction

session = create_session()

def is_fidelity_format(df):
    expected = ['run date', 'account', 'action', 'symbol', 'description', 'type', 
                'quantity', 'price ($)', 'commission ($)', 'fees ($)', 
                'accrued interest ($)', 'amount ($)', 'settlement date']
    return all(col.lower().strip() in df.columns for col in expected)

def load_fidelity_csv(filepath):
    # Force UTF-8 decoding and ensure header is properly read
    df = pd.read_csv(filepath, dtype=str, encoding='utf-8', skip_blank_lines=True)
    df.columns = [c.strip().lower() for c in df.columns]

    if not is_fidelity_format(df):
        print(f"❌ Skipped: {filepath} (not recognized as Fidelity format)")
        return None

    # Drop completely empty rows and rows without "action"
    df = df[df['action'].notna() & df['symbol'].notna() & df['amount ($)'].notna()]

    # Normalize the "action" field (strip, upper-case)
    df['action'] = df['action'].astype(str).str.upper().str.strip()

    # Filter for dividend-related rows
    df = df[df['action'].str.upper().str.contains('DIVIDEND RECEIVED|REINVESTMENT')]


    if df.empty:
        print(f"ℹ️ No dividend transactions found in {filepath}")
        return None

    # Normalize fields
    df['date'] = pd.to_datetime(df['run date'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount ($)'].str.replace(',', '').str.strip(), errors='coerce').fillna(0)
    df['quantity'] = pd.to_numeric(df['quantity'].str.strip(), errors='coerce').fillna(0)
    df['price'] = pd.to_numeric(df['price ($)'].str.strip(), errors='coerce').fillna(0)
    df['symbol'] = df['symbol'].str.strip().fillna('UNKNOWN')
    df['is_qualified'] = False  # Default for now

    # Extract account number from string (e.g., "ROTH IRA 224294217")
    df['account'] = 'FIDELITY-' + df['account'].str.extract(r'(\d+)', expand=False).fillna('UNKNOWN')

    df['type'] = 'DIVIDEND'

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
        )
        if inserted:
            imported_count += 1
        else:
            skipped_count += 1

    session.commit()
    print(f"✅ Imported: {imported_count} | ⏭️ Skipped (duplicate): {skipped_count}")
    return imported_count, skipped_count

if __name__ == "__main__":
    from workflows import run_fidelity_import

    run_fidelity_import()
