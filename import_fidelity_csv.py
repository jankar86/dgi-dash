# import_fidelity_csv.py

import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Account, Security, Transaction, TxnType
from datetime import datetime

engine = create_engine('sqlite:///dividends.db')
Session = sessionmaker(bind=engine)
session = Session()

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
        acct = session.query(Account).filter_by(name=row['account']).first()
        if not acct:
            acct = Account(name=row['account'])
            session.add(acct)

        sec = session.query(Security).filter_by(ticker=row['symbol']).first()
        if not sec:
            sec = Security(ticker=row['symbol'])
            session.add(sec)

        session.flush()

        exists = session.query(Transaction).filter_by(
            account_id=acct.id,
            security_id=sec.id,
            txn_type=TxnType.DIVIDEND,
            date=row['date'],
            amount=row['amount']
        ).first()

        if not exists:
            txn = Transaction(
                account=acct,
                security=sec,
                txn_type=TxnType.DIVIDEND,
                date=row['date'],
                quantity=row['quantity'],
                price=row['price'],
                amount=row['amount'],
                is_qualified=row['is_qualified'],
            )
            session.add(txn)
            imported_count += 1
        else:
            skipped_count += 1

    session.commit()
    print(f"✅ Imported: {imported_count} | ⏭️ Skipped (duplicate): {skipped_count}")

if __name__ == "__main__":
    filepath = "data/fidelity/fid-dev.csv"  # Change to match your actual file
    df = load_fidelity_csv(filepath)
    if df is not None:
        import_transactions(df)
