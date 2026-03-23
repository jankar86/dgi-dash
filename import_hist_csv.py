import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Account, Security, Transaction, TxnType
from datetime import datetime

engine = create_engine('sqlite:///dividends.db')
Session = sessionmaker(bind=engine)
session = Session()

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
    df['account'] = 'Historical'

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
    filepath = "data/archived/historical_divs.csv"  # Update to your path
    df = load_custom_historical(filepath)
    if df is not None:
        import_transactions(df)
