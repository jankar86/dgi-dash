# import_csv.py
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Account, Security, Transaction, TxnType
from datetime import datetime

engine = create_engine('sqlite:///dividends.db')
Session = sessionmaker(bind=engine)
session = Session()

def load_csv(filepath):
    df = pd.read_csv(filepath)
    df.columns = [col.strip().lower() for col in df.columns]
    return df

def import_transactions(df):
    for _, row in df.iterrows():
        account_name = row['account']
        symbol = row['symbol']
        txn_type = TxnType[row['type'].upper()]
        date = datetime.strptime(row['date'], "%Y-%m-%d").date()

        acct = session.query(Account).filter_by(name=account_name).first()
        if not acct:
            acct = Account(name=account_name)
            session.add(acct)

        sec = session.query(Security).filter_by(ticker=symbol).first()
        if not sec:
            sec = Security(ticker=symbol)
            session.add(sec)

        exists = session.query(Transaction).filter_by(
            account=acct, security=sec, txn_type=txn_type,
            date=date, amount=row.get('amount', 0)
        ).first()

        if not exists:
            txn = Transaction(
                account=acct,
                security=sec,
                txn_type=txn_type,
                date=date,
                quantity=row.get('quantity', 0),
                price=row.get('price', 0),
                amount=row.get('amount', 0),
            )
            session.add(txn)

    session.commit()
    print("Transactions imported.")

if __name__ == "__main__":
    df = load_csv("your_brokerage_dump.csv")  # Replace with your file path
    import_transactions(df)
