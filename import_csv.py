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
    for _, row in df.iterrows():
        txn_type = TxnType[row['type'].upper()]
        date = datetime.strptime(row['date'], "%Y-%m-%d").date()
        upsert_transaction(
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

    session.commit()
    print("Transactions imported.")

if __name__ == "__main__":
    df = load_csv("your_brokerage_dump.csv")  # Replace with your file path
    import_transactions(df)
