import pandas as pd
import os
from datetime import datetime, date
from db import create_database, get_session, Account, Dividend
from sqlalchemy.exc import IntegrityError

def load_and_process_csv(file_path):
    # Read the first row to get the account number for specific format
    with open(file_path, 'r') as file:
        first_row = file.readline().strip()
    
    # Determine the format based on the first row
    if 'Action' in first_row:
        # Format 1 - Fidelity
        print(f"Data from {file_path} identified as Format #1 Fidelity:")
        data = pd.read_csv(file_path)

        # Rename columns to match the table schema
        data.columns = [
            'transaction_date', 'account', 'transaction_type', 'symbol', 'description', 'type', 
            'quantity', 'price', 'commission', 'fees', 'accrued_interest', 
            'amount', 'settlement_date'
        ]

        # Ensure transaction_date is in a consistent format
        data['transaction_date'] = pd.to_datetime(data['transaction_date'], errors='coerce').dt.date

        # Filter rows where the transaction_type/action is "DIVIDEND RECEIVED"
        filtered_data = data[data['transaction_type'].str.contains('DIVIDEND RECEIVED', case=False, na=False)].copy()
        #print(filtered_data['transaction_date'])

    elif 'Account' in first_row:
        # Format 2 - Etrade CSV need to read first line to get account number data.
        print(f"Data from {file_path} identified as Format #2 Etrade:")
        data = pd.read_csv(file_path, skiprows=1)
        account_number = first_row.split(',')[-1].strip()

        print(f"Account Number detected: {account_number}")

        data.columns = [
            'transaction_date', 'transaction_type', 'security_type', 'symbol', 
            'quantity', 'amount', 'price', 'commission', 'description'
        ]

        # Convert transaction_date to a four-digit year format
        #data['transaction_date'] = pd.to_datetime(data['transaction_date'], errors='coerce').dt.date
        data['transaction_date'] = pd.to_datetime(data['transaction_date'], format='%m/%d/%y', errors='coerce').dt.date
        #print(data['transaction_date'])

        # Filter rows where the transaction_type is "Dividend"
        filtered_data = data[data['transaction_type'].str.contains('Dividend', case=False, na=False)].copy()

        # Append account number to each row
        filtered_data.loc[:, 'account'] = account_number
    
    elif 'HistoricalData' in first_row:
        # Format 3 - Historical Data to import to database
        print(f"Data from {file_path} identified as Format #3 Historical data import:")
        data = pd.read_csv(file_path, skiprows=1)
        account_number = "####1234"

        data.columns = [
            'transaction_date', 'transaction_type', 'symbol', 
            'amount', 'description','quantity','price','commission'
        ]

        # Convert transaction_date to a four-digit year format
        data['transaction_date'] = pd.to_datetime(data['transaction_date'], errors='coerce').dt.date
        #print(data['transaction_date'])

        # Filter rows where the transaction_type is "Dividend"
        filtered_data = data

        # Append account number to each row
        filtered_data.loc[:, 'account'] = account_number

    else:
        raise ValueError("Unknown CSV format")

    return filtered_data

def insert_into_db(filtered_data, session):
    # Insert unique account numbers into the accounts table with error handling
    account_numbers = filtered_data['account'].unique()

    for account_number in account_numbers:
        account = Account(account_number=account_number)
        try:
            session.merge(account)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            if "UNIQUE constraint failed" in str(e):
                print(f"Account {account_number} already exists in the database. Skipping.")
            else:
                raise

    # Retrieve account IDs
    accounts = session.query(Account).all()
    account_id_map = {account.account_number: account.account_id for account in accounts}
    filtered_data['account_id'] = filtered_data['account'].map(account_id_map)

    # Select only the columns needed for insertion, including the new account_id column and excluding 'account'
    # Only needed for Format 1 data fidelity?
    columns_to_insert = [
        'transaction_date', 'transaction_type', 'symbol', 'description', 
        'quantity', 'price', 'commission', 'amount', 'account_id'
    ]

    # Insert the filtered data into the database with error handling for duplicates
    for _, row in filtered_data[columns_to_insert].iterrows():
        dividend = Dividend(
            transaction_date=row['transaction_date'],
            transaction_type=row['transaction_type'],
            symbol=row['symbol'],
            description=row['description'],
            quantity=row['quantity'],
            price=row['price'],
            commission=row['commission'],
            amount=row['amount'],
            account_id=row['account_id']
        )
        try:
            session.add(dividend)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            if "UNIQUE constraint failed" in str(e):
                 print(f"Skipping duplicate entry: Date: {row['transaction_date']}, Symbol: {row['symbol']}, Amount: {row['amount']}")
            else:
                raise

    # Verify the data has been inserted
    rows = session.query(Dividend).limit(5).all()
    
    return rows

# Directory containing CSV files
#csv_directory = 'data/archived/'
csv_directory = 'data/'

# Database URL (Change this to switch between SQLite and MySQL)
db_url = 'sqlite:///data/dividends_dev.db'  # SQLite
# db_url = 'mysql+pymysql://username:password@host:port/database'  # MySQL

# Create or initialize the database
engine = create_database(db_url)

# Process each CSV file in the directory
for file_name in os.listdir(csv_directory):
    if file_name.endswith('.csv'):
    #if file_name.startswith('etrade-3006'):
    #if file_name.startswith('fid-dev'):
    #if file_name.startswith('histor'):
        file_path = os.path.join(csv_directory, file_name)
        try:
            filtered_data = load_and_process_csv(file_path)
            session = get_session(engine)
            rows = insert_into_db(filtered_data, session)
            print(f"Data from {file_path} inserted successfully:")
            print(rows)
        except ValueError as e:
            print(f"Error processing {file_path}: {e}")
