import pandas as pd
import os
from sqlite3 import IntegrityError
from datetime import datetime
from db import create_database, get_db_connection

def load_and_process_csv(file_path):
    # Read the first row to get the account number for specific format
    with open(file_path, 'r') as file:
        first_row = file.readline().strip()
    
    # Determine the format based on the first row
    if 'Action' in first_row:
        # Format 1 - Fidelity
        data = pd.read_csv(file_path)

        # Rename columns to match the table schema
        data.columns = [
            'transaction_date', 'account', 'transaction_type', 'symbol', 'description', 'type', 
            'quantity', 'price', 'commission', 'fees', 'accrued_interest', 
            'amount', 'settlement_date'
        ]

        # Ensure transaction_date is in a consistent format
        data['transaction_date'] = pd.to_datetime(data['transaction_date'], errors='coerce').dt.strftime('%m/%d/%Y')

        # Filter rows where the transaction_type/action is "DIVIDEND RECEIVED"
        filtered_data = data[data['transaction_type'].str.contains('DIVIDEND RECEIVED', case=False, na=False)]
    
    elif 'Account' in first_row:
        # Format 2 - Etrade CSV need to read first line to get account number data.
        data = pd.read_csv(file_path, skiprows=1)
        account_number = first_row.split(',')[-1].strip()

        data.columns = [
            'transaction_date', 'transaction_type', 'security_type', 'symbol', 
            'quantity', 'amount', 'price', 'commission', 'description'
        ]

        # Convert transaction_date to a four-digit year format
        data['transaction_date'] = pd.to_datetime(data['transaction_date'], errors='coerce').dt.strftime('%m/%d/%Y')

        # Filter rows where the transaction_type is "Dividend"
        filtered_data = data[data['transaction_type'].str.contains('Dividend', case=False, na=False)]

        # Append account number to each row
        filtered_data['account'] = account_number
    
    else:
        raise ValueError("Unknown CSV format")

    return filtered_data

def insert_into_db(filtered_data, db_path):
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # Insert unique account numbers into the accounts table with error handling
    accounts = filtered_data[['account']].drop_duplicates().rename(columns={'account': 'account_number'})

    for index, row in accounts.iterrows():
        try:
            cursor.execute('''
            INSERT INTO accounts (account_number) VALUES (?)
            ''', (row['account_number'],))
            conn.commit()
        except IntegrityError as e:
            print(f"Duplicate account entry found: {e}")

    # Retrieve account IDs
    account_id_map = pd.read_sql_query('SELECT account_id, account_number FROM accounts', conn)
    filtered_data = filtered_data.merge(account_id_map, left_on='account', right_on='account_number', how='left')

    # Select only the columns needed for insertion, including the new account_id column and excluding 'account'
    columns_to_insert = [
        'transaction_date', 'transaction_type', 'symbol', 'description', 
        'quantity', 'price', 'commission', 'amount', 'account_id'
    ]

    # Insert the filtered data into the SQLite database with error handling for duplicates
    for index, row in filtered_data[columns_to_insert].iterrows():
        try:
            cursor.execute('''
            INSERT INTO dividends (transaction_date, transaction_type, symbol, description, quantity, price, commission, amount, account_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', tuple(row))
            conn.commit()
        except IntegrityError as e:
            print(f"Duplicate dividend entry found: {e}")

    # Verify the data has been inserted
    cursor.execute('SELECT * FROM dividends LIMIT 5')
    rows = cursor.fetchall()

    # Close the connection
    conn.close()
    
    return rows

# Directory containing CSV files
csv_directory = 'data/'
db_path = 'data/dividends_dev.db'

# Create or initialize the database
create_database(db_path)

# Process each CSV file in the directory
for file_name in os.listdir(csv_directory):
    if file_name.endswith('.csv'):
        file_path = os.path.join(csv_directory, file_name)
        try:
            filtered_data = load_and_process_csv(file_path)
            rows = insert_into_db(filtered_data, db_path)
            print(f"Data from {file_path} inserted successfully:")
            print(rows)
        except ValueError as e:
            print(f"Error processing {file_path}: {e}")
