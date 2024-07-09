import pandas as pd
import sqlite3
import os
from datetime import datetime

def load_and_process_csv(file_path):
    # Read the first row to get the account number for specific format
    with open(file_path, 'r') as file:
        first_row = file.readline().strip()
    
    # Determine the format based on the first row # If Action detected in first row Account_number doesn't need to be added
    if 'Action' in first_row:
        print("'Action' detected for Fid CSV type - Unsupported function for now!")
        # Do not skip the first row
        data = pd.read_csv(file_path)

        # Format 1 - Fidelity

        # Add logic here

        # Filter rows where the action is "DIVIDEND RECEIVED"
        #filtered_data = data[data['action'].str.contains('DIVIDEND RECEIVED', case=False, na=False)]
        
    
    elif 'Account' in first_row:
        # Format 2 - Etrade CSV need to read first line to get account number data.

        # Skip the first row and read the rest of the CSV file
        data = pd.read_csv(file_path, skiprows=1)
        # Extract account number from the account_info string if applicable
        account_number = first_row.split(',')[-1].strip()
        
        
        data.columns = [
            'transaction_date', 'transaction_type', 'security_type', 'symbol', 
            'quantity', 'amount', 'price', 'commission', 'description'
        ]
        # Convert transaction_date to a four-digit year format
        data['transaction_date'] = pd.to_datetime(data['transaction_date'], errors='coerce').dt.strftime('%m/%d/%Y')
        
        # Filter rows where the transaction_type is "Dividend"
        filtered_data = data[data['transaction_type'].str.contains('Dividend', case=False, na=False)]
    
    else:
        raise ValueError("Unknown CSV format")
    
    # Append account number to each row
    filtered_data['account_number'] = account_number
    
    return filtered_data

def insert_into_db(filtered_data, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables in the SQLite database
    create_accounts_table_query = '''
    CREATE TABLE IF NOT EXISTS accounts (
        account_id INTEGER PRIMARY KEY,
        account_number TEXT UNIQUE
    )
    '''
    cursor.execute(create_accounts_table_query)

    create_dividends_table_query = '''
    CREATE TABLE IF NOT EXISTS dividends (
        id INTEGER PRIMARY KEY,
        transaction_date DATE,
        transaction_type TEXT,
        security_type TEXT,
        symbol TEXT,
        quantity REAL,
        amount REAL,
        price REAL,
        commission REAL,
        description TEXT,
        account_id INTEGER,
        FOREIGN KEY(account_id) REFERENCES accounts(account_id),
        UNIQUE(transaction_date, transaction_type, symbol, amount, account_id)
    )
    '''
    cursor.execute(create_dividends_table_query)
    conn.commit()
    
    # Insert account number into accounts table
    account_number = filtered_data['account_number'].iloc[0]
    cursor.execute('INSERT OR IGNORE INTO accounts (account_number) VALUES (?)', (account_number,))
    conn.commit()
    
    # Get the account_id
    cursor.execute('SELECT account_id FROM accounts WHERE account_number = ?', (account_number,))
    account_id = cursor.fetchone()[0]
    
    # Prepare the filtered data for insertion
    filtered_data = filtered_data.drop(columns=['account_number'])
    filtered_data['account_id'] = account_id
    
    # Insert the filtered data into the SQLite database
    for _, row in filtered_data.iterrows():
        try:
            insert_query = '''
            INSERT INTO dividends (transaction_date, transaction_type, security_type, symbol, quantity, amount, price, commission, description, account_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            cursor.execute(insert_query, tuple(row))
        except sqlite3.IntegrityError:
            # Skip the row if it violates the UNIQUE constraint
            print(f"Skipping duplicate row: {row.to_dict()}")
    
    conn.commit()
    
    # Verify the data has been inserted
    cursor.execute('SELECT * FROM dividends LIMIT 5')
    rows = cursor.fetchall()
    
    # Close the connection
    conn.close()
    
    return rows

# Directory containing CSV files
csv_directory = 'data/'
#db_path = 'data/etrade-dividends.db'
db_path = 'data/dividends_dev.db'

# Process each CSV file in the directory
for file_name in os.listdir(csv_directory):
    if file_name.startswith('etrade-'):
        file_path = os.path.join(csv_directory, file_name)
        try:
            filtered_data = load_and_process_csv(file_path)
            rows = insert_into_db(filtered_data, db_path)
            print(f"Data from {file_path} inserted successfully:")
            print(rows)
        except ValueError as e:
            print(f"Error processing {file_path}: {e}")