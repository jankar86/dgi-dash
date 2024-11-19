import pandas as pd
import sqlite3
from sqlite3 import IntegrityError
from datetime import datetime

# Load the CSV file
csv_file_path = 'data/fid-dev.csv'
data = pd.read_csv(csv_file_path)

# Rename columns to match the table schema
data.columns = [
    'transaction_date', 'account', 'transaction_type', 'symbol', 'description', 'type', 
    'quantity', 'price', 'commission', 'fees', 'accrued_interest', 
    'amount', 'settlement_date'
]

# Ensure transaction_date is in a consistent format

# Convert transaction_date to a four-digit year format
data['transaction_date'] = pd.to_datetime(data['transaction_date'], errors='coerce').dt.strftime('%m/%d/%Y')

# Filter rows where the transaction_type/action is "DIVIDEND RECEIVED"
filtered_data = data[data['transaction_type'].str.contains('DIVIDEND RECEIVED', case=False, na=False)]

# Create an SQLite database
db_path = 'data/dividends_dev.db'
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
conn.commit()

# Create the dividends table with a foreign key reference to the accounts table
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
    FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    UNIQUE(transaction_date, account_id, symbol, amount)
)
'''

cursor.execute(create_dividends_table_query)
conn.commit()

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

print(rows)
