import pandas as pd
import sqlite3

# Load the CSV file
csv_file_path = 'data/fid-dev.csv'
data = pd.read_csv(csv_file_path)

# Rename columns to match the table schema
data.columns = [
    'transaction_date', 'account', 'transaction_type', 'symbol', 'description', 'type', 
    'quantity', 'price', 'commission', 'fees', 'accrued_interest', 
    'amount', 'settlement_date'
]

# Filter rows where the transaction_type/action is "DIVIDEND RECEIVED"
filtered_data = data[data['transaction_type'].str.contains('DIVIDEND RECEIVED', case=False, na=False)]

# Create an SQLite database
db_path = 'data/dividends.db'
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
    transaction_date TEXT,
    transaction_type TEXT,
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

# Insert unique account numbers into the accounts table
accounts = filtered_data[['account']].drop_duplicates().rename(columns={'account': 'account_number'})
accounts.to_sql('accounts', conn, if_exists='append', index=False)

# Retrieve account IDs
account_id_map = pd.read_sql_query('SELECT account_id, account_number FROM accounts', conn)
filtered_data = filtered_data.merge(account_id_map, left_on='account', right_on='account_number', how='left')

# Select only the columns needed for insertion, including the new account_id column and excluding 'account'
columns_to_insert = [
    'transaction_date', 'transaction_type', 'symbol', 'description', 
    'quantity', 'price', 'commission', 'amount', 'account_id'
]

# Insert the filtered data into the SQLite database
filtered_data[columns_to_insert].to_sql('dividends', conn, if_exists='append', index=False)

# Verify the data has been inserted
cursor.execute('SELECT * FROM dividends LIMIT 5')
rows = cursor.fetchall()

# Close the connection
conn.close()

print(rows)
