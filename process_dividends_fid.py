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
db_path = 'data/fid-dividends.db'
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

# Create a table in the SQLite database
table_creation_query = '''
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
    account TEXT,
    UNIQUE(transaction_date, account, symbol, amount)
)
'''

cursor.execute(table_creation_query)
conn.commit()

# Select only the columns needed for insertion, excluding 'type'
columns_to_insert = [
    'transaction_date', 'account', 'transaction_type', 'symbol', 'description', 
    'quantity', 'price', 'commission', 'amount'
]

# Insert the filtered data into the SQLite database
#filtered_data.to_sql('dividends', conn, if_exists='append', index=False)
filtered_data[columns_to_insert].to_sql('dividends', conn, if_exists='append', index=False)


# Verify the data has been inserted
cursor.execute('SELECT * FROM dividends LIMIT 5')
rows = cursor.fetchall()

# Close the connection
conn.close()

print(rows)