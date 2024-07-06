import pandas as pd
import sqlite3

# Load the CSV file
#csv_file_path = 'data/fid-dev.csv'
csv_file_path = 'data/etrade-dev.csv'
data = pd.read_csv(csv_file_path)

# Rename columns to match the table schema
data.columns = [
    'run_date', 'account', 'action', 'symbol', 'description', 'type', 
    'quantity', 'price', 'commission', 'fees', 'accrued_interest', 
    'amount', 'settlement_date'
]

# Filter rows where the action is "DIVIDEND RECEIVED"
filtered_data = data[data['action'].str.contains('DIVIDEND RECEIVED', case=False, na=False)]

# Create an SQLite database
db_path = 'data/dividends.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create a table in the SQLite database
table_creation_query = '''
CREATE TABLE IF NOT EXISTS dividends (
    id INTEGER PRIMARY KEY,
    run_date TEXT,
    account TEXT,
    action TEXT,
    symbol TEXT,
    description TEXT,
    type TEXT,
    quantity REAL,
    price REAL,
    commission REAL,
    fees REAL,
    accrued_interest REAL,
    amount REAL,
    settlement_date TEXT
)
'''
cursor.execute(table_creation_query)
conn.commit()

# Insert the filtered data into the SQLite database
filtered_data.to_sql('dividends', conn, if_exists='append', index=False)

# Verify the data has been inserted
cursor.execute('SELECT * FROM dividends LIMIT 5')
rows = cursor.fetchall()

# Close the connection
conn.close()

print(rows)
