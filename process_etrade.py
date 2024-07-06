import pandas as pd
import sqlite3

# Load the new CSV file
new_csv_file_path = 'data/etrade-dev.csv'
new_data = pd.read_csv(new_csv_file_path)

# Display the first few rows of the dataframe to understand its structure
print(new_data.head())

# Rename columns to match the table schema (adjust the column names based on the new CSV structure)
new_data.columns = [
    'run_date', 'account', 'action', 'symbol', 'description', 'type', 
    'quantity', 'price', 'commission', 'fees', 'accrued_interest', 
    'amount', 'settlement_date'
]

# Filter rows where the action is "DIVIDEND RECEIVED"
filtered_data = new_data[new_data['action'].str.contains('DIVIDEND RECEIVED', case=False, na=False)]

# Create an SQLite database
db_path = 'path_to_your_file/dividends.db'
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
