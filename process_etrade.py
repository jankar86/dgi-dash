import pandas as pd
import sqlite3

# Load the new CSV file
new_csv_file_path = 'data/etrade-9153.csv'
#new_data = pd.read_csv(new_csv_file_path)

# Display the first few rows of the dataframe to understand its structure
#print(new_data.head())

with open(new_csv_file_path, 'r') as file:
        account_info = file.readline().strip()
    
# Extract account number from the account_info string
account_number = account_info.split(',')[-1].strip()
print(account_number)
# Read the rest of the CSV file, skipping the first row
data = pd.read_csv(new_csv_file_path, skiprows=2)
print(data.head())

# Rename columns to match the table schema (adjust the column names based on the new CSV structure)
data.columns = [
    'TransactionDate', 'TransactionType', 'SecurityType', 'Symbol', 'Quantity', 'Amount', 
    'Price', 'Commission', 'Description'
]

# Filter rows where the action is "DIVIDEND RECEIVED"
filtered_data = data[data['TransactionType'].str.contains('Dividend', case=False, na=False)]

# Add an empty 'account' column to match the database schema
filtered_data['account'] = account_number

# Create an SQLite database
db_path = 'data/dividends_etrade.db'
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
filtered_data.to_sql('Dividend', conn, if_exists='append', index=False)

# Verify the data has been inserted
cursor.execute('SELECT * FROM Dividend LIMIT 25')
rows = cursor.fetchall()

# Close the connection
conn.close()

print(rows)
