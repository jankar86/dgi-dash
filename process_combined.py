import pandas as pd
import sqlite3

def load_and_process_csv(file_path):
    data = pd.read_csv(file_path)
    
    # Detect the format based on column names
    if 'Action' in data.columns:
        # Format 1 - etrade
        data.columns = [
            'run_date', 'account', 'action', 'symbol', 'description', 'type', 
            'quantity', 'price', 'commission', 'fees', 'accrued_interest', 
            'amount', 'settlement_date'
        ]
        # Filter rows where the action is "DIVIDEND RECEIVED"
        filtered_data = data[data['action'].str.contains('DIVIDEND RECEIVED', case=False, na=False)]
    elif 'TransactionType' in data.columns:
        # Format 2 fidelity
        data.columns = [
            'transaction_date', 'transaction_type', 'security_type', 'symbol', 
            'quantity', 'amount', 'price', 'commission', 'description'
        ]
        # Filter rows where the transaction_type is "Dividend"
        filtered_data = data[data['transaction_type'].str.contains('Dividend', case=False, na=False)]
    else:
        raise ValueError("Unknown CSV format")
    
    return filtered_data

def insert_into_db(filtered_data, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create a table in the SQLite database
    table_creation_query = '''
    CREATE TABLE IF NOT EXISTS dividends (
        id INTEGER PRIMARY KEY,
        transaction_date TEXT,
        transaction_type TEXT,
        security_type TEXT,
        symbol TEXT,
        quantity REAL,
        amount REAL,
        price REAL,
        commission REAL,
        description TEXT
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
    
    return rows

# Paths to your CSV files and SQLite database
csv_file_paths = ['data/etrade-dev.csv', 'data/fid-dev.csv']
db_path = 'data/dividends.db'

# Process each CSV file
for file_path in csv_file_paths:
    try:
        filtered_data = load_and_process_csv(file_path)
        rows = insert_into_db(filtered_data, db_path)
        print(f"Data from {file_path} inserted successfully:")
        print(rows)
    except ValueError as e:
        print(f"Error processing {file_path}: {e}")