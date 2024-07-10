import sqlite3
from sqlite3 import IntegrityError

def create_database(db_path):
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
    conn.close()

def get_db_connection(db_path):
    return sqlite3.connect(db_path)
