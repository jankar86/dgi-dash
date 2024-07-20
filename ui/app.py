from flask import Flask, render_template, jsonify
from sqlalchemy import create_engine
import pandas as pd
import os

app = Flask(__name__)

# Get the absolute path to the database file
db_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '../data/dividends_dev.db')
db_url = f'sqlite:///{db_path}'
engine = create_engine(db_url)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    query = """
    SELECT transaction_date, symbol, amount
    FROM dividends
    """
    df = pd.read_sql(query, engine)
    
    # Parse the date and extract year and month
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

    # Debugging: Print the rows where the date parsing failed
    nat_rows = df[df['transaction_date'].isna()]
    if not nat_rows.empty:
        print("Failed to parse dates for the following rows:")
        print(nat_rows[['transaction_date']].head(10))  # Print the first 10 problematic rows

    df['year'] = df['transaction_date'].dt.year
    df['month'] = df['transaction_date'].dt.to_period('M')

    # Filter out rows with NaT dates
    df = df.dropna(subset=['transaction_date'])

    # Calculate dividends per year
    dividends_per_year = df.groupby('year')['amount'].sum().reset_index()

    # Calculate dividends per month
    dividends_per_month = df.groupby(['year', 'month'])['amount'].sum().reset_index()
    dividends_per_month['month'] = dividends_per_month['month'].astype(str)

    # Calculate total dividends per symbol for each year
    dividends_per_symbol = df.groupby(['year', 'symbol'])['amount'].sum().reset_index()

    data = {
        'dividends_per_year': dividends_per_year.to_dict(orient='records'),
        'dividends_per_month': dividends_per_month.to_dict(orient='records'),
        'dividends_per_symbol': dividends_per_symbol.to_dict(orient='records')
    }
    
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
