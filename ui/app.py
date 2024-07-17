# app.py
from flask import Flask, render_template, jsonify
from sqlalchemy import create_engine
import pandas as pd

app = Flask(__name__)

# Database URL (SQLite in this case)
db_url = 'sqlite:///data/dividends_dev.db'
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
    data = df.to_dict(orient='records')
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
