import requests
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql

# PostgreSQL connection details
db_config = {
    'dbname': 'concourse',
    'user': 'concourse_user',
    'password': 'concourse_pass',
    'host': '192.168.3.109',
    'port': 5432
}

# List of companies and their symbols for eodhd.com API (NSE)
companies = {
    'RELIANCE': 'RELIANCE.NSE',
    'ONGC': 'ONGC.NSE',
    'IOC': 'IOC.NSE',
    'BPCL': 'BPCL.NSE',
    'HINDPETRO': 'HINDPETRO.NSE',
    'GAIL': 'GAIL.NSE',
    'MGL': 'MGL.NSE',
    'IGL': 'IGL.NSE',
    'OIL': 'OIL.NSE',
    'PETRONET': 'PETRONET.NSE'
}

# API token
api_token = '66da7442a643d7.31571270'

# Date range
end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')

# Prepare a list to collect data
all_data = []

# Function to fetch data from eodhd.com API
def fetch_data(symbol):
    url = f'https://eodhd.com/api/eod/{symbol}?from={start_date}&to={end_date}&api_token={api_token}&fmt=json'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Failed to fetch data for {symbol}")
        return None

# Fetch and store data for each company
for company, symbol in companies.items():
    data = fetch_data(symbol)
    
    if data:
        for entry in data:
            all_data.append({
                'Company': company,
                'Date': entry.get('date'),
                'Open': entry.get('open', None),
                'High': entry.get('high', None),
                'Low': entry.get('low', None),
                'Close': entry.get('close', None),
                'Volume': entry.get('volume', None)
            })

# Connect to PostgreSQL and insert data
try:
    # Connect to the database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create table if not exists
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS api_data (
        Company VARCHAR(50),
        Date DATE,
        Open FLOAT,
        High FLOAT,
        Low FLOAT,
        Close FLOAT,
        Volume BIGINT
    );
    '''
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into the table
    insert_query = '''
    INSERT INTO api_data (Company, Date, Open, High, Low, Close, Volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    '''

    # Insert each row of data
    for record in all_data:
        cursor.execute(insert_query, (
            record['Company'],
            record['Date'],
            record['Open'],
            record['High'],
            record['Low'],
            record['Close'],
            record['Volume']
        ))

    # Commit the transaction
    conn.commit()

    print("Data has been successfully inserted into the 'api_data' table.")

except Exception as e:
    print(f"Error occurred: {e}")

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
