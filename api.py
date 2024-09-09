import yfinance as yf
import pandas as pd
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

# List of stock symbols
companies = {
    'Reliance': 'RELIANCE.NS',
    'ONGC': 'ONGC.NS',
    'IOC': 'IOC.NS',
    'BPCL': 'BPCL.NS',
    'HINDPETRO': 'HINDPETRO.NS',
    'GAIL': 'GAIL.NS',
    'MGL': 'MGL.NS',
    'IGL': 'IGL.NS',
    'OIL': 'OIL.NS',
    'PETRONET': 'PETRONET.NS'
}

def fetch_and_process_data(stock_symbol, company_name):
    # Download historical data
    stock_data = yf.download(stock_symbol, start="2012-04-01", end="2024-03-31", interval='1d')

    # Convert index to column
    stock_data.reset_index(inplace=True)

    # Calculate yearly statistics
    stock_data['Year'] = stock_data['Date'].dt.year
    yearly_data = stock_data.groupby('Year').agg({
        'Open': 'mean',
        'High': 'max',
        'Low': 'min',
        'Close': 'mean',
        'Volume': 'mean'
    }).reset_index()

    # Rename columns to match the requested output
    yearly_data.rename(columns={
        'Open': 'Average_Open',
        'High': 'Max_High',
        'Low': 'Min_Low',
        'Close': 'Average_Close',
        'Volume': 'Average_Volume'
    }, inplace=True)
    
    # Add company name column
    yearly_data['Company'] = company_name

    # Round data to 2 decimal places
    yearly_data = yearly_data.round(2)

    return yearly_data

def insert_data_into_db(data):
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Create table if not exists
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS api_ohlc_data (
            Company VARCHAR(50),
            Year INT,
            Average_Open FLOAT,
            Max_High FLOAT,
            Min_Low FLOAT,
            Average_Close FLOAT,
            Average_Volume FLOAT
        );
        '''
        cursor.execute(create_table_query)
        conn.commit()

        # Insert data into the table
        insert_query = '''
        INSERT INTO api_ohlc_data (Company, Year, Average_Open, Max_High, Min_Low, Average_Close, Average_Volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        '''

        # Insert each row of data
        for _, row in data.iterrows():
            cursor.execute(insert_query, (
                row['Company'],
                row['Year'],
                row['Average_Open'],
                row['Max_High'],
                row['Min_Low'],
                row['Average_Close'],
                row['Average_Volume']
            ))

        # Commit the transaction
        conn.commit()

        print("Data has been successfully inserted into the 'api_ohlc_data' table.")

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Process data for all companies
all_data = pd.DataFrame()

for company_name, symbol in companies.items():
    company_data = fetch_and_process_data(symbol, company_name)
    all_data = pd.concat([all_data, company_data], ignore_index=True)

# Insert data into PostgreSQL
insert_data_into_db(all_data)
