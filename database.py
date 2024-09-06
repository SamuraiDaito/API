import pandas as pd
from sqlalchemy import create_engine, text

# Database connection parameters
db_name = "concourse"
db_user = "concourse_user"
db_password = "concourse_pass"
db_host = "192.168.3.109"
db_port = "5432"

# Path to the CSV file
csv_file_path = "profit_loss_data/profit_loss_data.csv"

# Create SQLAlchemy engine
db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

def clean_data(value):
    # Remove any unwanted symbols like +, %, commas
    if isinstance(value, str):
        value = value.replace("+", "").replace("%", "").replace(",", "").strip()
        # Convert to integer or float if it's a number, otherwise return None
        if value.replace('.', '', 1).isdigit():  # Check if value is numeric
            try:
                return float(value)  # Use float to handle decimal values
            except ValueError:
                return None
        return value  # Return text as is if it's not numeric
    return value

try:
    # Read the CSV file into a DataFrame, assuming that the first row contains headers
    df = pd.read_csv(csv_file_path, thousands=',', skipinitialspace=True)

    # Remove any leading/trailing spaces from column names
    df.columns = [col.strip() for col in df.columns]

    # Clean and convert data in all columns except the first one (which is assumed to be non-numeric like years)
    for col in df.columns[1:]:
        df[col] = df[col].apply(clean_data)

    # Handle missing or inappropriate values
    df = df.fillna(0)  # Replace missing values with 0 or adjust as necessary

    # # Extract the years to update
    # years_to_update = df["Year"].tolist()

    # Create a connection to the database
    # with engine.connect() as conn:
    #     # Delete existing rows with matching years (double quotes around the column name)
    #     delete_query = text('DELETE FROM relianceprofitlost WHERE "Year" IN :years')
    #     conn.execute(delete_query, {'years': tuple(years_to_update)})

    #     # Commit the deletion operation to ensure data is cleared before new insertion
    #     conn.commit()

    # Append the new data after deleting existing rows
    df.to_sql('profit_loss_oil_gas', engine, if_exists='append', index=False)

    print("Data inserted successfully into PostgreSQL!")
except Exception as e:
    print(f"Error processing data or inserting into PostgreSQL: {e}")
