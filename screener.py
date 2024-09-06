import pandas as pd
import requests
from bs4 import BeautifulSoup
import os

# Load credentials from environment variables or set them directly
email = os.getenv("EMAIL")
password = os.getenv("PASSWORD")

# List of company URLs to scrape data from
company_urls = {
    "RELIANCE": "https://www.screener.in/company/RELIANCE/consolidated/",
    "ONGC": "https://www.screener.in/company/ONGC/consolidated/",
    "IOC": "https://www.screener.in/company/IOC/consolidated/",
    "BPCL": "https://www.screener.in/company/BPCL/consolidated/",
    "HINDPETRO": "https://www.screener.in/company/HINDPETRO/consolidated/",
    "GAIL": "https://www.screener.in/company/GAIL/consolidated/",
    "IGL": "https://www.screener.in/company/IGL/",
    "MGL": "https://www.screener.in/company/MGL/",
    "OIL": "https://www.screener.in/company/OIL/consolidated/",
    "PETRONET": "https://www.screener.in/company/PETRONET/"
}

# Login URL
login_url = "https://www.screener.in/login/"

# Directory to save the cleaned data
csv_dir_path = 'profit_loss_data'
os.makedirs(csv_dir_path, exist_ok=True)
csv_file_path = os.path.join(csv_dir_path, 'profit_loss_data.csv')

# Use requests to log in to Screener.in
session = requests.Session()
response = session.get(login_url)
soup = BeautifulSoup(response.text, 'html.parser')

# Extract CSRF token
csrf_token = soup.find('input', {'name': 'csrfmiddlewaretoken'})
if csrf_token:
    csrf_token = csrf_token['value']
else:
    print("CSRF token not found!")
    exit()

# Prepare login data with the extracted CSRF token
login_data = {
    "username": email,
    "password": password,
    "csrfmiddlewaretoken": csrf_token
}

# Post the login data to the form
login_response = session.post(login_url, data=login_data, headers={"Referer": login_url})

# Check if login was successful
if login_response.url == "https://www.screener.in/dash/":
    print("Login successful!")

    # Initialize an empty list to store all company data
    all_data = []

    # Loop through each company and scrape the data
    for company_name, company_url in company_urls.items():
        print(f"Scraping data for {company_name}...")
        
        # Fetch the page content
        page_response = session.get(company_url)
        page_soup = BeautifulSoup(page_response.text, 'html.parser')

        # Find the "Profit & Loss" section
        profit_loss_section = page_soup.find('h2', string="Profit & Loss")
        
        if profit_loss_section:
            table = profit_loss_section.find_next('table')
            
            if table:
                # Extract table headers (Years)
                years = [header.get_text(strip=True) for header in table.find_all('th')]

                # Extract table rows (Sales, Expenses, etc.)
                categories = []
                data = []
                rows = table.find_all('tr')
                for row in rows:
                    columns = row.find_all('td')
                    if columns:
                        # The first column is the category (Sales, Expenses, etc.)
                        category = columns[0].get_text(strip=True)
                        categories.append(category)
                        
                        # The remaining columns are data (numbers corresponding to the years)
                        row_data = [column.get_text(strip=True) for column in columns[1:]]
                        data.append(row_data)
                
                if data:
                    # Convert the list of categories and data to a DataFrame
                    df = pd.DataFrame(data, index=categories, columns=years[1:])  # Skip the first "Parameters" header
                    
                    # Transpose the DataFrame so that years become rows and categories become columns
                    df_transposed = df.transpose().reset_index()
                    df_transposed.rename(columns={'index': 'Year'}, inplace=True)
                    
                    # Add the company name to each row
                    df_transposed['Company'] = company_name
                    
                    # Ensure all data is treated as strings before replacement
                    df_transposed = df_transposed.apply(lambda x: x.map(str))
                    
                    # Clean data: Remove symbols like +, %, and , and fill NaN with 0
                    df_transposed = df_transposed.apply(lambda x: x.str.replace('[,|+|%]', '', regex=True))
                    df_transposed.fillna(0, inplace=True)

                    # Append the company's DataFrame to the list of all data
                    all_data.append(df_transposed)
                else:
                    print(f"No data found in the table for {company_name}!")
            else:
                print(f"Table not found in Profit & Loss section for {company_name}!")
        else:
            print(f"Profit & Loss section not found for {company_name}!")

    if all_data:
        # Concatenate all company data into a single DataFrame
        final_df = pd.concat(all_data, ignore_index=True)

        # Save the concatenated DataFrame to CSV
        final_df.to_csv(csv_file_path, index=False)
        print(f"Data saved to {csv_file_path}")
    else:
        print("No data to save.")
else:
    print("Login failed!")
