import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
GOOGLE_SHEET_NAME = os.getenv("SHEET_NAME")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

# Define the required scope
SCOPES = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']


# Extract data from Google Sheet
def fetch_google_sheets_data():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open(GOOGLE_SHEET_NAME).sheet1
    data = sheet.get_all_records()
    return pd.DataFrame(data)

# Get summary statistics
def data_summary(df):
    print("Data summary statistics")
    print("***********************")
    print(f"{df.head()}\n")
    print(f"{df.info()}\n")
    print(f"{df.describe()}\n")
    print(f"{df.isnull().sum()}\n")

# Convert date column to date format
def convert_date_format(df):
    df['Date_Bought'] = pd.to_datetime(df['Date_Bought'])
    print("Date format converted!")


# Convert quantity to numeric
def convert_quantity_to_numeric(df):
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
    print("Quantity data type converted!")


# Convert unit price and total_price to numeric
def convert_price_to_numeric(df):
    # Remove '$' and ',' characters
    df['Unit_Price'] = df['Unit_Price'].astype(str).str.strip().str.replace(r'[$,]', '', regex=True)
    df['Total_Price'] = df['Total_Price'].astype(str).str.strip().str.replace(r'[$,]', '', regex=True)

    # Convert to numeric, replacing errors with NaN
    df['Unit_Price'] = pd.to_numeric(df['Unit_Price'], errors='coerce')
    df['Total_Price'] = pd.to_numeric(df['Total_Price'], errors='coerce')
    print("Unit price and total price converted!")


# Check and handle missing data
def handle_missing_data(df):
    before = df.shape[0]
    # Convert empty strings to NaN
    df.replace("", pd.NA, inplace=True)

    # Drop rows where 'Product_Name' or 'Short_Name' is missing
    df.dropna(subset=['Product_Name', 'Short_Name'], inplace=True)

    # Fill missing categorical values with 'Unknown'
    categorical_columns = ['Brand', 'Store', 'Product_Type', 'Product_Category', 'Product_Purpose']
    df[categorical_columns] = df[categorical_columns].fillna('Unknown')

    # Fill missing 'Quantity' with 1
    df['Quantity'] = df['Quantity'].fillna(1).astype(int)

    # Drop rows where 'Unit_Price' is missing or 0
    df = df.dropna(subset=['Unit_Price'])
    df = df[df['Unit_Price'] > 0]

    # Fill missing 'Total_Price' using Unit_Price * Quantity
    df['Total_Price'] = df['Total_Price'].fillna(df['Unit_Price'] * df['Quantity'])

    after = df.shape[0]
    print(f"Missing data handled - {before - after} rows dropped!")
    
    return df


# Add a 'Price_Category' column based on 'Unit_Price'.
def add_price_category(df):
    def categorize(price):
        if price < 10:
            return "Low"
        elif 10 <= price < 50:
            return "Medium"
        else:
            return "High"

    df['Price_Category'] = df['Unit_Price'].apply(categorize)
    return df


# Removes duplicate data and keep first occurrence.
def remove_duplicates(df, columns=None):
    before = df.shape[0]

    df = df.drop_duplicates(keep='first').reset_index(drop=True)
    after = df.shape[0]

    print(f"Removed {before - after} duplicate rows.\n")
    return df



beauty_data = fetch_google_sheets_data()
data_summary(beauty_data)
# print(beauty_data.head())
# print(beauty_data.info())

convert_date_format(beauty_data)
convert_quantity_to_numeric(beauty_data)
convert_price_to_numeric(beauty_data)
beauty_data_cleaned = handle_missing_data(beauty_data)
beauty_data_cleaned = add_price_category(beauty_data_cleaned)
beauty_data_cleaned = remove_duplicates(beauty_data_cleaned)
# print(beauty_data_cleaned.head())
# print(beauty_data_cleaned.info())
data_summary(beauty_data)
data_summary(beauty_data_cleaned)