import pandas as pd

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
    df['Date_Bought'] = pd.to_datetime(df['Date_Bought'], errors='coerce').dt.date
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

    # Drop rows where 'Date_Bought' is missing
    df.dropna(subset=['Date_Bought'], inplace=True)

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

# Removes duplicate data and keeps the first occurrence.
def remove_duplicates(df, columns=None):
    before = df.shape[0]

    df = df.drop_duplicates(keep='first').reset_index(drop=True)
    after = df.shape[0]

    print(f"Removed {before - after} duplicate rows.\n")
    return df


# Generates Product_Name
def product_name_conversion(df):
    # Drop the Product_Name column
    df = df.drop(columns=['Product_Name'], errors='ignore')

    # Create a new Product_Name by concatenating 'Brand' and 'Short_Name'
    df['Product_Name'] = (df['Brand'] + ' - ' + df['Short_Name']).str.upper()

   # Drop the 'Short_Name' column
    df = df.drop(columns=['Short_Name'], errors='ignore')

    # Convert all column names to uppercase
    df.columns = [col.upper() for col in df.columns]
    print("Product_Name column created and in uppercase!\n")
    return df


# Runs all cleaning on the data
def clean_data(df):
    convert_date_format(df)
    convert_quantity_to_numeric(df)
    convert_price_to_numeric(df)
    df = handle_missing_data(df)
    df = add_price_category(df)
    df = remove_duplicates(df)
    df = product_name_conversion(df)
    # data_summary(df)
    print("Data cleaned!\n")
    return df



# Test the cleaning process
# if __name__ == "__main__":
#     beauty_data = pd.read_csv('data/raw_data.csv')  # Load raw data
#     beauty_data_cleaned = clean_data(beauty_data)
#     data_summary(beauty_data_cleaned)
#     beauty_data_cleaned.to_csv("data/cleaned_data.csv", index=False)  # Save cleaned data
#     print("Data cleaning and transformation complete!")
