from extract import fetch_google_sheets_data
from cleaning import clean_data
from load_to_snowflake import load_data
import pandas as pd

def run_pipeline():
    print("🚀 Starting data pipeline...\n")

    # Step 1: Extract data
    df = fetch_google_sheets_data()
    print("✅ Data extraction complete.\n")

    # Step 2: Clean and transform data
    df_cleaned = clean_data(df)
    print("✅ Data cleaning complete.\n")

    # Step 3: Load data into snowflake
    load_data(df_cleaned)
    print("✅ Data loading complete.\n")

    print("🎉 Data pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()
