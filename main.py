from extract import fetch_google_sheets_data
from transform import data_summary, clean_data
from load_to_snowflake import get_latest_date_and_records, filter_data, load_data
import pandas as pd

def run_pipeline():
    print("🚀 Starting data pipeline...\n")

    # Step 1: Extract data
    df = fetch_google_sheets_data()
    print("✅ Data extraction complete.\n")

    # Step 2: Clean and transform data
    cleaned_df = clean_data(df)
    data_summary(cleaned_df)
    print("✅ Data cleaning complete.\n")

    # Step 3: Load data into snowflake
    latest_date, latest_snowflake_df = get_latest_date_and_records()
    filtered_df = filter_data(cleaned_df, latest_snowflake_df, latest_date)
    data_summary(filtered_df)
    load_data(filtered_df)
    print("✅ Data loading complete.\n")

    print("🎉 Data pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()
