from extract import fetch_google_sheets_data
from transform import clean_data
import pandas as pd

def run_pipeline():
    print("🚀 Starting Data Pipeline...")

    # Step 1: Extract Data
    df = fetch_google_sheets_data()
    # df.to_csv("data/raw_data.csv", index=False)  # Save raw backup
    print("✅ Data Extraction Complete.")

    # Step 2: Transform Data
    df_cleaned = clean_data(df)
    # df_cleaned.to_csv("data/cleaned_data.csv", index=False)  # Save cleaned data
    print("✅ Data Cleaning Complete.")

    print("🎉 Data Pipeline Completed Successfully!")

if __name__ == "__main__":
    run_pipeline()
