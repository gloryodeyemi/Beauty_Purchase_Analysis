import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
SNOWFLAKE_USER=os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD=os.getenv("SNOWFLAKE_PASS")
SNOWFLAKE_ACCOUNT=os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WH=os.getenv("SNOWFLAKE_WH")
SNOWFLAKE_DB=os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA=os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE=os.getenv("SNOWFLAKE_TB")

# Snowflake connection parameters
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WH,
    database=SNOWFLAKE_DB,
    # schema=SNOWFLAKE_SCHEMA
)

cursor = conn.cursor()
cursor.execute("USE ROLE beauty_role")
cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")


def get_latest_date_and_records():
    # Get the latest purchase date from Snowflake
    cursor.execute(f"SELECT MAX(DATE_BOUGHT) FROM {SNOWFLAKE_TABLE}")
    latest_date = cursor.fetchone()[0]
    latest_date = latest_date if latest_date else "2000-01-01"
    # print(f"Latest date: \n{latest_date}\n")
    
    # Fetch all purchase records from the latest date
    query = f"""
        SELECT * FROM {SNOWFLAKE_TABLE}
        WHERE DATE_BOUGHT = '{latest_date}'
    """
    latest_snowflake_df = pd.read_sql(query, conn)
    # print(f"Latest snowflake: \n{latest_snowflake_df.head()}\n")

    return latest_date, latest_snowflake_df


# Filter for new records only to avoid duplicate entry
def filter_data(raw_df, latest_snowflake_df, latest_date):
    if latest_date != "2000-01-01":
        df_new = raw_df[raw_df['DATE_BOUGHT'] >= latest_date]
    else:
        df_new = raw_df  # if no data exists in Snowflake, load everything
    
    # print(f"Before filtering: \n{df_new.head()}\n")

    # Check for duplicates purchase records
    df_new = df_new.merge(latest_snowflake_df, how="left", indicator=True).query('_merge == "left_only"').drop(columns=['_merge'])
    # print(f"Filtered df: \n{df_new.head()}\n")
    return df_new


# Load data into Snowflake
def load_data(df):
    success, num_chunks, num_rows, output = write_pandas(conn, df, SNOWFLAKE_TABLE)
    print(f"Success: {success}, Number of Chunks: {num_chunks}, Rows Inserted: {num_rows}")
    conn.close()
