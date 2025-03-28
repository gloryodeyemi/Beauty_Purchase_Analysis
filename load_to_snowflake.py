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

# Snowflake connection parameters
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WH,
    database=SNOWFLAKE_DB,
    # schema=SNOWFLAKE_SCHEMA
)

# Load data into Snowflake
def load_data(df):
    conn.cursor().execute("USE ROLE beauty_role")
    conn.cursor().execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    # success, num_chunks, num_rows, output = write_pandas(conn, df, f"{SNOWFLAKE_SCHEMA}.raw_data")
    success, num_chunks, num_rows, output = write_pandas(conn, df, 'RAW_DATA')
    print(f"Success: {success}, Number of Chunks: {num_chunks}, Rows Inserted: {num_rows}")
