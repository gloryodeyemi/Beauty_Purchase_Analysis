from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.extract import fetch_google_sheets_data
from etl.transform import clean_data
from etl.loading import get_latest_date_and_records, filter_data, load_data, insert_data_into_tables, append_to_csv


# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

# The DAG
with DAG(
    "beauty_purchase_pipeline",
    default_args=default_args,
    description="Automates the ETL process for beauty purchases",
    schedule="0 0 * * 0",  # runs weekly at midnight on Sundays
    catchup=False,
) as dag:


    def extract():
        return fetch_google_sheets_data()


    def transform(ti):
        df = ti.xcom_pull(task_ids="extract")
        cleaned_df = clean_data(df)
        return cleaned_df
    
    
    def filter(ti):
        latest_date, latest_snowflake_df = get_latest_date_and_records()
        cleaned_df = ti.xcom_pull(task_ids="transform")
        filtered_df = filter_data(cleaned_df, latest_snowflake_df, latest_date)
        append_to_csv(filtered_df)
        return filtered_df
    

    def load(ti):
        filtered_df = ti.xcom_pull(task_ids="filter")
        load_data(filtered_df)


    def insert():
        insert_data_into_tables()


    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    filter_task = PythonOperator(
        task_id="filter",
        python_callable=filter,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    insert_task = PythonOperator(
    task_id="insert",
    python_callable=insert,
)

    extract_task >> transform_task >> filter_task >> load_task >> insert_task  # task dependency
