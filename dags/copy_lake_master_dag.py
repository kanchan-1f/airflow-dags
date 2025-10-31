from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery
from airflow.utils.email import send_email
import os
import psycopg2
import pandas as pd
from multiprocessing import Pool
import requests
import json


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'copy_lake_master_dag',
    default_args=default_args,
    description='Extract LakeMaster data from Postgres and load into BigQuery like cloud fn',
    schedule_interval='10 0 * * *',
    catchup=False
)

# BigQuery credentials
PROJECT_ID = 'analytics-1f'
DATASET_ID = 'Copy_LakeMaster'

# PostgreSQL
PSQL_USERNAME = 'read_user'
PSQL_PASSWORD = 'ThisIsNot4u'
PSQL_HOST = '43.205.197.150'
PSQL_PORT='8791'
PSQL_DATABASE = 'LakeMaster'

def send_notification(message):
    chat_webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAuQ0M9Ko/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=1S5_gZwX1zrjjhDkGaHR-2KcJUhuIOP3E48uHTqtV-k'
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    data = {
        'text': message
    }
    response = requests.post(chat_webhook_url, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"Failed to send notification. Status code: {response.status_code}")
    else:
        print("Notification sent successfully.")

def clean_columns(df):
    df.columns = df.columns.str.replace(r'[.\s\/,:-]', '', regex=True)
    return df

def load_table(table_name):
    try:
        table_id = f'{DATASET_ID}.{table_name}'
        print(f"Loading Table {table_id}")

        # Establish a connection to the PostgreSQL database
        pg_conn = psycopg2.connect(
            host=PSQL_HOST,
            port=PSQL_PORT,
            database=PSQL_DATABASE,
            user=PSQL_USERNAME,
            password=PSQL_PASSWORD
        )

        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, pg_conn)
        df = clean_columns(df)

        # Establish a connection to BigQuery
        client = bigquery.Client()

        # Load data into BigQuery
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        print(f"Successfully Data Ingested {table_id}")

        # Close the PostgreSQL connection
        pg_conn.close()

        return table_name
    except Exception as e:
        message = f"Issues in Copy_Lake_Master fn : Error Loading Table {table_name}: {e}"
        send_notification(message)
        print(f"Error Loading Table {table_name}: {e}")
        return None

def main():
    try:
        tables = ['assets',
            'category',
            'customer_details',
            'customer_profile',
            'expenses',
            'experian_user_details',
            'customer_dependent',
            'liabilities',
            'insurance',
            'user_financial_scorecard',
            'user_ratios',
            'isdependent',
            'income',
            'mutual_funds_holdings',
            'user_verified_details']
        
        loaded_tables = []
        for table in tables:
            result = load_table(table)
            if result is not None:
                loaded_tables.append(result)

        if len(loaded_tables) == len(tables):
            # message = f"All {len(tables)} tables are loaded by Copy_Lake_Master fn in Airflow "
            # send_notification(message)
            print(f"All {len(tables)} tables are loaded")
        else:
            message = f"Only {len(loaded_tables)} out of {len(tables)} tables are loaded by Copy_Lake_Master fn in Airflow"
            send_notification(message)
            print(f"Only {len(loaded_tables)} out of {len(tables)} tables are loaded by Copy_Lake_Master fn ")

        return "ETL Executed"
    except Exception as e:
        message = f"Issues in copy_lake_master fn in Airflow: {e}"
        send_notification(message)
        print(f"Error {e}")
        return "ETL Failed"




copy_lake_master_etl = PythonOperator(
        task_id='copy_lake_master_etl',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

copy_lake_master_etl