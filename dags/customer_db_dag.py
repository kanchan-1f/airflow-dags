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
import pandas_gbq as pd_gbq



os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'customer_db_dag',
    default_args=default_args,
    description='Extract LakeMaster data from Postgres and load into BigQuery like cloud fn',
    schedule_interval='0 0 * * *',
    catchup=False
)

# BigQuery credentials
PROJECT_ID = 'analytics-1f'
DATASET_ID = 'Customer_Airflow'

# PostgreSQL
PSQL_USERNAME = 'read_user'
PSQL_PASSWORD = 'ThisIsNot4u'
PSQL_HOST = '43.205.197.150'
PSQL_PORT='8791'
PSQL_DATABASE = 'CustomerDB'


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
        print(f"Error Loading Table {table_name}: {e}")
        return None

def main():
    try:
        tables = [
            "_prisma_migrations",
            "products",
            "customer_fcm_tokens",
            "otps",
            "consents",
            "email_communication",
            "customer_app_instance_ids",
            "waitlist",
            "customers",
            "meeting_booking",
            "coupon_code",
            "appointment_booking",
            "salesforce_log",
            "user_story",
            "app_updates",
            "whatsapp_communication",
            "meeting_link",
            "app_version_feature",
            "push_notification_sender",
            "fcm_result",
            "push_notification_master",
            "membership_details",
            "qfa_meeting",
            "delete_otps",
            "deleted_customers_log",
            "log",
            "recommended_action_plan",
            "waitlisted_customer"
        ]

        loaded_tables = []
        for table in tables:
            result = load_table(table)
            if result is not None:
                loaded_tables.append(result)

        if len(loaded_tables) == len(tables):
            print(f"All {len(tables)} tables are loaded")
        else:
            print(f"Only {len(loaded_tables)} out of {len(tables)} tables are loaded")

        return "ETL Executed"
    except Exception as e:
        print(f"Error {e}")
        return "ETL Failed"


customer_db_etl = PythonOperator(
        task_id='customer_db_etl',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

customer_db_etl