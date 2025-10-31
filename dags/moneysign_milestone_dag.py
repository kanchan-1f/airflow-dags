from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
from google.cloud import bigquery
import logging
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

# Initialize the logger
logger = logging.getLogger("airflow.task")

# Postgres connection details
DATABASE_NAME = "Moneysign_milestone"
USERNAME = "pguser"
PASSWORD = "R5sWDsMWc7aYHfQc"
HOST = "ec2-13-235-15-173.ap-south-1.compute.amazonaws.com"
PORT = 5432

# Initialize the BigQuery client
bq_client = bigquery.Client()

def bigquery_type_to_postgres_type(bq_type):
    """ Maps BigQuery data types to PostgreSQL data types. """
    return {
        'STRING': 'TEXT',
        'BYTES': 'BYTEA',
        'INTEGER': 'BIGINT',  # Updated from 'INT' to 'BIGINT'
        'INT64': 'BIGINT',    # Ensuring BIGINT is used for large integers
        'FLOAT': 'FLOAT8',
        'FLOAT64': 'DOUBLE PRECISION',
        'BOOLEAN': 'BOOLEAN',
        'BOOL': 'BOOLEAN',
        'TIMESTAMP': 'TIMESTAMP',
        'DATE': 'DATE',
        'TIME': 'TIME',
        'DATETIME': 'TIMESTAMP',
        'NUMERIC': 'NUMERIC',
        'BIGNUMERIC': 'NUMERIC'
    }.get(bq_type, 'TEXT')  # Default to TEXT if no match found


def get_bq_table_schema(table_name):
    """ Fetches the schema of a BigQuery table. """
    dataset_ref = bq_client.dataset("MoneySignDB")
    table_ref = dataset_ref.table(table_name)
    table = bq_client.get_table(table_ref)
    return table.schema

def extract_and_load(table_name, **kwargs):
    """ Extracts data from BigQuery and loads it into a PostgreSQL table. """
    try:
        with psycopg2.connect(dbname=DATABASE_NAME, user=USERNAME, password=PASSWORD, host=HOST) as conn:
            with conn.cursor() as cursor:
                logger.info(f"Starting to load data into {table_name}")

                # Create or replace table
                schema = get_bq_table_schema(table_name)
                columns = ", ".join([f"{field.name} {bigquery_type_to_postgres_type(field.field_type)}" for field in schema])
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
                cursor.execute(create_table_sql)

                # Truncate table
                cursor.execute(f"TRUNCATE TABLE {table_name};")

                # Extract data from BigQuery
                query = f"SELECT * FROM `MoneySignDB.{table_name}`"
                query_job = bq_client.query(query)
                results = query_job.result()

                # Insert data
                insert_query = f"INSERT INTO {table_name} ({', '.join([field.name for field in schema])}) VALUES ({', '.join(['%s'] * len(schema))});"
                batch = []
                for row in results:
                    batch.append(row.values())
                    if len(batch) >= 5000:  # Batch size can be adjusted
                        cursor.executemany(insert_query, batch)
                        batch = []
                if batch:
                    cursor.executemany(insert_query, batch)
                
                conn.commit()
                logger.info(f"Data successfully loaded into {table_name}")

    except Exception as e:
        logger.error(f"An error occurred for table {table_name}: {str(e)}")
        raise  # Ensures that Airflow marks the task as failed

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'Moneysign_milestone',
    default_args=default_args,
    description='Load data from BigQuery to PostgreSQL',
    schedule_interval='45 1 * * *',  # Runs daily at 7:15 AM
    catchup =False
)

tables = [
    'answer', 'user_moneysign', 'user_ms_data', 'user_profile', 'user_score', 'usermoneysigndata', 'internal_users', 'question'
]

tasks = {}

for table in tables:
    tasks[table] = PythonOperator(
        task_id=f'load_{table}',
        python_callable=extract_and_load,
        op_kwargs={'table_name': table},
        dag=dag,
    )

# Optional: Define dependencies if there are specific order requirements
# tasks['customer_profile'] >> tasks['user_financial_scorecard']

# # Initialize the BigQuery client
# bq_client = bigquery.Client()

# def get_bq_table_schema(table_name):
#     """ Fetches the schema of a BigQuery table. """
#     dataset_ref = bq_client.dataset("MoneySignDB")
#     table_ref = dataset_ref.table(table_name)
#     table = bq_client.get_table(table_ref)
#     return table.schema

# def extract_and_load_all_tables(**kwargs):
#     """ Extracts data from BigQuery and loads it into PostgreSQL for all specified tables. """
#     tables = [
#         'answer', 'ms_master', 'user_answer', 'user_moneysign', 'user_ms_data', 'user_profile', 'user_score', 'usermoneysigndata', 'internal_users', 'question'
#     ]
    
#     for table_name in tables:
#         try:
#             with psycopg2.connect(dbname=DATABASE_NAME, user=USERNAME, password=PASSWORD, host=HOST) as conn:
#                 with conn.cursor() as cursor:
#                     logger.info(f"Starting to load data into {table_name}")

#                     # Create or replace table
#                     schema = get_bq_table_schema(table_name)
#                     columns = ", ".join([f"{field.name} {field.field_type}" for field in schema])
#                     create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
#                     cursor.execute(create_table_sql)

#                     # Truncate table
#                     cursor.execute(f"TRUNCATE TABLE {table_name};")

#                     # Extract data from BigQuery
#                     query = f"SELECT * FROM `MoneySignDB.{table_name}`"
#                     query_job = bq_client.query(query)
#                     results = query_job.result()

#                     # Insert data
#                     insert_query = f"INSERT INTO {table_name} ({', '.join([field.name for field in schema])}) VALUES ({', '.join(['%s'] * len(schema))});"
#                     batch = []
#                     for row in results:
#                         batch.append(row.values())
#                         if len(batch) >= 100:  # Batch size can be adjusted
#                             cursor.executemany(insert_query, batch)
#                             batch = []
#                     if batch:
#                         cursor.executemany(insert_query, batch)
                    
#                     conn.commit()
#                     logger.info(f"Data successfully loaded into {table_name}")

#         except Exception as e:
#             logger.error(f"An error occurred for table {table_name}: {str(e)}")
#             raise  # Ensures that Airflow marks the task as failed

# # DAG Configuration
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 4, 23),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'catchup': False
# }

# dag = DAG(
#     'Moneysign_milestone',
#     default_args=default_args,
#     description='Load data from BigQuery to PostgreSQL for all tables in one task',
#     schedule_interval='30 1 * * *'
# )

# task_load_all_tables = PythonOperator(
#     task_id='load_all_tables',
#     python_callable=extract_and_load_all_tables,
#     dag=dag,
# )

# task_load_all_tables

