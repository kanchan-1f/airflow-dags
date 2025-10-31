from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery,secretmanager
import os
import psycopg2
import pandas as pd
import os
import json
import requests
from google.cloud import bigquery
from datetime import datetime, timedelta


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'user_answer',
    default_args=default_args,
    description='Extract user_answer data from Postgres and load into BigQuery like cloud fn',
    schedule_interval='30 1 * * *',
    catchup = False
)
# BigQuery credentials
PROJECT_ID = 'analytics-1f'
DATASET_ID = 'MoneySignDB'
PSQL_USERNAME = 'read_user'
PSQL_PASSWORD = "J'k%S$uf4M;y3#"
PSQL_HOST = '43.204.206.86'
PSQL_PORT = '8791'
PSQL_DATABASE = 'MoneySignDB'

# Get today's date
# today = date.today()

# Yesterday date
# yesterday = today - timedelta(days = 1)
# print("Yesterday was: ", yesterday)
# yesterday = str(yesterday)
# print(type(yesterday))

yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
filter_condition = f"DATE(created_date) = DATE '{yesterday}'"
print(filter_condition)

# def get_secret(secret_name, version_id='1'):
#     project_id = os.getenv('GCP_PROJECT', '430695588957') 
#     client = secretmanager.SecretManagerServiceClient()
#     secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/{version_id}"
#     response = client.access_secret_version(request={"name": secret_path})
#     secret_data = response.payload.data.decode("UTF-8")
#     return json.loads(secret_data) 

# psql_creds = get_secret("psql_creds") 
# PSQL_USERNAME = psql_creds["username"]
# PSQL_PASSWORD = psql_creds["password"]
# PSQL_HOST = psql_creds["host"]
# PSQL_PORT = psql_creds["port"]

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

def main():

    try:
        #engine = create_engine(con_uri, pool_recycle=3600, schema="public").connect()
        conn = psycopg2.connect(
            host=PSQL_HOST,
            port=PSQL_PORT,
            database=PSQL_DATABASE,
            user=PSQL_USERNAME,
            password=PSQL_PASSWORD
        )

        #cursor = conn.cursor()
        # tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'customer_data'"
        # #cursor.execute(tables_query)
        # list_tables = pd.read_sql_query(tables_query, conn)
        # print(list_tables)
        # for index, row in list_tables.iterrows():
        table_id = f"{DATASET_ID}.user_answer"
        print("Loading Table {}".format(table_id))
        query = f"SELECT * FROM user_answer where {filter_condition}"
        df = pd.read_sql_query(query, conn)
        print(df.head)
        # for i in df.columns:
        #     if df[i].dtypes == 'object':
        #         df[i] = df[i].astype('string')
        try:
             # Establish a connection to BigQuery
            client = bigquery.Client()
            # Load data into BigQuery
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            print(f"Successfully ingested {len(df)} rows in {table_id} by Airflow")
            # message = f"Successfully ingested {len(df)} rows in {table_id} by Airflow"
            # send_notification(message)
        except Exception as e:
            print("Error Loading Table {}".format(table_id))
            message = "Issues in user_answer fn in Airflow: " + str(e)
            send_notification(message)

        return "ETL Successful"
    except Exception as e:
        print("Error {}".format(e))
        message = "Issues in user_answer fn in Airflow: " + str(e)
        send_notification(message)
        return "ETL Failed"
    
user_answer = PythonOperator(
        task_id='user_answer',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

user_answer

