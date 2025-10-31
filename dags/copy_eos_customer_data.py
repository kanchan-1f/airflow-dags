from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery
import os
import psycopg2
import pandas as pd
import pandas_gbq as pd_gbq
import json
import requests
from datetime import date, timedelta

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'copy_eos_customer_data',
    default_args=default_args,
    description='Extract eos_customer_data table from Postgres and load into BigQuery like cloud fn',
    schedule_interval='7 0 * * *',
    catchup=False
)

# Get today's date
today = date.today()
# Yesterday date
yesterday = today - timedelta(days = 1)
print("Yesterday was: ", yesterday)
yesterday = str(yesterday)


# BigQuery credentials
PROJECT_ID = 'analytics-1f'
DATASET_ID = 'Copy_EosDB'

# PostgreSQL
PSQL_USERNAME = 'read_user'
PSQL_PASSWORD = 'ThisIsNot4u'
PSQL_HOST = '43.205.197.150'
PSQL_PORT='8791'
PSQL_DATABASE = 'EOSDB'



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
   
    #engine = create_engine(con_uri, pool_recycle=3600, schema="public").connect()
    filter_condition = f"DATE(last_updated_at) = '{yesterday}'"
    conn = psycopg2.connect(host='43.205.197.150', port='8791', dbname='EOSDB', user='read_user', password='ThisIsNot4u')
    table_id = f"{DATASET_ID}.customer_data"
    print("Loading Table {}".format(table_id))
    query = f"SELECT * FROM customer_data WHERE {filter_condition}"
    # query = f"SELECT * FROM customer_data"
    df = pd.read_sql_query(query, conn)
    #print(df.head)
    
    project_name = "analytics-1f"
    client = bigquery.Client(project=project_name)
    df= df.astype('str')
    id_list = []
    for i in df['id']:
        id_list.append(i)
    ids = tuple(id_list)
    print(ids)
    table_ref = client.dataset("Copy_EosDB", project="analytics-1f").table("customer_data")
        # Delete old records
    query = f"DELETE FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` WHERE id in {ids}"
        
    query_job = client.query(query)
    query_job.result()

    # query_count = 0
    # for id in df['id']:
       
    #     table_ref = client.dataset("TestDB", project="analytics-1f").table("test_customer_data")
    #     #Delete old records
    #     query = f"delete  FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` where id = {id}"
    #     query_job = client.query(query)
    #     query_job.result()
    #     query_count += 1
    # print(f"Total number of queries deleted: {query_count}")
    # df =df.astype(str)
       
    
   
    try:
        # Write DataFrame to BigQuery
        print(df)
    #     query_job = client.query(f"SELECT id FROM `analytics-1f.Live_eos_data.customer_data`")
    #     results = query_job.result()
    #     existing_code = [row.id for row in results]
            
    #         # check if Id is already in BigQuery
    #     df = df[~df['id'].isin(existing_code)]

        #pd_gbq.to_gbq(df, table_id, project_id=PROJECT_ID, if_exists='append', progress_bar=True)
        client = bigquery.Client()

        # Load data into BigQuery
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        # message = f"Successfully wrote {len(df)} rows to BigQuery from copy_eos_customer_data by Airflow"
        # send_notification(message)
        print(f"Successfully wrote {len(df)} rows to BigQuery")
        return "ETL successful"
 
    except Exception as e:
        message = f"Issues in copy_eos_customer_data fn in Airflow: {e}"
        send_notification(message)
        print(f"Issues occurred: {e}")
        return "ETL Failed"




copy_eos_customer_data = PythonOperator(
        task_id='copy_eos_customer_data',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

copy_eos_customer_data