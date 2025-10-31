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
# from multiprocessing import Pool
import pandas_gbq as pd_gbq
import json
import requests


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'copy_lake_master_insurance_dag',
    default_args=default_args,
    description='Extract LakeMaster Insurance data from Postgres and load into BigQuery like cloud fn',
    schedule_interval='17 0 * * *',
    catchup =False
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

def main():  

      try:
            #engine = create_engine(con_uri, pool_recycle=3600, schema="public").connect()
            conn = psycopg2.connect(host='43.205.197.150', port='8791', dbname='LakeMaster', user='read_user', password='ThisIsNot4u')
            #cursor = conn.cursor()
            tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'insurance' AND table_name in ('insurance_policy_evaluation', 'ti_mst')"

            #cursor.execute(tables_query)
            list_tables = pd.read_sql_query(tables_query, conn)
            #print(list_tables)
            tables= ['insurance_policy_evaluation', 'ti_mst']
            loaded_tables = []
            for index, row in list_tables.iterrows():
                # if row['table_name'].startswith('_') and  row['table_name'].startswith('__') :
                #       continue
                table_id = '{}.{}'.format(DATASET_ID, row['table_name'])

                print("Loading Table {}".format(table_id))
                query = "SELECT * FROM insurance.{} ".format(row['table_name'])
                df = pd.read_sql_query(query, conn)
                df.columns=df.columns.str.replace(' ',"_")
                for i in df.columns:
                    if df[i].dtypes == 'object':
                        df[i] = df[i].astype('string')
                #   df = df.astype(str)
                print(df.dtypes)
                 
                #df.to_csv(f'{table_id}.csv')
                #print("Successfully Created CSV {}".format(table_id))
                try:
                # Establish a connection to BigQuery
                    client = bigquery.Client()

                    # Load data into BigQuery
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
                    job.result()

                    print(f"Successfully Data Ingested {table_id}")

                    loaded_tables.append(row['table_name'])
                    # message = f"Successfully inserted {table_id} by Lake_Master_Insurance fn"
                    # send_notification(message)

                except Exception as e:
                    message = "Issues in Copy_Lake_Master_Insurance fn in Airflow: " + str(e)
                    send_notification(message)
                    print("Error Loading Table {}".format(table_id))

            
            if len(loaded_tables) == len(tables):
                # message = f"All {len(tables)} tables are loaded by Copy_Lake_Master_Insurance fn in Airflow "
                # send_notification(message)
                print(f"All {len(tables)} tables are loaded by Copy_Lake_Master_Insurance fn")
            else:
                message = f"Only {len(loaded_tables)} out of {len(tables)} tables are loaded by Copy_Lake_Master_Insurance fn in Airflow "
                send_notification(message)
                print(f"Only {len(loaded_tables)} out of {len(tables)} tables are loaded by Copy_Lake_Master_Insurance fn")
                

            return "ETL Executed"
           
      except Exception as e:
            message = "Issues in Copy_Lake_Master_Insurance fn in Airflow, " + str(e)
            send_notification(message)
            print("Error Loading Table {}".format(table_id))
      



copy_lake_master_insurance = PythonOperator(
        task_id='copy_lake_master_insurance',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

copy_lake_master_insurance