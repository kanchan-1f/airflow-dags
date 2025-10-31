from airflow.models import DAG
from airflow.decorators import dag,task
from datetime import datetime, timedelta
from google.cloud import secretmanager 
from google.oauth2 import  service_account
from google.cloud import bigquery
from airflow.exceptions import AirflowException
import psycopg2
import logging
import os
import json
import pandas as pd
import requests

project_id = '430695588957'
version = '1'
project_name = 'analytics-1f'
dataset = 'LakeMaster'
table_name= 'user_financial_scorecard'
table_id = f'{project_name}.{dataset}.{table_name}'
pg_table = 'user_financial_scorecard'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

def send_notification(message):
    chat_webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAmtOc65Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EQRD33KYG-xWnOeIAwj-WJoY8EqL4wa_vgMYOd6iqq8'
    # chat_webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAzGKojWU/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=TNZXIHhFQtGYRvW1yBcKPFyQoS0mMWOqgEiQtQPGTb4'
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    data = {
        'text': message
    }
    response = requests.post(chat_webhook_url, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"Failed to send notification. Status code: {response.status_code}")
    else:
        print("Notification sent successfully.")

default_args = {
    'retries':1,
    'retry_delay' : timedelta(seconds= 60),
    'owner': 'airflow'
}

with DAG(dag_id ='user_financial_data_sat_dag',
      description= 'Truncate and load data from customer data from postgres into bigquery', 
      schedule='0 12 * * 6',
      start_date=datetime(2025,7,7),
      catchup= False, 
      default_args=default_args
) as dag:
    @task(task_id = 'load_the_secret_from_gcp')
    def data_load():
    
        try:
            project_id = '430695588957'
            secret_name= 'psql_new_read_sec'
            client = secretmanager.SecretManagerServiceClient()
            parent = f"projects/{project_id}"
            # list the number of secret credentials that we have.
            for secret in client.list_secrets(request={"parent": parent}):
                print(f"Found secret: {secret.name}")
            # fetch the desired secret credential 
            secret_name_ = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
            response = client.access_secret_version(name=secret_name_)
            payload = response.payload.data.decode('UTF-8') 
            secrets = json.loads(payload)
            return secrets
        except Exception as e:
            logging.error(e)
            raise AirflowException(e)

    
    @task(task_id = 'fetch_data_from_postgres_and_load_2_bigquery')
    def pg_data_fetch(payload):

        secret= payload
        print(f'this is a secret')
        print(type(secret))  # this is to verify the the data which we are getting is in the form of dictionary
        PSQL_HOST =secret['host']
        PSQL_PORT = secret['port']
        PSQL_USERNAME = secret['username']
        PSQL_PASSWORD= secret['password']
        PSQL_DATABASE = 'LakeMaster'
        print(PSQL_HOST)
        print(f'please verify weather the data is correct or not')

        try: 
            conn = psycopg2.connect(host=PSQL_HOST, port=PSQL_PORT, dbname=PSQL_DATABASE, user=PSQL_USERNAME, password=PSQL_PASSWORD)
            print(f'printing the conn {conn}')
        except Exception as e:
            print('the connection is not there, that means it is not able to connect to postgres')
            logging.error(e)
            raise AirflowException(e)
        else:
            try:
                query = f'select * from {pg_table}'
                df = pd.read_sql_query(query,conn)
                print(df.shape)
                #df = df.astype(str)    #making all the columns to a string to easy load
                print(f'the data is being read the converted to a dataframe')
                try:
                    BATCH_SIZE = 50000
                    bg_client = bigquery.Client(project=project_id)
                    job_config = bigquery.LoadJobConfig()
                    write_mode = "WRITE_TRUNCATE"
                    for i, chunk in enumerate(range(0, len(df), BATCH_SIZE)):
                        df_chunk = df.iloc[chunk:chunk + BATCH_SIZE]
                        if df_chunk.empty:
                            break

                        job_config = bigquery.LoadJobConfig(
                            write_disposition=write_mode
                        )

                        job = bg_client.load_table_from_dataframe(df_chunk, table_id, job_config=job_config)
                        job.result()
                        print(f"✅ Batch {i+1}: Loaded {len(df_chunk)} rows")

                        write_mode = "WRITE_APPEND"
                    table = bg_client.get_table(table_id)  # Make an API request.
                    print(
                        "Loaded {} rows and {} columns to {}".format(
                            table.num_rows, len(table.schema), table_id
                        )
                    )
                    return 'ETL successful'
                except Exception as e:
                    logging.error(e)
                    print('etl to bigquery failed')
                    raise Exception()
                return df
            except Exception as e:
                print('error while reading the data from postgres')
                logging.error(e)
                raise Exception()

    get_secret = data_load()
    pg_2_bg = pg_data_fetch(get_secret)

    get_secret >> pg_2_bg
