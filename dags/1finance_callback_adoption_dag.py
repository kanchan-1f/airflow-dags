from datetime import date
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import os
import psycopg2
import json
import pandas
import requests


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    '1finance_callback_adoption_dag',
    default_args=default_args,
    description='1finance_callback_adoption data',
    schedule_interval='10 4 * * *',
    catchup=False
)

today = date.today()
# Yesterday date
yesterday = today - timedelta(days = 1)

client = bigquery.Client()

def send_notification(message):
    chat_webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAmtOc65Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EQRD33KYG-xWnOeIAwj-WJoY8EqL4wa_vgMYOd6iqq8'
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
        # Query for Website Adoption
        adoption = """
        with cte AS (SELECT a.created_at,SPLIT(response, ',')[SAFE_OFFSET(1)] AS Name,SPLIT(response, ',')[SAFE_OFFSET(0)] AS Mobile, a.request, a.customer_code, b.Customer_name FROM `analytics-1f.Live_eos_data.customer_data` a LEFT JOIN `analytics-1f.MasterDB.1_source_t-1` b  
        ON a.customer_code = b.customer_ID
        where category_id = '151' and request = 'Website - Adoption'
        and Date(created_at) = DATE_SUB(CURRENT_DATE(), interval 1 DAY)
        and b.Internal_Test_Tagging is null
        and Customer_name is null
        ) SELECT
        COUNT(DISTINCT(Mobile)) AS count, 
        FROM cte
        where Name not LIKE '%test%' AND Name not LIKE '%TEST%' AND Name not LIKE '%Test%' AND Name not LIKE '%dvvfd%' AND Name not LIKE '%website%'
        """

        query_job = client.query(adoption)

        web_adoption= query_job.to_dataframe().iloc[0, 0]

        # Query for Website ITR

        itr = """
        with cte AS (SELECT a.created_at,SPLIT(response, ',')[SAFE_OFFSET(1)] AS Name,SPLIT(response, ',')[SAFE_OFFSET(0)] AS Mobile, a.request, a.customer_code, b.Customer_name FROM `analytics-1f.Live_eos_data.customer_data` a LEFT JOIN `analytics-1f.MasterDB.1_source_t-1` b  
        ON a.customer_code = b.customer_ID
        where category_id = '151' and request = 'Website - ITR'
        and Date(created_at) = DATE_SUB(CURRENT_DATE(), interval 1 DAY)
        and b.Internal_Test_Tagging is null
        and Customer_name is null
        ) SELECT
        COUNT(DISTINCT(Mobile)) AS count, 
        FROM cte
        where Name not LIKE '%test%' AND Name not LIKE '%TEST%' AND Name not LIKE '%Test%' AND Name not LIKE '%dvvfd%' AND Name not LIKE '%website%'
        """
        query_job = client.query(itr)

        web_itr = query_job.to_dataframe().iloc[0, 0]

    
        print(f"{yesterday}\n------------------------------\nCallback Request Data\n------------------------------\nWebsite Adoption = {web_adoption}\nWebsite ITR = {web_itr}")

        message = f"{yesterday}\n------------------------------\nCallback Request Data\n------------------------------\nWebsite Adoption = {web_adoption}\nWebsite ITR = {web_itr}"
        send_notification(message)


    except Exception as e:
        print(f"An Error Occurred : {e}")
        message = f"An Error Occurred : {e}"
        send_notification(message)

    return "Adoption ETL Executed!"




callback_adoption_etl = PythonOperator(
        task_id='callback_adoption_etl',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

callback_adoption_etl