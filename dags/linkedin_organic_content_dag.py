from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.exceptions import AirflowException
import logging
from google.cloud import bigquery
from google.cloud import storage
import os
import sys
import requests
import json
sys.path.append('/opt/bitnami/airflow/data')
from linkedin_content_data import *

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')
storage_client = storage.Client()
bg_client = bigquery.Client()

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

default_args = { 
    'owner' : 'airflow',
    'retries' : 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id = 'linkedin_content_data',
    default_args= default_args,
    start_date= datetime(2025,7,21),
    description = 'This dag provides with the linkedin content data from file data/linkedin_content_data.py',
    # schedule_interval= timedelta(minutes=20),
    schedule_interval = "30 5 * * *",
    catchup= False,
    tags = ['Data engineer']
)
def all_task():
    @task(task_id= 'get_data_from_cloud_storage')
    def get_filenames_from_storage():
        try:
            latest_file,latest_time = fetch_new_file_storage()
            return latest_file, latest_time 
        except Exception as e:
            logging.error(e)
            send_notification(e)
            raise AirflowException
        
    @task(task_id = 'file_existence_in_bg_mapping')
    def bg_mapping(values):
        try:
            latest_file, latest_time = values
            latest_filename, latest_upload_date, should_load= check_in_temp_bg(latest_file,latest_time)
            return latest_filename, latest_upload_date, should_load
        except Exception as e:
            logging.error(e)
            send_notification(e)
            raise AirflowException
    
    @task(task_id = 'load_into_the_bg')
    def load_2_bg(values):
        try:
            latest_filename, latest_upload_date, should_load = values
            if not should_load:
                print("File already processed. Skipping upload.")
                return  # Exit early — no reload
            else:
                append_new_2_bg(latest_filename,latest_upload_date)
        except Exception as e:
            logging.error(e)
            send_notification(e)
            raise AirflowException
   
    get_files = get_filenames_from_storage()
    get_mapping = bg_mapping(get_files)
    load_2_bg(get_mapping)

dag = all_task()
