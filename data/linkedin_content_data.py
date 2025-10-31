import logging
from google.cloud import bigquery
from google.cloud import storage
import os
import subprocess
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO
import requests
from pathlib import Path
import sys
import json
sys.path.append('/opt/bitnami/airflow')
from data import *

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')
storage_client = storage.Client()
bucket_name = 'organic_campaign'
bucket = storage_client.bucket(bucket_name)
bg_client = bigquery.Client()
# BASE_DIR = Path(__file__).resolve().parent.parent  # points to airflow_docker/
# file_path = BASE_DIR / 'data' / "excel_29.xlsx"
# "Untitled_spreadsheet_Sheet1.csv"
# 'Untitled_spreadsheet.xlsx'
# BASE_DIR = Path("/tmp/airflow_data")
# BASE_DIR.mkdir(parents=True, exist_ok=True) 
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

def fetch_new_file_storage():
    blobs = storage_client.list_blobs(bucket)
    latest_file = None
    latest_time = None
    for blob in blobs:
        if latest_time is None or blob.updated.date() > latest_time:
            latest_time = blob.updated.date()  # if you want just the date
            latest_file = blob.name
    return latest_file,latest_time



def check_in_temp_bg(latest_file,latest_time):

    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                                    bigquery.ScalarQueryParameter("filename", "STRING",latest_file ),
                                    bigquery.ScalarQueryParameter("file_updated", "DATE",latest_time)
                                ])
    # formatted_file_updated = latest_time.strftime("%Y-%m-%d")                        )
    query = """ select filename, file_updated from
                `analytics-1f.organic_campaign.content_projection_filenames` 
                where filename = @filename and file_updated = @file_updated  """
    
    result = bg_client.query(query, job_config = job_config)
    rows = list(result)
    if rows:
        print(f'The file {latest_file} added on {latest_time} is present in the bigquery, you are up-to-date with the files')
        return None, None , False
    else:
        blobs = storage_client.list_blobs(bucket)
        file_list = []
        for blob in blobs:
            file_list.append({
                            'bucket_name': blob.bucket.name,
                            'filename': blob.name,
                            'file_size': blob.size,
                            'file_updated': blob.updated.date(),
                            'bq_append_date': pd.to_datetime('today').strftime("%Y-%m-%d")
                        })
        print(file_list)
        projection_df = pd.DataFrame(file_list)
        try:
            table_id = 'analytics-1f.organic_campaign.content_projection_filenames'
            # projection_df= projection_df.astype(str)
            job_config = bigquery.LoadJobConfig(autodetect = False, write_disposition = 'WRITE_append')
            job = bg_client.load_table_from_dataframe(projection_df, table_id, job_config = job_config)
            job.result()
        except Exception as e:
            logging.error(e)
            send_notification(e)
            raise e
        
    return latest_file, latest_time, True

   
def append_new_2_bg(latest_file, latest_time):
    print(latest_file,latest_time)
  
    blob = bucket.blob(latest_file)
    excel_data = blob.download_as_bytes()
    df = pd.read_excel(BytesIO(excel_data), sheet_name=1, skiprows=1)
    print(df.head())
    """ undo this later"""
    # df = pd.read_excel(file_path, sheet_name = 1, skiprows=1)
    # df= pd.read_csv(file_path)
    print(df.shape)
    column_name_map = {
                col: col.replace("(", "").replace(")", "").replace(" ", "_") for col in df.columns
            }
    df.rename(columns=column_name_map, inplace= True)
    df['gcs_loaded_date']= latest_time 
    # - timedelta(days=1)
    # if (datetime.now().date() - latest_time) == timedelta(days=1):
    
    try:
        today = datetime.now()
        # ninety_days_ago = today - timedelta(days=90)
        ninety_days_ago = (datetime.now() - timedelta(days=90)).date()
        df['Created_date'] = pd.to_datetime(df['Created_date']).dt.date
        
        min_date_in_data = df['Created_date'].min()
        # decide whether to filter out or not
        if min_date_in_data < ninety_days_ago:
            df = df.loc[df['Created_date']>= min_date_in_data]
        
        df['Post_title'] = df['Post_title'].fillna('vote')
        df['Post_title'] = df['Post_title'].apply(lambda x: 'vote' if str(x).strip() == '' else x)

        table_id = 'analytics-1f.organic_campaign.linkedin_content_data'
        df.fillna(0, inplace = True)
        df= df.astype(str)
        job_config = bigquery.LoadJobConfig(autodetect = False, write_disposition = 'WRITE_append')
        job = bg_client.load_table_from_dataframe(df, table_id, job_config = job_config)
        job.result()
        
        return 'data loaded successfully'
    except Exception as e:
        print(e)
        send_notification(e)
        return 'error while loading linkedin content data'