from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.decorators import TaskDecorator, task, dag
from airflow.exceptions import AirflowException
import logging
from google.cloud import bigquery
import os
import sys
import pandas as pd
import numpy as np
import json
sys.path.append('/opt/bitnami/airflow/data')
from linkedin_organic_main import *
import requests
from decimal import Decimal

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')
end_date = datetime.now()
start_date = end_date - timedelta(days=400)
start_epoch = int(start_date.timestamp() * 1000)
end_epoch = int(end_date.timestamp() * 1000)

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
    dag_id = 'linkedin_followers_data',
    default_args= default_args,
    start_date= datetime(2025,7,21),
    description = 'This dag provides with the linkedin organic data from file data/linkedin_organic_data.py',
    # schedule_interval= timedelta(minutes=20),
    schedule_interval = "30 3 * * *",
    catchup= False,
    tags = ['Data engineer']
)
def all_task():
    @task(task_id = 'get_followers_data')
    def get_followers_data():
        try:
            followers = get_followers_count(start_epoch=start_epoch, end_epoch= end_epoch)
            seniority_data,industry_data, function_data, geo_data, company_size_data= mapping_of_fields()
            visitors_data = linkedin_visitors()
            # create_df(followers, *k) 
            print('data created from dag at .}/plugins:/opt/airflow/plugins')
            return followers,seniority_data,industry_data, function_data, geo_data, company_size_data, visitors_data
        except Exception as e:
            print(f'Error occured while loading the data from api \n\t\n\t {logging.error(e)}')
            send_notification(e)
            raise AirflowException(e)
    @task(task_id = 'load_2_bigquery')
    def load_2_bigquery(values):
        followers, seniority_data,industry_data, function_data, geo_data, company_size_data, visitors_data = values
        # service_acc = '/opt/airflow/atrina_auth.json'
        # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_acc
        bg_client = bigquery.Client()
        # def to_decimal(val):
        #     try:
        #         return Decimal(str(val)) if pd.notnull(val) else None
        #     except Exception as e:
        #         print(f"Conversion error on value {val}: {e}")
        #         return None
        try:
            followers_df = pd.DataFrame(followers)
            # followers_df.loc(followers_df['Start Date'] 
            # followers_df = followers_df.astype(str)
            # print(followers_df.head())
            cols = {"Organic Followers":"Organic_Followers",
                    "Sponsored Followers":"Sponsored_Followers",
                    "Auto Invited Followers":"Auto_Invited_Followers",
                    "Total Followers":"Total_Followers",
                    # "Start Date":"Start_Date",
                    "End Date":"Created_at"}
            followers_df.rename(columns=cols, inplace=True)

            visitors_df = pd.DataFrame(visitors_data)
            bg_table_name =['linkedin_new_followers', 'linkedin_visitors']
            for table in bg_table_name:
                table_id = f'analytics-1f.organic_campaign.{table}'
                query = f"SELECT MAX(Created_at) AS max_end_date FROM `{table_id}`"
                existing_dates = bg_client.query(query).result()
                existing_date = list(existing_dates)[0][0]
                if table == 'linkedin_new_followers':
                    # table_ = followers_df[followers_df['Created_at'] > existing_date]
                    table_ = followers_df[pd.to_datetime(followers_df['Created_at']) > existing_date]
                elif table == 'linkedin_visitors':
                    table_ = visitors_df[pd.to_datetime(visitors_df['Created_at']) > existing_date]

                # Convert columns: Created_at → date, others → numeric
                for col in table_.columns:
                    if col == 'Created_at':
                        table_[col] = pd.to_datetime(table_[col])
                    else:
                        table_[col] = pd.to_numeric(table_[col], errors='coerce').astype('Int64')
                        # table_[col] = table_[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)
                        # table_[col] = table_[col].map(to_decimal)
                        # table_[col] = table_[col].astype(int)

            # existing_start_date = set(rows.Start_Date for rows in existing_dates)
            # schema = [
            #     bigquery.SchemaField("Organic_Followers", 'string'),
            #     bigquery.SchemaField("Sponsored_Followers", 'string'),
            #     bigquery.SchemaField("Auto_Invited_Followers", 'string'),
            #     bigquery.SchemaField("Total_Followers", 'string'),
            #     bigquery.SchemaField("Start_Date", 'string'),
            #     bigquery.SchemaField("End_Date", 'string')
            # ]
                table_.drop_duplicates(inplace = True)
                job_config = bigquery.LoadJobConfig(autodetect = False,
                                                    write_disposition = "WRITE_append")
            # job_config.write_disposition = 'WRITE_APPEND'
                                                    # write_deposition = bigquery.write_deposition.WRITE_APPEND)
                job = bg_client.load_table_from_dataframe(table_, table_id, job_config = job_config)
                job.result()
                bg_uploaded_table = bg_client.get_table(table_id)
                print(f'The number of new rows {table_.shape[0]} and columns are {table_.shape[1]} added into {table_id} ')
                print('total {} rows and {} columns for {} table into bigquery'.format(bg_uploaded_table.num_rows, len(bg_uploaded_table.schema), table_id))
            
        except Exception as e:
            print(f'An exception occured while loading {table_id} the error is  \n\t\n\t {e}')
            send_notification(e)
            raise AirflowException(e)
        
        
        
        try:
            data_json = {"linkedin_followers_seniority":seniority_data,
                        "linkedin_followers_industry":industry_data, 
                        "linkedin_followers_job_function":function_data, 
                        "linkedin_followers_geo":geo_data, 
                        "linkedin_followers_company_size":company_size_data}
                        # "linkedin_visitors":visitors_data}
            for key,values in data_json.items():
                
                # if key == 'linkedin_visitors':
                #     json_df = pd.DataFrame(values)
                #     exclude_columns = [
                #                 'Life_unique_visitors_desktop',
                #                 'Life_unique_visitors_mobile',
                #                 'Life_unique_visitors_total'
                #             ]
                #     for columns in json_df.columns:
                #         if columns in exclude_columns:
                #             json_df[columns]= json_df[columns].astype('Int64')
                #         elif columns not in exclude_columns:
                #             json_df[columns] = json_df[columns].astype(str)
                # else: 
                json_df = pd.DataFrame(values)
                json_df = json_df.sort_values(by="total_followers", ascending = False).reset_index(drop= True)
                # json_df = json_df.astype(str)
                for col in json_df.columns:
                    if col == 'total_followers':
                        # json_df[col] = pd.to_datetime(json_df[col]).dt.date
                        json_df[col] = pd.to_numeric(json_df[col], errors='coerce').astype('Int64')
                    else:
                        # table_[col] = pd.to_numeric(json_df[col], errors='coerce')
                        json_df[col] = json_df[col].astype(str)
                # json_df = json_df.astype(str)
                job_config = bigquery.LoadJobConfig(autodetect = False,
                                                    write_disposition = "WRITE_TRUNCATE")
                # job_config.write_deposition = 'WRITE_APPEND'
                table_id = f'analytics-1f.organic_campaign.{key}'
                job = bg_client.load_table_from_dataframe(json_df, table_id, job_config = job_config)
                job.result()
                table= bg_client.get_table(table_id)
                print('loaded {} rows and {} columns for {} table into bigquery'.format(table.num_rows, len(table.schema), table_id))
        except Exception as e:
            print(f'An exception occured while loading {table_id} the error is  \n\t\n\t {e}')
            send_notification(e)
            raise AirflowException(e)

        return 'Followers data loaded successfully'

    kan =get_followers_data()
    load_2_bigquery(kan)

dag = all_task()