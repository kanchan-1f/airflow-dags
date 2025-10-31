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
import sys


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    '1finance_fwp_dag',
    default_args=default_args,
    description='1finance_fwp_dag data',
    schedule_interval='20 4 * * *',
    catchup=False
)

today = date.today()
# Yesterday date
yesterday = today - timedelta(days = 1)

client = bigquery.Client()

def send_notification(message):
    # chat_webhook_ng = 'https://chat.googleapis.com/v1/spaces/AAAAmtOc65Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EQRD33KYG-xWnOeIAwj-WJoY8EqL4wa_vgMYOd6iqq8'
    # chat_webhook_test = 'https://chat.googleapis.com/v1/spaces/AAAAzGKojWU/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=TNZXIHhFQtGYRvW1yBcKPFyQoS0mMWOqgEiQtQPGTb4'
    chat_webhook_adoption = 'https://chat.googleapis.com/v1/spaces/AAAAWezURKM/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=WAHyjSk0u4Ejvpo3TeqittzEjkUAvASoeeKdY3flYvY'    
   
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    data = {
        'text': message
    }
    response = requests.post(chat_webhook_adoption, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"Failed to send notification. Status code: {response.status_code}")
    else:
        print("Notification sent successfully.")


def send_notification2(message):
    # Define Google Chat webhook URL
    # chat_webhook_adoption = 'https://chat.googleapis.com/v1/spaces/AAAAmtOc65Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EQRD33KYG-xWnOeIAwj-WJoY8EqL4wa_vgMYOd6iqq8'
    chat_webhook_test = 'https://chat.googleapis.com/v1/spaces/AAAAzGKojWU/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=TNZXIHhFQtGYRvW1yBcKPFyQoS0mMWOqgEiQtQPGTb4'
    # chat_webhook_ng = 'https://chat.googleapis.com/v1/spaces/AAAAWezURKM/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=WAHyjSk0u4Ejvpo3TeqittzEjkUAvASoeeKdY3flYvY'    

    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    data = {
        "text": f"<users/payal.soni@1finance.co.in> {message}",  # Tag Payal Soni in the message text
        "cards": [
            {
                "header": {
                    "title": "Error Notification",
                    "subtitle": "An issue occurred while executing a query",
                },
                "sections": [
                    {
                        "widgets": [
                            {
                                "textParagraph": {"text": f"<users/payal.soni@1finance.co.in> {message}"}
                            }
                        ]
                    }
                ]
            }
        ]
    }

    # Send the request to Google Chat
    response = requests.post(chat_webhook_test, headers=headers, json=data)
    if response.status_code != 200:
        print(f"Failed to send notification. Status code: {response.status_code}")
        print(f"Response: {response.text}")
    else:
        print("Notification sent successfully.")




def main():
    try:
        # Function to execute a BigQuery query and return a single result, handling errors
        def fetch_single_result(query, query_name):
            """Executes a BigQuery query and returns a single result, handling errors."""
            try:
                client = bigquery.Client()
                query_job = client.query(query)
                result = query_job.result().to_dataframe()

                if result.empty:
                    warning_msg = f"Warning: Query '{query_name}' returned no data in doculocker_etl."
                    print(warning_msg)
                    send_notification2(warning_msg)
                    return "NA"
                
                return result.iloc[0][0]
            except Exception as e:
                error_msg = f"<users/payal.soni@1finance.co.in> Error executing query '{query_name}': {e} in doculocker_etl"
                print(error_msg)
                send_notification2(error_msg)
                return "NA"


        query_for_fwp_sent = """
            select count(distinct customer_ID) from `MasterDB.1_source_t-1`
            where FWP_Sent_Date is not null
            and date(FWP_Sent_Date) = DATE_SUB(CURRENT_DATE(), interval 1 DAY)
            and Internal_Test_Tagging is null
        """

        query_for_mtd_figures = """
            SELECT count( DISTINCT customer_ID )
            FROM `MasterDB.1_source_t-1`
            WHERE FWP_Sent_Date IS NOT NULL
            AND DATE(FWP_Sent_Date) >= DATE_TRUNC(CURRENT_DATE(), MONTH) 
            AND DATE(FWP_Sent_Date) < CURRENT_DATE()  
            AND Internal_Test_Tagging IS NULL;
        """
 

        # Fetch data for each metric with error handling
        fwp_sent = fetch_single_result(query_for_fwp_sent, "FWP_Sent")
        mtd_figures = fetch_single_result(query_for_mtd_figures, "MTD_Figures")


        # Generate the message without extra spaces
        message = (
            f"{yesterday}\n"
            "---------------------\n"
            f"FWP Sent - {fwp_sent}\n"
            "---------------------\n"
            "MTD Figures\n" 
            f"FWP Sent - {mtd_figures}"
        )

        # Print the formatted message
        print(message)

        send_notification(message)


    except Exception as e:
        print(f"An Error Occurred in doculocker_etl: {e}")
        message = f"<users/payal.soni@1finance.co.in> An Error Occurred in doculocker_etl : {e}"
        send_notification2(message)

    return "1finance_fwp ETL Executed!"




fwp_etl = PythonOperator(
        task_id='fwp_etl',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

fwp_etl