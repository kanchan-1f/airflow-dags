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
import logging

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'advisory_payment_adoption',
    default_args=default_args,
    description='1finance advisory payment adoption dag',
    schedule_interval='0 4 * * *',
    catchup=False
)

client = bigquery.Client()

def send_notification(message):
    # chat_webhook_test = 'https://chat.googleapis.com/v1/spaces/AAQA0Qv-LWI/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=zGAVl4m-MrC_iWOxM4E7D8UyIEA33vd6P_7CsYjvCYg'
    chat_webhook_adoption = 'https://chat.googleapis.com/v1/spaces/AAAAmtOc65Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EQRD33KYG-xWnOeIAwj-WJoY8EqL4wa_vgMYOd6iqq8'    
    # k_test= 'https://chat.googleapis.com/v1/spaces/AAQA0Qv-LWI/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=zGAVl4m-MrC_iWOxM4E7D8UyIEA33vd6P_7CsYjvCYg'
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    data = {
        'text': message
    }
    response = requests.post(chat_webhook_adoption, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"Failed to send notification. Status code: {response.status_code}")
    else:
        print("Notification sent successfully.")

def main():
    try:
        query_for_total = """ select count(*) as yesterday from `app_analytics.payments` where payment_date = current_date() -1 """

        mtd= """
                WITH base1 AS (
                SELECT COUNT(*) AS MTD_count
                FROM `app_analytics.payments`
                WHERE EXTRACT(YEAR FROM payment_date) = EXTRACT(YEAR FROM CURRENT_DATE())
                    AND EXTRACT(MONTH FROM payment_date) = EXTRACT(MONTH FROM CURRENT_DATE())
                    AND payment_date < CURRENT_DATE()
                ),

                base2 AS (
                SELECT COUNT(*) AS MTD_count_priority
                FROM `app_analytics.payments`
                WHERE EXTRACT(YEAR FROM payment_date) = EXTRACT(YEAR FROM CURRENT_DATE())
                    AND EXTRACT(MONTH FROM payment_date) = EXTRACT(MONTH FROM CURRENT_DATE())
                    AND payment_date < CURRENT_DATE()
                    AND payment > 6000
                )

                SELECT 
                CONCAT(base1.MTD_count, ' (', base2.MTD_count_priority, ' Priority', ')') AS summary
                FROM base1, base2;
                """
        priority_total= """select count(*) from `app_analytics.payments` where payment>6000 and payment_date < CURRENT_DATE()"""

        priority_payment= """
                WITH base AS (
                SELECT 
                    current_cycle,
                    COUNT(*) AS cycle_count,
                    SUM(COUNT(*)) OVER () AS total_count
                FROM `app_analytics.payments`
                WHERE 
                    DATE(payment_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                    and payment>6000
                GROUP BY current_cycle
                )

                SELECT coalesce(
                CONCAT(
                    MAX(total_count),
                    " ( ",
                    STRING_AGG(CONCAT("C", current_cycle, " - ", cycle_count), " , "),
                    " )"
                ),'0') AS summary
                FROM base;
                """
        
        advisory_payment = """WITH base AS (
        
                SELECT 
                    current_cycle,
                    COUNT(*) AS cycle_count,
                    SUM(COUNT(*)) OVER () AS total_count
                FROM `app_analytics.payments`
                WHERE 
                    DATE(payment_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                    and payment<6000
                GROUP BY current_cycle
                order by current_cycle
                )

                SELECT 
                CONCAT(
                    
                    MAX(total_count),
                    " ( ",
                    STRING_AGG(CONCAT("C", current_cycle, " - ", cycle_count), " , "),
                    " )"
                ) AS summary
                FROM base;

                """
        
        def fetch_single_result(query, query_name):
                try:
                    # Initialize BigQuery client inside the function
                    client = bigquery.Client()
                    query_job = client.query(query)
                    result = query_job.result().to_dataframe()

                    if result.empty:
                        warning_msg = f"<users/kanchan.yadav@1finance.co.in> Warning: Query '{query_name}' returned no data."
                        print(warning_msg)
                        # send_notification2(warning_msg)
                        return "NA"
                    
                    return result.iloc[0][0]
                except Exception as e:
                    error_msg = f"<users/kanchan.yadav@1finance.co.in> Error executing query '{query_name}': {e}"
                    print(error_msg)
                    # send_notification2(error_msg)
                    return "NA"
        total = fetch_single_result(query_for_total, "query_for_total")
        mtd_total = fetch_single_result(mtd, "mtd")
        priority_total_ = fetch_single_result(priority_total, "priority_tool")

        priority_payment_total = fetch_single_result(priority_payment, "priority_payment")

        advisory_payment_total = fetch_single_result(advisory_payment, "advisory_payment")
        report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        message = (
            "                               \n"
        f"Advisory Payments - {report_date}\n"
        "                               \n"
        f"Total - {total} \n"
        "                               \n"
        f"Advisory payment - {advisory_payment_total} \n"
        f"Priority payment - {priority_payment_total}\n"
        "                               \n"
        f"MTD - {mtd_total}\n"
        f"Priority total - {priority_total_}\n"
    
        )
        print(message)
        
        send_notification(message=message)
    
    except Exception as e:
        print(f"An Error Occurred in adoption etl : {e}")
        # message = f"Hello <users/payal.soni@1finance.co.in> An Error Occurred in adoption etl : {e}"
        # send_notification2(message)

    return "Adoption ETL Executed!"


adoption_etl = PythonOperator(
        task_id='advisory_payment_adoption',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

adoption_etl