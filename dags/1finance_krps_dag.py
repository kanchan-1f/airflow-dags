
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from google.cloud import bigquery
# import os
# import psycopg2
# import json
# import pandas
# import requests
# import logging
# from all_queries import krp_installs,krp_registration,krp_money_sign,krp_meeting,krp_uninstall

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-e16ff6d4c343.json')

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2024, 8, 29),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=2),
# }

# dag = DAG(
#     '1finance_krps_dag',
#     default_args=default_args,
#     # concurrency=10, max_active_runs=2,
#     description='1finance_krp_data',
#     schedule_interval='30 3 * * *',
#     catchup=False
# )

# # today = date.today()
# # # Yesterday date
# # yesterday = today - timedelta(days = 1)

# yesterday = (datetime.now() - timedelta(days=1))
# formatted_date = yesterday.strftime("%d %b'%y")

# client = bigquery.Client()

# def send_notification(message):
#     chat_webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAA8J7hTns/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=z25XJaHuTXHJ8849qsdMW91Uukk4vvs-QSBVtYmclJU'
#     headers = {'Content-Type': 'application/json; charset=UTF-8'}
#     data = {
#         'text': message
#     }
#     response = requests.post(chat_webhook_url, headers=headers, data=json.dumps(data))
#     if response.status_code != 200:
#         print(f"Failed to send notification. Status code: {response.status_code}")
#     else:
#         print("Notification sent successfully.")

# # Function to execute a BigQuery query and return the result
# def fetch_value(query):
#     client = bigquery.Client()
#     query_job = client.query(query)
#     result = query_job.result()
#     # Assuming the query returns a single row with a single column
#     for row in result:
#         return row[0]
    
# and_per_install = """
#     WITH totalInstalls AS (
#     SELECT COUNT(Install_Time) AS total_installs 
#     FROM  `analytics-1f.AppsflyerDB.Installs-v2`
#     WHERE  DATE(Install_Time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),
#     AndroidInstalls AS (
#     SELECT COUNT(Install_Time) AS android_installs 
#     FROM `analytics-1f.AppsflyerDB.Installs-v2`
#     WHERE DATE(Install_Time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
#     AND Platform = 'android')
#     SELECT  CONCAT(ROUND((a.android_installs / t.total_installs) * 100), '%') AS android_install_percentage
#     FROM TotalInstalls t
#     JOIN AndroidInstalls a ON 1=1
#     """
# query_job = client.query(and_per_install)
# and_per_installs = query_job.to_dataframe().iloc[0, 0]

# ios_per_install =  """
#     WITH TotalInstalls AS (
#     SELECT COUNT(Install_Time) AS total_installs 
#     FROM `analytics-1f.AppsflyerDB.Installs-v2`
#     WHERE DATE(Install_Time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),
#     iOSInstalls AS (
#     SELECT COUNT(Install_Time) AS ios_installs 
#     FROM `analytics-1f.AppsflyerDB.Installs-v2`
#     WHERE DATE(Install_Time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND Platform = 'ios')
#     SELECT CONCAT(ROUND((i.ios_installs / t.total_installs) * 100), '%') AS ios_install_percentage
#     FROM TotalInstalls t
#     JOIN iOSInstalls i ON 1=1
#     """
# query_job = client.query(ios_per_install)
# ios_per_installs = query_job.to_dataframe().iloc[0, 0]
# # android = fetch_value(android_installs)
# # ios = fetch_value(ios_installs)

# # Fetch values
# def main():
#     try:
#     # install = {key: fetch_value(query) for key, query in installs.items()}
#         install_data = {key: fetch_value(query) for key, query in krp_installs.items()}
#         # registrations = {key: fetch_value(query) for key, query in registration.items()}

#         registrations_data = {key: fetch_value(query) for key, query in krp_registration.items()}
#         money_sign = {key: fetch_value(query) for key, query in krp_money_sign.items()}
#         meeting = {key: fetch_value(query) for key, query in krp_meeting.items()}
#         uninstall = {key: fetch_value(query) for key, query in krp_uninstall.items()}

#         message = f"Data update for: {formatted_date}\n------------------------------\n"
#         message += "App Data"
#         message += "\n------------------------------\n"
#         # message += "\n".join([f"{key} = {value}" for key, value in install.items()])
#         # message += "\n------------------------------\n"
#         message += "\n".join([f"{key} = {value}" for key, value in install_data.items()])
#         # message += "\n------------------------------\n"
#         message += f" (Android {and_per_installs} / iOS {ios_per_installs})\n"
#         # message += "\n".join([f"{key} = {value}" for key, value in registrations.items()])
#         # message += "\n------------------------------\n"
#         message += "\n".join([f"{key} = {value}" for key, value in registrations_data.items()])
#         message += "\n"
#         message += "\n".join([f"{key} = {value}" for key, value in money_sign.items()])
#         message += "\n------------------------------\n"
#         message += "Meeting Data"
#         message += "\n------------------------------\n"
#         message += "\n".join([f"{key} = {value}" for key, value in meeting.items()])
#         message += "\n------------------------------\n"
#         message += "\n".join([f"{key} = {value}" for key, value in uninstall.items()])
#         # message += "\n------------------------------\n"

#         send_notification(message)
#         print(message)
#     except Exception as e:
#         print(f"An Error Occurred : {e}")
#         message = f"An Error Occurred : {e}"
#         send_notification(message)

#     return "Adoption ETL Executed!"




# krps_dag = PythonOperator(
#         task_id='krps_dag',
#         python_callable=main,
#         provide_context=True,
#         dag=dag,
#         )

# krps_dag