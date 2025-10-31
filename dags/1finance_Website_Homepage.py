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
    'start_date': datetime(2024, 12, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    '1finance_Website_Homepage',
    default_args=default_args,
    description='1finance_Website_Homepage data',
    schedule_interval='15 4 * * *',
    catchup=False
)

today = date.today()
# Yesterday date
yesterday = today - timedelta(days = 1)

client = bigquery.Client()

def send_notification(message):
    chat_webhook_ng = 'https://chat.googleapis.com/v1/spaces/AAAAmtOc65Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EQRD33KYG-xWnOeIAwj-WJoY8EqL4wa_vgMYOd6iqq8'
    # chat_webhook_test = 'https://chat.googleapis.com/v1/spaces/AAAAzGKojWU/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=TNZXIHhFQtGYRvW1yBcKPFyQoS0mMWOqgEiQtQPGTb4'
    # chat_webhook_adoption = 'https://chat.googleapis.com/v1/spaces/AAAAWezURKM/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=WAHyjSk0u4Ejvpo3TeqittzEjkUAvASoeeKdY3flYvY'    
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    data = {
        'text': message
    }
    response = requests.post(chat_webhook_ng, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"Failed to send notification. Status code: {response.status_code}")
    else:
        print("Notification sent successfully.")

def send_notification2(message):
    # chat_webhook_ng = 'https://chat.googleapis.com/v1/spaces/AAAAmtOc65Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EQRD33KYG-xWnOeIAwj-WJoY8EqL4wa_vgMYOd6iqq8'
    chat_webhook_test = 'https://chat.googleapis.com/v1/spaces/AAAAzGKojWU/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=TNZXIHhFQtGYRvW1yBcKPFyQoS0mMWOqgEiQtQPGTb4'
    # chat_webhook_adoption = 'https://chat.googleapis.com/v1/spaces/AAAAWezURKM/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=WAHyjSk0u4Ejvpo3TeqittzEjkUAvASoeeKdY3flYvY'    
   
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    data = {
        'text': message
    }
    response = requests.post(chat_webhook_test, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"Failed to send notification. Status code: {response.status_code}")
    else:
        print("Notification sent successfully.")

def main():
    try:
        # Query for installs
        overall_installs_query = """
        SELECT
        COUNT(DISTINCT(appsflyer_id)),
        FROM
        `analytics-1f.AppsflyerDB.Installs-v2`
        WHERE
        media_source IN ('Website-to-App',
            'Website',
            'Landing_Page')
        AND DATE(install_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        """
        query_job = client.query(overall_installs_query)

        overall_installs = query_job.to_dataframe().iloc[0, 0]

        homepage_installs_query = """
        SELECT
        COUNT(DISTINCT(appsflyer_id)),
        FROM
        `analytics-1f.AppsflyerDB.Installs-v2`
        WHERE
        media_source IN ('Website-to-App',
            'Website',
            'Landing_Page')
        AND Campaign IN ('Website _1F_Homepage_Button_Download _Web _2024',
            'Website_Hompage_KYFP_Download_App')
        AND DATE(install_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        """

        query_job = client.query(homepage_installs_query)

        homepage_installs = query_job.to_dataframe().iloc[0, 0]

        # Query for registrations
        registration_query = """
        SELECT
        COUNT(DISTINCT customer_ID) AS registrations
        FROM
        `analytics-1f.MasterDB.1_source_t-1`
        WHERE
        DATE(Registration_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND media_source IN ('Website-to-App',
            'Website',
            'Landing_Page')
        AND Campaign IN ('Website _1F_Homepage_Button_Download _Web _2024',
            'Website_Hompage_KYFP_Download_App')
        AND Internal_Test_Tagging IS NULL
        AND System_Status = 'Live'
        """
        query_job = client.query(registration_query)

        total_registrations = query_job.to_dataframe().iloc[0, 0]

        # Query for MoneySign
        moneysign_query = """
        SELECT
        COUNT(DISTINCT customer_ID) AS MONEYSIGN
        FROM
        `analytics-1f.MasterDB.1_source_t-1`
        WHERE
        DATE(MoneySignGeneratedDate) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND media_source IN ('Website-to-App',
            'Website',
            'Landing_Page')
        AND Campaign IN ('Website _1F_Homepage_Button_Download _Web _2024',
            'Website_Hompage_KYFP_Download_App')
        AND Internal_Test_Tagging IS NULL
        AND System_Status = 'Live'
        """

        query_job = client.query(moneysign_query)

        total_moneysign = query_job.to_dataframe().iloc[0, 0]

        # Query for discovery booked
        discovery_booked_query = """
        SELECT
        COUNT(Distinct customer_ID) AS discovery_booked
        FROM
        `analytics-1f.MasterDB.1_source_t-1`
        WHERE
        DATE(Discovery_Booking_Date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND media_source IN ('Website-to-App',
            'Website',
            'Landing_Page')
        AND Campaign IN ('Website _1F_Homepage_Button_Download _Web _2024',
            'Website_Hompage_KYFP_Download_App')
        AND Internal_Test_Tagging IS NULL
        AND System_Status = 'Live'
        """

        query_job = client.query(discovery_booked_query)

        total_discovery_booked = query_job.to_dataframe().iloc[0, 0]

        # Query for discovery attended
        discovery_attended_query = """
        SELECT
        COUNT(Distinct customer_ID) AS discovery_attended
        FROM
        `analytics-1f.MasterDB.1_source_t-1`
        WHERE
        DATE(Discovery_Appointment_Date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND Discovery_Meeting_Status = 'Completed' 
        AND media_source IN ('Website-to-App',
            'Website',
            'Landing_Page')
        AND Campaign IN ('Website _1F_Homepage_Button_Download _Web _2024',
            'Website_Hompage_KYFP_Download_App')
        AND Internal_Test_Tagging IS NULL
        AND System_Status = 'Live'
        """

        query_job = client.query(discovery_attended_query)

        total_discovery_attended = query_job.to_dataframe().iloc[0, 0]

        # Query for uninstalls
        uninstall_query = """
        SELECT 
        COUNT(DISTINCT AppsFlyer_ID) AS uninstalls
        FROM
        `analytics-1f.AppsflyerDB.actual_uninstalls`
        WHERE
        DATE(Event_Time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND media_source IN ('Website-to-App',
            'Website',
            'Landing_Page')
        AND Campaign IN ('Website _1F_Homepage_Button_Download _Web _2024',
            'Website_Hompage_KYFP_Download_App')
        """

        query_job = client.query(uninstall_query)

        total_uninstalls = query_job.to_dataframe().iloc[0, 0]

        # Compose and send the message
        message = (
            f"{yesterday}\n"
            "-----------------------------------\n"
            "Website_To_App\n"
            "-----------------------------------\n"
            f"Total Installs From Website = {overall_installs}\n"
            "-----------------------------------\n"
            "Install From Homepage\n\n"
            f"Installs = {homepage_installs}\n"
            f"Registrations = {total_registrations}\n"
            f"MoneySign = {total_moneysign}\n"
            f"Discovery Booked = {total_discovery_booked}\n"
            f"Discovery Attended = {total_discovery_attended}\n"
            f"Uninstalls = {total_uninstalls}"
        )

        send_notification(message)
        print(message)

    except Exception as e:
        message = f"An Error Occurred: {e}"
        send_notification2(message)
        print(message)

    return "Website_Homepage ETL Executed!"




Website_Homepage = PythonOperator(
        task_id='Website_Homepage',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

Website_Homepage