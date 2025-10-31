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
    'start_date': datetime(2024, 8, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    '1finance_Website_To_App_A_B',
    default_args=default_args,
    description='finance_Website_To_App_A/B data',
    schedule_interval='15 4 * * *',
    catchup=False
)

today = date.today()
# Yesterday date
yesterday = today - timedelta(days = 1)

client = bigquery.Client()

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


def main():
    try:
        # Query for installs

        overall_install = """
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
        query_job = client.query(overall_install)

        overall_installs= query_job.to_dataframe().iloc[0, 0]

        installA = """
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

        query_job = client.query(installA)

        installsA= query_job.to_dataframe().iloc[0, 0]


        installB = """
        SELECT
        COUNT(DISTINCT(appsflyer_id)),
        FROM
        `analytics-1f.AppsflyerDB.Installs-v2`
        WHERE
        media_source IN ('Website-to-App',
            'Website',
            'Landing_Page')
        AND Campaign IN ('Website _1F_HomepageV2MoneySign_Button_Download _Web _July25')
        AND DATE(install_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        """

        query_job = client.query(installB)

        installsB= query_job.to_dataframe().iloc[0, 0]

        total_installs = installsA + installsB


        # Query for Registration

        registrationA = """
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
        query_job = client.query(registrationA)

        registrationsA = query_job.to_dataframe().iloc[0, 0]

        registrationB = """
        SELECT
        COUNT(DISTINCT customer_ID) AS registrations
        FROM
        `analytics-1f.MasterDB.1_source_t-1`
        WHERE
        DATE(Registration_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND media_source IN ('Website-to-App',
            'Website',
            'Landing_Page')
        AND Campaign IN ('Website _1F_HomepageV2MoneySign_Button_Download _Web _July25')
        AND Internal_Test_Tagging IS NULL
        AND System_Status = 'Live'
        """
        query_job = client.query(registrationB)

        registrationsB = query_job.to_dataframe().iloc[0, 0]

        total_registrations = registrationsA + registrationsB

        # Query for Moneysign
        money_signA = """
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

        query_job = client.query(money_signA)

        moneysign_A = query_job.to_dataframe().iloc[0, 0]

        money_signB = """
        SELECT
        COUNT(DISTINCT customer_ID) AS MONEYSIGN
        FROM
        `analytics-1f.MasterDB.1_source_t-1`
        WHERE
        DATE(MoneySignGeneratedDate) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND media_source IN ('Website-to-App',
            'Website',
            'Landing_Page')
        AND Campaign IN ('Website _1F_HomepageV2MoneySign_Button_Download _Web _July25')
        AND Internal_Test_Tagging IS NULL
        AND System_Status = 'Live'
           """

        query_job = client.query(money_signB)

        moneysign_B = query_job.to_dataframe().iloc[0, 0]

        total_moneysign = moneysign_A + moneysign_B

        # Query for dISCOVERY BOOKED
        discovery_bookedA = """
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

        query_job = client.query(discovery_bookedA)

        discovery_booked_A = query_job.to_dataframe().iloc[0, 0]

        discovery_bookedB = """
            SELECT
            COUNT(Distinct customer_ID) AS discovery_booked
            FROM
            `analytics-1f.MasterDB.1_source_t-1`
            WHERE
            DATE(Discovery_Booking_Date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            AND media_source IN ('Website-to-App',
                'Website',
                'Landing_Page')
            AND Campaign IN ('Website _1F_HomepageV2MoneySign_Button_Download _Web _July25')
            AND Internal_Test_Tagging IS NULL
            AND System_Status = 'Live'
        """

        query_job = client.query(discovery_bookedB)

        discovery_booked_B = query_job.to_dataframe().iloc[0, 0]

        total_discovery_booked = discovery_booked_A + discovery_booked_B


        # Query for Discovery attended
        discovery_attendedA = """
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

        query_job = client.query(discovery_attendedA)

        discovery_attended_A = query_job.to_dataframe().iloc[0, 0]

        discovery_attendedB = """
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
            AND Campaign IN ('Website _1F_HomepageV2MoneySign_Button_Download _Web _July25')
            AND Internal_Test_Tagging IS NULL
            AND System_Status = 'Live'
        """

        query_job = client.query(discovery_attendedB)

        discovery_attended_B = query_job.to_dataframe().iloc[0, 0]

        total_discovery_attended = discovery_attended_A + discovery_attended_B


        # Query for uninstalls
        uninstallA = """
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

        query_job = client.query(uninstallA)

        uninstallsA = query_job.to_dataframe().iloc[0, 0]

        uninstallB = """
        SELECT 
        COUNT(DISTINCT AppsFlyer_ID) AS uninstalls
        FROM
        `analytics-1f.AppsflyerDB.actual_uninstalls`
        WHERE
        DATE(Event_Time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND media_source IN ('Website-to-App',
            'Website',
            'Landing_Page')
        AND Campaign IN ('Website _1F_HomepageV2MoneySign_Button_Download _Web _July25')
        """

        query_job = client.query(uninstallB)

        uninstallsB = query_job.to_dataframe().iloc[0, 0]

        total_uninstalls = uninstallsA + uninstallsB



        # print(f"\n{yesterday}\n------------------------------\nA/B Version Website_To_App\n------------------------------\nInstalls = {total_installs}\nA = {installsA}\nB = {installsB}\n\nRegistrations = {total_registrations}\nA = {registrationsA}\nB = {registrationsB}\n\nMoneySign = {total_moneysign}\nA = {moneysign_A}\nB = {moneysign_B}\n\nDiscovery_Booked = {total_discovery_booked}\nA = {discovery_booked_A}\nB = {discovery_booked_B}\nDiscovery_Attended = {total_discovery_attended}\nA = {discovery_attended_A}\nB = {discovery_attended_B}\n\nUninstalls = {total_uninstalls}\n A = {uninstallsA}\nB = {uninstallsB}")


        # message = f"{yesterday}\n------------------------------\A/B Version Website_To_App\n------------------------------\nInstalls = {total_installs}\nRegistrations = {registrations}\nMoneySign = {moneysign}\nDiscovery_Booked = {discovery_booked}\nUninstalls = {uninstalls}"
        message = (
            f"{yesterday}\n"
            "-----------------------------------\n"
            "A/B Version Website_To_App\n"
            "-----------------------------------\n"
            f"Total Installs From Website = {overall_installs}\n"
            "-----------------------------------\n"
            f"Install from Homepage for A/B\n"
            f"Installs = {total_installs}\n"
            f"A = {installsA}\n"
            f"B = {installsB}\n\n"
            f"Registrations = {total_registrations}\n"
            f"A = {registrationsA}\n"
            f"B = {registrationsB}\n\n"
            f"MoneySign = {total_moneysign}\n"
            f"A = {moneysign_A}\n"
            f"B = {moneysign_B}\n\n"
            f"Discovery_Booked = {total_discovery_booked}\n"
            f"A = {discovery_booked_A}\n"
            f"B = {discovery_booked_B}\n\n"
            f"Discovery_Attended = {total_discovery_attended}\n"
            f"A = {discovery_attended_A}\n"
            f"B = {discovery_attended_B}\n\n"
            f"Uninstalls = {total_uninstalls}\n"
            f"A = {uninstallsA}\n"
            f"B = {uninstallsB}"
        )

        send_notification(message)
        print(message)


    except Exception as e:
        message = f"An Error Occurred : {e}"
        send_notification(message)
        print(message)
        
    return "Website_To_App ETL Executed!"




Website_To_App = PythonOperator(
        task_id='Website_To_App',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

Website_To_App