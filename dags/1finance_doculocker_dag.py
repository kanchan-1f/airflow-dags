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
    '1finance_doculocker_dag',
    default_args=default_args,
    description='1finance_doculocker_dag data',
    schedule_interval='5 4 * * *',
    catchup=False
)

# today = date.today()
# # Yesterday date
# yesterday = today - timedelta(days = 1)

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

        # Define queries
        # query_for_total_users_b_flow = """
        #     with final_flow as (
        #     with reg
        #     as (
        #     select distinct customer_ID  , app_version from `MasterDB.1_source_t-1` where
        #     Internal_Test_Tagging is null  and System_Status ='Live'  and app_version ='2.5.0'
        #     and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
        #     )
        #     , flow as (
        #     with main as (select distinct v.appsflyer_ID , flow , m.customer_ID from (
        #     select distinct appsflyer_ID , max(event_time) as event_time , max(flow)  as flow  from `AppsflyerDB.in_app_events_no_test_v4`
        #     group by 1
        #     ) v left join `MasterDB.1_source_t-1` m on v.appsflyer_ID = m.appsflyer_ID
        #     where m.Internal_Test_Tagging is null)
        #     select customer_ID , flow  from main where customer_ID is not null
        #     union all
        #     select distinct customer_code ,  'B' as flow from `EosDB.customer_data` where category_id = '134' and parameters = '3')
        #     select  distinct reg.customer_ID ,  app_version ,flow.flow from reg left join flow on reg.customer_ID = flow.customer_ID )
        #     select COUNT(*) from final_flow where flow = 'B'
        # """

        query_for_users_2_5_0 = """
            select count(distinct customer_ID) from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null 
            and System_Status ='Live'  and app_version ='2.5.0'
            and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
        """
        query_for_users_2_5_1 = """
            select count(distinct customer_ID) from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null 
            and System_Status ='Live'  and app_version ='2.5.1'
            and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
        """
        query_for_users_2_5_2 = """
            select count(distinct customer_ID) from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null 
            and System_Status ='Live'  and app_version ='2.5.2'
            and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
        """

        query_for_activated_meeting_booked = """
        with base as (
        select distinct customer_ID from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null 
        and System_Status ='Live'  and app_version >= '2.5.0'
        and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null))
        , 
        meeting_booked as (
        select distinct customer_ID  , discovery_booking_date from `MasterDB.1_source_t-1` 
        )
        select count(base.customer_ID )from base left join meeting_booked m on base.customer_ID = m.customer_ID 
        where m.discovery_booking_date is not null
        """
        query_for_clicked = """
 
            with main_base as (with base as (
            select distinct customer_ID from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null 
            and System_Status ='Live'  and app_version >= '2.5.0'
            and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null))
            , 
            meeting_booked as (
            select distinct customer_ID  , discovery_booking_date from `MasterDB.1_source_t-1` 
            )
            select base.customer_ID from base left join meeting_booked m on base.customer_ID = m.customer_ID 
            where m.discovery_booking_date is not null 
            ) , 
            docu_clicked   as (
            select distinct customer_ID from `MasterDB.1_source_t-1` where appsflyer_ID in (
                select distinct appsflyer_ID from `AppsflyerDB.in_app_events_no_test_v4`  where event_name IN( "doculocker_card_click",
            "doculocker_Category_screen_visited",
            "doculocker_carousel_scroll",
            "doculocker_category_click",
            "sub_category_screen_visited",
            "out_of_storage_pop",
            "doculocker_sub_category_click",
            "Doculocker_upload_click",
            "doculocker_camera_access_bottomsheet",
            "doculocker_open_settings_click") )
            )
            select count(distinct mb.customer_ID) from main_base mb where mb.customer_ID in (select * from docu_clicked)

        """
        query_for_total_uploads = """
        select count(*) from  `analytics-1f.stored_functions.financial_upload_user_for_automation`() 
        where role is not null
        """
        # Queries for upload types by category
        query_for_system_generated = """
        select count(*) from  `analytics-1f.stored_functions.financial_upload_user_for_automation`() 
        where lower(role) like '%system%'
        """
        query_for_fc_uploads = """
        select count(*) from  `analytics-1f.stored_functions.financial_upload_user_for_automation`() 
        where lower(role) like '%fc%'
        """

        query_for_user_uploads = """
        select count(*) from  `analytics-1f.stored_functions.financial_upload_user_for_automation`() 
        where lower(role) like '%user%'
        """
        query_for_shared = """
        select count(*) from  `analytics-1f.stored_functions.financial_upload_user_for_automation`()
        where lower(role) like '%user%' and is_shared = true
        """

        # Fetch data for each metric with error handling
        total_users = fetch_single_result(query_for_users_2_5_0, "Total Users 2.5.0") + fetch_single_result(query_for_users_2_5_1, "Total Users 2.5.1") + fetch_single_result(query_for_users_2_5_2, "Total Users 2.5.2") 
        activated_meeting_booked = fetch_single_result(query_for_activated_meeting_booked, "Activated (Meeting Booked)")
        clicked = fetch_single_result(query_for_clicked, "Clicked")
        total_uploads = fetch_single_result(query_for_total_uploads, "Total Uploads")
        shared_w_fc = fetch_single_result(query_for_shared, "Shared with fc")

        # Fetch system-generated, FC, and user uploads
        system_generated = fetch_single_result(query_for_system_generated, "System Generated Uploads")
        fc_uploads = fetch_single_result(query_for_fc_uploads, "FC Uploads")
        user_uploads = fetch_single_result(query_for_user_uploads, "User Uploads")

        # Calculate the percentage of activated users
        activation_percentage = round((activated_meeting_booked / total_users) * 100) if total_users != "NA" and total_users > 0 else "NA"

        # Generate the message without extra spaces
        message = (
            "Doculocker\n"
            "---------------------\n"
            f"Total users on (v2.5.0 + v2.5.1 + v2.5.2)- {total_users}\n"
            f"Activated(meet booked)- {activated_meeting_booked} ({activation_percentage}%)\n"
            f"Clicked- {clicked}\n"
            "----------------------\n"
            f"Total uploads- {total_uploads}\n"
            # "------------------------------------\n"
            f"-System generated- {system_generated}\n"
            # "--------------------------------------\n"
            f"-FC Uploads- {fc_uploads}\n"
            # "--------------------------------------\n"
            f"-User Uploads- {user_uploads} (Shared with FC- {shared_w_fc} )"
        )

        # Print the formatted message
        print(message)

            # message = f"{yesterday}\n------------------------------\nInstalls = {installs}\niOS = {installs_ios} | Android = {installs_android}\n------------------------------\nRegistration = {registrations}\niOS = {registrations_ios} | Android = {registrations_android}\n------------------------------\nMoneySign Complete = {moneysign}\nRIA Signed = {riasigned}\nPayment Done = {payments}\n------------------------------\nUninstalls = {uninstalls}\n------------------------------\nTotal Users on v2.4.0 = {totalusers240}\n------------------------------\nTotal Users on v2.4.1={totaluserslatest}\nFrom Installs = {latestinstalls}\nFrom Upgrades = {upgrades}"
        send_notification(message)


    except Exception as e:
        print(f"An Error Occurred in doculocker_etl: {e}")
        message = f"<users/payal.soni@1finance.co.in> An Error Occurred in doculocker_etl : {e}"
        send_notification2(message)

    return "1finance_doculocker ETL Executed!"




doculocker_etl = PythonOperator(
        task_id='doculocker_etl',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

doculocker_etl