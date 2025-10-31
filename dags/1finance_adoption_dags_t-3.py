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
    'start_date': datetime(2024, 8, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    '1finance_adoption_dag_t-3',
    default_args=default_args,
    description='1finance_adoption data for friday, saturday and sunday clubbed up and sent on monday',
    schedule_interval='5 4 * * 1',
    catchup=False
)


client = bigquery.Client()

def send_notification(message):
    # chat_webhook_ng = 'https://chat.googleapis.com/v1/spaces/AAAAmtOc65Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EQRD33KYG-xWnOeIAwj-WJoY8EqL4wa_vgMYOd6iqq8'
    # chat_webhook_test = 'https://chat.googleapis.com/v1/spaces/AAAAzGKojWU/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=TNZXIHhFQtGYRvW1yBcKPFyQoS0mMWOqgEiQtQPGTb4'
    # k_test= 'https://chat.googleapis.com/v1/spaces/AAQA0Qv-LWI/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=zGAVl4m-MrC_iWOxM4E7D8UyIEA33vd6P_7CsYjvCYg'
    chat_webhook_adoption = 'https://chat.googleapis.com/v1/spaces/AAAAWezURKM/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=WAHyjSk0u4Ejvpo3TeqittzEjkUAvASoeeKdY3flYvY'    
    # test= 'https://chat.googleapis.com/v1/spaces/AAQAhqmAf2Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=j_efnhF1F1PXMkhyE-AB5QNK-00WioTWfeILPS63NG4'
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
    chat_webhook_adoption = 'https://chat.googleapis.com/v1/spaces/AAAAmtOc65Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EQRD33KYG-xWnOeIAwj-WJoY8EqL4wa_vgMYOd6iqq8'
    # chat_webhook_test = 'https://chat.googleapis.com/v1/spaces/AAAAzGKojWU/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=TNZXIHhFQtGYRvW1yBcKPFyQoS0mMWOqgEiQtQPGTb4'
    #  chat_webhook_ng = 'https://chat.googleapis.com/v1/spaces/AAAAWezURKM/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=WAHyjSk0u4Ejvpo3TeqittzEjkUAvASoeeKdY3flYvY'    
    # k_test= 'https://chat.googleapis.com/v1/spaces/AAQA0Qv-LWI/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=zGAVl4m-MrC_iWOxM4E7D8UyIEA33vd6P_7CsYjvCYg'
    # test= 'https://chat.googleapis.com/v1/spaces/AAQAhqmAf2Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=j_efnhF1F1PXMkhyE-AB5QNK-00WioTWfeILPS63NG4'
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    data = {
        'text': message,
        "annotations": [
            {
                "type": "USER_MENTION",
                "startIndex": 6,
                "length": 25,  # Length of "<users/payal.soni@1finance.co.in>"
                "userMention": {
                    "user": {
                        "email": ["payal.soni@1finance.co.in", "kanchan.yadav@1finance.co.in", "raj.rohra@1finance.org.in"]
                    },
                    "type": "ADD"
                }
            }
        ]
    }
    response = requests.post(chat_webhook_adoption, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"Failed to send notification. Status code: {response.status_code}")
    else:
        print("Notification sent successfully.")


def main():
    try:
        # Initialize BigQuery client
        client = bigquery.Client()
        # Define queries with triple quotes for readability
        query_for_installs = """
            with base as (select distinct appsflyer_ID , install_date from `AppsflyerDB.Installs-v2` ),
            test as (
            select distinct appsflyer_ID ,Internal_Test_Tagging  from `MasterDB.1_source_t-1`
            where Internal_Test_Tagging is not null  and appsflyer_ID is not null
            )

            select count(distinct base.appsflyer_ID) from base left join test on base.appsflyer_ID = test.appsflyer_ID where test.appsflyer_ID is null
            and date(install_date) between DATE_SUB(CURRENT_DATE(), interval 3 DAY) and DATE_SUB(CURRENT_DATE(), interval 1 DAY)
        """

        query_for_ios_installs = """
            select count(distinct v.appsflyer_ID) from `AppsflyerDB.Installs-v2` v left join `MasterDB.1_source_t-1` m on v.AppsFlyer_ID = m.appsflyer_id
            where date(v.install_date) between DATE_SUB(CURRENT_DATE(), interval 3 DAY) and DATE_SUB(CURRENT_DATE(), interval 1 DAY) and 
            m.Internal_Test_Tagging is null and v.platform = 'ios'

        """

        query_for_android_installs = """
            select count(distinct v.appsflyer_ID)  from `AppsflyerDB.Installs-v2` v left join `MasterDB.1_source_t-1` m on v.AppsFlyer_ID = m.appsflyer_id
            where date(v.install_date) between DATE_SUB(CURRENT_DATE(), interval 3 DAY) and DATE_SUB(CURRENT_DATE(), interval 1 DAY) and 
            m.Internal_Test_Tagging is null and v.platform = 'android'
        """
    
        query_for_registration = """
            select count( distinct customer_ID ) from `MasterDB.1_source_t-1` where date(Registration_date)  between DATE_SUB(CURRENT_DATE(), interval 3 DAY) and DATE_SUB(CURRENT_DATE(), interval 1 DAY)
            and Internal_Test_Tagging is null 
        # """

        query_for_sameday = """
            SELECT
            COUNT( DISTINCT customer_ID )
            FROM
            `MasterDB.1_source_t-1`
            WHERE
            DATE(Registration_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
            AND Internal_Test_Tagging IS NULL
            AND DATE(install_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)       
        """
   

        query_for_money_sign_complete = """
            select  count(distinct customer_ID)  from `MasterDB.1_source_t-1` where date(MoneySignGeneratedDate)  between DATE_SUB(CURRENT_DATE(), interval 3 DAY) and DATE_SUB(CURRENT_DATE(), interval 1 DAY)
            and Internal_Test_Tagging is null
        """

        query_for_ria_signed = """
            select count(distinct customer_ID) from `MasterDB.1_source_t-1` where 
            RIA_Signed_Date is not null and 
            date(RIA_Signed_Date) between DATE_SUB(CURRENT_DATE(), interval 3 DAY) and DATE_SUB(CURRENT_DATE(), interval 1 DAY)  and Internal_Test_Tagging is null;
        """

        query_for_uninstalls = """
        select count(distinct v.appsflyer_ID)  from `AppsflyerDB.actual_uninstalls` v left join `MasterDB.1_source_t-1` m on v.AppsFlyer_ID = m.appsflyer_id  
        where date(v.Event_date) between DATE_SUB(CURRENT_DATE(), interval 3 DAY) and DATE_SUB(CURRENT_DATE(), interval 1 DAY)
        and m.Internal_Test_Tagging is null ;
        """

        query_for_users_v2_4_1 = """
            select count(distinct customer_ID) from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null 
            and System_Status ='Live'  and app_version ='2.4.1'
            and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
            
        """

        query_for_users_v2_5_0 = """
            select count(distinct customer_ID) from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null 
            and System_Status ='Live'  and app_version ='2.5.0'
            and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
        """

        query_for_users_v2_5_1 = """
            select count(distinct customer_ID) from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null 
            and System_Status ='Live'  and app_version ='2.5.1'
            and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
        """
        query_for_users_v2_5_2 = """
            select count(distinct customer_ID) from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null 
            and System_Status ='Live'  and app_version ='2.5.2'
            and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
        """

        query_for_installs_v2_5_2 = """
            with base as (select *  from (
            select distinct t.customer_ID , t.install_date ,  t.app_version , min(v.app_version)  minap from `MasterDB.1_source_t-1` t left join
            `AppsflyerDB.in_app_events_no_test_v4` v on t.appsflyer_ID = v.appsflyer_ID 
            where Internal_Test_Tagging is null 
            and System_Status ='Live'  and t.app_version ='2.5.2'
            and t.appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
            group by 1 , 2 ,3)
            where minap = '2.5.2'
            ),
            total as (select count(distinct customer_ID) as a from base ), 
            added as (select count(distinct customer_ID)  as b from base where date(install_date)  between DATE_SUB(CURRENT_DATE(), interval 3 DAY) and DATE_SUB(CURRENT_DATE(), interval 1 DAY))
            select concat(a,  ' ' , '(+' , (b),')')  from total , added
        """

   
        query_for_added_in_v2_5_2 = """
        
            WITH upg AS (
                WITH final_main AS (
                    WITH main AS (
                        WITH updated AS (
                            SELECT DISTINCT appsflyer_id, MIN(event_time) AS updated_date FROM `analytics-1f.AppsflyerDB.in_app_events_no_test_v4`
                            WHERE app_version = "2.5.2"
                            GROUP BY appsflyer_id
                        ),
                        current_version AS (
                            SELECT DISTINCT appsflyer_id,
                                app_version
                            FROM `analytics-1f.AppsflyerDB.in_app_events_no_test_v4`
                            WHERE app_version = "2.5.2"
                        ),
                        previous_version AS (
                            SELECT appsflyer_id,
                                MAX(app_version) AS updated_from
                            FROM `analytics-1f.AppsflyerDB.in_app_events_no_test_v4`
                            WHERE app_version < "2.5.2"
                            GROUP BY appsflyer_id
                        )
                        SELECT *
                        FROM updated
                        JOIN current_version USING (appsflyer_id)
                        JOIN previous_version USING (appsflyer_id)
                    )
                    SELECT DISTINCT 
                        main.appsflyer_ID, 
                        DATE(main.updated_date) AS updated_date, 
                        main.updated_from, 
                        MAX(customer_ID) AS customer_id,  
                        system_status, 
                        internal_test_tagging 
                    FROM main 
                    LEFT JOIN `MasterDB.1_source_t-1` m 
                        ON main.appsflyer_ID = m.appsflyer_id
                    GROUP BY 1,2,3,5,6
                ),
                one AS (
                    SELECT COUNT(*) AS cnt 
                    FROM final_main 
                    WHERE customer_ID IS NOT NULL 
                    AND internal_test_tagging IS NULL
                    AND system_status = 'Live'
                ),
                two AS (
                    SELECT COUNT(*) AS cnt
                    FROM final_main 
                    WHERE customer_ID IS NOT NULL 
                    AND internal_test_tagging IS NULL
                    AND system_status = 'Live' 
                    AND DATE(updated_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) 
                )
                SELECT cnt FROM two
            ),
            ins AS (
                SELECT COUNT(*) AS cnt
                FROM (
                    SELECT DISTINCT customer_ID, av
                    FROM `MasterDB.1_source_t-1` m
                    LEFT JOIN (
                        SELECT DISTINCT appsflyer_ID, MIN(app_version) av
                        FROM `AppsflyerDB.in_app_events_no_test_v4`
                        GROUP BY 1
                    ) v USING(appsflyer_ID)
                    WHERE Internal_Test_Tagging IS NULL
                    AND System_Status = 'Live'
                    AND app_version = '2.5.2'
                    AND appsflyer_id NOT IN (
                        SELECT DISTINCT appsflyer_ID 
                        FROM `AppsflyerDB.actual_uninstalls` 
                        WHERE appsflyer_ID IS NOT NULL
                    )
                    AND DATE(install_date) 
                        BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
                            AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                ) 
                WHERE av = '2.5.2'
            )
            SELECT COALESCE(
                CAST((SELECT cnt FROM ins) AS INT64) +  
                CAST((SELECT cnt FROM upg) AS INT64),
            0) AS total_users;
        """
    
        query_for_upgrades_v2_5_2 = """

            with final_main as (with main as (
            WITH updated AS (
                SELECT DISTINCT appsflyer_id,
                    MIN(event_time) AS updated_date
                FROM `analytics-1f.AppsflyerDB.in_app_events_no_test_v4`
                WHERE app_version = "2.5.2"
                GROUP BY appsflyer_id
            ),
            current_version AS (
                SELECT DISTINCT appsflyer_id,
                    app_version
                FROM `analytics-1f.AppsflyerDB.in_app_events_no_test_v4`
                WHERE app_version = "2.5.2"
            ),
            previous_version AS (
                SELECT appsflyer_id,
                    MAX(app_version) AS updated_from
                FROM `analytics-1f.AppsflyerDB.in_app_events_no_test_v4`
                WHERE app_version < "2.5.2"
                GROUP BY appsflyer_id
            )
            SELECT *
            FROM updated
            JOIN current_version USING (appsflyer_id)
            JOIN previous_version USING (appsflyer_id)
            ORDER BY appsflyer_id)
            select distinct main.appsflyer_ID , date(main.updated_date) as updated_date , main.updated_from , max(customer_ID) as customer_id  , system_status , internal_test_tagging from main left join `MasterDB.1_source_t-1` m on 
            main.appsflyer_ID = m.appsflyer_id
            group by 1 ,2 , 3 ,5, 6 ) , 
            one as (
            select count(*) from final_main where customer_ID is not null and internal_test_tagging is null
            and system_status = 'Live') , 
            two as (
            select count(*) from final_main where customer_ID is not null and internal_test_tagging is null
            and system_status = 'Live' and date(updated_date)  between DATE_SUB(CURRENT_DATE(), interval 3 DAY) and DATE_SUB(CURRENT_DATE(), interval 1 DAY))
            select concat((select * from one) , '' ,'(','+', (select * from two), ')')

        """
       
        query_for_all_versions = """
                SELECT
                COUNT(DISTINCT customer_ID)
                FROM
                `MasterDB.1_source_t-1`
                WHERE
                app_version >= '2.5.0'
                AND Internal_Test_Tagging IS NULL
                AND System_Status ='Live'
                AND appsflyer_id NOT IN (
                SELECT
                    DISTINCT appsflyer_ID
                FROM
                    `AppsflyerDB.actual_uninstalls`
                WHERE
                    appsflyer_ID IS NOT NULL)

                    
            """
        # Function to execute query and get a single result
        def fetch_single_result(query, query_name):
            try:
                # Initialize BigQuery client inside the function
                client = bigquery.Client()
                query_job = client.query(query)
                result = query_job.result().to_dataframe()

                if result.empty:
                    warning_msg = f"<users/payal.soni@1finance.co.in> Warning: Query '{query_name}' returned no data."
                    print(warning_msg)
                    send_notification2(warning_msg)
                    return "NA"
                
                return result.iloc[0][0]
            except Exception as e:
                error_msg = f"<users/payal.soni@1finance.co.in> Error executing query '{query_name}': {e}"
                print(error_msg)
                send_notification2(error_msg)
                return "NA"

        installs_total = fetch_single_result(query_for_installs, "Installs Total")
        ios_installs = fetch_single_result(query_for_ios_installs, "iOS Installs")
        android_installs = fetch_single_result(query_for_android_installs, "Android Installs")

        registration_total = fetch_single_result(query_for_registration, "Registration Total")

        same_day = fetch_single_result(query_for_sameday, "Sameday Total")
        spill_over = registration_total-same_day

        money_sign_complete = fetch_single_result(query_for_money_sign_complete, "MoneySign Complete")
        ria_signed = fetch_single_result(query_for_ria_signed, "RIA Signed")
        # payment_done = fetch_single_result(query_for_payment_done, "Payment Done")

        uninstalls = fetch_single_result(query_for_uninstalls, "Uninstalls")

        # users_v2_4_0 = fetch_single_result(query_for_users_v2_4_0, "Users v2.4.0")
        users_v2_4_1 = fetch_single_result(query_for_users_v2_4_1, "Users v2.4.1")

        users_v2_5_0 = fetch_single_result(query_for_users_v2_5_0, "Users v2.5.0")
        users_v2_5_1 = fetch_single_result(query_for_users_v2_5_1, "Users v2.5.1")

        users_v2_5_2 = fetch_single_result(query_for_users_v2_5_2, "Users v2.5.2")
        added_in_v2_5_2 = fetch_single_result(query_for_added_in_v2_5_2, "Added in v2.5.2")
        installs_v2_5_2 = fetch_single_result(query_for_installs_v2_5_2, "Installs v2.5.2")
        upgrades_v2_5_2 = fetch_single_result(query_for_upgrades_v2_5_2, "Upgrades v2.5.2")

        all_versions = fetch_single_result(query_for_all_versions, "All Versions")
        # Date for the report
        report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        report_date_2 = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        report_date_3 = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')

        ins_to_reg = round(((registration_total)/installs_total) * 100)
        same_day_per = round(((same_day)/installs_total) * 100)
        spill_over_per = round(((spill_over)/registration_total) * 100)
        moneysign_per = round(((money_sign_complete)/registration_total) * 100)

        # Generate the message
        message = (
    f"Adoption Report for 3 days:- "
    f"{report_date}, {report_date_2}, {report_date_3}\n"
    "------------------------------\n"
    f"Installs = {installs_total}\n"
    f"iOS = {ios_installs} | Android = {android_installs}\n"
    "------------------------------\n"
    f"Registration = {registration_total}\n"
    # f"A = {registration_a} | B = {registration_b}\n"
    f"Same day = {same_day} ({same_day_per}%) | Spill-over = {spill_over}\n"
    "------------------------------\n"
    f"MoneySign Complete = {money_sign_complete} ({moneysign_per}%)\n"
    f"RIA Signed = {ria_signed}\n"
    # f"Payment Done = {payment_done}\n"
    "------------------------------\n"
    f"Uninstalls = {uninstalls}\n"
    "------------------------------\n"
    f"Total Users on v2.4.1 = {users_v2_4_1}\n"
    "------------------------------\n"
    f"Total Users on v2.5.0 = {users_v2_5_0}\n"
    "------------------------------\n"
    # (+{added_in_v2_5_0})\n"  # Updated line
    # f"A = {users_v2_5_0_a} | B = {users_v2_5_0_b}\n"
    f"Total Users on v2.5.1 = {users_v2_5_1}\n"
    "------------------------------\n"
    f"Total Users on v2.5.2 = {users_v2_5_2} (+{added_in_v2_5_2})\n"
    f"From Installs = {installs_v2_5_2}\n"
    f"From Upgrades = {upgrades_v2_5_2}\n"
    "------------------------------\n"
    f"2.5.0, 2.5.1, 2.5.2= {all_versions}"
)

        # Print the formatted message
        print(message)

        send_notification(message)


    except Exception as e:
        print(f"An Error Occurred in adoption etl : {e}")
        message = f"Hello <users/payal.soni@1finance.co.in> An Error Occurred in adoption etl : {e}"
        send_notification2(message)

    return "Adoption ETL Executed!"




adoption_etl = PythonOperator(
        task_id='adoption_etl',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

adoption_etl