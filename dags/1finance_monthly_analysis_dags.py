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
    'start_date': datetime(2025, 2, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    '1finance_monthly_analysis_dag',
    default_args=default_args,
    description='1finance_monthly_analysis_dag data',
    schedule_interval='30 4 1 * *',
    catchup=False
)

client = bigquery.Client()

def send_notification(message):
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


def send_notification2(message):
    # chat_webhook_adoption = 'https://chat.googleapis.com/v1/spaces/AAAAmtOc65Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EQRD33KYG-xWnOeIAwj-WJoY8EqL4wa_vgMYOd6iqq8'
    chat_webhook_test = 'https://chat.googleapis.com/v1/spaces/AAAAzGKojWU/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=TNZXIHhFQtGYRvW1yBcKPFyQoS0mMWOqgEiQtQPGTb4'
     # chat_webhook_ng = 'https://chat.googleapis.com/v1/spaces/AAAAWezURKM/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=WAHyjSk0u4Ejvpo3TeqittzEjkUAvASoeeKdY3flYvY'    
   
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
                        "email": "payal.soni@1finance.co.in"
                    },
                    "type": "ADD"
                }
            }
        ]
    }
    response = requests.post(chat_webhook_test, headers=headers, data=json.dumps(data))
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
            and date(install_date) = DATE_SUB(CURRENT_DATE(), interval 1 DAY)
        """

        query_for_ios_installs = """
            select count(distinct v.appsflyer_ID) from `AppsflyerDB.Installs-v2` v left join `MasterDB.1_source_t-1` m on v.AppsFlyer_ID = m.appsflyer_id
            where date(v.install_date) = DATE_SUB(CURRENT_DATE(), interval 1 DAY) and 
            m.Internal_Test_Tagging is null and v.platform = 'ios'

        """

        query_for_android_installs = """
            select count(distinct v.appsflyer_ID)  from `AppsflyerDB.Installs-v2` v left join `MasterDB.1_source_t-1` m on v.AppsFlyer_ID = m.appsflyer_id
            where date(v.install_date) = DATE_SUB(CURRENT_DATE(), interval 1 DAY) and 
            m.Internal_Test_Tagging is null and v.platform = 'android'
        """

        query_for_registration = """
            select count( distinct customer_ID ) from `MasterDB.1_source_t-1` where date(Registration_date) = DATE_SUB(CURRENT_DATE(), interval 1 DAY)
            and Internal_Test_Tagging is null 
        """
        query_for_sameday = """
            SELECT
            COUNT( DISTINCT customer_ID )
            FROM
            `MasterDB.1_source_t-1`
            WHERE
            DATE(Registration_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            AND Internal_Test_Tagging IS NULL
            AND DATE(install_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)       
        """

        query_for_money_sign_complete = """
            select  count(distinct customer_ID)  from `MasterDB.1_source_t-1` where date(MoneySignGeneratedDate) = DATE_SUB(CURRENT_DATE(), interval 1 DAY)
            and Internal_Test_Tagging is null 
        """

        query_for_ria_signed = """
            select count(distinct customer_ID) from `MasterDB.1_source_t-1` where 
            RIA_Signed_Date is not null and 
            date(RIA_Signed_Date)= DATE_SUB(CURRENT_DATE(), interval 1 DAY)  and Internal_Test_Tagging is null;
        """

        query_for_payment_done = """
            with cte as (select Payment.Payment_Date as Transaction_date,Payment.c_key,Payment.transaction_id,Payment.user_code,member_id as RIA_Member_ID,Pm.Customer_name,Internal_Test_Tagging,Payment.amount,Payment.source,Payment.invoice_doc_link,Payment.description,Payment.transaction_type,
            Payment.payment_status,
            Current_Cycle,
            status as is_refund,
            DATE_ADD(FWP_Sent_Date, INTERVAL 85 DAY) AS Eligible_Date
            from
            (select DATE(TIMESTAMP (created_at), "Asia/Kolkata")  as Payment_Date,case when source like '%Phonepe%' then merchant_transaction_id else transaction_id end as c_key,user_code,transaction_id,amount,source,invoice_doc_link,description,transaction_type,payment_status,null as status from `EosDB.payment_transactions`
            where is_active and is_success and transaction_type= 'Credit' and transaction_id is not null
            and payment_status = 'COMPLETED'
            and amount>0
            union all
            select DATE(TIMESTAMP (created_at), "Asia/Kolkata") as Payment_Date,case when source like '%Phonepe%' and merchant_transaction_id like '%REF%' then REGEXP_EXTRACT(merchant_transaction_id, r'[^-]-([^ ])') else transaction_id end as c_key,user_code,transaction_id,amount,source,invoice_doc_link,description,transaction_type,payment_status,'Yes'as status from `EosDB.payment_transactions`
            where is_active and is_success and transaction_type= 'Debit' and transaction_id is not null
            and payment_status = 'COMPLETED'
            and amount>0)
            as Payment
            left join `analytics-1f.MasterDB.1_source_t-1` as mb on Payment.user_code = mb.customer_ID
            left join `MasterDB.Payment_master` as pm on Payment.user_code = pm.Customer_id
            where Internal_Test_Tagging  is null  and member_id  not  in ('1F2400799', '1FAC230105' , '1FAB230103')
            and status is null and pm.is_refund = false and Payment.Payment_Date = DATE_SUB(CURRENT_DATE(), interval  1 DAY) ),
            cte2 as (
            select distinct customer_ID as ID, mobile_number as mn  from `MasterDB.Payment_master` where payment_status = 'COMPLETED'
            and Test_internal is null  and is_refund =false
            )
            select  count(distinct user_code)
            from cte left join cte2 on cte.user_code = cte2.ID
            where cte2.mn not in (select cast(contact as string) from `MasterDB.internal_test_user_latest`  where contact is not null)

        """

        query_for_uninstalls = """
        select count(distinct v.appsflyer_ID)  from `AppsflyerDB.actual_uninstalls` v left join `MasterDB.1_source_t-1` m on v.AppsFlyer_ID = m.appsflyer_id  
        where date(v.Event_date) = DATE_SUB(CURRENT_DATE(), interval 1 DAY)
        and m.Internal_Test_Tagging is null ;
        """

        # query_for_users_v2_4_0 = """
        #     select count(distinct customer_ID) from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null 
        #     and System_Status ='Live'  and app_version ='2.4.0'
        #     and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
        # """

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

    
        query_for_installs_v2_5_0 = """
            select  concat( count(distinct customer_ID ), ' ' , '(+' , (
            select count(distinct customer_ID ) from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null
            and System_Status ='Live'
            and app_version ='2.5.0'
            and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
            and date(install_date) =DATE_SUB(CURRENT_DATE(), interval 1 DAY)
            ) , ')') from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null
            and System_Status ='Live'
            and app_version ='2.5.0'
            and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
            and install_date >= '2024-10-30'

        """

        query_for_upgrades_v2_5_0 = """
            with final_main as (with main as (
            WITH updated AS (
                SELECT DISTINCT appsflyer_id,
                    MIN(event_time) AS updated_date
                FROM `analytics-1f.AppsflyerDB.in_app_events_no_test_v4`
                WHERE app_version = "2.5.0"
                GROUP BY appsflyer_id
            ),
            current_version AS (
                SELECT DISTINCT appsflyer_id,
                    app_version
                FROM `analytics-1f.AppsflyerDB.in_app_events_no_test_v4`
                WHERE app_version = "2.5.0"
            ),
            previous_version AS (
                SELECT appsflyer_id,
                    MAX(app_version) AS updated_from
                FROM `analytics-1f.AppsflyerDB.in_app_events_no_test_v4`
                WHERE app_version < "2.5.0"
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
            and system_status = 'Live' and date(updated_date) = DATE_SUB(CURRENT_DATE(), interval 1 DAY) )
            select concat((select * from one) , '' ,'(','+', (select * from two), ')')

        """
        query_for_added_in_v2_5_0 = """
        with upg as (
        with final_main as (with main as (
        WITH updated AS (
            SELECT DISTINCT appsflyer_id,
                MIN(event_time) AS updated_date
            FROM `analytics-1f.AppsflyerDB.in_app_events_no_test_v4`
            WHERE app_version = "2.5.0"
            GROUP BY appsflyer_id
        ),
        current_version AS (
            SELECT DISTINCT appsflyer_id,
                app_version
            FROM `analytics-1f.AppsflyerDB.in_app_events_no_test_v4`
            WHERE app_version = "2.5.0"
        ),
        previous_version AS (
            SELECT appsflyer_id,
                MAX(app_version) AS updated_from
            FROM `analytics-1f.AppsflyerDB.in_app_events_no_test_v4`
            WHERE app_version < "2.5.0"
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
        and system_status = 'Live' and date(updated_date) = DATE_SUB(CURRENT_DATE(), interval 1 DAY) )
        select *  from two ) ,
        ins as (
        select count(distinct customer_ID ) from `MasterDB.1_source_t-1` where Internal_Test_Tagging is null
        and System_Status ='Live'
        and app_version ='2.5.0'
        and appsflyer_id Not in ( select distinct appsflyer_ID from `AppsflyerDB.actual_uninstalls` where appsflyer_ID is not null)
        and date(install_date) =DATE_SUB(CURRENT_DATE(), interval 1 DAY)
        )
        select cast((select * from ins) as int64) +  cast((select * from upg) as int64)

        """
        query_for_all_versions = """
        SELECT
        COUNT(DISTINCT customer_ID)
        FROM
        `MasterDB.1_source_t-1`
        WHERE
        app_version IN ('2.4.1',
            '2.5.0')
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
        installs_last_month_query = """
with base as (select distinct appsflyer_ID , install_date from `AppsflyerDB.Installs-v2` ),
test as (
  select distinct appsflyer_ID ,Internal_Test_Tagging  from `MasterDB.1_source_t-1`
where Internal_Test_Tagging is not null  and appsflyer_ID is not null
)

select count(distinct base.appsflyer_ID) from base left join test on base.appsflyer_ID = test.appsflyer_ID where test.appsflyer_ID is null
and date(install_date) BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH) 
                             AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH)  """

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
        # ios_installs = fetch_single_result(query_for_ios_installs, "iOS Installs")
        # android_installs = fetch_single_result(query_for_android_installs, "Android Installs")

        registration_total = fetch_single_result(query_for_registration, "Registration Total")
        # # registration_a = fetch_single_result(query_for_registration_a, "Registration A")
        # # registration_b = fetch_single_result(query_for_registration_b, "Registration B")
        # same_day = fetch_single_result(query_for_sameday, "Sameday Total")

        installs_last_month = fetch_single_result(installs_last_month_query, "Install Last Month")

        # money_sign_complete = fetch_single_result(query_for_money_sign_complete, "MoneySign Complete")
        # ria_signed = fetch_single_result(query_for_ria_signed, "RIA Signed")
        # payment_done = fetch_single_result(query_for_payment_done, "Payment Done")

        # uninstalls = fetch_single_result(query_for_uninstalls, "Uninstalls")

        # # users_v2_4_0 = fetch_single_result(query_for_users_v2_4_0, "Users v2.4.0")
        # users_v2_4_1 = fetch_single_result(query_for_users_v2_4_1, "Users v2.4.1")

        # users_v2_5_0 = fetch_single_result(query_for_users_v2_5_0, "Users v2.5.0")
        # # users_v2_5_0_a = fetch_single_result(query_for_users_v2_5_0_a, "Users v2.5.0 A")
        # # users_v2_5_0_b = fetch_single_result(query_for_users_v2_5_0_b, "Users v2.5.0 B")

        # installs_v2_5_0 = fetch_single_result(query_for_installs_v2_5_0, "Installs v2.5.0")
        # upgrades_v2_5_0 = fetch_single_result(query_for_upgrades_v2_5_0, "Upgrades v2.5.0")

        # added_in_v2_5_0 = fetch_single_result(query_for_added_in_v2_5_0, "Added in v2.5.0")
        # all_versions = fetch_single_result(query_for_all_versions, "All Versions")
        # # Date for the report
        report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        ins_to_reg = round(((registration_total)/installs_total) * 100)
        # same_day_per = round(((same_day)/installs_total) * 100)
        # moneysign_per = round(((money_sign_complete)/registration_total) * 100)

        # Generate the message
        message = (
    f"{report_date}\n"
    "------------------------------\n"
    f"Installs = {installs_total}\n"
    f"installs_last_month = {installs_last_month}"
    # f"iOS = {ios_installs} | Android = {android_installs}\n"
    # "------------------------------\n"
    # f"Registration = {registration_total} ({ins_to_reg}%)\n"
    # # f"A = {registration_a} | B = {registration_b}\n"
    # f"Same day = {same_day} ({same_day_per}%)\n"
    # "------------------------------\n"
    # f"MoneySign Complete = {money_sign_complete} ({moneysign_per}%)\n"
    # f"RIA Signed = {ria_signed}\n"
    # f"Payment Done = {payment_done}\n"
    # "------------------------------\n"
    # f"Uninstalls = {uninstalls}\n"
    # "------------------------------\n"
    # f"Total Users on v2.4.1 = {users_v2_4_1}\n"
    # "------------------------------\n"
    # f"Total Users on v2.5.0 = {users_v2_5_0} (+{added_in_v2_5_0})\n"  # Updated line
    # # f"A = {users_v2_5_0_a} | B = {users_v2_5_0_b}\n"
    # f"From Installs = {installs_v2_5_0}\n"
    # f"From Upgrades = {upgrades_v2_5_0}\n"
    # "------------------------------\n"
    # f"2.4.1 and 2.5.0 = {all_versions}"
)

        # Print the formatted message
        print(message)

        # message = f"{yesterday}\n------------------------------\nInstalls = {installs}\niOS = {installs_ios} | Android = {installs_android}\n------------------------------\nRegistration = {registrations}\niOS = {registrations_ios} | Android = {registrations_android}\n------------------------------\nMoneySign Complete = {moneysign}\nRIA Signed = {riasigned}\nPayment Done = {payments}\n------------------------------\nUninstalls = {uninstalls}\n------------------------------\nTotal Users on v2.4.0 = {totalusers240}\n------------------------------\nTotal Users on v2.4.1={totaluserslatest}\nFrom Installs = {latestinstalls}\nFrom Upgrades = {upgrades}"
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