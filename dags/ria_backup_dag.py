from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import os
import psycopg2
import pandas as pd
import os
import json
import requests
from datetime import datetime, timedelta


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'ria_backup_dag',
    default_args=default_args,
    description='ria_backup',
    schedule_interval='00 1 * * *',
    catchup = False
)
# BigQuery credentials
PROJECT_ID = 'analytics-1f'
DATASET_ID = 'MasterDB'


# yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
# filter_condition = f"DATE(`Member ID Generated Date`) = DATE '{yesterday}'"
# print(filter_condition)

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

def main():

    try:
        
        table_id = f"{DATASET_ID}.ria_backup"
        print("Loading Table {}".format(table_id))
        query = """
            WITH
            netcore AS ( 
            WITH
                RankedEmailLog AS ( 
                SELECT
                email,
                log_type,
                created_at,
                netcore_status,
                ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at DESC) AS rn
                FROM (
                SELECT
                    DISTINCT email,
                    log_type,
                    created_at,
                    CASE
                    WHEN netcore_status LIKE '%sent%' OR netcore_status LIKE '%opened%' THEN 'Sent'
                    ELSE
                    netcore_status
                END
                    AS netcore_status
                FROM
                    `NetCoreDB.email_log`
                WHERE
                    subject LIKE '%Important: Welcome to 1 Finance%'
                    AND log_type = 'Response'
                    AND netcore_status IN ('sent',
                    'opened',
                    'bounced')
                UNION ALL
                SELECT
                    DISTINCT email,
                    log_type,
                    created_at,
                    'Sent' AS netcore_status
                FROM (
                    SELECT
                    CASE email
                        WHEN 'abhijit.korti@gmail.com' THEN 'ankit.shah.17041988@gmail.com'
                        WHEN 'ruthvik.salunke@atriina.com' THEN 'v8x5qdpxfw@privaterelay.appleid.com'
                        WHEN 'kale.ashutosh@gmail.com' THEN 'anuragk.kendrya2@gmail.com'
                        WHEN 'peje.vinayak@gmail.com' THEN '9kc9ddvzqg@privaterelay.appleid.com'
                        WHEN 'shameer7087.growth@gmail.com' THEN 'arjunrn1991@gmail.com'
                        WHEN 'cool1982@gmail.com' THEN 'Ykudupu@yahoo.co.in'
                    ELSE
                    NULL
                    END
                    AS email,
                    log_type,
                    netcore_status,
                    created_at
                    FROM
                    `NetCoreDB.email_log`
                    WHERE
                    subject LIKE 'Important: Welcome to 1 Finance Member Community!'
                    AND log_type = 'Response'
                    AND netcore_status = 'sent' )
                WHERE
                    email IS NOT NULL ) )
            SELECT
                email,
                log_type,
                netcore_status
            FROM 
                RankedEmailLog 
            WHERE 
                rn = 1), 
            ria AS ( 
            WITH 
                RIA AS ( 
                WITH 
                cte AS ( 
                SELECT 
                    DISTINCT(profile.user_code), 
                    member_id,
                    pan_name,
                    mobile_number,
                    email,
                    pan_no,
                    dob.dob,
                    address_line_1,
                    address_line_2,
                    address_line_3,
                    address_proof,
                    address_proof_ref,
                    address_proof_date,
                    dob.city,
                    dob.pincode,
                    dob.state
                FROM
                    `LakeMaster.customer_profile` AS profile
                LEFT JOIN (
                    SELECT
                    DISTINCT(details.user_code),
                    dob,
                    address_line_1,
                    address_line_2,
                    address_line_3,
                    city,
                    pincode,
                    state,
                    address_proof,
                    address_proof_ref,
                    address_proof_date
                    FROM
                    `LakeMaster.user_verified_details` AS details
                    LEFT JOIN (
                    SELECT
                        DISTINCT(user_code),
                        address_line_1,
                        address_line_2,
                        address_line_3,
                        city,
                        pincode,
                        state,
                        CASE
                        WHEN address_proof LIKE '%Aadhaar%' OR address_proof LIKE '%AADHAAR%' OR address_proof LIKE '%E-KYC%' THEN 'Aadhaar'
                        WHEN address_proof LIKE '%VOTER IDENTITY%'
                        OR address_proof LIKE '%Voters Identity%' THEN 'Voter ID'
                        ELSE
                        address_proof
                    END
                        AS address_proof,
                        address_proof_ref,
                        address_proof_date
                    FROM
                        `LakeMaster.user_address`
                    WHERE
                        (address_type = 'correspondence'
                        OR address_type = 'correspondence - KRA')
                        AND is_active = TRUE ) AS address
                    ON
                    details.user_code = address.user_code
                    WHERE
                    details.is_active = TRUE
                    ORDER BY
                    user_code ) AS dob
                ON
                    profile.user_code = dob.user_code
                WHERE
                    member_id IS NOT NULL )
                SELECT
                DATE(Esign_Date) AS agreement_Execution_Date,
                cte.user_code AS `Customer ID`,
                cte.member_id AS `Member Code`,
                cte.pan_name AS `Name as per PAN`,
                CASE
                    WHEN cte.mobile_number LIKE '%D%' THEN 'Deleted'
                ELSE
                'Active'
                END
                AS Status,
                cte.mobile_number AS `Mobile Number Verified`,
                cte.email AS `Verified email id`,
                cte.dob AS `DOB`,
                cte.pan_no AS `Pan Number`,
                agreement.parameters AS `Signed Agreement Link`,
                cte.address_line_1 AS `Address line 1`,
                cte.address_line_2 AS `Address line 2`,
                cte.address_line_3 AS `Address line 3`,
                CASE
                    WHEN cte.address_proof LIKE '%Aadhaar%' OR cte.address_proof LIKE '%AADHAAR%' OR cte.address_proof LIKE '%E-KYC%' THEN 'Aadhaar'
                    WHEN cte.address_proof LIKE '%VOTER IDENTITY%'
                OR cte.address_proof LIKE '%Voters Identity%' THEN 'Voter ID'
                ELSE
                cte.address_proof
                END
                AS `Address Proof Document`,
                address_proof_ref AS `Address Proof ref`,
                CASE
                    WHEN cte.address_proof IN ('Aadhaar', 'Voter ID') THEN NULL
                ELSE
                address_proof_date
                END
                AS `Address Proof Date`,
                cte.city AS `City`,
                cte.pincode AS `Pincode`,
                cte.state AS `State`,
                netcore.netcore_status AS `Welcome Mail`
                FROM
                cte
                LEFT JOIN (
                SELECT
                    customer_code,
                    Esign_Date,
                    parameters
                FROM (
                    SELECT
                    customer_code,
                    created_at AS Esign_Date,
                    parameters,
                    ROW_NUMBER() OVER (PARTITION BY customer_code ORDER BY created_at DESC) AS rn
                    FROM
                    `EosDB.customer_data`
                    WHERE
                    is_active = TRUE
                    AND category_id IN (57,
                        59) ) subquery
                WHERE
                    rn = 1
                ORDER BY
                    customer_code ) AS agreement
                ON
                cte.user_code = agreement.customer_code
                LEFT JOIN
                netcore
                ON
                cte.email = netcore.email
                ORDER BY
                DATE(Esign_Date))
            SELECT
                * EXCEPT(`Welcome Mail`,
                user_id,
                user_code),
                CASE
                WHEN `Welcome Mail` IS NULL THEN 'Not Sent'
                ELSE
                `Welcome Mail`
            END
                AS `Welcome Mail`
            FROM
                RIA
            LEFT JOIN (
                SELECT
                user_id,
                `FWP Booking Date`,
                Meeting_date
                FROM (
                SELECT
                    DISTINCT(meeting_scheduled_by) AS user_id,
                    DATE(created_at) AS `FWP Booking Date`,
                    DATE(start_time)AS Meeting_date,
                    ROW_NUMBER() OVER (PARTITION BY meeting_scheduled_by ORDER BY created_at DESC) AS rn
                FROM
                    `analytics-1f.EosDB.user_calendar_booking`
                WHERE
                    is_cancelled = FALSE
                    AND is_rescheduled = FALSE
                    AND meeting_name IN ('Financial Plan Consultation',
                    'Financial Wellness Plan presentation',
                    'Financial Wellness Plan presentation',
                    'Consultation with Financial Concierge Specialist',
                    'Quarterly Review with FC Specialist - Yash Chheda'))
                WHERE
                rn = 1 ) AS meeting
            ON
                RIA.`Customer ID` = meeting.user_id
            LEFT JOIN
            `analytics-1f.MasterDB.internal_test_user` AS Test
            ON
            RIA.`Mobile Number Verified` = Test.Contact
            LEFT JOIN (
                SELECT
                user_code,
                alert_count AS `PMLA Status`
                FROM
                `analytics-1f.LakeMaster.user_pmla_details`) AS pmla
            ON
                RIA.`Customer ID` = pmla.user_code
            ORDER BY
                agreement_Execution_Date)
            SELECT
            DISTINCT (`Customer ID`),
            CASE
                WHEN Member_id.MemberID_Generated_Date IS NULL THEN
                    agreement_Execution_Date
                ELSE
                    Member_id.MemberID_Generated_Date
            END AS `Member id Generated Date`,
            `Member Code`,
            `Name as per PAN`,
            case when Remark is not null then concat(Remark,' ',Status) else concat("Member",' ',Status) end as Status,
            `Mobile Number Verified`,
            `Verified email id`,
            `DOB`,
            `Pan Number`,
            `Signed Agreement Link`,
            `Address line 1`,
            `Address line 2`,
            `Address line 3`,
            `Address Proof Document`
            `Address Proof ref`,
            Resident.resident_status,
            Resident.nationality,
            `Address Proof Date`,
            `City`,
            `Pincode`,
            `State`,
            `FWP Booking Date`,
            `Meeting_date`,
            `PMLA Status`,
            `Welcome Mail`,
            Payment_link.invoice_doc_link as `Payment Link`
            FROM
            ria
            left join (select user_code,nationality,resident_status from `LakeMaster.user_verified_details` where resident_status is not null) as Resident on ria.`Customer ID` = Resident.user_code
            left join ((select distinct(customer_code),max(date(created_at)) as MemberID_Generated_Date from `EosDB.customer_data`
            where category_id = 64 group by 1)) as Member_id on ria.`Customer ID` = member_id.customer_code
            left join (SELECT user_code,amount,invoice_doc_link FROM `analytics-1f.EosDB.payment_transactions` where invoice_doc_link is not null and is_active) as Payment_link on ria.`Customer ID` = Payment_link.user_code
            where `Member Code` not in ('')
            ORDER BY
            `Member ID Generated Date`
        """

        df = pd.read_gbq(query, project_id='analytics-1f')
        print(df.head)
        df.columns=df.columns.str.replace(' ',"_")
        print(df.info())

        try:
             # Establish a connection to BigQuery
            client = bigquery.Client()
            # Load data into BigQuery
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_REPLACE")
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            print("Successfully Data Ingested {}".format(table_id))
            message = "Successfully ingested ria_backup data by Airflow"
            send_notification(message)
        except Exception as e:
            print("Error Loading Table {}".format(table_id))
            message = "Issues in ria_backup fn: " + str(e)
            send_notification(message)

        return "ETL Successful"
    except Exception as e:
        print("Error {}".format(e))
        message = "Issues in ria_backup fn: " + str(e)
        send_notification(message)
        return "ETL Failed"
    
ria_backup = PythonOperator(
        task_id='ria_backup',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

ria_backup

