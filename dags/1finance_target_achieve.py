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
    'start_date': datetime(2024, 8, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    '1finance_target_achieve',
    default_args=default_args,
    description='1finance_target_achieve',
    schedule_interval='20 4 * * *',
    catchup=False
)

today = date.today()
# Yesterday date
yesterday = today - timedelta(days = 1)

discovery_target = 1048
consultation_target = 801

client = bigquery.Client()

def send_notification(message):
    chat_webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAmtOc65Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EQRD33KYG-xWnOeIAwj-WJoY8EqL4wa_vgMYOd6iqq8'
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    data = {
        'text': message
    }
    response = requests.post(chat_webhook_url, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        print(f"Failed to send notification. Status code: {response.status_code}")
    else:
        print("Notification sent successfully.")

def run_query(query):
    client = bigquery.Client()
    query_job = client.query(query)
    results = query_job.result()
    return [row[0] for row in results]


def main():
    try:
        
        # Queries
        Q1 = """
        select count(distinct customer_ID) from `MasterDB.1_source_t-1` 
        where System_Status = 'Live'
        and Internal_Test_Tagging is null
        and Discovery_Meeting_Status = 'Completed'
        and date(Discovery_Appointment_Date) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
        """

        Q2 = """
        select count(distinct customer_ID) from `MasterDB.1_source_t-1` 
        where System_Status = 'Live'
        and Internal_Test_Tagging is null
        and FWP_Meeting_Status in ( 'Completed' , 'Not Interested' , 'Future Prospect' , 'completed') 
        and date(FWP_Appointment_Date) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
        """

        Q3 = """
        WITH ExcludedDates AS (
        SELECT date_d
        FROM UNNEST([
            DATE '2024-01-26', DATE '2024-03-08', DATE '2024-03-25', DATE '2024-03-29',
            DATE '2024-04-10', DATE '2024-04-14', DATE '2024-04-17', DATE '2024-04-21',
            DATE '2024-05-01', DATE '2024-06-17', DATE '2024-07-17', DATE '2024-08-15',
            DATE '2024-09-07', DATE '2024-10-02', DATE '2024-10-13', DATE '2024-11-01',
            DATE '2024-11-02', DATE '2024-11-15', DATE '2024-12-25' , DATE '2024-08-19'
        ]) AS date_d
        ),
        RemainingDays AS (
        SELECT 
            LAST_DAY(CURRENT_DATE()) AS end_of_month,
            CURRENT_DATE() AS start_date
        ),
        AllDaysInMonth AS (
        SELECT 
            DATE_ADD(start_date, INTERVAL n DAY) AS day
        FROM 
            RemainingDays,
            UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_of_month, start_date, DAY))) AS n
        )
        SELECT 
        COUNT(day) - COUNT(ExcludedDates.date_d) AS DaysLeft
        FROM 
        AllDaysInMonth
        LEFT JOIN 
        ExcludedDates
        ON 
        AllDaysInMonth.day = ExcludedDates.date_d
        WHERE 
        AllDaysInMonth.day BETWEEN CURRENT_DATE() AND LAST_DAY(CURRENT_DATE());
        """

        Q4 = f"""
        select {discovery_target} - (select count(distinct customer_ID) from `MasterDB.1_source_t-1` 
        where System_Status = 'Live'
        and Internal_Test_Tagging is null
        and Discovery_Meeting_Status = 'Completed'
        and date(Discovery_Appointment_Date) >= DATE_TRUNC(CURRENT_DATE(), MONTH))
        """

        Q5 = f"""
        with a as (
        select {discovery_target} - COUNT(DISTINCT customer_ID) AS total_count
        FROM `MasterDB.1_source_t-1`
        WHERE System_Status = 'Live'
            AND Internal_Test_Tagging IS NULL
            AND Discovery_Meeting_Status = 'Completed'
            AND DATE(Discovery_Appointment_Date) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
        ), 
        b as (
            WITH ExcludedDates AS (
        SELECT date_d
        FROM UNNEST([
            DATE '2024-01-26', DATE '2024-03-08', DATE '2024-03-25', DATE '2024-03-29',
            DATE '2024-04-10', DATE '2024-04-14', DATE '2024-04-17', DATE '2024-04-21',
            DATE '2024-05-01', DATE '2024-06-17', DATE '2024-07-17', DATE '2024-08-15',
            DATE '2024-09-07', DATE '2024-10-02', DATE '2024-10-13', DATE '2024-11-01',
            DATE '2024-11-02', DATE '2024-11-15', DATE '2024-12-25' , DATE '2024-08-19'
        ]) AS date_d
        ),
        RemainingDays AS (
        SELECT 
            LAST_DAY(CURRENT_DATE()) AS end_of_month,
            CURRENT_DATE() AS start_date
        ),
        AllDaysInMonth AS (
        SELECT 
            DATE_ADD(start_date, INTERVAL n DAY) AS day
        FROM 
            RemainingDays,
            UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_of_month, start_date, DAY))) AS n
        )
        SELECT 
        COUNT(day) - COUNT(ExcludedDates.date_d) AS DaysLeft
        FROM 
        AllDaysInMonth
        LEFT JOIN 
        ExcludedDates
        ON 
        AllDaysInMonth.day = ExcludedDates.date_d
        WHERE 
        AllDaysInMonth.day BETWEEN CURRENT_DATE() AND LAST_DAY(CURRENT_DATE())
        )
        select
        CAST(CEIL(CAST(a.total_count AS FLOAT64) / b.daysleft) AS INT64) as Difference
        FROM a, b;
        """

        Q6 = f"""
        select {consultation_target} - (select count(distinct customer_ID) from `MasterDB.1_source_t-1` 
        where System_Status = 'Live'
        and Internal_Test_Tagging is null
        and FWP_Meeting_Status in ( 'Completed' , 'Not Interested' , 'Future Prospect' , 'completed') 
        and date(FWP_Appointment_Date) >= DATE_TRUNC(CURRENT_DATE(), MONTH))
        """

        Q7 = f"""
        with a as (
        select {consultation_target} -  count(distinct customer_ID) as total_count 
        from `MasterDB.1_source_t-1` 
        where System_Status = 'Live'
        and Internal_Test_Tagging is null
        and FWP_Meeting_Status in ( 'Completed' , 'Not Interested' , 'Future Prospect' , 'completed') 
        and date(FWP_Appointment_Date) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
        ), 
        b as (
         WITH ExcludedDates AS (
        SELECT date_d
        FROM UNNEST([
            DATE '2024-01-26', DATE '2024-03-08', DATE '2024-03-25', DATE '2024-03-29',
            DATE '2024-04-10', DATE '2024-04-14', DATE '2024-04-17', DATE '2024-04-21',
            DATE '2024-05-01', DATE '2024-06-17', DATE '2024-07-17', DATE '2024-08-15',
            DATE '2024-09-07', DATE '2024-10-02', DATE '2024-10-13', DATE '2024-11-01',
            DATE '2024-11-02', DATE '2024-11-15', DATE '2024-12-25' , DATE '2024-08-19'
        ]) AS date_d
        ),
        RemainingDays AS (
        SELECT 
            LAST_DAY(CURRENT_DATE()) AS end_of_month,
            CURRENT_DATE() AS start_date
        ),
        AllDaysInMonth AS (
        SELECT 
            DATE_ADD(start_date, INTERVAL n DAY) AS day
        FROM 
            RemainingDays,
            UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_of_month, start_date, DAY))) AS n
        )
        SELECT 
        COUNT(day) - COUNT(ExcludedDates.date_d) AS DaysLeft
        FROM 
        AllDaysInMonth
        LEFT JOIN 
        ExcludedDates
        ON 
        AllDaysInMonth.day = ExcludedDates.date_d
        WHERE 
        AllDaysInMonth.day BETWEEN CURRENT_DATE() AND LAST_DAY(CURRENT_DATE())
        )
        select
        CAST(CEIL(CAST(a.total_count AS FLOAT64) / b.daysleft) AS INT64) as Difference
        FROM a, b;
        """

        # Running queries and storing the results
        discovery_completed = run_query(Q1)[0]
        consultation_completed = run_query(Q2)[0]
        days_left = run_query(Q3)[0]
        discovery_to_be_done = run_query(Q4)[0]
        discovery_asking_rate = run_query(Q5)[0]
        consultation_to_be_done = run_query(Q6)[0]
        consultation_asking_rate = run_query(Q7)[0]


        # Displaying the results
        message = (
            f"{today}\n"
            "-----------------------------------------\n"
            "Target to be achieved-\n"
            f"Discovery Meetings - {discovery_target}\n"
            f"Consultation meetings - {consultation_target}\n"
            "-----------------------------------------\n"
            f"Discovery Meetings Completed: {discovery_completed}\n"
            f"Consultation Meetings Completed: {consultation_completed}\n"
            "-----------------------------------------\n"
            f"Days Left: {days_left}\n"
            f"Discovery Meetings to be Done: {discovery_to_be_done}\n"
            f"Discovery Meetings Asking Rate: {discovery_asking_rate}\n"
            "-----------------------------------------\n"
            f"Consultation Meetings to be Done: {consultation_to_be_done}\n"
            f"Consultation Meetings Asking Rate: {consultation_asking_rate}"
        )
        print(message)
        
        send_notification(message)


    except Exception as e:
        print(f"An Error Occurred : {e}")
        message = f"An Error Occurred in target_achieve_etl: {e}"
        send_notification(message)

    return "target_achieve ETL Executed!"




target_achieve_etl = PythonOperator(
        task_id='target_achieve_etl',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

target_achieve_etl