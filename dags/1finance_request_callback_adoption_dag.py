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
    'start_date': datetime(2024, 10, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    '1finance_callback_request_dag',
    default_args=default_args,
    description='1finance_callback_adoption data',
    schedule_interval='10 4 * * *',
    catchup=False
)

today = date.today()
# Yesterday date
yesterday = today - timedelta(days = 1)

client = bigquery.Client()

def send_notification(message):
    chat_webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAmtOc65Y/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EQRD33KYG-xWnOeIAwj-WJoY8EqL4wa_vgMYOd6iqq8'
    # chat_webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAzGKojWU/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=TNZXIHhFQtGYRvW1yBcKPFyQoS0mMWOqgEiQtQPGTb4'
    test = "https://chat.googleapis.com/v1/spaces/AAQA0Qv-LWI/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=zGAVl4m-MrC_iWOxM4E7D8UyIEA33vd6P_7CsYjvCYg"
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    data = {
        'text': message
    }
    response = requests.post(chat_webhook_url, headers=headers, data=json.dumps(data))
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
        # List of requests to query
        requests_list = [
        "Website - Adoption",
        "Website - Commission",
        "Website - FPC",
        "Website - Investment Pillar",
        "Website - NPS",
        "Website - Overlap",
        "Website - ScoringRankingMutualFund",
        "Website - Will&EstatePillar",
        "Website-IncomeExpensePillar",
        "Home_Page_CTA_B",
        "Website - CreditCard1st",
        "Website - ITR",
        "Website - Retirement Planning",
        "Website - Surrender",
        "Website - TermComparePage",
        "Website-FloatingRateForm",
        "Website - Health Insurance DetailPage",
        "Website - Health Insurance Home",
        "Website-InsurancePillar",
        "Website - CreditCardCompare",
        "Website - HomeLoanCompareGetinTouch",
        "Website - AdvanceTaxCalculator",
        "Website - CreditCard",
        "Website - HomeLoan1stGetInTouchNow",
        "Website - HomeLoan2ndGetInTouchNow",
        "Website - LoanPrepaymentCalculator",
        "Website - MacroEconomic Outlook Blog",
        "Website - OldVsNewCalculator",
        "Website - ScoringRankingNPS_1stTalkToFinancialAdvisor",
        "Website - ScoringRankingNPS_2ndTalkToFinancialAdvisor",
        "Website - ScoringRankingNpsLanding",
        "Website - Tax Pillar",
        "Website - WebStories",
        "Website-FloatingRateFormMobile",
        "Website-IncomeTaxPillar",
        "Website-LoanPillar",
        "Website - HomeLoan_2ndTalktoFinancialAdvisor",
        "Website - ScoringRanking",
        "Website - ScoringRankingHealthInsurance-FilterOption",
        "Website - findLendersCTAgetInTouch"
        ]

        # Dictionary for custom request names, with new mappings added
        custom_names = {
        "Website - Adoption":'Homepage',
        "Website - Commission":'Mutual Fund Commission',
        "Website - FPC":'FPC',
        "Website - Investment Pillar":'Investment Pillar',
        "Website - NPS":'NPS',
        "Website - Overlap":'Mutual Fund Overlap',
        "Website - ScoringRankingMutualFund":'S&R Mutual Fund',
        "Website - Will&EstatePillar":'Will&EstatePillar',
        "Website-IncomeExpensePillar":'IncomeExpensePillar',
        "Home_Page_CTA_B":'Home_Page_CTA_B',
        "Website - CreditCard1st":'CreditCard1st',
        "Website - ITR":'ITR',
        "Website - Retirement Planning":'Retirement Planning',
        "Website - Surrender":'Surrender',
        "Website - TermComparePage":'TermComparePage',
        "Website-FloatingRateForm":'FloatingRateForm',
        "Website - Health Insurance DetailPage":'Health Insurance DetailPage',
        "Website - Health Insurance Home":'Health Insurance Home',
        "Website-InsurancePillar":'InsurancePillar',
        "Website - CreditCardCompare":'CreditCardCompare',
        "Website - HomeLoanCompareGetinTouch":'HomeLoanCompareGetinTouch',
        "Website - AdvanceTaxCalculator":'AdvanceTaxCalculator',
        "Website - CreditCard":'CreditCard',
        "Website - HomeLoan1stGetInTouchNow":'HomeLoan1stGetInTouchNow',
        "Website - HomeLoan2ndGetInTouchNow":'HomeLoan2ndGetInTouchNow',
        "Website - LoanPrepaymentCalculator":'LoanPrepaymentCalculator',
        "Website - MacroEconomic Outlook Blog":'MacroEconomic Outlook Blog',
        "Website - OldVsNewCalculator":'OldVsNewCalculator',
        "Website - ScoringRankingNPS_1stTalkToFinancialAdvisor":'ScoringRankingNPS_1stTalkToFinancialAdvisor',
        "Website - ScoringRankingNPS_2ndTalkToFinancialAdvisor":'ScoringRankingNPS_2ndTalkToFinancialAdvisor',
        "Website - ScoringRankingNpsLanding":'ScoringRankingNpsLanding',
        "Website - Tax Pillar":'Tax Pillar',
        "Website - WebStories":'WebStories',
        "Website-FloatingRateFormMobile":'FloatingRateFormMobile',
        "Website-IncomeTaxPillar":'IncomeTaxPillar',
        "Website-LoanPillar":'LoanPillar',
        "Website - HomeLoan_2ndTalktoFinancialAdvisor":'HomeLoan_2ndTalktoFinancialAdvisor',
        "Website - ScoringRanking":'ScoringRanking',
        "Website - ScoringRankingHealthInsurance-FilterOption":'ScoringRankingHealthInsurance-FilterOption',
        "Website - findLendersCTAgetInTouch":'findLendersCTAgetInTouch'     
        }

        counts = {request: 0 for request in requests_list}
        # Define SQL query to get counts for all requests
        query = """
        WITH cte AS (
            SELECT a.created_at, 
                SPLIT(response, ',')[SAFE_OFFSET(1)] AS Name, 
                SPLIT(response, ',')[SAFE_OFFSET(0)] AS Mobile, 
                a.request, 
                a.customer_code, 
                b.Customer_name 
            FROM `analytics-1f.Live_eos_data.customer_data` a 
            LEFT JOIN `analytics-1f.MasterDB.1_source_t-1` b   
                ON a.customer_code = b.customer_ID
            WHERE category_id = '151'
            AND request IN UNNEST(@requests)  -- Bind the list of requests here
            AND DATE(a.created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            AND b.Internal_Test_Tagging IS NULL
            AND b.Customer_name IS NULL
        )
        SELECT request, COUNT(DISTINCT(Mobile)) AS count
        FROM cte
        WHERE Name NOT LIKE '%test%' 
        AND Name NOT LIKE '%TEST%' 
        AND Name NOT LIKE '%Test%' 
        AND Name NOT LIKE '%dvvfd%' 
        AND Name NOT LIKE '%website%'
        GROUP BY request;
        """

        # Configure query parameters
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("requests", "STRING", requests_list)
            ]
        )

        # Execute the query
        query_job = client.query(query, job_config=job_config)
        result = query_job.result()

        # Process query results
        for row in result:
            request = row.request
            count = row.count
            counts[request] = count  # Update the count for each request

        # Initialize variables to store total count and message
        total_count = 0
        message = f"Callback Adoption data for {yesterday}:\n----------------------------------------\n"

        # Iterate over all requests and build the message
        for request in requests_list:
            count = counts.get(request, 0)
            
            # Apply custom naming if applicable, otherwise remove "Website" prefix
            clean_request_name = custom_names.get(request, request.replace("Website - ", ""))
            
            # Append to the message
            message += f"{clean_request_name} = {count}\n"
            
            # Add to the total count
            total_count += count


        # Append total count to the message
        message += "----------------------------------------"
        message += f"\nTotal = {total_count}"

        # Print the message
        print(message)        
        send_notification(message)


    except Exception as e:
        print(f"An Error Occurred : {e}")
        message = f"An Error Occurred : {e}"
        send_notification2(message)


    return "Adoption ETL Executed!"




callback_request_etl = PythonOperator(
        task_id='callback_request_etl',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

callback_request_etl