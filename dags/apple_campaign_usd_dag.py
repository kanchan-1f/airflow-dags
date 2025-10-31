
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from airflow.utils.email import send_email
import requests
import pandas as pd
import os
import json
from google.cloud import bigquery
from google.cloud import secretmanager
from google.oauth2 import service_account
# import freecurrencyapi    

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'apple_campaign_usd_dag',
    default_args=default_args,
    description='Extract apple_campaign_dag data from Linkedin Campaign API and load into BigQuery like cloud fn',
    schedule_interval='50 1 * * *',
    catchup = False
)

project_id = 'analytics-1f'
dataset_id = 'CampaignsDB'
table_id = 'apple_campaigns_usd'

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

# def get_secret(secret_name, version_id='1'):
#     # project_id = os.getenv('GCP_PROJECT', '430695588957')  # Default to your project ID if the env var is not set
#     client = secretmanager.SecretManagerServiceClient()
#     name = f"projects/430695588957/secrets/{secret_name}/versions/{version_id}"
#     response = client.access_secret_version(name=name)
#     secret = response.payload.data.decode('UTF-8')
#     return secret


def main():
    try:
        # Retrieve the service account key from Secret Manager
        # secret_name = 'analytics_1f_secret'  # Use the secret name from Secret Manager
        # service_account_key = get_secret(secret_name)

        # Convert the service account key JSON string to a dictionary
        # service_account_info = json.loads(service_account_key)

        # Initialize the credentials with the service account info directly
        # credentials = service_account.Credentials.from_service_account_info(service_account_info)
        # print(credentials)



        token_url = "https://appleid.apple.com/auth/oauth2/token?"  # Replace with the actual token endpoint URL

        token_params = {
            "grant_type": "client_credentials",
            "client_id": "SEARCHADS.13892315-8f22-48e1-8939-0edd38ff1b90",
            "client_secret": "eyJhbGciOiJFUzI1NiIsImtpZCI6IjQ2YWNiOWYxLTM5YjgtNDNiNC05ZmNlLTRlNGM1NjliYzFkZSIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJTRUFSQ0hBRFMuMTM4OTIzMTUtOGYyMi00OGUxLTg5MzktMGVkZDM4ZmYxYjkwIiwiYXVkIjoiaHR0cHM6Ly9hcHBsZWlkLmFwcGxlLmNvbSIsImlhdCI6MTc1NDQwODQxOCwiZXhwIjoxNzY5OTYwNDE4LCJpc3MiOiJTRUFSQ0hBRFMuMTM4OTIzMTUtOGYyMi00OGUxLTg5MzktMGVkZDM4ZmYxYjkwIn0.KSjT_MAXehHMksm_s-kkrLFmuCBRGRn5qf6zefOcBr0IpxJ5p5uRPrF3Ot7hUQM4gueWn5OtzfThsKf1Ss3QJA",
            "scope":"searchadsorg"
        }

        response = requests.post(token_url, data=token_params)

        if response.status_code == 200:
            access_token = response.json().get("access_token")
            print("Access Token:", access_token)
        else:
            print("Error:", response.status_code, response.text)


        YOUR_ACCESS_TOKEN = access_token
        OrgId="7638080"


        # Define your API endpoint and authentication headers
        api_url = "https://api.searchads.apple.com/api/v5/reports/campaigns"
        headers = {
            "Authorization": f"Bearer {YOUR_ACCESS_TOKEN}",
            "X-AP-Context": f"orgId={OrgId}"
        }

        yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        print(yesterday)


        request_payload = {
                "startTime": yesterday,
                "endTime": yesterday,
                "selector": {
                "orderBy": [
                    {
                    "field": "countryOrRegion",
                    "sortOrder": "ASCENDING"
                    }
                ],
                "conditions": [
                    {
                    "field": "countriesOrRegions",
                    "operator": "CONTAINS_ANY",
                    "values": [
                        "IN",
                        "GB"
                    ]
                    },
                    {
                    "field": "countryOrRegion",
                    "operator": "IN",
                    "values": [
                        "IN"
                    ]
                    }
                ],
                "pagination": {
                    "offset": 0,
                    "limit": 1000
                }
                },
                "groupBy": [
                "countryOrRegion"
                ],
                "timeZone": "ORTZ",
                "returnRecordsWithNoMetrics": True,
                "returnRowTotals": False,
                "granularity": "DAILY",
                "returnGrandTotals": False
            }


        # Make the API request
        response = requests.post(api_url, headers=headers, json=request_payload)

        # Check for a successful response
        if response.status_code == 200:
            campaign_reports = response.json()
            # print(campaign_reports)
            # Process the campaign-level reports as needed
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")

        data = []
        for row in campaign_reports['data']['reportingDataResponse']['row']:
            metadata = row.get('metadata', {})

            # Skip rows where campaignStatus is 'PAUSED'
            if metadata.get('campaignStatus') == 'PAUSED':
                continue

            for granularity in row.get('granularity', []):
                date = granularity.get('date', 'N/A')

                # Check if 'impressions' data is available in granularity or falls back to 'total' key
                if 'impressions' in granularity:
                    impressions = granularity.get('impressions', 0)
                    taps = granularity.get('taps', 0)
                    installs = granularity.get('installs', 0)
                    new_downloads = granularity.get('newDownloads', 0)
                    redownloads = granularity.get('redownloads', 0)
                    lat_on_installs = granularity.get('latOnInstalls', 0)
                    lat_off_installs = granularity.get('latOffInstalls', 0)
                    ttr = granularity.get('ttr', 0.0)
                    avg_cpa = float(granularity.get('avgCPA', {}).get('amount', 0))
                    avg_cpt = float(granularity.get('avgCPT', {}).get('amount', 0))
                    avg_cpm = float(granularity.get('avgCPM', {}).get('amount', 0))
                    local_spend = float(granularity.get('localSpend', {}).get('amount', 0))
                    conversion_rate = granularity.get('conversionRate', 0.0)
                elif 'total' in row:
                    total = row['total']
                    impressions = total.get('impressions', 0)
                    taps = total.get('taps', 0)
                    installs = total.get('installs', 0)
                    new_downloads = total.get('newDownloads', 0)
                    redownloads = total.get('redownloads', 0)
                    lat_on_installs = total.get('latOnInstalls', 0)
                    lat_off_installs = total.get('latOffInstalls', 0)
                    ttr = total.get('ttr', 0.0)
                    avg_cpa = float(total.get('avgCPA', {}).get('amount', 0))
                    avg_cpt = float(total.get('avgCPT', {}).get('amount', 0))
                    avg_cpm = float(total.get('avgCPM', {}).get('amount', 0))
                    local_spend = float(total.get('localSpend', {}).get('amount', 0))
                    conversion_rate = total.get('conversionRate', 0.0)
                else:
                    continue

                data.append({
                    'Date': date,
                    'Campaign_Id': metadata.get('campaignId', 'N/A'),
                    'Campaign_Name': metadata.get('campaignName', 'N/A'),
                    'Impressions': impressions,
                    'Clicks': taps,
                    'Installs': installs,
                    'Total_Spend': local_spend,
                    'CPA': avg_cpa,
                    'CPC': avg_cpt,
                    'CPM': avg_cpm,
                    'CTR': ttr,
                    'Conversion_Rate': conversion_rate,
                    'New_Downloads': new_downloads,
                    'Re_Downloads': redownloads
                })

        df = pd.DataFrame(data)
        print(df)
        
        
        
        client = bigquery.Client(project=project_id)

        # Define the dataset and table references
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        # Create or load your DataFrame (df)

        # Configure the job to append data
        job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND', autodetect=True)  # Append data, auto-detect schema

        # Load the DataFrame data to append to the existing table
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        return "ETL Succesful!"
    
    except Exception as e:
        message = f"Issues in apple_campaign_usd_dag in Airflow : {e}"
        send_notification(message)
        print(f"An error occurred: {e}")
        return "ETL Failed !"
    
apple_campaign_usd_etl = PythonOperator(
        task_id='apple_campaign_usd_etl',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

apple_campaign_usd_etl