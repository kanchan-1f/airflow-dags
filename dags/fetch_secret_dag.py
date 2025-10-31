# import json
# import pandas as pd
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from google.cloud import secretmanager
# from google.cloud import bigquery
# from google.oauth2 import service_account
# from datetime import datetime
# import os

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

# # Function to fetch secret from Secret Manager
# # def fetch_secret():
# client = secretmanager.SecretManagerServiceClient()
# name = f"projects/430695588957/secrets/analytics_1f_secret/versions/1"
# response = client.access_secret_version(request={"name": name})
# secret = response.payload.data.decode("UTF-8")
# credentials_json = json.loads(secret)
# # Authenticate using the fetched credentials
# credentials = service_account.Credentials.from_service_account_info(credentials_json)

#     # Save the credentials to a temporary file
# with open('/tmp/credentials.json', 'w') as f:
#     json.dump(credentials_json, f)

# # Set the environment variable to point to the credentials file
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/credentials.json'
# # Function to fetch data from BigQuery and convert to DataFrame
# def fetch_bigquery_data():
#     project_id = 'analytics-1f'  # Replace with your GCP project ID
    
#     # Fetch credentials from Secret Manager
#     # Fetch credentials from Secret Manager
#     # credentials_json = fetch_secret()
    
#     # Print credentials for verification
#     # print("Fetched Credentials: ", credentials_json)

#     # Authenticate using the fetched credentials
#     # credentials = service_account.Credentials.from_service_account_info(credentials_json)
#     #     # Save the credentials to a temporary file
#     # with open('/tmp/credentials.json', 'w') as f:
#     #     json.dump(credentials_json, f)

#     # # Set the environment variable to point to the credentials file
#     # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/credentials.json'

#     client = bigquery.Client(project=project_id)
   
#     query = "SELECT * FROM `analytics-1f.LakeMaster.effective_tax_rate` LIMIT 20"  # Replace with your query
    
#     # Execute query and fetch data as DataFrame
#     df = client.query(query).to_dataframe()
    
#     print(df)
#     return df

# # Define default args for the DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 6, 13),
#     'retries': 1,
# }

# # Define the DAG
# dag = DAG(
#     'fetch_bigquery_data_with_secrets_dag',
#     default_args=default_args,
#     description='Fetch data from BigQuery using credentials from Secret Manager',
#     schedule_interval='@once',
#     catchup=False
# )

# fetch_secret_task = PythonOperator(
#         task_id='fetch_secret_task',
#         python_callable=fetch_bigquery_data,
#         provide_context=True,
#         dag=dag,
#         )

# fetch_secret_task