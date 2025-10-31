from datetime import date
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from airflow.utils.email import send_email
import os
import psycopg2
import pandas as pd
import pandas_gbq as pd_gbq
import json
import requests



os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-f761b3593ea3.json')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'linkedin_campaign_dag',
    default_args=default_args,
    description='Extract linkedin_campaign_dag data from Linkedin Campaign API and load into BigQuery like cloud fn',
    schedule_interval='20 0 * * *',
)

# BigQuery credentials
# PROJECT_ID = 'analytics-1f'
# DATASET_ID = 'CampaignsDB'

access_token = 'AQUJhfxpguQwHkL5u9a711gETJ-xp4rapqccgBvUlzbzg_jUZoHfeTwqpjdqd-cOZrtR2dCaDZ4sYJyvVrLoYCQiGOBbAcBQRp5yOZyKE9_WhLUr9FijI9i8hyM9Cx3ity3JwKSSpYmmTazgTgfpb2HdJdVM89ka7_qcorEvZyuAt-wT3jLpofEGCWQYkH2Hb4ibEN3rI8FP-0hJTj9QVupFZ9CRvkSxRbe06quzwe4AOmTkolNm_ptWWP-aVROpt88v0cppctLueXclYQ_xaCpetETNssjgf7aBd2aNn6AXz4YE-cLInAwzz7E__cQZDvlUG0orFtBSG59_NFjGqm1mtVMofw'

# refresh_token = 'AQW7-ipkfyVd0FS3zrTVoJ2viPdYvsgxswyrAhOBKtzNfz0rsh9dWHxXWDvhkqEXG6MI9p_kG79Oig9_9Qah4LHx-7-en7ZH9EYcq3SGbz21skMQDcoAmEqBGlYq3z7sfqfw9dOTw6ZyOiPsyH66J8hWovpV-UmwyjwtlcYVAivRGgBEwGAKuJWqZ2UHS-0eo9GBuE30Ex9WWB5qpYgWwCyUP-rIWsotSEmOF8KsjfwzLtibtYOYZTNbDiXs1MwPqJBneKUComz2_BiCKGsxNpPRnO7M6kZUoq_Buwg6DLO8wdBWsjQza4uSpsb3yUA5GxNCiqgaNJwitYsZ8t8-1guSSlwZPw'

# Set up API request headers
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json',
}


## Date Extraction

# Get today's date
today = date.today()

# Yesterday date
yesterday = today - timedelta(days = 1)
print("Yesterday was: ", yesterday)

day = yesterday.day
month = yesterday.month
year = yesterday.year



# API endpoint for retrieving campaign IDs
analytics_endpoint = f"https://api.linkedin.com/v2/adAnalyticsV2?q=statistics&pivots[0]=CAMPAIGN&pivots[1]=COMPANY&dateRange.start.day={day}&dateRange.start.month={month}&dateRange.start.year={year}&dateRange.end.day={day}&dateRange.end.month={month}&dateRange.end.year={year}&timeGranularity=DAILY&accounts=urn:li:sponsoredAccount:509085249&fields=pivotValues,clicks,companyPageClicks,conversionValueInLocalCurrency,costInLocalCurrency,dateRange,impressions"
# API endpoint for retrieving campaign details
campaigns_endpoint = "https://api.linkedin.com/v2/adCampaignsV2?q=search&fields=id,name,costType,unitCost,status"

def main():
    try: 
        # Function to fetch data and create a DataFrame
        def fetch_and_create_df(endpoint):
            response = requests.get(endpoint, headers=headers)
            data = response.json()
            df = pd.DataFrame(data.get("elements", []))
            # print(df.columns)
            return df
            
        # Fetch campaign data
        analytics_df = fetch_and_create_df(analytics_endpoint)

        # Create a DataFrame for campaign ID and name mapping
        campaigns_df = fetch_and_create_df(campaigns_endpoint)
        campaigns_df = campaigns_df.rename(columns={"id": "Campaign_ID", "name": "Campaign_Name"})



        # print(campaigns_df)
        # Extract campaign_id from pivotValues column in campaigns_df
        analytics_df['Campaign_ID'] = analytics_df['pivotValues'].str.get(0).str.extract(r'(\d+)')
        analytics_df['Company_ID'] = analytics_df['pivotValues'].str.get(1).str.extract(r'(\d+)')
        # campaigns_df['Campaign_ID']

        # Convert both DataFrames to strings
        analytics_df = analytics_df.astype(str)
        campaigns_df = campaigns_df.astype(str)


        # Merge data with campaign_id_name_df on 'Campaign_ID'
        final_df = pd.merge(campaigns_df,analytics_df, on='Campaign_ID', how='inner')
        # final_df['Date_Range']
        # Rename columns as needed
        final_df = final_df.rename(columns={
            "clicks": "Clicks",
            "companyPageClicks": "Company_Page_Clicks",
            "conversionValueInLocalCurrency": "Conversion_Value_Local",
            "costInLocalCurrency": "Total_Cost",
            "dateRange": "Date_Range",
            "impressions": "Impressions",
        })
        final_df['unitCost'] = final_df['unitCost'].apply(lambda x: json.loads(x.replace("'", "\""))['amount'])
        final_df['Date_Range'] = final_df['Date_Range'].apply(lambda x: json.loads(x.replace("'", "\"")))

        # Extract 'start' date values
        final_df['Date'] = pd.to_datetime(final_df['Date_Range'].apply(lambda x: f"{x['start']['year']}-{x['start']['month']}-{x['start']['day']}"))

        # Format the extracted date as desired ("%d-%m-%Y")
        final_df['Date'] = final_df['Date'].dt.strftime("%d-%m-%Y")
        final_df.drop(columns=['Date_Range','pivotValues'], inplace=True)
        final_df["Date"] = pd.to_datetime(final_df["Date"],format="%d-%m-%Y")
        columns_to_convert = ['Total_Cost', 'Clicks', 'Impressions']
        final_df[columns_to_convert] = final_df[columns_to_convert].apply(pd.to_numeric, errors='coerce')
        final_df['CTR'] = round(((final_df['Clicks'] / final_df['Impressions']) * 100), 2)
        final_df['CPC'] = round((final_df['Total_Cost'] / final_df['Clicks']), 2)
        final_df['CPM'] = round(((final_df['Total_Cost'] / final_df['Impressions']) * 1000), 2)
        final_df.sort_values('Date', ascending=True,inplace=True,ignore_index=True)
        final_df=final_df.astype(str)

        project_id = 'analytics-1f'
        table_name = 'CampaignsDB.linkedin_campaigns'

       
        final_df.to_gbq(destination_table=table_name, project_id=project_id, if_exists='append', progress_bar=True)
        print("Done")
        return "ETL Successful"
        
    except Exception as e:
        print("ETL Failed")
        return(f"An error occurred: {e}")
        
      

linkedin_campaign_etl = PythonOperator(
        task_id='linkedin_campaign_etl',
        python_callable=main,
        provide_context=True,
        dag=dag,
        )

linkedin_campaign_etl