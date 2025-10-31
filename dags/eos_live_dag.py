# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
# from google.cloud import bigquery
# from airflow.utils.email import send_email
# import os
# import psycopg2
# import pandas as pd
# import pandas_gbq as pd_gbq


# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join('/opt/bitnami/airflow/data', 'analytics-1f-607f3e39b749.json')

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 12, 14, 6, 35),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=2),
# }

# dag = DAG(
#     'live_eos_dag',
#     default_args=default_args,
#     description='Live Eos data from Postgres and load into BigQuery like cloud fn',
#     schedule_interval= "*/30 * * * *",
#     catchup=False

# )

# # BigQuery credentials
# PROJECT_ID = 'analytics-1f'
# DATASET_ID = 'Test_Airflow1'

# # PostgreSQL
# PSQL_USERNAME = 'read_user'
# PSQL_PASSWORD = 'ThisIsNot4u'
# PSQL_HOST = '43.205.197.150'
# PSQL_PORT='8791'
# PSQL_DATABASE = 'EOSDB'

# def eos_live_etl():  

#       try:
#             #engine = create_engine(con_uri, pool_recycle=3600, schema="public").connect()
#             conn = psycopg2.connect(host='43.205.197.150', port='8791', dbname='EOSDB', user='read_user', password='ThisIsNot4u')
#             #cursor = conn.cursor()
#             tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name in ('customer_data','customer_mapping','users')"
#             #cursor.execute(tables_query)
#             list_tables = pd.read_sql_query(tables_query, conn)
#             print(list_tables)
#             for index, row in list_tables.iterrows():
#                 table_id = '{}.{}'.format(DATASET_ID, row['table_name'])
#                 print("Loading Table {}".format(table_id))
#                 query = "SELECT * FROM {} ".format(row['table_name'])
#                 df = pd.read_sql_query(query, conn)
#                 df.columns=df.columns.str.replace(r'\s+', ' ', regex=True)
#                 df.columns=df.columns.str.replace('.','')
#                 df.columns=df.columns.str.replace('/','')
#                 df.columns=df.columns.str.replace('-','')
#                 df.columns=df.columns.str.replace(',','')
#                 df.columns=df.columns.str.replace(':','')
#                 df.columns=df.columns.str.replace(' ','')
#             #     for i in df.columns:
                
#             #         if df[i].dtypes == 'object':
#             #                 df[i] = df[i].astype('string')
#             #     #df.to_csv(f'{table_id}.csv')
#             # #   print("Successfully Created CSV {}".format(table_id))
#                 try:
#                     pd_gbq.to_gbq(df, table_id, project_id=PROJECT_ID, if_exists='replace', progress_bar=True)
#                     print("Successfully Data Ingested {}".format(table_id))
#                 except:
#                     print("Error Loading Table {}".format(table_id))
#             return "ETL Successful"
#       except Exception as e:
#             print("Error {}".format(e))
#             return "ETL Failed"
      

# eos_live_etl = PythonOperator(
#         task_id='eos_live_etl',
#         python_callable=eos_live_etl,
#         provide_context=True,
#         dag=dag,
#         )
