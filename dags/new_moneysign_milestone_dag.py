from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import io
from datetime import datetime, timedelta

# Direct PostgreSQL Credentials
SRC_DB_CONFIG = {
    "dbname": "MoneySignDB",
    "user": "read_user",
    "password": "J'k%S$uf4M;y3#",
    "host": "43.204.206.86",
    "port": "8791"
}

DEST_DB_CONFIG = {
    "dbname": "Moneysign_milestone",
    "user": "pguser",
    "password": "R5sWDsMWc7aYHfQc",
    "host": "ec2-13-235-15-173.ap-south-1.compute.amazonaws.com",
    "port": "5432"
}

TABLES_TO_TRANSFER = ['answer', 'user_moneysign', 'user_ms_data', 'user_profile', 'user_score', 'usermoneysigndata', 'internal_users', 'question']

# Airflow DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'new_moneysign_milestone',
    default_args=default_args,
    description='ETL pipeline to transfer PostgreSQL tables using direct credentials',
    schedule_interval='@daily',
    catchup=False,
)


def get_table_schema(table_name):
    """Extracts table schema from source PostgreSQL"""
    try:
        print(f"Extracting schema for {table_name}...")
        with psycopg2.connect(**SRC_DB_CONFIG) as src_conn:
            with src_conn.cursor() as src_cursor:
                src_cursor.execute(f"""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' AND table_name = '{table_name}'
                    ORDER BY ordinal_position;
                """)

                columns = src_cursor.fetchall()
                schema_sql = f"CREATE TABLE {table_name} ("

                for col_name, col_type, col_length in columns:
                    if col_type == 'character varying' and col_length:
                        schema_sql += f"{col_name} {col_type}({col_length}), "
                    else:
                        schema_sql += f"{col_name} {col_type}, "

                schema_sql = schema_sql.rstrip(", ") + ");"

        print(f"Schema extracted for {table_name}")
        return schema_sql

    except Exception as e:
        print(f"Error extracting schema for {table_name}: {e}")
        return None


def create_table_if_not_exists(table_name):
    """Creates table in destination DB if not exists"""
    try:
        schema_sql = get_table_schema(table_name)
        if not schema_sql:
            return

        with psycopg2.connect(**DEST_DB_CONFIG) as dest_conn:
            with dest_conn.cursor() as dest_cursor:
                # Check if table exists
                dest_cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');")
                table_exists = dest_cursor.fetchone()[0]

                if not table_exists:
                    print(f"Creating table {table_name} in destination DB...")
                    dest_cursor.execute(schema_sql)
                    dest_conn.commit()
                    print(f"Table {table_name} created!")

    except Exception as e:
        print(f"Error creating table {table_name}: {e}")


def transfer_table(table_name):
    """Truncate & Replace: Transfers a table from source to destination using PostgreSQL COPY"""
    try:
        print(f"Starting transfer for {table_name}...")

        # Ensure table exists before transferring data
        create_table_if_not_exists(table_name)

        with psycopg2.connect(**DEST_DB_CONFIG) as dest_conn:
            with dest_conn.cursor() as dest_cursor:
                # Step 1: Truncate the table
                print(f"Truncating {table_name} in destination DB...")
                dest_cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")
                dest_conn.commit()
                print(f"{table_name} truncated successfully!")

        with psycopg2.connect(**SRC_DB_CONFIG) as src_conn, psycopg2.connect(**DEST_DB_CONFIG) as dest_conn:
            with src_conn.cursor() as src_cursor, dest_conn.cursor() as dest_cursor:
                
                # Step 2: Use in-memory buffer for efficient transfer
                mem_buffer = io.StringIO()

                # COPY from source to in-memory buffer
                print(f"Extracting data from source {table_name}...")
                src_cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH CSV HEADER", mem_buffer)
                mem_buffer.seek(0)  # Move cursor to start of buffer

                # COPY from in-memory buffer to destination
                print(f"Loading data into destination {table_name}...")
                dest_cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", mem_buffer)

                dest_conn.commit()

                # Step 3: Get row count from destination for verification
                dest_cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                row_count = dest_cursor.fetchone()[0]

                print(f"{table_name} transferred successfully! Total Rows: {row_count}")
                return "ETL Executed!"

    except Exception as e:
        print(f"Error transferring {table_name}: {e}")

# Define Airflow Tasks Dynamically
for table in TABLES_TO_TRANSFER:
    transfer_task = PythonOperator(
        task_id=f"transfer_{table}",
        python_callable=transfer_table,
        op_args=[table],
        dag=dag,
    )
