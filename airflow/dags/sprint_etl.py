from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd
import logging

logging.info("DAG file location: {}".format(__file__))

# Define a function to parse 'sprint_time'
def parse_sprint_time(value):
    if pd.isnull(value) or value in ['\\N', 'NULL']:
        return None
    else:
        # Additional check for quotes and apostrophes
        value_str = str(value)
        if '"' in value_str or "'" in value_str:
            value_str = value_str.replace('"', '').replace("'", '')

        # Try parsing the time, and if successful, return the time without microseconds
        try:
            parsed_time = pd.to_datetime(value_str, format='%H:%M:%S', errors='coerce')
            formatted_time = parsed_time.strftime('%H:%M:%S') if not pd.isnull(parsed_time) else None
            return formatted_time
        except ValueError:
            return None

# Define a function to parse 'sprint_date'
def parse_sprint_date(value):
    if pd.isnull(value) or value in ['\\N', 'NULL']:
        return None
    else:
        # Try parsing the date, and if successful, return the date
        try:
            return pd.to_datetime(value).date()
        except ValueError:
            # If parsing fails, return None
            return None

def extract_local_sprint_data():
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'
    df = pd.read_csv(full_file_path)

    # Apply time and date parsing functions
    df['sprint_time'] = df['sprint_time'].apply(parse_sprint_time)
    df['sprint_date'] = df['sprint_date'].apply(parse_sprint_date)

    sprint_df = df[['sprint_date', 'sprint_time', 'raceId']].drop_duplicates(subset=['raceId'])
    sprint_df = sprint_df.dropna(subset=['sprint_date'])  # Drop rows with null sprint_date

    # Sort by raceId
    sprint_df = sprint_df.sort_values(by='raceId')

    data = sprint_df.to_dict(orient='records')
    return data

def transform_sprint_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_sprint_data')

    # Transform data as needed
    transformed_data = [
        {
            'sprint_date': item['sprint_date'],
            'sprint_time': item['sprint_time'],
            'raceId': item['raceId'],
        }
        for item in data
    ]

    return transformed_data

def insert_sprint_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_sprint_data')

    # Build SQL queries for each row
    sql_queries = []
    for row in transformed_data:
        # Format sprint_time as string or NULL
        sprint_time = "'{}'".format(row['sprint_time']) if row['sprint_time'] is not None else 'NULL'

        # Use CONVERT to format time without microseconds
        sql_query = "INSERT INTO [Formula2].[dbo].[Sprint] (sprint_date, sprint_time, raceId) " \
                    "VALUES ('{}', CONVERT(time, {}), {})".format(row['sprint_date'], sprint_time, row['raceId'])
        sql_queries.append(sql_query)

    ti.xcom_push(key='sql_queries', value=sql_queries)




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG('sprint_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # First phase: Extract local data for Sprint
    t1 = PythonOperator(task_id='extract_local_sprint_data', python_callable=extract_local_sprint_data)

    # Transform data for Sprint
    t2 = PythonOperator(task_id='transform_sprint_data', python_callable=transform_sprint_data)

    # Insert data into SQL for Sprint
    t3 = PythonOperator(task_id='insert_sprint_data_to_sql', python_callable=insert_sprint_data_to_sql)

    # Load data into SQL Server for Sprint
    t4 = PythonOperator(
        task_id='load_sprint_data_to_sql',
        python_callable=lambda **context: [
            MsSqlOperator(
                task_id=f'execute_load_sprint_data_to_sql_{i}',
                mssql_conn_id='mssql',
                sql=item,
                autocommit=True,
                dag=dag
            ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_sprint_data_to_sql', key='sql_queries'))
        ]
    )

    # Set task dependencies
    t1 >> t2 >> t3 >> t4
