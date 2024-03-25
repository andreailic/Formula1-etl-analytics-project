from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd
import logging

logging.info("DAG file location: {}".format(__file__))


def parse_race_time(value):
    if pd.isnull(value) or value in ['\\N', 'NULL']:
        return None
    else:
        try:
            if ':' in value:
                formatted_race_time = pd.to_datetime(value).strftime('%H:%M:%S.%f')
            elif value.startswith('+'):
                reference_time = pd.to_datetime('00:00:00.000', format='%H:%M:%S.%f')
                relative_time = pd.to_timedelta(value).total_seconds()
                formatted_race_time = (reference_time + pd.to_timedelta(relative_time, unit='s')).strftime('%H:%M:%S.%f')
            else:
                formatted_race_time = None
        except ValueError:
            formatted_race_time = None

        return formatted_race_time


def extract_and_transform_time_data():
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'
    df = pd.read_csv(full_file_path)

    # Select specified columns and drop duplicates based on 'raceId'
    df_selected = df[['raceId', 'time', 'time_races']].drop_duplicates(subset=['raceId'])

    time_data = []

    for index, row in df_selected.iterrows():
        race_id = row['raceId']
        race_duration = parse_race_time(row['time'])
        start_time = parse_race_time(row['time_races'])

        time_data.append({
            'raceId': race_id,
            'race_duration': race_duration,
            'start_time': start_time
        })

    return time_data


def transform_race_time(race_time_df):
    formatted_race_time = None

    if not pd.isna(race_time_df) and race_time_df != '\\N':
        try:
            if ':' in race_time_df:
                formatted_race_time = pd.to_datetime(race_time_df).strftime('%H:%M:%S.%f')
            elif race_time_df.startswith('+'):
                reference_time = pd.to_datetime('00:00:00.000', format='%H:%M:%S.%f')
                relative_time = pd.to_timedelta(race_time_df).total_seconds()
                formatted_race_time = (reference_time + pd.to_timedelta(relative_time, unit='s')).strftime('%H:%M:%S.%f')
            else:
                formatted_race_time = None
        except ValueError:
            formatted_race_time = None

    return formatted_race_time

def insert_time_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    time_data = ti.xcom_pull(task_ids='extract_and_transform_time_data')

    # Build SQL queries for each row
    sql_queries = []
    for row in time_data:
        race_id = row['raceId']
        race_duration = row['race_duration']
        start_time = row['start_time']

        # Handle 'None' values and replace with appropriate default values
        race_duration = f"CAST('{race_duration}' AS time)" if race_duration is not None else 'NULL'
        start_time = f"CAST('{start_time}' AS time)" if start_time is not None else 'NULL'

        query = f"INSERT INTO [Formula2].[dbo].[TimeDimension] (raceId, race_duration, start_time) VALUES ({race_id}, {race_duration}, {start_time})"
        sql_queries.append(query)

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

with DAG('time_dimension_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # Extract and transform time data
    t1 = PythonOperator(task_id='extract_and_transform_time_data', python_callable=extract_and_transform_time_data)

    # Insert data into SQL for TimeDimension
    t2 = PythonOperator(task_id='insert_time_data_to_sql', python_callable=insert_time_data_to_sql)

    # Load data into SQL Server for TimeDimension
    t3 = PythonOperator(
        task_id='load_time_data_to_sql',
        python_callable=lambda **context: [
            MsSqlOperator(
                task_id=f'execute_load_time_data_to_sql_{i}',
                mssql_conn_id='mssql',
                sql=item,
                autocommit=True,
                dag=dag
            ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_time_data_to_sql', key='sql_queries'))
        ]
    )

    # Set task dependencies
    t1 >> t2 >> t3