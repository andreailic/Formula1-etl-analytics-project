from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd
import logging

logging.info("DAG file location: {}".format(__file__))


def extract_local_laps_data():
    # Specify the absolute path to the CSV file
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)  # Read CSV using pandas

    # Select columns and drop duplicates based on 'lapsId'
    df_selected = df[['raceId', 'driverId', 'laps', 'lap', 'time_laptimes', 'position_laptimes', 'milliseconds_laptimes']]

    # Convert the 'lapsId' column to a list
    data = df_selected.to_dict(orient='records')

    return data


def transform_laps_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_laps_data')

    # Transform data as needed
    transformed_data = []

    # Set for tracking already added combinations of 'raceId', 'driverId', and 'lap'
    added_combinations = set()

    for row in data:
        # Extract relevant columns
        race_id = row['raceId']
        driver_id = row['driverId']
        laps = row['laps']
        lap = row['lap']
        time_laptimes = pd.to_datetime(row['time_laptimes'], errors='coerce')

        # Handle NaT (Not a Time) values
        time_laptimes_str = time_laptimes.strftime('%H:%M:%S.%f')[:-3] if pd.notna(time_laptimes) else None

        position_laptimes = row['position_laptimes']
        milliseconds_laptimes = row['milliseconds_laptimes']

        # Check uniqueness of the combination 'raceId', 'driverId', and 'lap'
        combination_key = (race_id, driver_id, lap)
        if combination_key in added_combinations:
            continue

        # Add the combination to the set
        added_combinations.add(combination_key)

        # Append transformed data
        transformed_data.append({
            'raceId': race_id,
            'driver_id': driver_id,
            'laps': laps,
            'lap': lap,
            'time_laptimes': time_laptimes_str,
            'position_laptimes': position_laptimes,
            'milliseconds_laptimes': milliseconds_laptimes
        })

    return transformed_data


def insert_laps_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_laps_data')

    # Build SQL queries for each row
    sql_queries = []
    for row in transformed_data:
        # Use placeholders for None values
        time_laptimes_placeholder = f"'{row['time_laptimes']}'" if row['time_laptimes'] is not None else 'NULL'

        # Construct the SQL query
        sql_query = (
            "INSERT INTO [Formula2].[dbo].[Laps] "
            "(raceId, driver_id, laps, lap, time_laptimes, position_laptimes, milliseconds_laptimes) "
            f"VALUES ({row['raceId']}, {row['driver_id']}, {row['laps']}, {row['lap']}, {time_laptimes_placeholder}, "
            f"{row['position_laptimes']}, {row['milliseconds_laptimes']})"
        )

        # Append the SQL query to the list
        sql_queries.append(sql_query)

    # Push the list of SQL queries to XCom
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

with DAG('laps_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # First phase: Extract local data for Laps
    t1 = PythonOperator(task_id='extract_local_laps_data', python_callable=extract_local_laps_data)

    # Transform data for Laps
    t2 = PythonOperator(task_id='transform_laps_data', python_callable=transform_laps_data)

    # Insert data into SQL for Laps
    t3 = PythonOperator(task_id='insert_laps_data_to_sql', python_callable=insert_laps_data_to_sql)

    # Load data into SQL Server for Laps
    t4 = PythonOperator(
        task_id='load_laps_data_to_sql',
        python_callable=lambda **context: [
            MsSqlOperator(
                task_id=f'execute_load_laps_data_to_sql_{i}',
                mssql_conn_id='mssql',
                sql=item,
                autocommit=True,
                dag=dag
            ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_laps_data_to_sql', key='sql_queries'))
        ]
    )

    # Set task dependencies
    t1 >> t2 >> t3 >> t4
