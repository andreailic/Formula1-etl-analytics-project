from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd  # Import pandas for data processing
import logging

logging.info("DAG file location: {}".format(__file__))


def extract_local_driver_standings_data():
    # Specify the absolute path to the CSV file
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)  # Read CSV using pandas

    # Select columns and drop duplicates based on 'driverStandingsId'
    df_selected = df[['driverStandingsId', 'raceId', 'driverId', 'points_driverstandings', 'position_driverstandings', 'wins']].drop_duplicates(subset=['driverStandingsId'])

    # Convert the 'driverStandingsId' column to a list
    data = df_selected[['driverStandingsId', 'raceId', 'driverId', 'points_driverstandings', 'position_driverstandings', 'wins']].to_dict(orient='records')

    return data


def transform_driver_standings_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_driver_standings_data')

    # Transform data as needed
    transformed_data = [
        {
            'driverStandingsId': item['driverStandingsId'],
            'raceId': item['raceId'],
            'driverId': item['driverId'],
            'points_driverstandings': item['points_driverstandings'],
            'position_driverstandings': item['position_driverstandings'],
            'wins': item['wins']
        }
        for item in data
    ]

    return transformed_data


def insert_driver_standings_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_driver_standings_data')

    # Build SQL queries for each row
    sql_queries = [
        f"INSERT INTO [Formula2].[dbo].[DriverStandings] (driverStandingsId, raceId, driverId, points_driverstandings, position_driverstandings, wins) " \
        f"VALUES ({row['driverStandingsId']}, {row['raceId']}, {row['driverId']}, {row['points_driverstandings']}, {row['position_driverstandings']}, {row['wins']})"
        for row in transformed_data
    ]

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

with DAG('driver_standings_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # First phase: Extract local data for driver_standings
    t1 = PythonOperator(task_id='extract_local_driver_standings_data', python_callable=extract_local_driver_standings_data)

    # Transform data for driver_standings
    t2 = PythonOperator(task_id='transform_driver_standings_data', python_callable=transform_driver_standings_data)

    # Insert data into SQL for driver_standings
    t3 = PythonOperator(task_id='insert_driver_standings_data_to_sql', python_callable=insert_driver_standings_data_to_sql)

    # Load data into SQL Server for driver_standings
    t4 = PythonOperator(
        task_id='load_driver_standings_data_to_sql',
        python_callable=lambda **context: [
            MsSqlOperator(
                task_id=f'execute_load_driver_standings_data_to_sql_{i}',
                mssql_conn_id='mssql',
                sql=item,
                autocommit=True,
                dag=dag
            ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_driver_standings_data_to_sql', key='sql_queries'))
        ]
    )

    # Set task dependencies
    t1 >> t2 >> t3 >> t4
