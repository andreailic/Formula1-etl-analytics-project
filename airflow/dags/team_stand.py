from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd 
import logging

logging.info("DAG file location: {}".format(__file__))


def extract_local_team_standings_data():
    # Specify the absolute path to the CSV file
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)  # Read CSV using pandas

    # Select columns and drop duplicates based on 'constructorStandingsId'
    df_selected = df[['constructorStandingsId', 'points_constructorstandings', 'position_constructorstandings', 'wins_constructorstandings', 'raceId', 'constructorId']].drop_duplicates(subset=['constructorStandingsId'])

    # Convert the 'constructorStandingsId' column to a list
    data = df_selected[['constructorStandingsId', 'points_constructorstandings', 'position_constructorstandings', 'wins_constructorstandings', 'raceId', 'constructorId']].to_dict(orient='records')

    return data


def transform_team_standings_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_team_standings_data')

    # Transform data as needed
    transformed_data = [
        {
            'constructorStandingsId': item['constructorStandingsId'],
            'points_constructorstandings': item['points_constructorstandings'],
            'position_constructorstandings': item['position_constructorstandings'],
            'wins_constructorstandings': item['wins_constructorstandings'],
            'race_id': item['raceId'],
            'constructorId': item['constructorId'],
        }
        for item in data
    ]

    return transformed_data


def insert_team_standings_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_team_standings_data')

    # Build SQL queries for each row
    sql_queries = [
        f"INSERT INTO [Formula2].[dbo].[TeamStandings] (constructorStandingsId, points_constructorstandings, position_constructorstandings, wins_constructorstandings, race_id, constructorId) " \
        f"VALUES ({row['constructorStandingsId']}, {row['points_constructorstandings']}, {row['position_constructorstandings']}, {row['wins_constructorstandings']}, {row['race_id']}, {row['constructorId']})"
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

with DAG('team_standings_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # First phase: Extract local data for team_standings
    t1 = PythonOperator(task_id='extract_local_team_standings_data', python_callable=extract_local_team_standings_data)

    # Transform data for team_standings
    t2 = PythonOperator(task_id='transform_team_standings_data', python_callable=transform_team_standings_data)

    # Insert data into SQL for team_standings
    t3 = PythonOperator(task_id='insert_team_standings_data_to_sql', python_callable=insert_team_standings_data_to_sql)

    # Load data into SQL Server for team_standings
    t4 = PythonOperator(
        task_id='load_team_standings_data_to_sql',
        python_callable=lambda **context: [
            MsSqlOperator(
                task_id=f'execute_load_team_standings_data_to_sql_{i}',
                mssql_conn_id='mssql',
                sql=item,
                autocommit=True,
                dag=dag
            ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_team_standings_data_to_sql', key='sql_queries'))
        ]
    )

    # Set task dependencies
    t1 >> t2 >> t3 >> t4
