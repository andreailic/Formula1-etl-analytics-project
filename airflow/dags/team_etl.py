from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd  # Import pandas for data processing
import logging

logging.info("DAG file location: {}".format(__file__))


def extract_local_team_data():
    # Specify the absolute path to the CSV file
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)  # Read CSV using pandas

    # Select columns and drop duplicates based on 'constructorId', then sort by 'constructorId'
    df_selected = df[['constructorId', 'name', 'constructorRef', 'nationality_constructors', 'url_constructors']].drop_duplicates('constructorId').sort_values(by='constructorId', ascending=True)

    # Convert the 'constructorId' column to a list
    data = df_selected[['constructorId', 'name', 'constructorRef', 'nationality_constructors', 'url_constructors']].to_dict(orient='records')

    return data


def transform_team_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_team_data')

    # Transformacije podataka prema potrebama
    transformed_data = [{'constructorId': item['constructorId'],
                         'name_team': item['name'],
                         'constructorRef': item['constructorRef'],
                         'nationality_constructors': item['nationality_constructors'],
                         'url_constructors': item['url_constructors']}
                        for item in data]

    return transformed_data


def insert_team_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_team_data')

    # Build SQL query for each row
    sql_queries = []
    for row in transformed_data:
        sql_query = f"INSERT INTO [Formula2].[dbo].[Team] (constructorId, name_team, constructorRef, nationality_constructors, url_constructors) VALUES ({row['constructorId']}, '{row['name_team']}', '{row['constructorRef']}', '{row['nationality_constructors']}', '{row['url_constructors']}')"
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

with DAG('team_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # Prva faza: Ekstrakcija lokalnih podataka za timove
    t1 = PythonOperator(task_id='extract_local_team_data', python_callable=extract_local_team_data)

    # Transformacija podataka za timove
    t2 = PythonOperator(task_id='transform_team_data', python_callable=transform_team_data)

    # Insert podataka u SQL za timove
    t3 = PythonOperator(task_id='insert_team_data_to_sql', python_callable=insert_team_data_to_sql)

    # Load data into SQL Server for timove
    t4 = PythonOperator(
        task_id='load_team_data_to_sql',
        python_callable=lambda **context: [MsSqlOperator(
            task_id='execute_load_team_data_to_sql_{}'.format(i),
            mssql_conn_id='mssql',
            sql=item,
            autocommit=True,
            dag=dag
        ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_team_data_to_sql', key='sql_queries'))]
    )

    # Postavite zavisnosti između zadataka
    t1 >> t2 >> t3 >> t4
