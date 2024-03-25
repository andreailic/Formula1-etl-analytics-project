from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd
import logging

logging.info("DAG file location: {}".format(__file__))

def extract_local_race_data():
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'
    df_race = pd.read_csv(full_file_path)
    selected_columns = ['raceId', 'date', 'round', 'circuitId']
    df_selected = df_race[selected_columns].drop_duplicates('raceId')
    return df_selected.to_dict(orient='records')

def transform_race_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_race_data')
    transformed_data = [{'race_id': item['raceId'],
                         'date': convert_to_date(item['date']),
                         'round': item['round'],
                         'locationId': item['circuitId']}
                        for item in data]
    return transformed_data

def insert_race_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_race_data')
    sql_queries = []
    for row in transformed_data:
        sql_query = f"INSERT INTO [Formula2].[dbo].[Race] (race_id, date, round, locationId) VALUES ({row['race_id']}, '{row['date']}', {row['round']}, {row['locationId']})"
        sql_queries.append(sql_query)
    ti.xcom_push(key='sql_queries', value=sql_queries)

def convert_to_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG('race_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    t1 = PythonOperator(task_id='extract_local_race_data', python_callable=extract_local_race_data)
    t2 = PythonOperator(task_id='transform_race_data', python_callable=transform_race_data)
    t3 = PythonOperator(task_id='insert_race_data_to_sql', python_callable=insert_race_data_to_sql)
    t4 = PythonOperator(
        task_id='load_race_data_to_sql',
        python_callable=lambda **context: [MsSqlOperator(
            task_id='execute_load_race_data_to_sql_{}'.format(i),
            mssql_conn_id='mssql',
            sql=item,
            autocommit=True,
            dag=dag
        ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_race_data_to_sql', key='sql_queries'))]
    )

    t1 >> t2 >> t3 >> t4
