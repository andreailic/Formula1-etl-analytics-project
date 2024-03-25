from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd  # Import pandas for data processing
import logging

logging.info("DAG file location: {}".format(__file__))


def extract_local_status_data():
    # Specify the absolute path to the CSV file
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)  # Read CSV using pandas

    # Select columns and drop duplicates based on 'statusId', then sort by 'id'
    df_selected = df[['statusId', 'status']].drop_duplicates('statusId').sort_values(by='statusId', ascending=True)

    # Convert the 'statusId' column to a list
    data = df_selected[['statusId', 'status']].to_dict(orient='records')

    return data


def transform_status_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_status_data')

    # Transformacije podataka prema potrebama
    transformed_data = [{'statusId': item['statusId'], 'statusDescription': item["status"]}
                        for item in data]

    return transformed_data


def insert_status_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_status_data')

    # Build SQL query for each row
    sql_queries = []
    for row in transformed_data:
        sql_query = f"INSERT INTO [Formula2].[dbo].[StatusDimension] (statusId, statusDescription) VALUES ({row['statusId']}, '{row['statusDescription']}')"
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

with DAG('status_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # Prva faza: Ekstrakcija lokalnih podataka za status
    t1 = PythonOperator(task_id='extract_local_status_data', python_callable=extract_local_status_data)

    # Transformacija podataka za status
    t2 = PythonOperator(task_id='transform_status_data', python_callable=transform_status_data)

    # Insert podataka u SQL za status
    t3 = PythonOperator(task_id='insert_status_data_to_sql', python_callable=insert_status_data_to_sql)

    # Load data into SQL Server for status
    t4 = PythonOperator(
    task_id='load_status_data_to_sql',
    python_callable=lambda **context: [MsSqlOperator(
        task_id='execute_load_status_data_to_sql_{}'.format(i),
        mssql_conn_id='mssql',
        sql=item,
        autocommit=True,
        dag=dag
    ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_status_data_to_sql', key='sql_queries'))]
)



    # Postavite zavisnosti između zadataka
    t1 >> t2 >> t3 >> t4
