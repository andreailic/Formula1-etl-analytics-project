from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd  # Import pandas for data processing
import logging

logging.info("DAG file location: {}".format(__file__))

def extract_local_data():
    # Specify the absolute path to the CSV file
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)  # Read CSV using pandas
    df_selected = df[['date']].drop_duplicates().sort_values(by='date', ascending=False)

    # Convert the 'date' column to a list
    data = df_selected['date'].tolist()

    return data

def transform_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_data')

    # Process date data (split into day, month, year)
    processed_data = []
    for date_value in data:
        year, month, day = map(int, date_value.split('-'))
        processed_data.append({'date': date_value, 'day': day, 'month': month, 'year': year})

    return processed_data

def insert_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    sql_queries = []

    # Build SQL query for each row
    for row in transformed_data:
        sql_query = f"INSERT INTO [Formula2].[dbo].[DateDimension] (date, day, month, year) VALUES ('{row['date']}', {row['day']}, {row['month']}, {row['year']})"
        sql_queries.append(sql_query)

    # Execute SQL queries
    mssql_conn_id = 'mssql'  # Replace with your MSSQL connection ID
    mssql_task = MsSqlOperator(
        task_id='execute_sql_queries',
        mssql_conn_id=mssql_conn_id,
        sql=sql_queries,
        autocommit=True,
        dag=dag,
    )
    
    mssql_task.execute(context=kwargs)

# Rest of your code remains unchanged


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('date_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # Extract local data
    t1 = PythonOperator(task_id='extract_local_data', python_callable=extract_local_data)

    # Transform data
    t2 = PythonOperator(task_id='transform_data', python_callable=transform_data)

    # Insert data to SQL
    t3 = PythonOperator(task_id='insert_data_to_sql', python_callable=insert_data_to_sql)

    # Set task dependencies
    t1 >> t2 >> t3
