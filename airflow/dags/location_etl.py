# Importing required libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator  # Corrected import
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd
import logging

logging.info("DAG file location: {}".format(__file__))

def extract_local_location_data():
    # Specify the absolute path to the CSV file
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    # Read CSV using pandas
    df = pd.read_csv(full_file_path)

    # Select columns and drop duplicates based on 'circuitId', then sort by 'circuitId'
    df_selected = df[['circuitId', 'circuitRef', 'name_x', 'location', 'country', 'lat', 'lng', 'url_x']].drop_duplicates('circuitId').sort_values(by='circuitId', ascending=True)

    # Convert the 'circuitId' column to a list of dictionaries
    data = df_selected.to_dict(orient='records')

    return data

def transform_location_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_location_data')

    # Transform data according to requirements
    transformed_data = [{'locationId': item['circuitId'],
                         'circuitRef': item['circuitRef'],
                         'name_loc': item['name_x'],
                         'location': item['location'],
                         'country': item['country'],
                         'lat': item['lat'],
                         'lng': item['lng'],
                         'url_location': item['url_x']} for item in data]

    return transformed_data

def insert_location_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_location_data')

    # Build SQL query for each row
    sql_queries = []
    for row in transformed_data:
        insert_query = f"INSERT INTO [Formula2].[dbo].[LocationDimension] (locationId, name_loc, circuitRef, location, country, lat, lng, url_location) " \
                       f"VALUES ({row['locationId']}, '{row['name_loc']}', '{row['circuitRef']}', '{row['location']}', '{row['country']}', {row['lat']}, {row['lng']}, '{row['url_location']}')"
        sql_queries.append(insert_query)

    ti.xcom_push(key='sql_queries', value=sql_queries)

# Rest of your code remains unchanged
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}
    
with DAG('location_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
        # Prva faza: Ekstrakcija lokalnih podataka za lokaciju
        t1 = PythonOperator(task_id='extract_local_location_data', python_callable=extract_local_location_data)

        # Transformacija podataka za lokaciju
        t2 = PythonOperator(task_id='transform_location_data', python_callable=transform_location_data)

        # Insert podataka u SQL za lokaciju
        t3 = PythonOperator(task_id='insert_location_data_to_sql', python_callable=insert_location_data_to_sql)

        # Load data into SQL Server for lokaciju
        t4 = PythonOperator(
            task_id='load_location_data_to_sql',
            python_callable=lambda **context: [MsSqlOperator(
                task_id='execute_load_location_data_to_sql_{}'.format(i),
                mssql_conn_id='mssql',
                sql=item,
                autocommit=True,
                dag=dag
            ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_location_data_to_sql', key='sql_queries'))]
        )

        # Postavite zavisnosti između zadataka
        t1 >> t2 >> t3 >> t4
