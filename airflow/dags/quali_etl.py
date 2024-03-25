from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd  # Import pandas for data processing
import logging

logging.info("DAG file location: {}".format(__file__))

# Define a set to keep track of added combinations
added_combinations = set()

def extract_local_quali_data():
    # Specify the absolute path to the CSV file
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'
    
    df = pd.read_csv(full_file_path)  # Read CSV using pandas

    # Select columns and drop duplicates based on 'qualiId', then sort by 'id'
    df_selected = df[['quali_date', 'quali_time', 'raceId', 'driverId', 'position']].sort_values(by='raceId', ascending=True)

    # Convert the 'qualiId' column to a list
    data = df_selected[['quali_date', 'quali_time', 'raceId', 'driverId', 'position']].to_dict(orient='records')

    # Provera i izbacivanje duplikata samo kada je race_id i driver_id isti
    global added_combinations
    filtered_data = []
    for item in data:
        combination_key = (item['driverId'], item['raceId'])
        if combination_key in added_combinations:
            print(f"Duplicate combination: {combination_key}")
        else:
            added_combinations.add(combination_key)
            filtered_data.append(item)

    return filtered_data

def transform_quali_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_quali_data')

    # Transformacije podataka prema potrebama
    transformed_data = [{'quali_date': item["quali_date"], 'quali_time': item["quali_time"], 'raceId': item['raceId'], 'driverId': item['driverId'], 'position': item['position']}
                        for item in data]

    return transformed_data

def insert_quali_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_quali_data')

    # Build SQL query for each row
    sql_queries = []
    for row in transformed_data:
        quali_date = row.get('quali_date')
        quali_time = row.get('quali_time')
        position = int(row['position']) if row['position'] != '\\N' else 0

        # Provera i obrada vrednosti 'quali_date'
        if pd.notna(quali_date):
            try:
                quali_date = pd.to_datetime(quali_date, errors='coerce').strftime('%Y-%m-%d')
            except (ValueError, pd.errors.OutOfBoundsDatetime):
                quali_date = None
        else:
            quali_date = None

        # Adjust the conversion of quali_time
        if pd.notna(quali_time) and quali_time != '\\N':
            try:
                quali_time = pd.to_datetime(quali_time, format='%H:%M:%S').strftime('%H:%M:%S')
            except (ValueError, pd.errors.OutOfBoundsDatetime):
                quali_time = None
        else:
            quali_time = None

        # Use placeholders for None values
        quali_date_placeholder = f"'{quali_date}'" if quali_date is not None else 'NULL'
        quali_time_placeholder = f"CONVERT(time, '{quali_time}')" if quali_time is not None else 'NULL'

        sql_query = (
            "INSERT INTO [Formula2].[dbo].[Qualification] "
            "(quali_date, quali_time, race_id, driver_id, position) "
            f"VALUES ({quali_date_placeholder}, {quali_time_placeholder}, '{row['raceId']}', '{row['driverId']}', {position})"
        )

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

with DAG('qualifying_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # Prva faza: Ekstrakcija lokalnih podataka za quali
    t1 = PythonOperator(task_id='extract_local_quali_data', python_callable=extract_local_quali_data)

    # Transformacija podataka za quali
    t2 = PythonOperator(task_id='transform_quali_data', python_callable=transform_quali_data)

    # Insert podataka u SQL za quali
    t3 = PythonOperator(task_id='insert_quali_data_to_sql', python_callable=insert_quali_data_to_sql)

    # Load data into SQL Server for quali
    t4 = PythonOperator(
        task_id='load_quali_data_to_sql',
        python_callable=lambda **context: [MsSqlOperator(
            task_id=f'execute_load_quali_data_to_sql_{i}',
            mssql_conn_id='mssql',
            sql=item,
            autocommit=True,
            dag=dag
        ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_quali_data_to_sql', key='sql_queries'))]
    )

    # Postavite zavisnosti između zadataka
    t1 >> t2 >> t3 >> t4