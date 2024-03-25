from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd  # Import pandas for data processing
import logging

logging.info("DAG file location: {}".format(__file__))

def extract_local_results_data():
    # Specify the absolute path to the CSV file
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)  # Read CSV using pandas

    # Print columns present in the extracted data
    logging.info(f"Columns in the extracted data: {df.columns}")

    # Select columns and drop duplicates based on 'resultId'
    df_selected = df[['resultId', 'raceId', 'driverId', 'positionOrder', 'points', 'laps', 'constructorId', 'statusId', 'grid', 'rank','fastestLap', 'fastestLapTime', 'fastestLapSpeed']].drop_duplicates(subset=['resultId'])

    # Convert the 'resultId' column to a list
    data = df_selected[['resultId', 'raceId', 'driverId', 'positionOrder', 'points', 'laps', 'constructorId', 'statusId', 'grid', 'rank','fastestLap', 'fastestLapTime', 'fastestLapSpeed']].to_dict(orient='records')

    return data



def transform_results_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_results_data')

    transformed_data = []

    for row in data:
        try:
            # Process the 'fastestLapTime' column
            fastest_lap_time = row.get('fastestLapTime')
            if fastest_lap_time and fastest_lap_time != '\\N':
                fastest_lap_time = pd.to_datetime(fastest_lap_time, errors='coerce').strftime('%H:%M:%S.%f')

            # Initialize fastest_lap_speed before assignment
            fastest_lap_speed = None

            # Process and convert 'fastest_lap_speed' column
            fastest_lap_speed = float(row.get('fastestLapSpeed'))

        except Exception as e:
            print(f"Error processing row: {row}. Error: {e}")

        # Handle 'None' values and replace with appropriate default values
        transformed_data.append({
            'resultId': row.get('resultId'),
            'raceId': row.get('raceId'),
            'driverId': row.get('driverId'),
            'position_order': row.get('positionOrder'),
            'points': row.get('points'),
            'laps': row.get('laps'),
            'constructorId': row.get('constructorId'),
            'statusId': row.get('statusId'),
            'grid': row.get('grid'),
            'rank': row.get('rank'),
            'fastestLap': row.get('fastestLap'),
            'fastestLapTime': fastest_lap_time,
            'fastestLapSpeed': fastest_lap_speed,
        })



    return transformed_data



def insert_results_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_results_data')

    # Build SQL queries for each row
    sql_queries = []
    for row in transformed_data:
        try:
            # Process the 'fastestLapTime' column
            fastest_lap_time = row.get('fastestLapTime')
            if fastest_lap_time and fastest_lap_time != '\\N':
                fastest_lap_time = pd.to_datetime(fastest_lap_time, errors='coerce').strftime('%H:%M:%S.%f')

            # Initialize fastest_lap_speed before assignment
            fastest_lap_speed = None

            # Process and convert 'fastest_lap_speed' column
            if 'fastestLapSpeed' in row:
                fastest_lap_speed = float(row.get('fastestLapSpeed'))

            # Print the row before insertion
            print(f"Row to be inserted: {row}")

        except Exception as e:
            print(f"Error processing row: {row}. Error: {e}")

        # Convert '\\N' values to None for integer columns
        for key, value in row.items():
            if value == '\\N':
                row[key] = None

        # Add 'fastestLapTime' and 'fastestLapSpeed' to the columns list
        columns = [f"[{col}]" for col in row.keys() if row[col] is not None]
        values = [f"'{row[val]}'" if isinstance(row[val], str) else f"{row[val]}" for val in row.keys() if row[val] is not None]

        query = f"INSERT INTO [Formula2].[dbo].[Results] ({', '.join(columns)}) VALUES ({', '.join(values)})"
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

with DAG('results_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # First phase: Extract local data for results
    t1 = PythonOperator(task_id='extract_local_results_data', python_callable=extract_local_results_data)

    # Transform data for results
    t2 = PythonOperator(task_id='transform_results_data', python_callable=transform_results_data)

    # Insert data into SQL for results
    t3 = PythonOperator(task_id='insert_results_data_to_sql', python_callable=insert_results_data_to_sql)

    # Load data into SQL Server for results
    t4 = PythonOperator(
        task_id='load_results_data_to_sql',
        python_callable=lambda **context: [
            MsSqlOperator(
                task_id=f'execute_load_results_data_to_sql_{i}',
                mssql_conn_id='mssql',
                sql=item,
                autocommit=True,
                dag=dag
            ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_results_data_to_sql', key='sql_queries'))
        ]
    )

    # Set task dependencies
    t1 >> t2 >> t3 >> t4