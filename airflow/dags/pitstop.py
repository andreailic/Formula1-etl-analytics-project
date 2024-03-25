from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd
import logging

logging.info("DAG file location: {}".format(__file__))


def extract_local_pitstop_data():
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)
    df_selected = df[['raceId', 'driverId', 'stop', 'lap_pitstops', 'time_pitstops', 'duration', 'milliseconds_pitstops']].sort_values(by='raceId', ascending=True)
    data = df_selected.to_dict(orient='records')

    return data


def transform_pitstop_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_pitstop_data')

    transformed_data = []
    added_combinations = set()

    for row in data:
        formatted_time_pitstops = None
        formatted_duration = None

        time_pitstops_value = row['time_pitstops']
        if not pd.isna(time_pitstops_value) and time_pitstops_value != '\\N':
            try:
                if ':' in str(time_pitstops_value):
                    formatted_time_pitstops = pd.to_datetime(time_pitstops_value, format='%H:%M:%S', errors='coerce').strftime('%H:%M:%S')
            except ValueError:
                pass

        duration_value = row['duration']
        if not pd.isna(duration_value) and duration_value != '\\N':
            try:
                formatted_duration = float(duration_value)
                if formatted_duration.is_integer():
                    formatted_duration = int(formatted_duration)
            except ValueError:
                pass

        # Check uniqueness of the combination
        combination_key = (row['raceId'], row['driverId'], row['stop'])
        if combination_key in added_combinations:
            continue

        # Add the combination to the set
        added_combinations.add(combination_key)

        transformed_data.append({
            'raceId': row['raceId'],
            'driverId': row['driverId'],
            'stop': row['stop'],
            'lap_pitstops': row['lap_pitstops'],
            'time_pitstops': formatted_time_pitstops,
            'duration': formatted_duration,
            'milliseconds_pitstops': row['milliseconds_pitstops']
        })

    return transformed_data



def insert_pitstop_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_pitstop_data')

    sql_queries = []
    for row in transformed_data:
        insert_query = f"""
        INSERT INTO [Formula2].[dbo].[PitStop] (race_id, driver_id, stop_number, lap_pitstops, time_pitstops, duration, milliseconds_pitstops)
        VALUES ({row['raceId']}, {row['driverId']}, {row['stop']}, {row['lap_pitstops']},
                {'NULL' if row['time_pitstops'] is None else f"'{row['time_pitstops']}'"},
                {'NULL' if row['duration'] is None else row['duration']},
                {row['milliseconds_pitstops']}
        )
        """
        sql_queries.append(insert_query)

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

with DAG('pitstop_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    t1 = PythonOperator(task_id='extract_local_pitstop_data', python_callable=extract_local_pitstop_data)
    t2 = PythonOperator(task_id='transform_pitstop_data', python_callable=transform_pitstop_data)
    t3 = PythonOperator(task_id='insert_pitstop_data_to_sql', python_callable=insert_pitstop_data_to_sql)
    t4 = PythonOperator(
        task_id='load_pitstop_data_to_sql',
        python_callable=lambda **context: [MsSqlOperator(
            task_id=f'execute_load_pitstop_data_to_sql_{i}',
            mssql_conn_id='mssql',
            sql=item,
            autocommit=True,
            dag=dag
        ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_pitstop_data_to_sql', key='sql_queries'))]
    )

    t1 >> t2 >> t3 >> t4
