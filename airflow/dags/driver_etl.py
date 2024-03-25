
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd  # Import pandas for data processing
import logging

logging.info("DAG file location: {}".format(__file__))


def extract_local_driver_data():
    # Specify the absolute path to the CSV file
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'
    

    df = pd.read_csv(full_file_path)  # Read CSV using pandas

    # Select columns and drop duplicates based on 'driverId', then sort by 'id'
    df_selected = df[['driverId', 'forename', 'surname', 'dob', 'nationality', 'url', 'number', 'constructorRef', 'driverRef', 'code']].drop_duplicates('driverId').sort_values(by='driverId', ascending=True)

    # Convert column to a list
    data = df_selected[['driverId', 'forename', 'surname', 'dob', 'nationality', 'url', 'number', 'constructorRef', 'driverRef', 'code']].to_dict(orient='records')

    return data


def transform_driver_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_driver_data')

    # Transformacije podataka prema potrebama
    transformed_data = [{'driverId': item['driverId'], 'forename': item["forename"], 'surname': item["surname"],  'dob': item["dob"], 'nationality': item["nationality"], 'url_driver': item["url"], 'number': item['number'], 'constructorRef': item['constructorRef'], 'driverRef': item["driverRef"], 'code': item["code"]}
                        for item in data]

    return transformed_data


def insert_driver_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_driver_data')

    # Build SQL query for each row
   
    sql_queries = []
    for row in transformed_data:
        dob_datetime = pd.to_datetime(row['dob'], errors='coerce')
        if pd.notnull(dob_datetime):  # Check if dob_datetime is not null (valid date)
            age = datetime.now().year - dob_datetime.year
            surname = row['surname'].replace("'", "''")
            url = row['url_driver'].replace("'", "''")

            sql_query = f"INSERT INTO [Formula2].[dbo].[Driver]  (driverId, driverRef, constructorRef, number, code, forename, surname, dob, nationality, url_driver, age) " \
            f"VALUES ({row['driverId']}, '{row['driverRef']}', '{row['constructorRef']}', {row['number']}, '{row['code']}', " \
            f"'{row['forename']}', '{surname}', '{row['dob']}', '{row['nationality']}', '{url}', {age})"

            sql_queries.append(sql_query)
        else:
            # Handle the case where 'dob' is not a valid date
            logging.warning(f"Invalid date format for driverId {row['driverId']}. Skipping the record.")

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

with DAG('driver_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # Prva faza: Ekstrakcija lokalnih podataka za driver
    t1 = PythonOperator(task_id='extract_local_driver_data', python_callable=extract_local_driver_data)

    # Transformacija podataka za driver
    t2 = PythonOperator(task_id='transform_driver_data', python_callable=transform_driver_data)

    # Insert podataka u SQL za driver
    t3 = PythonOperator(task_id='insert_driver_data_to_sql', python_callable=insert_driver_data_to_sql)

    # Load data into SQL Server for driver
    t4 = PythonOperator(
    task_id='load_driver_data_to_sql',
    python_callable=lambda **context: [MsSqlOperator(
        task_id='execute_load_driver_data_to_sql_{}'.format(i),
        mssql_conn_id='mssql',
        sql=item,
        autocommit=True,
        dag=dag
    ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_driver_data_to_sql', key='sql_queries'))]
)


    # Postavite zavisnosti između zadataka
    t1 >> t2 >> t3 >> t4