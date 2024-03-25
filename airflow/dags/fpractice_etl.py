from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import pandas as pd  # Import pandas for data processing
import logging

logging.info("DAG file location: {}".format(__file__))

def extract_local_freepractice_data():
    # Specify the absolute path to the CSV file
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)  # Read CSV using pandas

    # Pretvaranje '\\N' u NaN
    df.replace('\\N', pd.NA, inplace=True)

    # Funkcija za pretvaranje stringa u datum
    def convert_to_date(date_str):
        if pd.notna(date_str):
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            return None

    # Funkcija za formatiranje vremena
    def format_time(value):
        if not pd.isna(value):
            try:
                # Dodatna provera pre konverzije
                if ':' in str(value):
                    formatted_time = pd.to_datetime(value, format='%H:%M:%S', errors='coerce').strftime('%H:%M:%S')
                    return formatted_time
            except ValueError:
                pass
        return None

    # Primena funkcija na odgovarajuće kolone
    df['fp1_date'] = df['fp1_date'].apply(convert_to_date)
    df['fp2_date'] = df['fp2_date'].apply(convert_to_date)
    df['fp3_date'] = df['fp3_date'].apply(convert_to_date)
    df['fp1_time'] = df['fp1_time'].apply(format_time)
    df['fp2_time'] = df['fp2_time'].apply(format_time)
    df['fp3_time'] = df['fp3_time'].apply(format_time)

    # Izbacivanje redova gde su sve kolone jednake NULL
    df.dropna(subset=['fp1_date', 'fp1_time', 'fp2_date', 'fp2_time', 'fp3_date', 'fp3_time'], how='all', inplace=True)

    # Sortiranje i uklanjanje duplikata po 'raceId'
    df = df.sort_values('raceId').drop_duplicates('raceId')

    # Convert the 'raceId' column to a list
    data = df[['raceId', 'fp1_time', 'fp2_time', 'fp3_time', 'fp1_date', 'fp2_date', 'fp3_date']].to_dict(orient='records')

    return data


def transform_freepractice_data(*args, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_local_freepractice_data')

    # Debugging information
    logging.info("Transforming data: {}".format(data))

    # Transformacije podataka prema potrebama
    transformed_data = [
        {
            'raceId': item['raceId'],
            'fp1_time': item["fp1_time"],
            'fp2_time': item["fp2_time"],
            'fp3_time': item["fp3_time"],
            'fp1_date': item["fp1_date"],
            'fp2_date': item["fp2_date"],
            'fp3_date': item["fp3_date"]
        }
        for item in data
    ]

    logging.info("Transformed data: {}".format(transformed_data))

    ti.xcom_push(key='transformed_data', value=transformed_data)


def insert_freepractice_data_to_sql(*args, **kwargs):
    ti = kwargs['ti']
    
    # Retrieve transformed data from XCom
    transformed_data = ti.xcom_pull(task_ids='transform_freepractice_data', key='transformed_data')

    # Check if transformed_data is None or empty
    if transformed_data is None or not transformed_data:
        logging.warning("No transformed data available. Exiting task.")
        return

    # Debugging information
    logging.info("Inserting data: {}".format(transformed_data))

    # Build SQL query for each row
    sql_queries = []
    for row in transformed_data:
        sql_query = """
            INSERT INTO [Formula2].[dbo].[FreePractice]
            (raceId, fp1_time, fp2_time, fp3_time, fp1_date, fp2_date, fp3_date)
            VALUES (?, CAST(? AS TIME), CAST(? AS TIME), CAST(? AS TIME), ?, ?, ?)
            """
        params = (row['raceId'], row['fp1_time'], row['fp2_time'], row['fp3_time'], row['fp1_date'], row['fp2_date'], row['fp3_date'])
        sql_queries.append((sql_query, params))

    logging.info("SQL queries: {}".format(sql_queries))

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

with DAG('frepractice_etl', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # Prva faza: Ekstrakcija lokalnih podataka za freepractice
    t1 = PythonOperator(task_id='extract_local_freepractice_data', python_callable=extract_local_freepractice_data)

    # Transformacija podataka za freepractice
    t2 = PythonOperator(task_id='transform_freepractice_data', python_callable=transform_freepractice_data)

    # Insert podataka u SQL za freepractice
    t3 = PythonOperator(task_id='insert_freepractice_data_to_sql', python_callable=insert_freepractice_data_to_sql)

    # Load data into SQL Server for freepractice
    t4 = PythonOperator(
    task_id='load_freepractice_data_to_sql',
    python_callable=lambda **context: [
        MsSqlOperator(
            task_id=f'execute_load_freepractice_data_to_sql_{i}',
            mssql_conn_id='mssql',
            sql=item[0].replace('?', '%s'),  # Replace ? with %s in the SQL query
            parameters=tuple(item[1]),  # Convert the list to a tuple
            autocommit=True,
            dag=dag
        ).execute(context=context) for i, item in enumerate(context['ti'].xcom_pull(task_ids='insert_freepractice_data_to_sql', key='sql_queries') or [])
    ]
)




    # Postavite zavisnosti između zadataka
    t1 >> t2 >> t3 >> t4
