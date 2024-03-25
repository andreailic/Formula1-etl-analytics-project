from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from typing import List, Dict
from collections import defaultdict
import json
import pandas as pd  # Import pandas for data processing
import logging

logging.info("DAG file location: {}".format(__file__))

@task
def extract_local_data1():
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)

    df_selected_date = df[['date']].drop_duplicates().sort_values(by='date', ascending=False)
    data_date = df_selected_date.to_dict(orient='records')

    df_selected_location = df[['circuitId', 'circuitRef', 'name_x', 'location', 'country', 'lat', 'lng', 'url_x']].drop_duplicates('circuitId').sort_values(by='circuitId', ascending=True)
    data_location = df_selected_location.to_dict(orient='records')

    df_selected_status = df[['statusId', 'status']].drop_duplicates('statusId').sort_values(by='statusId', ascending=True)
    data_status = df_selected_status.to_dict(orient='records')

    df_selected_driver = df[['driverId', 'forename', 'surname', 'dob', 'nationality', 'url', 'number', 'constructorRef', 'driverRef', 'code']].drop_duplicates('driverId').sort_values(by='driverId', ascending=True)
    data_driver = df_selected_driver.to_dict(orient='records')

    df_selected_team = df[['constructorId', 'name', 'constructorRef', 'nationality_constructors', 'url_constructors']].drop_duplicates('constructorId').sort_values(by='constructorId', ascending=True)
    data_team = df_selected_team.to_dict(orient='records')

    df_selected_race = df[['raceId', 'date', 'round', 'circuitId']].drop_duplicates().sort_values(by='raceId', ascending=False)
    data_race= df_selected_race.to_dict(orient='records')

    return {
        'date': data_date,
        'location': data_location,
        'status': data_status,
        'driver': data_driver,
        'team': data_team,
        'race': data_race
    }

@task
def extract_local_data2():
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)

    df_selected_freep = df[['raceId', 'fp1_time', 'fp2_time', 'fp3_time', 'fp1_date', 'fp2_date', 'fp3_date']].drop_duplicates(subset=['raceId'])
    data_freep = df_selected_freep.to_dict(orient='records')

    df_selected_pitstops = df[['raceId', 'driverId', 'stop', 'lap_pitstops', 'time_pitstops', 'duration', 'milliseconds_pitstops']].sort_values(by='raceId', ascending=True)
    data_pitstops = df_selected_pitstops.to_dict(orient='records')

    df_selected_quali = df[['quali_date', 'quali_time', 'raceId', 'driverId', 'position']].sort_values(by='raceId', ascending=True)
    data_quali = df_selected_quali.to_dict(orient='records')

    df_selected_driverstand = df[['driverStandingsId', 'raceId', 'driverId', 'points_driverstandings', 'position_driverstandings', 'wins']].drop_duplicates(subset=['driverStandingsId'])
    data_driverstand = df_selected_driverstand.to_dict(orient='records')

    return {
        'freep': data_freep,
        'pitstops': data_pitstops,
        'quali': data_quali,
        'driverstand': data_driverstand
    }

@task
def extract_local_data3():
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)

    df_selected_sprint = df[['sprint_date', 'sprint_time', 'raceId']].drop_duplicates(subset=['raceId'])
    data_sprint = df_selected_sprint.to_dict(orient='records')

    df_selected_teamstand = df[['constructorStandingsId', 'points_constructorstandings', 'position_constructorstandings', 'wins_constructorstandings', 'raceId', 'constructorId']].drop_duplicates(subset=['constructorStandingsId'])
    data_teamstand = df_selected_teamstand.to_dict(orient='records')

    df_selected_time = df[['raceId', 'time', 'time_races']].drop_duplicates(subset=['raceId'])
    data_time = df_selected_time.to_dict(orient='records')

    df_selected_results = df[['resultId', 'raceId', 'driverId', 'positionOrder', 'points', 'laps', 'constructorId', 'statusId', 'grid', 'rank', 'fastestLap', 'fastestLapTime', 'fastestLapSpeed']].drop_duplicates(subset=['resultId'])
    data_results = df_selected_results.to_dict(orient='records')


    return {
        'sprint': data_sprint,
        'teamstand': data_teamstand,
        'time': data_time,
        'results': data_results
    }

@task
def extract_local_dataLaps():
    full_file_path = '/opt/airflow/dags/data/dataEngineeringDataset.csv'

    df = pd.read_csv(full_file_path)

    df_selected_laps = df[['raceId', 'driverId', 'laps', 'lap', 'time_laptimes', 'position_laptimes', 'milliseconds_laptimes']]
    data_laps = df_selected_laps.to_dict(orient='records')

    return {
        'laps': data_laps
    }

@task
def transform_data(data):
    selected_date = data['date']
    selected_location = data['location']
    selected_status = data['status']
    selected_driver = data['driver']
    selected_team = data['team']
    selected_race = data['race']

    processed_date = []
    for date_value in selected_date:
        year, month, day = map(int, date_value['date'].split('-'))
        processed_date.append({'date': date_value['date'], 'day': day, 'month': month, 'year': year})


    processed_location = []  
    for row in selected_location:
        processed_location.append({
            'circuitId': row['circuitId'],
            'name_x': row['name_x'],
            'circuitRef': row['circuitRef'], 
            'location': row['location'], 
            'country': row['country'],
            'lat': row['lat'],
            'lng': row['lng'],
            'url_x': row['url_x']
        })

    # Process status data
    processed_status = []
    for row in selected_status:
        processed_status.append({
            'statusId': row['statusId'],
            'status': row['status']
        })


    # Process driver data
    processed_driver = []
    for row in selected_driver:
        processed_driver.append({
            'driverId': row['driverId'],
            'forename': row['forename'],
            'surname': row['surname'],
            'dob': row['dob'],
            'nationality': row['nationality'],
            'url': row['url'],
            'number': row['number'],
            'constructorRef': row['constructorRef'],
            'driverRef': row['driverRef'],
            'code': row['code']
        })


    # Process team data
    processed_team = []
    for row in selected_team:
        processed_team.append({
            'constructorId': row['constructorId'],
            'name': row['name'],
            'constructorRef': row['constructorRef'],
            'nationality_constructors': row['nationality_constructors'],
            'url_constructors': row['url_constructors']
        })


    # Process race data
    processed_race = []
    for row in selected_race:
        processed_race.append({
            'raceId': row['raceId'],
            'date': row['date'],
            'round': row['round'],
            'circuitId': row['circuitId']
        })

    return {
        'date': processed_date,
        'location': selected_location,
        'status': selected_status,
        'driver': selected_driver,
        'team': selected_team,
        'race': selected_race
    }

@task
def insert_data_to_sql(data):
    if data is None or not data:
        logging.warning("No data received from the previous task or data is empty.")
        return

    # Initialize SQL queries list
    sql_queries = []

    # Insert data for DateDimension table
    for row in data['date']:
        sql_query = f"INSERT INTO [Formula2].[dbo].[DateDimension] (date, day, month, year) VALUES ('{row['date']}', {row['day']}, {row['month']}, {row['year']})"
        sql_queries.append(sql_query)



    # Insert data for Driver table
    for row in data['driver']:
        dob_datetime = pd.to_datetime(row['dob'], errors='coerce')
        if pd.notnull(dob_datetime):  # Check if dob_datetime is not null (valid date)
            age = datetime.now().year - dob_datetime.year
            surname = row['surname'].replace("'", "''")
            url = row['url'].replace("'", "''")

            sql_query = f"INSERT INTO [Formula2].[dbo].[Driver]  (driverId, driverRef, constructorRef, number, code, forename, surname, dob, nationality, url_driver, age) " \
            f"VALUES ({row['driverId']}, '{row['driverRef']}', '{row['constructorRef']}', {row['number']}, '{row['code']}', " \
            f"'{row['forename']}', '{surname}', '{row['dob']}', '{row['nationality']}', '{url}', {age})"
            logging.info(f"Inserting row into Driver table: {row}")
            sql_queries.append(sql_query)
        else:
            # Handle the case where 'dob' is not a valid date
            logging.warning(f"Invalid date format for driverId {row['driverId']}. Skipping the record.")

    # Insert data for LocationDimension table
    for row in data['location']:
        insert_query = f"INSERT INTO [Formula2].[dbo].[LocationDimension] (locationId, name_loc, circuitRef, location, country, lat, lng, url_location) " \
                       f"VALUES ({row['circuitId']}, '{row['name_x']}', '{row['circuitRef']}', '{row['location']}', '{row['country']}', {row['lat']}, {row['lng']}, '{row['url_x']}')"
        logging.info(f"Inserting row into LocationDimension table: {row}")
        sql_queries.append(insert_query)

    # Insert data for Team table
    for row in data['team']:
        sql_query = f"INSERT INTO [Formula2].[dbo].[Team] (constructorId, name_team, constructorRef, nationality_constructors, url_constructors) VALUES ({row['constructorId']}, '{row['name']}', '{row['constructorRef']}', '{row['nationality_constructors']}', '{row['url_constructors']}')"
        logging.info(f"Inserting row into Team table: {row}")
        sql_queries.append(sql_query)

    # Insert data for StatusDimension table
    for row in data['status']:
        sql_query = f"INSERT INTO [Formula2].[dbo].[StatusDimension] (statusId, statusDescription) VALUES ({row['statusId']}, '{row['status']}')"
        logging.info(f"Inserting row into StatusDimension table: {row}")
        sql_queries.append(sql_query)


    try:
        mssql_conn_id = 'mssql'  # Replace with your MSSQL connection ID
        mssql_task = MsSqlOperator(
            task_id='execute_sql_queries',
            mssql_conn_id=mssql_conn_id,
            sql=sql_queries,
            autocommit=True,
            dag=dag,
        )
        mssql_task.execute(context=data)  # Passing data directly instead of kwargs
        logging.info("Data insertion to SQL completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred while inserting data to SQL: {str(e)}")

@task
def transform_race_data(data):
    processed_race = []  
    for row in data['race']:
        processed_race.append({
            'raceId': row['raceId'],
            'date': row['date'],
            'round': row['round'],
            'circuitId': row['circuitId']
        })
    return processed_race

@task
def insert_race_data_to_sql(data):
    if data is None or not data:
        logging.warning("No data received from the previous task or data is empty.")
        return

    # Initialize SQL queries list
    sql_queries = []

    # Insert data for RaceDimension table
    for row in data:
        race_date = pd.to_datetime(row['date'])
        sql_query = f"INSERT INTO [Formula2].[dbo].[Race] (race_id, date, round, locationId) VALUES ({row['raceId']}, '{race_date}', {row['round']}, {row['circuitId']})"
        logging.info(f"Inserting row into RaceDimension table: {row}")
        sql_queries.append(sql_query)

    # Execute SQL queries
    try:
        mssql_conn_id = 'mssql'  # Replace with your MSSQL connection ID
        mssql_task = MsSqlOperator(
            task_id='execute_sql_queries',
            mssql_conn_id=mssql_conn_id,
            sql=sql_queries,
            autocommit=True,
            dag=dag,
        )
        mssql_task.execute(context=data)  # Passing data directly instead of kwargs
        logging.info("Race data insertion to SQL completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred while inserting race data to SQL: {str(e)}")

@task
def transform_freep_data(data):
    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(data['freep'])

    # Drop rows with all date and time columns null
    df.dropna(subset=['fp1_date', 'fp1_time', 'fp2_date', 'fp2_time', 'fp3_date', 'fp3_time'], how='all', inplace=True)

    # Define a function to parse time
    def parse_time(value):
        if pd.isnull(value) or value in ['\\N', 'NULL']:
            return None
        else:
            try:
                return pd.to_datetime(value, format='%H:%M:%S').strftime('%H:%M:%S')
            except ValueError:
                return None

    # Define a function to parse date
    def parse_date(value):
        if pd.isnull(value) or value in ['\\N', 'NULL']:
            return None
        else:
            try:
                return pd.to_datetime(value).strftime('%Y-%m-%d')
            except ValueError:
                return None

    # Transform the data
    transformed_data = []
    for index, row in df.iterrows():
        filtered_item = defaultdict(lambda: None)
        for i in range(1, 4):  # Iterating through fp1, fp2, fp3
            fp_date = parse_date(row.get(f'fp{i}_date'))
            fp_time = parse_time(row.get(f'fp{i}_time'))
            filtered_item[f'fp{i}_date'] = fp_date
            filtered_item[f'fp{i}_time'] = fp_time

        race_id = row['raceId']
        combination_key = (race_id,)
        if combination_key in added_combinations:
            print(f"Duplicate combination: {combination_key}")
        else:
            added_combinations.add(combination_key)
            filtered_item['raceId'] = race_id
            transformed_data.append(filtered_item)

    return transformed_data

@task
def insert_freep_data_to_sql(data):
    if data is None or not data:
        logging.warning("No data received from the previous task or data is empty.")
        return

    sql_queries = []  # This line should be outside the if statement

    for row in data:
        race_id = row['raceId']
        fp1_time = row['fp1_time']
        fp2_time = row['fp2_time']
        fp3_time = row['fp3_time']
        fp1_date = row['fp1_date']
        fp2_date = row['fp2_date']
        fp3_date = row['fp3_date']

        sql_query = """
            INSERT INTO [Formula2].[dbo].[FreePractice]
            (raceId, fp1_time, fp2_time, fp3_time, fp1_date, fp2_date, fp3_date)
            VALUES (%(raceId)s, CAST(%(fp1_time)s AS TIME), CAST(%(fp2_time)s AS TIME), CAST(%(fp3_time)s AS TIME), %(fp1_date)s, %(fp2_date)s, %(fp3_date)s)
            """
        params = {
            'raceId': race_id,
            'fp1_time': fp1_time,
            'fp2_time': fp2_time,
            'fp3_time': fp3_time,
            'fp1_date': fp1_date,
            'fp2_date': fp2_date,
            'fp3_date': fp3_date
        }
        sql_queries.append((sql_query, params))

    try:
        mssql_conn_id = 'mssql'  # Replace with your MSSQL connection ID
        for sql_query, params in sql_queries:
            mssql_task = MsSqlOperator(
                task_id='execute_sql_queries_freep',
                mssql_conn_id=mssql_conn_id,
                sql=sql_query,
                parameters=params,  # Pass parameters separately
                autocommit=True
            )
            mssql_task.execute(context=data)
        logging.info("Free Practice data insertion to SQL completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred while inserting Free Practice data to SQL: {str(e)}")


@task
def transform_pitstop_data(data):
    processed_pitstops = []
    added_combinations = set()

    for row in data['pitstops']:
        time_pitstops_value = row['time_pitstops']
        formatted_time_pitstops = None
        if not pd.isna(time_pitstops_value) and time_pitstops_value != '\\N':
            try:
                if ':' in str(time_pitstops_value):
                    formatted_time_pitstops = pd.to_datetime(time_pitstops_value, format='%H:%M:%S', errors='coerce').strftime('%H:%M:%S')
            except ValueError:
                pass

        duration_value = row['duration']
        formatted_duration = None
        if not pd.isna(duration_value) and duration_value != '\\N':
            try:
                formatted_duration = float(duration_value)
                if formatted_duration.is_integer():
                    formatted_duration = int(formatted_duration)
            except ValueError:
                pass

        combination_key = (row['raceId'], row['driverId'], row['stop'])
        if combination_key in added_combinations:
            continue

        processed_pitstops.append({
            'raceId': row['raceId'],
            'driverId': row['driverId'],
            'stop': row['stop'],
            'lap_pitstops': row['lap_pitstops'],
            'time_pitstops': formatted_time_pitstops,
            'duration': formatted_duration,
            'milliseconds_pitstops': row['milliseconds_pitstops']
        })
        added_combinations.add(combination_key)

    return processed_pitstops

@task
def insert_pitstop_data_to_sql(data):
    if data is None or not data:
        logging.warning("No data received from the previous task or data is empty.")
        return

    # Initialize SQL queries list
    sql_queries = []

    # Insert data for PitStop table
    for row in data:
        sql_query = (
            f"INSERT INTO [Formula2].[dbo].[PitStop] "
            f"(race_id, driver_id, stop_number, lap_pitstops, time_pitstops, duration, milliseconds_pitstops) "
            f"VALUES ({row['raceId']}, {row['driverId']}, {row['stop']}, {row['lap_pitstops']}, "
            f"'{row['time_pitstops']}', {row['duration']}, {row['milliseconds_pitstops']})"
        )
        sql_queries.append(sql_query)

    # Execute SQL queries
    try:
        mssql_conn_id = 'mssql'
        mssql_task = MsSqlOperator(
            task_id='execute_sql_queries_pitstop',
            mssql_conn_id=mssql_conn_id,
            sql=sql_queries,
            autocommit=True
        )
        mssql_task.execute(context=data)
        logging.info("PitStop data insertion to SQL completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred while inserting PitStop data to SQL: {str(e)}")


added_combinations = set()

@task
def transform_qualification_data(data):
    global added_combinations
    filtered_data = []
    
    for row in data['quali']:
        combination_key = (row['driverId'], row['raceId'])
        if combination_key in added_combinations:
            print(f"Duplicate combination: {combination_key}")
        else:
            added_combinations.add(combination_key)
            filtered_data.append({
                'quali_date': row['quali_date'],
                'quali_time': row['quali_time'],
                'raceId': row['raceId'],
                'driverId': row['driverId'],
                'position': row['position']
            })
            
    return filtered_data

@task
def insert_qualification_data_to_sql(data):
    if data is None or not data:
        logging.warning("No data received from the previous task or data is empty.")
        return

    # Initialize SQL queries list
    sql_queries = []

    # Insert data for Qualification table
    for row in data:
        # Extract necessary fields from the row
        quali_date = row.get('quali_date')
        quali_time = row.get('quali_time')
        position = int(row['position']) if row['position'] != '\\N' else 0

        # Validate and process quali_date
        if pd.notna(quali_date):
            try:
                quali_date = pd.to_datetime(quali_date, errors='coerce').strftime('%Y-%m-%d')
            except (ValueError, pd.errors.OutOfBoundsDatetime):
                logging.error(f"Invalid quali_date format for row: {row}")
                continue  # Skip this row and proceed with the next one
        else:
            quali_date = None

        # Validate and process quali_time
        if pd.notna(quali_time) and quali_time != '\\N':
            try:
                quali_time = pd.to_datetime(quali_time, format='%H:%M:%S').strftime('%H:%M:%S')
            except (ValueError, pd.errors.OutOfBoundsDatetime):
                logging.error(f"Invalid quali_time format for row: {row}")
                continue  # Skip this row and proceed with the next one
        else:
            quali_time = None

        # Prepare placeholders for quali_date and quali_time
        quali_date_placeholder = f"'{quali_date}'" if quali_date is not None else 'NULL'
        quali_time_placeholder = f"CONVERT(time, '{quali_time}')" if quali_time is not None else 'NULL'

        # Construct SQL query
        sql_query = (
            "INSERT INTO [Formula2].[dbo].[Qualification] "
            "(quali_date, quali_time, race_id, driver_id, position) "
            f"VALUES ({quali_date_placeholder}, {quali_time_placeholder}, '{row['raceId']}', '{row['driverId']}', {position})"
        )

        sql_queries.append(sql_query)

    # Execute SQL queries
    try:
        mssql_conn_id = 'mssql'
        mssql_task = MsSqlOperator(
            task_id='execute_sql_queries_qualification',
            mssql_conn_id=mssql_conn_id,
            sql=sql_queries,
            autocommit=True
        )
        mssql_task.execute(context=data)
        logging.info("Qualification data insertion to SQL completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred while inserting Qualification data to SQL: {str(e)}")


@task
def transform_driver_standings_data(data):
    selected_driverstand = data['driverstand']

    processed_driverstand = [
        {
            'driverStandingsId': row['driverStandingsId'],
            'raceId': row['raceId'],
            'driverId': row['driverId'],
            'points_driverstandings': row['points_driverstandings'],
            'position_driverstandings': row['position_driverstandings'],
            'wins': row['wins']
        }
        for row in selected_driverstand
    ]

    return processed_driverstand

@task
def insert_driver_standings_data_to_sql(data):
    if data is None or not data:
        logging.warning("No data received from the previous task or data is empty.")
        return

    sql_queries = [
        f"INSERT INTO [Formula2].[dbo].[DriverStandings] (driverStandingsId, raceId, driverId, points_driverstandings, position_driverstandings, wins) " \
        f"VALUES ({row['driverStandingsId']}, {row['raceId']}, {row['driverId']}, {row['points_driverstandings']}, {row['position_driverstandings']}, {row['wins']})"
        for row in data
    ]

    try:
        mssql_conn_id = 'mssql'  # Replace with your MSSQL connection ID
        mssql_task = MsSqlOperator(
            task_id='execute_sql_queries_driverstandings',
            mssql_conn_id=mssql_conn_id,
            sql=sql_queries,
            autocommit=True,
            dag=dag,
        )
        mssql_task.execute(context=data)
        logging.info("Driver standings data insertion to SQL completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred while inserting driver standings data to SQL: {str(e)}")

@task
def transform_team_standings_data(data):
    selected_teamstand = data['teamstand']

    processed_teamstand = [
        {
            'constructorStandingsId': row['constructorStandingsId'],
            'points_constructorstandings': row['points_constructorstandings'],
            'position_constructorstandings': row['position_constructorstandings'],
            'wins_constructorstandings': row['wins_constructorstandings'],
            'race_id': row['raceId'],
            'constructorId': row['constructorId'],
        }
        for row in selected_teamstand
    ]

    return processed_teamstand

@task
def insert_team_standings_data_to_sql(data):
    if data is None or not data:
        logging.warning("No data received from the previous task or data is empty.")
        return

    sql_queries = [
        f"INSERT INTO [Formula2].[dbo].[TeamStandings] (constructorStandingsId, points_constructorstandings, position_constructorstandings, wins_constructorstandings, race_id, constructorId) " \
        f"VALUES ({row['constructorStandingsId']}, {row['points_constructorstandings']}, {row['position_constructorstandings']}, {row['wins_constructorstandings']}, {row['race_id']}, {row['constructorId']})"
        for row in data
    ]

    try:
        mssql_conn_id = 'mssql'  # Replace with your MSSQL connection ID
        mssql_task = MsSqlOperator(
            task_id='execute_sql_queries_teamstandings',
            mssql_conn_id=mssql_conn_id,
            sql=sql_queries,
            autocommit=True,
            dag=dag,
        )
        mssql_task.execute(context=data)
        logging.info("Team standings data insertion to SQL completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred while inserting team standings data to SQL: {str(e)}")


@task
def transform_time_data(data):
    # Define a function to parse 'time' and 'time_races'
    def parse_time(value):
        if pd.isnull(value) or value in ['\\N', 'NULL']:
            return None
        else:
            try:
                return pd.to_datetime(value, format='%H:%M:%S').strftime('%H:%M:%S')
            except ValueError:
                return None

    # Transform data as needed
    transformed_data = []
    for item in data['time']:
        time_value = parse_time(item['time'])
        time_races = parse_time(item['time_races'])

        transformed_data.append({
            'time': time_value,
            'time_races': time_races,
            'raceId': item['raceId']
        })
    return transformed_data

@task
def insert_time_data_to_sql(data):
    if data is None or not data:
        logging.warning("No data received from the previous task or data is empty.")
        return

    sql_queries = []
    for row in data:
        time_value = f"'{row['time']}'" if row['time'] else 'NULL'
        time_races = f"'{row['time_races']}'" if row['time_races'] else 'NULL'

        # Skip insertion if both time_value and time_races are NULL
        if time_value == 'NULL' and time_races == 'NULL':
            continue

        sql_query = f"INSERT INTO [Formula2].[dbo].[TimeDimension] (race_duration, start_time, raceId) " \
                    f"VALUES ({time_value}, {time_races}, '{row['raceId']}')"
        sql_queries.append(sql_query)

    try:
        mssql_conn_id = 'mssql'  # Replace with your MSSQL connection ID
        mssql_task = MsSqlOperator(
            task_id='execute_sql_queries_time',
            mssql_conn_id=mssql_conn_id,
            sql=sql_queries,
            autocommit=True
        )
        mssql_task.execute(context=data)
        logging.info("Time data insertion to SQL completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred while inserting time data to SQL: {str(e)}")


@task
def transform_sprint_data(data):
    # Define a function to parse 'sprint_time'
    def parse_sprint_time(value):
        if pd.isnull(value) or value in ['\\N', 'NULL']:
            return None
        else:
            try:
                return pd.to_datetime(value, format='%H:%M:%S').strftime('%H:%M:%S')
            except ValueError:
                return None

    # Define a function to parse 'sprint_date'
    def parse_sprint_date(value):
        if pd.isnull(value) or value in ['\\N', 'NULL']:
            return None
        else:
            try:
                return pd.to_datetime(value).strftime('%Y-%m-%d')
            except ValueError:
                return None

    # Transform data as needed
    transformed_data = []
    for item in data['sprint']:
        sprint_date = parse_sprint_date(item['sprint_date'])
        sprint_time = parse_sprint_time(item['sprint_time'])

        transformed_data.append({
            'sprint_date': sprint_date,
            'sprint_time': sprint_time,
            'raceId': item['raceId']
        })
    return transformed_data

@task
def insert_sprint_data_to_sql(data):
    if data is None or not data:
        logging.warning("No data received from the previous task or data is empty.")
        return

    sql_queries = []
    for row in data:
        sprint_date = f"'{row['sprint_date']}'" if row['sprint_date'] else 'NULL'
        sprint_time = f"'{row['sprint_time']}'" if row['sprint_time'] else 'NULL'

        # Skip insertion if both sprint_date and sprint_time are NULL
        if sprint_date == 'NULL' and sprint_time == 'NULL':
            continue

        sql_query = f"INSERT INTO [Formula2].[dbo].[Sprint] (sprint_date, sprint_time, raceId) " \
                    f"VALUES ({sprint_date}, {sprint_time}, '{row['raceId']}')"
        sql_queries.append(sql_query)

    try:
        mssql_conn_id = 'mssql'  # Replace with your MSSQL connection ID
        mssql_task = MsSqlOperator(
            task_id='execute_sql_queries_sprint',
            mssql_conn_id=mssql_conn_id,
            sql=sql_queries,
            autocommit=True
        )
        mssql_task.execute(context=data)
        logging.info("Sprint data insertion to SQL completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred while inserting sprint data to SQL: {str(e)}")

@task
def transform_laps_data(*args, **kwargs):
    ti = kwargs['ti']
    data_dict = ti.xcom_pull(task_ids='extract_local_dataLaps')

    data = data_dict['laps']  # Pristupanje ključu 'laps' u rečniku

    # Transformacija podataka po potrebi
    transformed_data = []

    # Skup za praćenje već dodatih kombinacija 'raceId', 'driverId' i 'lap'
    added_combinations = set()

    # Brojač za praćenje broja dodanih redova
    rows_added = 0

    for row in data:
        if rows_added >= 1000:
            break  # Zaustavljanje procesiranja ako je dodato 1000 redova
        # Izvlačenje relevantnih kolona
        race_id = row['raceId']
        driver_id = row['driverId']
        laps = row['laps']
        lap = row['lap']
        time_laptimes = pd.to_datetime(row['time_laptimes'], errors='coerce')

        # Tretiranje NaT (Not a Time) vrednosti
        time_laptimes_str = time_laptimes.strftime('%H:%M:%S.%f')[:-3] if pd.notna(time_laptimes) else None

        position_laptimes = row['position_laptimes']
        milliseconds_laptimes = row['milliseconds_laptimes']

        # Provera jedinstvenosti kombinacije 'raceId', 'driverId' i 'lap'
        combination_key = (race_id, driver_id, lap)
        if combination_key in added_combinations:
            continue

        # Dodavanje kombinacije u skup
        added_combinations.add(combination_key)

        # Dodavanje transformisanih podataka
        transformed_data.append({
            'raceId': race_id,
            'driver_id': driver_id,
            'laps': laps,
            'lap': lap,
            'time_laptimes': time_laptimes_str,
            'position_laptimes': position_laptimes,
            'milliseconds_laptimes': milliseconds_laptimes
        })

        rows_added += 1  # Inkrementiranje brojača

    return transformed_data

@task
def insert_laps_data_to_sql(data):
    if data is None or not data:
        logging.warning("No data received from the previous task or data is empty.")
        return

    # Initialize SQL queries list
    sql_queries = []

    # Insert data for Laps table
    for row in data:
        try:
            # Convert '\\N' values to None for integer columns
            for key, value in row.items():
                if value == '\\N':
                    row[key] = None

            # Add columns and values to the query
            columns = [f"[{col}]" for col in row.keys() if row[col] is not None]
            values = [f"'{row[val]}'" if isinstance(row[val], str) else f"{row[val]}" for val in row.keys() if row[val] is not None]

            query = f"INSERT INTO [Formula2].[dbo].[Laps] ({', '.join(columns)}) VALUES ({', '.join(values)})"
            sql_queries.append(query)

        except Exception as e:
            logging.error(f"Error processing row: {row}. Error: {e}")
            continue

    # Execute SQL queries
    try:
        mssql_conn_id = 'mssql' 
        mssql_task = MsSqlOperator(
            task_id='execute_sql_queries_laps',
            mssql_conn_id=mssql_conn_id,
            sql=sql_queries,
            autocommit=True,
            dag=dag,
        )
        mssql_task.execute(context=data)  # Passing data directly instead of kwargs
        logging.info("Laps data insertion to SQL completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred while inserting laps data to SQL: {str(e)}")


@task
def transform_results_data(data):
    if data is None or not data:
        logging.warning("No data received from the previous task or data is empty.")
        return
    
    transformed_data = []

    for row in data['results']:
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

        except Exception as e:
            logging.error(f"Error processing row: {row}. Error: {e}")
            continue

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

    return {
        'results': transformed_data
    }

@task
def insert_results_data_to_sql(data):
    if data is None or not data:
        logging.warning("No data received from the previous task or data is empty.")
        return

    # Initialize SQL queries list
    sql_queries = []

    # Insert data for Results table
    for row in data['results']:
        try:
            # Convert '\\N' values to None for integer columns
            for key, value in row.items():
                if value == '\\N':
                    row[key] = None

            # Add columns and values to the query
            columns = [f"[{col}]" for col in row.keys() if row[col] is not None]
            values = [f"'{row[val]}'" if isinstance(row[val], str) else f"{row[val]}" for val in row.keys() if row[val] is not None]

            query = f"INSERT INTO [Formula2].[dbo].[Results] ({', '.join(columns)}) VALUES ({', '.join(values)})"
            sql_queries.append(query)

        except Exception as e:
            logging.error(f"Error processing row: {row}. Error: {e}")
            continue

    # Execute SQL queries
    try:
        mssql_conn_id = 'mssql'  # Replace with your MSSQL connection ID
        mssql_task = MsSqlOperator(
            task_id='execute_sql_queries_results',
            mssql_conn_id=mssql_conn_id,
            sql=sql_queries,
            autocommit=True,
            dag=dag,
        )
        mssql_task.execute(context=data)  # Passing data directly instead of kwargs
        logging.info("Results data insertion to SQL completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred while inserting results data to SQL: {str(e)}")



with DAG("CompleteETL", start_date=datetime(2024, 3, 16), schedule_interval="@daily", catchup=False) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    extract_local_data1_task = extract_local_data1()
    extract_local_data2_task = extract_local_data2()
    extract_local_data3_task = extract_local_data3()
    extract_local_dataLaps_task = extract_local_dataLaps()

    with TaskGroup("Group1") as group1:
        transform_data_task = transform_data(extract_local_data1_task)
        insert_data_to_sql_task = insert_data_to_sql(transform_data_task)

    with TaskGroup("GroupRace") as GroupRace:
        transform_race_data_task = transform_race_data(extract_local_data1_task)
        insert_race_data_to_sql_task = insert_race_data_to_sql(transform_race_data_task)

    with TaskGroup("GroupFreePractice") as GroupFreePractice:
        transform_fp_data_task = transform_freep_data(extract_local_data2_task)
        insert_fp_data_to_sql_task = insert_freep_data_to_sql(transform_fp_data_task)

    with TaskGroup("GroupQualification") as GroupQualification:
        transform_qualification_data_task = transform_qualification_data(extract_local_data2_task)
        insert_qualification_data_to_sql_task = insert_qualification_data_to_sql(transform_qualification_data_task)
    
    with TaskGroup("GroupPitStop") as GroupPitStop:
        transform_pitstop_data_task = transform_pitstop_data(extract_local_data2_task)
        insert_pitstop_data_to_sql_task = insert_pitstop_data_to_sql(transform_pitstop_data_task)

    with TaskGroup("GroupDriverStand") as GroupDriverStand:
        transform_driver_standings_data_task = transform_driver_standings_data(extract_local_data2_task)
        insert_driver_standings_data_to_sql_task = insert_driver_standings_data_to_sql(transform_driver_standings_data_task)

    with TaskGroup("GroupTeamStand") as GroupTeamStand:
        transform_team_standings_data_task = transform_team_standings_data(extract_local_data3_task)
        insert_team_standings_data_to_sql_task = insert_team_standings_data_to_sql(transform_team_standings_data_task)

    with TaskGroup("GroupLaps") as GroupLaps:
        transform_laps_data_task = transform_laps_data(extract_local_dataLaps_task)
        insert_laps_data_to_sql_task = insert_laps_data_to_sql(transform_laps_data_task)

    with TaskGroup("GroupSprint") as GroupSprint:
        transform_sprint_data_task = transform_sprint_data(extract_local_data3_task)
        insert_sprint_data_to_sql_task = insert_sprint_data_to_sql(transform_sprint_data_task)

    with TaskGroup("GroupTime") as GroupTime:
        transformed_time_data_task = transform_time_data(extract_local_data3_task)
        inserted_time_data = insert_time_data_to_sql(transformed_time_data_task)

    with TaskGroup("GroupResults") as GroupResults:
        transform_results_data_task = transform_results_data(extract_local_data3_task)
        insert_results_data_to_sql_task = insert_results_data_to_sql(transform_results_data_task)



start >> extract_local_data1_task
start >> extract_local_data2_task
start >> extract_local_data3_task
start >> extract_local_dataLaps_task
extract_local_data1_task>> group1
extract_local_data1_task>> group1 >> GroupRace >> GroupQualification >> end
extract_local_data1_task>> group1 >> GroupRace >> GroupPitStop >> end
extract_local_data1_task>> group1 >> GroupRace >> GroupFreePractice >> end
extract_local_data1_task>> group1 >> GroupRace >>  GroupDriverStand >> end
extract_local_data1_task>> group1 >> GroupRace >> GroupTeamStand >> end
extract_local_data1_task>> group1 >> GroupRace >> GroupTime >> end
extract_local_data1_task>> group1 >> GroupRace >>  GroupSprint >> end
extract_local_data1_task>> group1 >> GroupRace >> GroupLaps >> end
extract_local_data1_task>> group1 >> GroupRace >> GroupResults >> end