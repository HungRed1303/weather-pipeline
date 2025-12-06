"""
DAG 1: Weather Data Ingestion
Fetches weather data from Open-Meteo API every hour and stores in PostgreSQL
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import sys

# Add plugins to path
sys.path.insert(0, '/opt/airflow/plugins')

from weather_api import fetch_weather_for_all_cities
from data_transformer import transform_weather_batch

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'weather_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'weather_ingestion',
    default_args=default_args,
    description='Fetch and store weather data hourly',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    catchup=False,
    tags=['weather', 'ingestion', 'etl'],
)


def fetch_weather_data(**context):
    """Task 1: Fetch weather data from API"""
    logger.info("Starting weather data fetch...")
    
    try:
        weather_data = fetch_weather_for_all_cities()
        
        if not weather_data:
            raise ValueError("No weather data returned from API")
        
        logger.info(f"Fetched data for {len(weather_data)} locations")
        
        # Push to XCom for next task
        context['task_instance'].xcom_push(key='weather_data', value=weather_data)
        
        return f"Successfully fetched data for {len(weather_data)} locations"
        
    except Exception as e:
        logger.error(f"Error fetching weather data: {str(e)}")
        raise


def validate_weather_data(**context):
    """Task 2: Validate fetched data"""
    logger.info("Validating weather data...")
    
    # Pull data from previous task
    weather_data = context['task_instance'].xcom_pull(
        task_ids='fetch_weather',
        key='weather_data'
    )
    
    if not weather_data:
        raise ValueError("No data to validate")
    
    validation_results = {}
    
    for location_id, data in weather_data.items():
        is_valid = True
        issues = []
        
        # Check current weather exists
        if not data.get('current'):
            is_valid = False
            issues.append("Missing current weather")
        
        # Check temperature is reasonable
        current_temp = data.get('current', {}).get('temperature')
        if current_temp is None or current_temp < -50 or current_temp > 60:
            is_valid = False
            issues.append(f"Invalid temperature: {current_temp}")
        
        # Check hourly data exists
        if not data.get('hourly'):
            is_valid = False
            issues.append("Missing hourly data")
        
        validation_results[location_id] = {
            'valid': is_valid,
            'issues': issues
        }
        
        if not is_valid:
            logger.warning(f"Validation failed for location {location_id}: {issues}")
    
    # Push validated data
    context['task_instance'].xcom_push(key='weather_data', value=weather_data)
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)
    
    valid_count = sum(1 for v in validation_results.values() if v['valid'])
    logger.info(f"Validation complete: {valid_count}/{len(validation_results)} valid")
    
    return f"Validated {valid_count}/{len(validation_results)} locations"


def transform_weather_data(**context):
    """Task 3: Transform and engineer features"""
    logger.info("Transforming weather data...")
    
    # Pull data from previous task
    weather_data = context['task_instance'].xcom_pull(
        task_ids='validate_weather',
        key='weather_data'
    )
    
    if not weather_data:
        raise ValueError("No data to transform")
    
    # Transform using our transformer
    transformed_data = transform_weather_batch(weather_data)
    
    if not transformed_data:
        raise ValueError("Transformation produced no results")
    
    logger.info(f"Transformed data for {len(transformed_data)} locations")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='transformed_data', value=transformed_data)
    context['task_instance'].xcom_push(key='raw_weather_data', value=weather_data)
    
    return f"Transformed {len(transformed_data)} locations"


def load_current_weather(**context):
    """Task 4a: Load current weather to database"""
    logger.info("Loading current weather to database...")
    
    weather_data = context['task_instance'].xcom_pull(
        task_ids='transform_weather',
        key='raw_weather_data'
    )
    
    if not weather_data:
        logger.warning("No weather data to load")
        return "No data"
    
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    inserted_count = 0
    
    try:
        for location_id, data in weather_data.items():
            current = data.get('current', {})
            
            if not current:
                continue
            
            # Insert current weather
            insert_sql = """
                INSERT INTO current_weather (
                    location_id, timestamp, temperature, relative_humidity,
                    weather_code, wind_speed, wind_direction, precipitation, pressure
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (location_id, timestamp) DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    relative_humidity = EXCLUDED.relative_humidity,
                    weather_code = EXCLUDED.weather_code,
                    wind_speed = EXCLUDED.wind_speed,
                    wind_direction = EXCLUDED.wind_direction,
                    precipitation = EXCLUDED.precipitation,
                    pressure = EXCLUDED.pressure,
                    created_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(insert_sql, (
                location_id,
                current.get('timestamp'),
                current.get('temperature'),
                current.get('humidity'),
                current.get('weather_code'),
                current.get('wind_speed'),
                current.get('wind_direction'),
                current.get('precipitation'),
                current.get('pressure')
            ))
            inserted_count += 1
        
        conn.commit()
        logger.info(f"Inserted {inserted_count} current weather records")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading current weather: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
    
    return f"Loaded {inserted_count} records"


def load_hourly_weather(**context):
    """Task 4b: Load hourly weather with features to database"""
    logger.info("Loading hourly weather to database...")
    
    transformed_data = context['task_instance'].xcom_pull(
        task_ids='transform_weather',
        key='transformed_data'
    )
    
    if not transformed_data:
        logger.warning("No transformed data to load")
        return "No data"
    
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    inserted_count = 0
    
    try:
        for location_id, df in transformed_data.items():
            for _, row in df.iterrows():
                insert_sql = """
                    INSERT INTO hourly_weather (
                        location_id, forecast_timestamp, temperature, precipitation,
                        weather_code, cloud_cover, temp_change_1h, temp_rolling_avg_3h,
                        temp_rolling_std_3h, hour, day_of_week, month, is_weekend,
                        is_morning, is_afternoon, is_evening, temp_lag_1h, temp_lag_3h,
                        temp_lag_24h, is_raining, rain_intensity
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (location_id, forecast_timestamp) DO UPDATE SET
                        temperature = EXCLUDED.temperature,
                        precipitation = EXCLUDED.precipitation,
                        weather_code = EXCLUDED.weather_code,
                        temp_change_1h = EXCLUDED.temp_change_1h
                """
                
                cursor.execute(insert_sql, (
                    location_id,
                    row.get('timestamp'),
                    row.get('temperature'),
                    row.get('precipitation'),
                    row.get('weather_code'),
                    row.get('cloud_cover'),
                    row.get('temp_change_1h'),
                    row.get('temp_rolling_avg_3h'),
                    row.get('temp_rolling_std_3h'),
                    row.get('hour'),
                    row.get('day_of_week'),
                    row.get('month'),
                    bool(row.get('is_weekend', 0)),
                    bool(row.get('is_morning', 0)),
                    bool(row.get('is_afternoon', 0)),
                    bool(row.get('is_evening', 0)),
                    row.get('temp_lag_1h'),
                    row.get('temp_lag_3h'),
                    row.get('temp_lag_24h'),
                    bool(row.get('is_raining', 0)),
                    str(row.get('rain_intensity', 'none'))
                ))
                inserted_count += 1
        
        conn.commit()
        logger.info(f"Inserted {inserted_count} hourly weather records")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading hourly weather: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
    
    return f"Loaded {inserted_count} records"


def log_etl_job(**context):
    """Task 5: Log ETL job execution"""
    logger.info("Logging ETL job...")
    
    # Get task statuses
    task_instance = context['task_instance']
    dag_run = context['dag_run']
    
    # Count processed records
    weather_data = task_instance.xcom_pull(
        task_ids='fetch_weather',
        key='weather_data'
    )
    records_processed = len(weather_data) if weather_data else 0
    
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        insert_sql = """
            INSERT INTO etl_job_logs (
                job_name, job_type, status, records_processed,
                started_at, completed_at, duration_seconds
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        duration = (datetime.now() - dag_run.start_date).total_seconds()
        
        cursor.execute(insert_sql, (
            'weather_ingestion',
            'ingestion',
            'success',
            records_processed,
            dag_run.start_date,
            datetime.now(),
            int(duration)
        ))
        
        conn.commit()
        logger.info("ETL job logged successfully")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error logging ETL job: {str(e)}")
    finally:
        cursor.close()
        conn.close()
    
    return "Job logged"


# Define tasks
fetch_weather = PythonOperator(
    task_id='fetch_weather',
    python_callable=fetch_weather_data,
    dag=dag,
)

validate_weather = PythonOperator(
    task_id='validate_weather',
    python_callable=validate_weather_data,
    dag=dag,
)

transform_weather = PythonOperator(
    task_id='transform_weather',
    python_callable=transform_weather_data,
    dag=dag,
)

load_current = PythonOperator(
    task_id='load_current_weather',
    python_callable=load_current_weather,
    dag=dag,
)

load_hourly = PythonOperator(
    task_id='load_hourly_weather',
    python_callable=load_hourly_weather,
    dag=dag,
)

log_job = PythonOperator(
    task_id='log_etl_job',
    python_callable=log_etl_job,
    dag=dag,
)

# Define task dependencies
fetch_weather >> validate_weather >> transform_weather >> [load_current, load_hourly] >> log_job