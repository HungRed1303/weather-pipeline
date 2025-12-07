"""
DAG 2: ML Forecast & Telegram Bot
Trains Prophet model, makes predictions, and sends to Telegram
Runs daily at 6 PM
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import sys
import pandas as pd
import numpy as np

# Add plugins to path
sys.path.insert(0, '/opt/airflow/plugins')

from ml_model import WeatherForecastModel, train_models_for_all_locations
from telegram_bot import TelegramWeatherBot, send_daily_forecast

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'weather_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

# Create DAG
dag = DAG(
    'weather_ml_forecast',
    default_args=default_args,
    description='Train ML model and send Telegram forecast',
    schedule_interval='0 18 * * *',  # Every day at 6 PM
    catchup=False,
    tags=['weather', 'ml', 'telegram', 'forecast'],
)


def convert_to_json_serializable(obj):
    """
    Convert numpy/pandas types to JSON-serializable Python types
    """
    if isinstance(obj, dict):
        return {convert_to_json_serializable(k): convert_to_json_serializable(v) 
                for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    elif isinstance(obj, pd.DataFrame):
        # Convert DataFrame to dict with JSON-serializable types
        return obj.to_dict('records')
    elif isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    else:
        return obj


def load_training_data(**context):
    """Task 1: Load historical weather data for training"""
    logger.info("Loading training data from database...")
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    
    try:
        # Load all available data (minimum 2 days needed for Prophet)
        query = """
            SELECT 
                location_id,
                forecast_timestamp as timestamp,
                temperature,
                precipitation,
                hour,
                day_of_week,
                month,
                is_weekend
            FROM hourly_weather
            WHERE forecast_timestamp < NOW()
            ORDER BY location_id, forecast_timestamp
        """
        
        df = pd.read_sql(query, conn)
        
        if df.empty:
            raise ValueError("No training data available")
        
        logger.info(f"Loaded {len(df)} training records")
        
        # Group by location and convert to JSON-serializable format
        training_data = {}
        for location_id in df['location_id'].unique():
            location_df = df[df['location_id'] == location_id].copy()
            
            # Convert location_id from numpy.int64 to Python int
            location_id_int = int(location_id)
            
            # Convert DataFrame to records (list of dicts)
            training_data[location_id_int] = location_df.to_dict('records')
            logger.info(f"Location {location_id_int}: {len(location_df)} records")
        
        # Convert all numpy types to Python types
        training_data = convert_to_json_serializable(training_data)
        
        # Push to XCom
        context['task_instance'].xcom_push(key='training_data', value=training_data)
        
        return f"Loaded data for {len(training_data)} locations"
        
    except Exception as e:
        logger.error(f"Error loading training data: {str(e)}")
        raise
    finally:
        conn.close()


def train_models(**context):
    """Task 2: Train Prophet models"""
    logger.info("Training ML models...")
    
    # Pull training data
    training_data_records = context['task_instance'].xcom_pull(
        task_ids='load_training_data',
        key='training_data'
    )
    
    if not training_data_records:
        raise ValueError("No training data available")
    
    # Convert records back to DataFrames
    training_data = {}
    for location_id, records in training_data_records.items():
        # Make sure location_id is int
        location_id = int(location_id)
        df = pd.DataFrame(records)
        # Convert timestamp strings back to datetime with ISO8601 format
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
        training_data[location_id] = df
    
    # Train models
    results = train_models_for_all_locations(training_data)
    
    trained_count = len(results['trained_models'])
    error_count = len(results['errors'])
    
    logger.info(f"Training complete: {trained_count} models trained, {error_count} errors")
    
    # Convert results to JSON-serializable format
    results = convert_to_json_serializable(results)
    
    # Push results
    context['task_instance'].xcom_push(key='training_results', value=results)
    
    if trained_count == 0:
        raise ValueError("No models were successfully trained")
    
    return f"Trained {trained_count} models"


def make_predictions(**context):
    """Task 3: Generate predictions for next 24 hours"""
    logger.info("Generating predictions...")
    
    # Initialize model manager
    model_manager = WeatherForecastModel()
    
    # Load trained models
    model_manager.load_all_models()
    
    if not model_manager.models:
        raise ValueError("No trained models available")
    
    # Make predictions (24 hours ahead)
    predictions = model_manager.predict_all_locations(hours_ahead=24)
    
    if not predictions:
        raise ValueError("No predictions generated")
    
    logger.info(f"Generated predictions for {len(predictions)} locations")
    
    # Convert predictions to JSON-serializable format
    predictions_serializable = {}
    for location_id, pred_df in predictions.items():
        location_id_int = int(location_id)
        predictions_serializable[location_id_int] = pred_df.to_dict('records')
    
    predictions_serializable = convert_to_json_serializable(predictions_serializable)
    
    # Push predictions
    context['task_instance'].xcom_push(key='predictions', value=predictions_serializable)
    
    return f"Predictions for {len(predictions)} locations"


def save_predictions_to_db(**context):
    """Task 4: Save predictions to database"""
    logger.info("Saving predictions to database...")
    
    predictions_records = context['task_instance'].xcom_pull(
        task_ids='make_predictions',
        key='predictions'
    )
    
    if not predictions_records:
        logger.warning("No predictions to save")
        return "No data"
    
    # Convert records back to DataFrames
    predictions = {}
    for location_id, records in predictions_records.items():
        location_id = int(location_id)
        pred_df = pd.DataFrame(records)
        if 'timestamp' in pred_df.columns:
            pred_df['timestamp'] = pd.to_datetime(pred_df['timestamp'], format='ISO8601')
        predictions[location_id] = pred_df
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    inserted_count = 0
    
    try:
        for location_id, pred_df in predictions.items():
            for _, row in pred_df.iterrows():
                insert_sql = """
                    INSERT INTO weather_predictions (
                        location_id, prediction_timestamp, predicted_temperature,
                        prediction_lower, prediction_upper, model_version
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (location_id, prediction_timestamp, model_version, created_at)
                    DO NOTHING
                """
                
                cursor.execute(insert_sql, (
                    location_id,
                    row['timestamp'],
                    row['predicted_temperature'],
                    row['lower_bound'],
                    row['upper_bound'],
                    'prophet_v1.0'
                ))
                inserted_count += 1
        
        conn.commit()
        logger.info(f"Saved {inserted_count} predictions to database")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving predictions: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
    
    return f"Saved {inserted_count} predictions"


def send_telegram_forecast(**context):
    """Task 5: Send forecast to Telegram"""
    logger.info("Sending forecast to Telegram...")
    
    # Pull predictions
    predictions_records = context['task_instance'].xcom_pull(
        task_ids='make_predictions',
        key='predictions'
    )
    
    if not predictions_records:
        raise ValueError("No predictions to send")
    
    # Convert records back to DataFrames
    predictions = {}
    for location_id, records in predictions_records.items():
        location_id = int(location_id)
        pred_df = pd.DataFrame(records)
        if 'timestamp' in pred_df.columns:
            pred_df['timestamp'] = pd.to_datetime(pred_df['timestamp'], format='ISO8601')
        predictions[location_id] = pred_df
    
    # Initialize bot
    bot = TelegramWeatherBot()
    
    # Test connection
    if not bot.test_connection():
        raise ValueError("Telegram bot connection failed")
    
    # Send forecast
    success = bot.send_forecast(predictions)
    
    if not success:
        raise ValueError("Failed to send Telegram message")
    
    logger.info("Forecast sent successfully to Telegram")
    
    return "Forecast sent"


def send_training_report(**context):
    """Task 6: Send training report to Telegram"""
    logger.info("Sending training report...")
    
    training_results = context['task_instance'].xcom_pull(
        task_ids='train_models',
        key='training_results'
    )
    
    if not training_results:
        logger.warning("No training results to report")
        return "No data"
    
    bot = TelegramWeatherBot()
    success = bot.send_training_report(training_results)
    
    if success:
        logger.info("Training report sent successfully")
    else:
        logger.warning("Failed to send training report")
    
    return "Report sent" if success else "Report failed"


def log_ml_job(**context):
    """Task 7: Log ML job execution"""
    logger.info("Logging ML job...")
    
    task_instance = context['task_instance']
    dag_run = context['dag_run']
    
    predictions_records = task_instance.xcom_pull(
        task_ids='make_predictions',
        key='predictions'
    )
    
    records_processed = 0
    if predictions_records:
        for records in predictions_records.values():
            records_processed += len(records)
    
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
            'weather_ml_forecast',
            'ml_training',
            'success',
            records_processed,
            dag_run.start_date,
            datetime.now(),
            int(duration)
        ))
        
        conn.commit()
        logger.info("ML job logged successfully")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error logging ML job: {str(e)}")
    finally:
        cursor.close()
        conn.close()
    
    return "Job logged"


# Define tasks
load_training = PythonOperator(
    task_id='load_training_data',
    python_callable=load_training_data,
    dag=dag,
)

train = PythonOperator(
    task_id='train_models',
    python_callable=train_models,
    dag=dag,
)

predict = PythonOperator(
    task_id='make_predictions',
    python_callable=make_predictions,
    dag=dag,
)

save_predictions = PythonOperator(
    task_id='save_predictions',
    python_callable=save_predictions_to_db,
    dag=dag,
)

send_forecast = PythonOperator(
    task_id='send_telegram_forecast',
    python_callable=send_telegram_forecast,
    dag=dag,
)

send_report = PythonOperator(
    task_id='send_training_report',
    python_callable=send_training_report,
    dag=dag,
)

log_job = PythonOperator(
    task_id='log_ml_job',
    python_callable=log_ml_job,
    dag=dag,
)

# Define task dependencies
load_training >> train >> predict >> [save_predictions, send_forecast] >> send_report >> log_job