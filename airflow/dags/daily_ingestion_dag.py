"""
Daily Data Ingestion DAG
Orchestrate daily ingestion from all data sources
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.ingestion.weather_api_ingestion import WeatherAPIIngestion
from src.ingestion.stock_api_ingestion import StockAPIIngestion
from src.ingestion.csv_ingestion import CSVIngestion
from src.ingestion.database_ingestion import DatabaseIngestion

# Default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='daily_data_ingestion',
    default_args=default_args,
    description='Daily ingestion from APIs, CSV, and Database',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['ingestion', 'daily', 'bronze']
)

# Task functions
def ingest_weather_data(**context):
    """Ingest weather data from API"""
    try:
        ingestion = WeatherAPIIngestion()
        success = ingestion.run()
        
        if not success:
            raise Exception("Weather ingestion failed")
        
        return "Weather data ingested successfully"
    except Exception as e:
        raise Exception(f"Weather ingestion error: {e}")


def ingest_stock_data(**context):
    """Ingest stock data from API"""
    try:
        ingestion = StockAPIIngestion()
        success = ingestion.run()
        
        if not success:
            raise Exception("Stock ingestion failed")
        
        return "Stock data ingested successfully"
    except Exception as e:
        raise Exception(f"Stock ingestion error: {e}")


def ingest_csv_data(**context):
    """Ingest CSV data"""
    try:
        ingestion = CSVIngestion()
        success = ingestion.process_and_upload('ecommerce_transactions.csv')
        
        if not success:
            raise Exception("CSV ingestion failed")
        
        return "CSV data ingested successfully"
    except Exception as e:
        raise Exception(f"CSV ingestion error: {e}")


def ingest_database_data(**context):
    """Ingest data from PostgreSQL"""
    try:
        ingestion = DatabaseIngestion(use_local=True)
        success = ingestion.run(table_name="users")
        
        if not success:
            raise Exception("Database ingestion failed")
        
        return "Database data ingested successfully"
    except Exception as e:
        raise Exception(f"Database ingestion error: {e}")


def validate_ingestion(**context):
    """Validate that all ingestion tasks completed"""
    ti = context['ti']
    
    # Get results from upstream tasks
    weather_result = ti.xcom_pull(task_ids='ingest_weather')
    stock_result = ti.xcom_pull(task_ids='ingest_stocks')
    csv_result = ti.xcom_pull(task_ids='ingest_csv')
    db_result = ti.xcom_pull(task_ids='ingest_database')
    
    print(f"Weather: {weather_result}")
    print(f"Stocks: {stock_result}")
    print(f"CSV: {csv_result}")
    print(f"Database: {db_result}")
    
    return "All ingestion tasks validated"


# Define tasks
task_start = BashOperator(
    task_id='start_ingestion',
    bash_command='echo "Starting daily data ingestion - $(date)"',
    dag=dag
)

task_weather = PythonOperator(
    task_id='ingest_weather',
    python_callable=ingest_weather_data,
    provide_context=True,
    dag=dag
)

task_stocks = PythonOperator(
    task_id='ingest_stocks',
    python_callable=ingest_stock_data,
    provide_context=True,
    dag=dag
)

task_csv = PythonOperator(
    task_id='ingest_csv',
    python_callable=ingest_csv_data,
    provide_context=True,
    dag=dag
)

task_database = PythonOperator(
    task_id='ingest_database',
    python_callable=ingest_database_data,
    provide_context=True,
    dag=dag
)

task_validate = PythonOperator(
    task_id='validate_ingestion',
    python_callable=validate_ingestion,
    provide_context=True,
    dag=dag
)

task_end = BashOperator(
    task_id='end_ingestion',
    bash_command='echo "Daily ingestion completed - $(date)"',
    dag=dag
)

# Define task dependencies
task_start >> [task_weather, task_stocks, task_csv, task_database]
[task_weather, task_stocks, task_csv, task_database] >> task_validate
task_validate >> task_end
