"""
Full End-to-End Pipeline DAG
Complete data pipeline: Ingestion → Transformation → Analytics
"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

# DAG definition
dag = DAG(
    dag_id='full_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline from ingestion to analytics',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['full_pipeline', 'etl', 'end_to_end']
)

# Define tasks
task_start = BashOperator(
    task_id='start_pipeline',
    bash_command='echo "Starting full ETL pipeline - $(date)"',
    dag=dag
)

# Trigger ingestion DAG
task_trigger_ingestion = TriggerDagRunOperator(
    task_id='trigger_ingestion',
    trigger_dag_id='daily_data_ingestion',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

# Wait between stages
task_wait = BashOperator(
    task_id='wait_between_stages',
    bash_command='sleep 60 && echo "Waiting complete"',
    dag=dag
)

# Trigger transformation DAG
task_trigger_transformation = TriggerDagRunOperator(
    task_id='trigger_transformation',
    trigger_dag_id='data_transformation_pipeline',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag
)

task_end = BashOperator(
    task_id='end_pipeline',
    bash_command='echo "Full ETL pipeline completed successfully - $(date)"',
    dag=dag
)

# Define dependencies
task_start >> task_trigger_ingestion >> task_wait >> task_trigger_transformation >> task_end
