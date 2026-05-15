from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# Assuming scripts are in a directory accessible to Airflow (e.g., mapped volume)
sys.path.insert(0, '/opt/airflow/dags/scripts')

from extract_nyc311 import NYC311Extractor
from transform_nyc311 import NYC311Transformer
from load_nyc311 import NYC311Loader

# Default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email': ['alerts@yourdomain.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'nyc311_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for NYC 311 service requests',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['nyc311', 'etl', 'postgres'],
)

def extract_task(**context):
    """Extract data from NYC 311 API"""
    extractor = NYC311Extractor()
    
    # Extract yesterday's data
    execution_date = context['execution_date']
    start_date = execution_date.strftime('%Y-%m-%d')
    end_date = (execution_date + timedelta(days=1)).strftime('%Y-%m-%d')
    
    df = extractor.extract_incremental(start_date, end_date)
    filepath = extractor.save_to_staging(df, staging_dir='/opt/airflow/data/staging')
    
    # Pass filepath to next task
    context['task_instance'].xcom_push(key='staging_filepath', value=filepath)
    
    return f"Extracted {len(df)} records"

def transform_task(**context):
    """Transform raw data"""
    import pandas as pd
    
    # Get filepath from previous task
    filepath = context['task_instance'].xcom_pull(key='staging_filepath', task_ids='extract')
    
    # Load and transform
    df_raw = pd.read_json(filepath)
    transformer = NYC311Transformer(df_raw)
    df_clean = transformer.transform_pipeline()
    
    # Save clean data
    clean_filepath = filepath.replace('raw', 'clean').replace('.json', '.csv')
    df_clean.to_csv(clean_filepath, index=False)
    
    # Pass to next task
    context['task_instance'].xcom_push(key='clean_filepath', value=clean_filepath)
    
    return f"Transformed {len(df_clean)} records"

def load_task(**context):
    """Load data to warehouse"""
    import pandas as pd
    
    # Get filepath
    filepath = context['task_instance'].xcom_pull(key='clean_filepath', task_ids='transform')
    
    # Load to warehouse
    df = pd.read_csv(filepath)
    conn_string = "postgresql://postgres:yourpassword@postgres-nyc311:5432/nyc311_warehouse"
    loader = NYC311Loader(conn_string)
    loader.execute_full_load(df)
    
    return f"Loaded {len(df)} records to warehouse"

def validate_task(**context):
    """Validate pipeline execution"""
    from sqlalchemy import create_engine
    import pandas as pd
    
    engine = create_engine("postgresql://postgres:yourpassword@postgres-nyc311:5432/nyc311_warehouse")
    
    # Count today's records
    execution_date = context['execution_date'].strftime('%Y%m%d')
    query = f"""
        SELECT COUNT(*) as record_count
        FROM warehouse.fact_complaints
        WHERE date_key = {execution_date}
    """
    result = pd.read_sql(query, engine)
    record_count = result['record_count'].iloc[0]
    
    if record_count == 0:
        raise ValueError(f"Validation failed: 0 records loaded for {execution_date}")
    
    return f"Validation passed: {record_count} records"

# Define tasks
task_extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    provide_context=True,
    dag=dag,
)

task_transform = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load',
    python_callable=load_task,
    provide_context=True,
    dag=dag,
)

task_validate = PythonOperator(
    task_id='validate',
    python_callable=validate_task,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task_extract >> task_transform >> task_load >> task_validate
