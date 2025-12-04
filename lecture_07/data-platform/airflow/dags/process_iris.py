"""
DAG for processing Iris dataset with dbt transformation and ML model training
This DAG works with the Docker-based data platform setup
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.exceptions import AirflowException
import logging
import os
import sys

# Add the parent directory to the path to import custom modules
sys.path.insert(0, os.path.dirname(__file__))

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='process_iris',
    default_args=default_args,
    description='Process Iris dataset with dbt and train ML model',
    schedule_interval='0 22 * * *',  # 1 AM Kyiv time (GMT+3) = 22:00 UTC (GMT+0)
    start_date=datetime(2025, 4, 22),
    end_date=datetime(2025, 4, 24),
    catchup=True,  # Process historical dates
    max_active_runs=1,
    tags=['iris', 'ml', 'dbt'],
)


def run_dbt_transformation(**context):
    """
    Execute dbt transformation pipeline for Iris dataset
    Runs dbt models for staging and mart layers
    """
    import subprocess
    
    execution_date = context['ds']  # Date string in YYYY-MM-DD format
    logging.info(f"Running dbt transformation for date: {execution_date}")
    
    # dbt project path in the Docker container
    dbt_project_path = "/opt/airflow/dbt/homework"
    
    try:
        # Run dbt seed to load iris dataset
        logging.info("Running dbt seed to load iris dataset...")
        seed_result = subprocess.run(
            ["dbt", "seed", "--project-dir", dbt_project_path, "--profiles-dir", "/opt/airflow/dbt"],
            capture_output=True,
            text=True,
            check=True
        )
        logging.info(f"dbt seed output: {seed_result.stdout}")
        
        # Run dbt models for staging layer
        logging.info("Running dbt staging models...")
        staging_result = subprocess.run(
            ["dbt", "run", "--models", "staging.*", "--project-dir", dbt_project_path, "--profiles-dir", "/opt/airflow/dbt"],
            capture_output=True,
            text=True,
            check=True
        )
        logging.info(f"dbt staging output: {staging_result.stdout}")
        
        # Run dbt models for mart layer (final transformations)
        logging.info("Running dbt mart models...")
        mart_result = subprocess.run(
            ["dbt", "run", "--models", "mart.*", "--project-dir", dbt_project_path, "--profiles-dir", "/opt/airflow/dbt"],
            capture_output=True,
            text=True,
            check=True
        )
        logging.info(f"dbt mart output: {mart_result.stdout}")
        
        logging.info(f"DBT transformation successful for {execution_date}")
        return {
            "status": "success",
            "execution_date": execution_date,
            "models_run": ["stg_iris", "iris_processed"]
        }
        
    except subprocess.CalledProcessError as e:
        error_msg = f"DBT transformation failed: {e.stderr}"
        logging.error(error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"DBT transformation failed: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)


def train_ml_model(**context):
    """
    Train ML classifier model on transformed Iris dataset
    Uses the iris_ml_processor.py script
    """
    execution_date = context['ds']
    logging.info(f"Training ML model for date: {execution_date}")
    
    try:
        # Import the ML processing function
        # This assumes iris_ml_processor.py is in the dags directory or accessible
        from iris_ml_processor import process_iris_data
        
        # Execute the ML training
        result = process_iris_data()
        
        logging.info(f"ML model training successful for {execution_date}")
        logging.info(f"Top features: {result['top_features']}")
        logging.info(f"Full model accuracy: {result['full_model_accuracy']:.4f}")
        logging.info(f"Top 5 features model accuracy: {result['top5_model_accuracy']:.4f}")
        
        return {
            "status": "success",
            "execution_date": execution_date,
            "results": result
        }
        
    except ImportError as e:
        error_msg = f"Could not import iris_ml_processor: {str(e)}"
        logging.error(error_msg)
        logging.error("Make sure iris_ml_processor.py is in the dags directory")
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"ML training failed: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)


# Task 1: Run dbt transformation
dbt_transform_task = PythonOperator(
    task_id='dbt_transform',
    python_callable=run_dbt_transformation,
    provide_context=True,
    dag=dag,
)

# Task 2: Train ML model
train_model_task = PythonOperator(
    task_id='train_ml_model',
    python_callable=train_ml_model,
    provide_context=True,
    dag=dag,
)

# Task 3: Send success email notification
send_email_task = EmailOperator(
    task_id='send_success_email',
    to='sytenok03@meta.ua',  # Replace with your email
    subject='Airflow: Iris Pipeline Success - {{ ds }}',
    html_content="""
    <h3>Iris Data Pipeline Completed Successfully</h3>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>DAG:</strong> {{ dag.dag_id }}</p>
    <p><strong>Run ID:</strong> {{ run_id }}</p>
    <p>All tasks completed successfully:</p>
    <ul>
        <li>✓ DBT Transformation (stg_iris + iris_processed)</li>
        <li>✓ ML Model Training (RandomForest Classifier)</li>
    </ul>
    <p>Check the Airflow UI for detailed logs and model metrics.</p>
    <p>Results saved to ml_results schema in PostgreSQL.</p>
    """,
    dag=dag,
)

# Define task dependencies
dbt_transform_task >> train_model_task >> send_email_task
