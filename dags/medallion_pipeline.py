from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
import logging
import subprocess
import os

# Import our custom quality checker
from quality_checks import run_single_quality_check, aggregate_quality_results, get_quality_checker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get configuration from Airflow variables (with defaults)
DB_CONN_ID = Variable.get('db_connection_id', default_var='postgres_default')
ARTIFACTS_PATH = Variable.get('artifacts_path', default_var='/opt/project/artifacts')
CONFIG_PATH = Variable.get('config_path', default_var='/opt/project/config/quality_checks.yaml')

# Initialize quality checker
quality_checker = get_quality_checker(CONFIG_PATH)

default_args = {
    'owner': Variable.get('pipeline_owner', default_var='data-eng'),
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 15),
    'email_on_failure': Variable.get('email_on_failure', default_var=False),
    'email_on_retry': Variable.get('email_on_retry', default_var=False),
    'retries': int(Variable.get('max_retries', default_var=1)),
    'retry_delay': timedelta(minutes=int(Variable.get('retry_delay_minutes', default_var=5))),
}

dag = DAG(
    'medallion_pipeline',
    default_args=default_args,
    description='Medallion Architecture Data Pipeline: Bronze -> Silver -> Gold',
    schedule_interval=Variable.get('schedule_interval', default_var=None),  # Manual trigger
    catchup=False,
)

# SQL Scripts content
def read_sql(filepath):
    try:
        with open(filepath, 'r') as f:
            return f.read()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return f"-- Error reading file: {e}"

init_sql = read_sql('/opt/project/migrations/postgres/00_init.sql')
ddl_bronze_sql = read_sql('/opt/project/migrations/postgres/01_ddl_bronze.sql')
ddl_silver_sql = read_sql('/opt/project/migrations/postgres/02_ddl_silver.sql')
ddl_gold_sql = read_sql('/opt/project/migrations/postgres/03_ddl_gold.sql')
load_bronze_sql = read_sql('/opt/project/migrations/postgres/04_load_bronze.sql')
load_silver_sql = read_sql('/opt/project/migrations/postgres/05_load_silver.sql')
quality_silver_sql = read_sql('/opt/project/tests/silver_quality_checks.sql')
quality_gold_sql = read_sql('/opt/project/tests/gold_quality_checks.sql')

# Init Task
init_db = PostgresOperator(
    task_id='init_db',
    sql=init_sql,
    postgres_conn_id=DB_CONN_ID,
    dag=dag,
)

# DDL Tasks
ddl_bronze = PostgresOperator(
    task_id='ddl_bronze',
    sql=ddl_bronze_sql,
    postgres_conn_id=DB_CONN_ID,
    dag=dag,
)

ddl_silver = PostgresOperator(
    task_id='ddl_silver',
    sql=ddl_silver_sql,
    postgres_conn_id=DB_CONN_ID,
    dag=dag,
)

ddl_gold = PostgresOperator(
    task_id='ddl_gold',
    sql=ddl_gold_sql,
    postgres_conn_id=DB_CONN_ID,
    dag=dag,
)

load_silver = PostgresOperator(
    task_id='load_silver',
    sql=load_silver_sql,
    postgres_conn_id=DB_CONN_ID,
    dag=dag,
)

# Great Expectations Validation Tasks
ge_silver_validation = PythonOperator(
    task_id='ge_silver_validation',
    python_callable=run_ge_validation,
    op_kwargs={
        'checkpoint_name': 'silver_quality_checkpoint',
        'ge_project_path': '/opt/project/great_expectations'
    },
    dag=dag,
)

ge_gold_validation = PythonOperator(
    task_id='ge_gold_validation',
    python_callable=run_ge_validation,
    op_kwargs={
        'checkpoint_name': 'gold_quality_checkpoint',
        'ge_project_path': '/opt/project/great_expectations'
    },
    dag=dag,
)

# Dynamic Quality Check Tasks Generation
def create_quality_check_tasks(layer: str, dag):
    """Create dynamic quality check tasks for parallel execution"""

    # Get check names
    check_names = quality_checker.get_check_names(layer)

    if not check_names:
        logger.warning(f"No quality checks defined for {layer} layer")
        return None, None

    # Create individual check tasks
    check_tasks = []
    for check_name in check_names:
        task = PythonOperator(
            task_id=f'quality_{layer}_{check_name}',
            python_callable=run_single_quality_check,
            op_kwargs={
                'layer': layer,
                'check_name': check_name,
                'postgres_conn_id': DB_CONN_ID
            },
            dag=dag,
        )
        check_tasks.append(task)

    # Create aggregation task
    aggregate_task = PythonOperator(
        task_id=f'quality_{layer}_aggregate',
        python_callable=aggregate_quality_results,
        op_kwargs={
            'layer': layer,
            'check_results': [f"{{{{ ti.xcom_pull(task_ids='quality_{layer}_{{check_name}}') }}}}" for check_name in check_names]
        },
        dag=dag,
    )

    # Set dependencies: all checks run in parallel, then aggregate
    for check_task in check_tasks:
        check_task >> aggregate_task

    return check_tasks, aggregate_task

# Great Expectations Validation Functions
def run_ge_validation(checkpoint_name: str, ge_project_path: str = '/opt/project/great_expectations'):
    """Run Great Expectations validation using checkpoint"""
    try:
        logger.info(f"Running Great Expectations checkpoint: {checkpoint_name}")

        # Change to GE project directory
        os.chdir(ge_project_path)

        # Run GE checkpoint
        cmd = ['great_expectations', 'checkpoint', 'run', checkpoint_name]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        logger.info(f"Great Expectations validation completed for {checkpoint_name}")
        logger.info(f"STDOUT: {result.stdout}")
        if result.stderr:
            logger.warning(f"STDERR: {result.stderr}")

        return {
            'status': 'success',
            'checkpoint': checkpoint_name,
            'output': result.stdout,
            'errors': result.stderr
        }

    except subprocess.CalledProcessError as e:
        logger.error(f"Great Expectations validation failed for {checkpoint_name}: {e}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        return {
            'status': 'failed',
            'checkpoint': checkpoint_name,
            'error': str(e),
            'stdout': e.stdout,
            'stderr': e.stderr
        }
    except Exception as e:
        logger.error(f"Unexpected error running GE validation for {checkpoint_name}: {e}")
        return {
            'status': 'failed',
            'checkpoint': checkpoint_name,
            'error': str(e)
        }

# Create quality check task groups
silver_check_tasks, silver_aggregate = create_quality_check_tasks('silver', dag)
gold_check_tasks, gold_aggregate = create_quality_check_tasks('gold', dag)

# Dependencies
init_db >> ddl_bronze >> ddl_silver >> ddl_gold >> load_bronze >> load_silver >> ge_silver_validation

# GE Silver validation must complete before Gold validation
ge_silver_validation >> ge_gold_validation

# Custom quality checks run after GE validation
if silver_check_tasks:
    ge_silver_validation >> silver_check_tasks

if gold_check_tasks and silver_aggregate:
    silver_aggregate >> gold_check_tasks