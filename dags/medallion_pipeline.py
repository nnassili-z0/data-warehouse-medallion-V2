from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
import logging

# Import our custom quality checker
from quality_checks import run_quality_checks

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get configuration from Airflow variables (with defaults)
DB_CONN_ID = Variable.get('db_connection_id', default_var='postgres_default')
ARTIFACTS_PATH = Variable.get('artifacts_path', default_var='/opt/project/artifacts')
CONFIG_PATH = Variable.get('config_path', default_var='/opt/project/config/quality_checks.yaml')

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

# Load Tasks
load_bronze = PostgresOperator(
    task_id='load_bronze',
    sql=load_bronze_sql,
    postgres_conn_id=DB_CONN_ID,
    dag=dag,
)

load_silver = PostgresOperator(
    task_id='load_silver',
    sql=load_silver_sql,
    postgres_conn_id=DB_CONN_ID,
    dag=dag,
)

# Quality Checks
quality_silver = PythonOperator(
    task_id='quality_silver',
    python_callable=run_quality_checks,
    op_kwargs={'layer': 'silver', 'config_path': CONFIG_PATH},
    dag=dag,
)

quality_gold = PythonOperator(
    task_id='quality_gold',
    python_callable=run_quality_checks,
    op_kwargs={'layer': 'gold', 'config_path': CONFIG_PATH},
    dag=dag,
)

# Dependencies
init_db >> ddl_bronze >> ddl_silver >> ddl_gold >> load_bronze >> load_silver >> quality_silver >> quality_gold