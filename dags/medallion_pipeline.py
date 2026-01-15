from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'medallion_pipeline',
    default_args=default_args,
    description='Medallion Architecture Data Pipeline: Bronze -> Silver -> Gold',
    schedule_interval=None,  # Manual trigger
    catchup=False,
)

# DDL Tasks
ddl_bronze = BashOperator(
    task_id='ddl_bronze',
    bash_command='psql postgresql://airflow:airflow@postgres:5432/datawarehouse -f /opt/project/migrations/postgres/01_ddl_bronze.sql',
    dag=dag,
)

ddl_silver = BashOperator(
    task_id='ddl_silver',
    bash_command='psql postgresql://airflow:airflow@postgres:5432/datawarehouse -f /opt/project/migrations/postgres/02_ddl_silver.sql',
    dag=dag,
)

ddl_gold = BashOperator(
    task_id='ddl_gold',
    bash_command='psql postgresql://airflow:airflow@postgres:5432/datawarehouse -f /opt/project/migrations/postgres/03_ddl_gold.sql',
    dag=dag,
)

# Load Tasks
load_bronze = BashOperator(
    task_id='load_bronze',
    bash_command='psql postgresql://airflow:airflow@postgres:5432/datawarehouse -f /opt/project/migrations/postgres/04_load_bronze.sql',
    dag=dag,
)

load_silver = BashOperator(
    task_id='load_silver',
    bash_command='psql postgresql://airflow:airflow@postgres:5432/datawarehouse -f /opt/project/migrations/postgres/05_load_silver.sql',
    dag=dag,
)

# Quality Checks
quality_silver = BashOperator(
    task_id='quality_silver',
    bash_command='psql postgresql://airflow:airflow@postgres:5432/datawarehouse -f /opt/project/tests/silver_quality_checks.sql',
    dag=dag,
)

quality_gold = BashOperator(
    task_id='quality_gold',
    bash_command='psql postgresql://airflow:airflow@postgres:5432/datawarehouse -f /opt/project/tests/gold_quality_checks.sql',
    dag=dag,
)

# Dependencies
ddl_bronze >> ddl_silver >> ddl_gold >> load_bronze >> load_silver >> quality_silver >> quality_gold