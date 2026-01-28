from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import requests
import json

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
    postgres_conn_id='postgres_default',
    dag=dag,
)

# DDL Tasks
ddl_bronze = PostgresOperator(
    task_id='ddl_bronze',
    sql=ddl_bronze_sql,
    postgres_conn_id='postgres_default',
    dag=dag,
)

ddl_silver = PostgresOperator(
    task_id='ddl_silver',
    sql=ddl_silver_sql,
    postgres_conn_id='postgres_default',
    dag=dag,
)

ddl_gold = PostgresOperator(
    task_id='ddl_gold',
    sql=ddl_gold_sql,
    postgres_conn_id='postgres_default',
    dag=dag,
)

# Load Tasks
load_bronze = PostgresOperator(
    task_id='load_bronze',
    sql=load_bronze_sql,
    postgres_conn_id='postgres_default',
    dag=dag,
)

load_silver = PostgresOperator(
    task_id='load_silver',
    sql=load_silver_sql,
    postgres_conn_id='postgres_default',
    dag=dag,
)

def run_quality_checks(layer, sql_file, output_prefix):
    import json
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Define checks based on layer
    if layer == 'silver':
        checks = [
            {
                'name': 'customer_duplicates',
                'table': 'silver.crm_cust_info',
                'issue_query': "SELECT cst_id, COUNT(*) AS count FROM silver.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1",
                'csv_file': '/opt/project/artifacts/silver_cust_duplicates.csv'
            },
            {
                'name': 'invalid_marital',
                'table': 'silver.crm_cust_info',
                'issue_query': "SELECT cst_id, cst_marital_status FROM silver.crm_cust_info WHERE cst_marital_status NOT IN ('Single', 'Married', 'n/a')",
                'csv_file': '/opt/project/artifacts/silver_invalid_marital.csv'
            },
            {
                'name': 'invalid_gender',
                'table': 'silver.crm_cust_info',
                'issue_query': "SELECT cst_id, cst_gndr FROM silver.crm_cust_info WHERE cst_gndr NOT IN ('Female', 'Male', 'n/a')",
                'csv_file': '/opt/project/artifacts/silver_invalid_gender.csv'
            },
            {
                'name': 'null_cst_id',
                'table': 'silver.crm_cust_info',
                'issue_query': "SELECT * FROM silver.crm_cust_info WHERE cst_id IS NULL",
                'csv_file': '/opt/project/artifacts/silver_null_cst_id.csv'
            },
            {
                'name': 'product_duplicates',
                'table': 'silver.crm_prd_info',
                'issue_query': "SELECT prd_key, COUNT(*) AS count FROM silver.crm_prd_info GROUP BY prd_key HAVING COUNT(*) > 1",
                'csv_file': '/opt/project/artifacts/silver_prd_duplicates.csv'
            },
            {
                'name': 'invalid_prd_line',
                'table': 'silver.crm_prd_info',
                'issue_query': "SELECT prd_id, prd_line FROM silver.crm_prd_info WHERE prd_line NOT IN ('Mountain', 'Road', 'Other Sales', 'Touring', 'n/a')",
                'csv_file': '/opt/project/artifacts/silver_invalid_prd_line.csv'
            },
            {
                'name': 'invalid_order_dates',
                'table': 'silver.crm_sales_details',
                'issue_query': "SELECT sls_ord_num, sls_order_dt FROM silver.crm_sales_details WHERE sls_order_dt IS NULL AND sls_ord_num IS NOT NULL",
                'csv_file': '/opt/project/artifacts/silver_invalid_order_dates.csv'
            }
        ]
    elif layer == 'gold':
        checks = [
            {
                'name': 'customer_key_duplicates',
                'table': 'gold.dim_customers',
                'issue_query': "SELECT customer_key, COUNT(*) AS duplicate_count FROM gold.dim_customers GROUP BY customer_key HAVING COUNT(*) > 1",
                'csv_file': '/opt/project/artifacts/gold_cust_key_duplicates.csv'
            },
            {
                'name': 'product_key_duplicates',
                'table': 'gold.dim_products',
                'issue_query': "SELECT product_key, COUNT(*) AS duplicate_count FROM gold.dim_products GROUP BY product_key HAVING COUNT(*) > 1",
                'csv_file': '/opt/project/artifacts/gold_prod_key_duplicates.csv'
            },
            {
                'name': 'orphaned_sales',
                'table': 'gold.fact_sales',
                'issue_query': "SELECT f.order_number, f.customer_key, f.product_key, c.customer_id, p.product_id FROM gold.fact_sales f LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key LEFT JOIN gold.dim_products p ON p.product_key = f.product_key WHERE c.customer_id IS NULL OR p.product_id IS NULL",
                'csv_file': '/opt/project/artifacts/gold_orphaned_sales.csv'
            }
        ]
    
    summary = {}
    for check in checks:
        # Get total records
        cursor.execute(f"SELECT COUNT(*) FROM {check['table']}")
        total = cursor.fetchone()[0]
        
        # Get issue records
        cursor.execute(check['issue_query'])
        issues = cursor.fetchall()
        issue_count = len(issues)
        
        # Calculate metrics
        clean_count = total - issue_count
        clean_percentage = (clean_count / total * 100) if total > 0 else 0
        issue_percentage = (issue_count / total * 100) if total > 0 else 0
        
        # Save CSV
        with open(check['csv_file'], 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([desc[0] for desc in cursor.description])
            writer.writerows(issues)
        
        # Add to summary
        summary[check['name']] = {
            'total_records': total,
            'issues_found': issue_count,
            'clean_records': clean_count,
            'clean_percentage': round(clean_percentage, 2),
            'issue_percentage': round(issue_percentage, 2),
            'status': 'PASS' if issue_count == 0 else 'FAIL'
        }
    
    # Save summary JSON
    with open(f'/opt/project/artifacts/{layer}_quality_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Also save a text summary
    with open(f'/opt/project/artifacts/{layer}_quality_report.txt', 'w') as f:
        f.write(f"Quality Report for {layer.upper()} Layer\n")
        f.write("=" * 40 + "\n\n")
        for check_name, metrics in summary.items():
            f.write(f"Check: {check_name}\n")
            f.write(f"  Total Records: {metrics['total_records']}\n")
            f.write(f"  Issues Found: {metrics['issues_found']}\n")
            f.write(f"  Clean Records: {metrics['clean_records']}\n")
            f.write(f"  Clean Percentage: {metrics['clean_percentage']}%\n")
            f.write(f"  Issue Percentage: {metrics['issue_percentage']}%\n")
            f.write(f"  Status: {metrics['status']}\n\n")
    
    summary = {}
    for check in checks:
        # Get total records
        cursor.execute(f"SELECT COUNT(*) FROM {check['table']}")
        total = cursor.fetchone()[0]
        
        # Get issue records
        cursor.execute(check['issue_query'])
        issues = cursor.fetchall()
        issue_count = len(issues)
        
        # Calculate metrics
        clean_count = total - issue_count
        clean_percentage = (clean_count / total * 100) if total > 0 else 0
        issue_percentage = (issue_count / total * 100) if total > 0 else 0
        
        # Save CSV
        with open(check['csv_file'], 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([desc[0] for desc in cursor.description])
            writer.writerows(issues)
        
        # Add to summary
        summary[check['name']] = {
            'total_records': total,
            'issues_found': issue_count,
            'clean_records': clean_count,
            'clean_percentage': round(clean_percentage, 2),
            'issue_percentage': round(issue_percentage, 2),
            'status': 'PASS' if issue_count == 0 else 'FAIL'
        }
    
    # Save summary JSON
    with open(f'/opt/project/artifacts/{layer}_quality_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Also save a text summary
    with open(f'/opt/project/artifacts/{layer}_quality_report.txt', 'w') as f:
        f.write(f"Quality Report for {layer.upper()} Layer\n")
        f.write("=" * 40 + "\n\n")
        for check_name, metrics in summary.items():
            f.write(f"Check: {check_name}\n")
            f.write(f"  Total Records: {metrics['total_records']}\n")
            f.write(f"  Issues Found: {metrics['issues_found']}\n")
            f.write(f"  Clean Records: {metrics['clean_records']}\n")
            f.write(f"  Clean Percentage: {metrics['clean_percentage']}%\n")
            f.write(f"  Issue Percentage: {metrics['issue_percentage']}%\n")
            f.write(f"  Status: {metrics['status']}\n\n")

def run_data_profiling(layer):
    import pandas as pd
    from ydata_profiling import ProfileReport
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    
    if layer == 'silver':
        tables = [
            ('silver.crm_cust_info', 'SELECT * FROM silver.crm_cust_info'),
            ('silver.crm_prd_info', 'SELECT * FROM silver.crm_prd_info'),
            ('silver.crm_sales_details', 'SELECT * FROM silver.crm_sales_details'),
        ]
    elif layer == 'gold':
        tables = [
            ('gold.dim_customers', 'SELECT * FROM gold.dim_customers'),
            ('gold.dim_products', 'SELECT * FROM gold.dim_products'),
            ('gold.fact_sales', 'SELECT * FROM gold.fact_sales'),
        ]
    
    for table_name, query in tables:
        df = pd.read_sql(query, conn)
        profile = ProfileReport(df, title=f"Profile for {table_name}", minimal=True)
        output_path = f'/opt/project/artifacts/{table_name.replace(".", "_")}_profile.html'
        profile.to_file(output_path)
        print(f"Profile generated for {table_name}")

def run_pandera_validation(layer):
    import pandas as pd
    from scripts.data_schemas import bronze_crm_cust_schema, silver_crm_cust_schema, gold_dim_customers_schema
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    
    if layer == 'bronze':
        df = pd.read_sql('SELECT * FROM bronze.crm_cust_info', conn)
        schema = bronze_crm_cust_schema
        table_name = 'bronze.crm_cust_info'
    elif layer == 'silver':
        df = pd.read_sql('SELECT * FROM silver.crm_cust_info', conn)
        schema = silver_crm_cust_schema
        table_name = 'silver.crm_cust_info'
    elif layer == 'gold':
        df = pd.read_sql('SELECT * FROM gold.dim_customers', conn)
        schema = gold_dim_customers_schema
        table_name = 'gold.dim_customers'
    
    try:
        validated_df = schema.validate(df)
        with open(f'/opt/project/artifacts/{table_name.replace(".", "_")}_pandera_validation.json', 'w') as f:
            json.dump({"status": "PASS", "message": "Validation successful"}, f)
        print(f"Pandera validation passed for {table_name}")
    except Exception as e:
        with open(f'/opt/project/artifacts/{table_name.replace(".", "_")}_pandera_validation.json', 'w') as f:
            json.dump({"status": "FAIL", "message": str(e)}, f)
        print(f"Pandera validation failed for {table_name}: {e}")
        raise e

def send_alert(message, webhook_url=None):
    if not webhook_url:
        webhook_url = os.getenv('SLACK_WEBHOOK_URL', 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK')
    payload = {"text": message}
    response = requests.post(webhook_url, json=payload)
    if response.status_code != 200:
        print(f"Failed to send alert: {response.text}")
    else:
        print("Alert sent successfully")

# Quality Checks
quality_silver = PythonOperator(
    task_id='quality_silver',
    python_callable=run_quality_checks,
    op_kwargs={'layer': 'silver', 'sql_file': '/opt/project/tests/silver_quality_checks.sql', 'output_prefix': 'silver'},
    dag=dag,
)

quality_gold = PythonOperator(
    task_id='quality_gold',
    python_callable=run_quality_checks,
    op_kwargs={'layer': 'gold', 'sql_file': '/opt/project/tests/gold_quality_checks.sql', 'output_prefix': 'gold'},
    dag=dag,
)

# Profiling Tasks
profile_silver = PythonOperator(
    task_id='profile_silver',
    python_callable=run_data_profiling,
    op_kwargs={'layer': 'silver'},
    dag=dag,
)

profile_gold = PythonOperator(
    task_id='profile_gold',
    python_callable=run_data_profiling,
    op_kwargs={'layer': 'gold'},
    dag=dag,
)

# Pandera Validation Tasks
pandera_bronze = PythonOperator(
    task_id='pandera_bronze',
    python_callable=run_pandera_validation,
    op_kwargs={'layer': 'bronze'},
    dag=dag,
)

pandera_silver = PythonOperator(
    task_id='pandera_silver',
    python_callable=run_pandera_validation,
    op_kwargs={'layer': 'silver'},
    dag=dag,
)

pandera_gold = PythonOperator(
    task_id='pandera_gold',
    python_callable=run_pandera_validation,
    op_kwargs={'layer': 'gold'},
    dag=dag,
)

# Alert Task (example for failures)
alert_failure = PythonOperator(
    task_id='alert_failure',
    python_callable=send_alert,
    op_kwargs={'message': 'Data quality validation failed in the pipeline'},
    dag=dag,
    trigger_rule='one_failed',  # Run if any upstream task fails
)

# Dependencies
init_db >> ddl_bronze >> ddl_silver >> ddl_gold >> load_bronze >> load_silver >> [quality_silver, profile_silver, pandera_silver] >> [quality_gold, profile_gold, pandera_gold] >> alert_failure