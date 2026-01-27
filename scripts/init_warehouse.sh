#!/bin/bash
set -e

echo "=== Initializing Data Warehouse ==="

echo "Creating schemas..."
psql -U airflow -d datawarehouse <<EOF
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
EOF

echo "Running DDL scripts..."
psql -U airflow -d datawarehouse < /docker-entrypoint-initdb.d/01_ddl_bronze.sql
psql -U airflow -d datawarehouse < /docker-entrypoint-initdb.d/02_ddl_silver.sql
psql -U airflow -d datawarehouse < /docker-entrypoint-initdb.d/03_ddl_gold.sql

echo "Loading bronze data..."
psql -U airflow -d datawarehouse <<EOF
TRUNCATE TABLE bronze.crm_cust_info;
COPY bronze.crm_cust_info FROM stdin WITH CSV HEADER;
EOF < /opt/project/datasets/source_crm/cust_info.csv

psql -U airflow -d datawarehouse <<EOF
TRUNCATE TABLE bronze.crm_prd_info;
COPY bronze.crm_prd_info FROM stdin WITH CSV HEADER;
EOF < /opt/project/datasets/source_crm/prd_info.csv

psql -U airflow -d datawarehouse <<EOF
TRUNCATE TABLE bronze.crm_sales_details;
COPY bronze.crm_sales_details FROM stdin WITH CSV HEADER;
EOF < /opt/project/datasets/source_crm/sales_details.csv

psql -U airflow -d datawarehouse <<EOF
TRUNCATE TABLE bronze.erp_loc_a101;
COPY bronze.erp_loc_a101 FROM stdin WITH CSV HEADER;
EOF < /opt/project/datasets/source_erp/LOC_A101.csv

psql -U airflow -d datawarehouse <<EOF
TRUNCATE TABLE bronze.erp_cust_az12;
COPY bronze.erp_cust_az12 FROM stdin WITH CSV HEADER;
EOF < /opt/project/datasets/source_erp/CUST_AZ12.csv

psql -U airflow -d datawarehouse <<EOF
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
COPY bronze.erp_px_cat_g1v2 FROM stdin WITH CSV HEADER;
EOF < /opt/project/datasets/source_erp/PX_CAT_G1V2.csv

echo "Loading silver data..."
psql -U airflow -d datawarehouse < /docker-entrypoint-initdb.d/05_load_silver.sql

echo "=== Data Warehouse Initialization Complete ==="