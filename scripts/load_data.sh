#!/bin/bash

echo "Loading CRM data..."
cat /opt/project/datasets/source_crm/cust_info.csv | docker exec -i docker-postgres-1 psql -U airflow -d datawarehouse -c "TRUNCATE TABLE bronze.crm_cust_info; COPY bronze.crm_cust_info FROM stdin WITH CSV HEADER;"

echo "Done loading cust_info"