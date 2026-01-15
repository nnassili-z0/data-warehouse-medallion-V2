/*
===============================================================================
Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This script loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the COPY command to load data from csv Files to bronze tables.
===============================================================================
*/

-- Loading CRM Tables
TRUNCATE TABLE bronze.crm_cust_info;
COPY bronze.crm_cust_info FROM '/opt/project/datasets/source_crm/cust_info.csv' WITH CSV HEADER;

TRUNCATE TABLE bronze.crm_prd_info;
COPY bronze.crm_prd_info FROM '/opt/project/datasets/source_crm/prd_info.csv' WITH CSV HEADER;

TRUNCATE TABLE bronze.crm_sales_details;
COPY bronze.crm_sales_details FROM '/opt/project/datasets/source_crm/sales_details.csv' WITH CSV HEADER;

-- Loading ERP Tables
TRUNCATE TABLE bronze.erp_loc_a101;
COPY bronze.erp_loc_a101 FROM '/opt/project/datasets/source_erp/LOC_A101.csv' WITH CSV HEADER;

TRUNCATE TABLE bronze.erp_cust_az12;
COPY bronze.erp_cust_az12 FROM '/opt/project/datasets/source_erp/CUST_AZ12.csv' WITH CSV HEADER;

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
COPY bronze.erp_px_cat_g1v2 FROM '/opt/project/datasets/source_erp/PX_CAT_G1V2.csv' WITH CSV HEADER;