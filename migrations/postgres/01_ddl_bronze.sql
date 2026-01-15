/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

DROP TABLE IF EXISTS bronze.crm_cust_info CASCADE;
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             TEXT,
    cst_firstname       TEXT,
    cst_lastname        TEXT,
    cst_marital_status  TEXT,
    cst_gndr            TEXT,
    cst_create_date     DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info CASCADE;
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      TEXT,
    prd_nm       TEXT,
    prd_cost     INT,
    prd_line     TEXT,
    prd_start_dt TIMESTAMP,
    prd_end_dt   TIMESTAMP
);

DROP TABLE IF EXISTS bronze.crm_sales_details CASCADE;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  TEXT,
    sls_prd_key  TEXT,
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

DROP TABLE IF EXISTS bronze.erp_loc_a101 CASCADE;
CREATE TABLE bronze.erp_loc_a101 (
    cid    TEXT,
    cntry  TEXT
);

DROP TABLE IF EXISTS bronze.erp_cust_az12 CASCADE;
CREATE TABLE bronze.erp_cust_az12 (
    cid    TEXT,
    bdate  DATE,
    gen    TEXT
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2 CASCADE;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           TEXT,
    cat          TEXT,
    subcat       TEXT,
    maintenance  TEXT
);