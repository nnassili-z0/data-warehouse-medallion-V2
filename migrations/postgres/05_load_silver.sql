/*
===============================================================================
Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This script performs the ETL process to populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
===============================================================================
*/

-- Loading silver.crm_cust_info
TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END AS cst_marital_status, -- Normalize marital status values to readable format
	CASE
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END AS cst_gndr, -- Normalize gender values to readable format
	cst_create_date
FROM (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1; -- Select the most recent record per customer

-- Loading silver.crm_prd_info
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,        -- Extract product key
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line, -- Normalize product line
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(prd_end_dt AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

-- Loading silver.crm_sales_details
TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE
		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
		ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
	END AS sls_order_dt,
	CASE
		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
		ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
	END AS sls_ship_dt,
	CASE
		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
		ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
	END AS sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details;

-- Loading silver.erp_loc_a101
TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
)
SELECT
	TRIM(cid) AS cid,
	TRIM(cntry) AS cntry
FROM bronze.erp_loc_a101;

-- Loading silver.erp_cust_az12
TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)
SELECT
	TRIM(cid) AS cid,
	bdate,
	CASE
		WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END AS gen -- Normalize gender
FROM bronze.erp_cust_az12;

-- Loading silver.erp_px_cat_g1v2
TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
)
SELECT
	TRIM(id) AS id,
	TRIM(cat) AS cat,
	TRIM(subcat) AS subcat,
	TRIM(maintenance) AS maintenance
FROM bronze.erp_px_cat_g1v2;