/*
===============================================================================
Silver Layer Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the transformations in the Silver Layer.
    These checks ensure:
    - Deduplication worked (unique cst_id)
    - Normalization of values (marital status, gender)
    - Data integrity (non-null keys, valid dates)
    - Justification for silver transformations

Results are exported to CSV files in artifacts.
===============================================================================
*/

-- Check for uniqueness of cst_id in silver.crm_cust_info
-- Expectation: No duplicates
\COPY (SELECT cst_id, COUNT(*) AS count FROM silver.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1) TO '/opt/project/artifacts/silver_cust_duplicates.csv' WITH CSV HEADER;

-- Check valid marital_status values
-- Expectation: Only 'Single', 'Married', 'n/a'
\COPY (SELECT cst_id, cst_marital_status FROM silver.crm_cust_info WHERE cst_marital_status NOT IN ('Single', 'Married', 'n/a')) TO '/opt/project/artifacts/silver_invalid_marital.csv' WITH CSV HEADER;

-- Check valid gender values
-- Expectation: Only 'Female', 'Male', 'n/a'
\COPY (SELECT cst_id, cst_gndr FROM silver.crm_cust_info WHERE cst_gndr NOT IN ('Female', 'Male', 'n/a')) TO '/opt/project/artifacts/silver_invalid_gender.csv' WITH CSV HEADER;

-- Check for null cst_id
-- Expectation: No nulls
\COPY (SELECT * FROM silver.crm_cust_info WHERE cst_id IS NULL) TO '/opt/project/artifacts/silver_null_cst_id.csv' WITH CSV HEADER;

-- Check prd_key uniqueness in silver.crm_prd_info
-- Expectation: Unique prd_key
\COPY (SELECT prd_key, COUNT(*) AS count FROM silver.crm_prd_info GROUP BY prd_key HAVING COUNT(*) > 1) TO '/opt/project/artifacts/silver_prd_duplicates.csv' WITH CSV HEADER;

-- Check valid prd_line values
-- Expectation: Only 'Mountain', 'Road', 'Other Sales', 'Touring', 'n/a'
\COPY (SELECT prd_id, prd_line FROM silver.crm_prd_info WHERE prd_line NOT IN ('Mountain', 'Road', 'Other Sales', 'Touring', 'n/a')) TO '/opt/project/artifacts/silver_invalid_prd_line.csv' WITH CSV HEADER;

-- Check date conversions in sales_details
-- Expectation: No invalid dates (null where should be date)
\COPY (SELECT sls_ord_num, sls_order_dt FROM silver.crm_sales_details WHERE sls_order_dt IS NULL AND sls_ord_num IS NOT NULL) TO '/opt/project/artifacts/silver_invalid_order_dates.csv' WITH CSV HEADER;