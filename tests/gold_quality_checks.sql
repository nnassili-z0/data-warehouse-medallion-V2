/*
===============================================================================
Gold Layer Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency,
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Results are exported to CSV files in artifacts.
===============================================================================
*/

-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results
\COPY (SELECT customer_key, COUNT(*) AS duplicate_count FROM gold.dim_customers GROUP BY customer_key HAVING COUNT(*) > 1) TO '/opt/project/artifacts/gold_cust_key_duplicates.csv' WITH CSV HEADER;

-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results
\COPY (SELECT product_key, COUNT(*) AS duplicate_count FROM gold.dim_products GROUP BY product_key HAVING COUNT(*) > 1) TO '/opt/project/artifacts/gold_prod_key_duplicates.csv' WITH CSV HEADER;

-- Check the data model connectivity between fact and dimensions
-- Expectation: No orphaned records
\COPY (SELECT f.order_number, f.customer_key, f.product_key, c.customer_id, p.product_id FROM gold.fact_sales f LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key LEFT JOIN gold.dim_products p ON p.product_key = f.product_key WHERE c.customer_id IS NULL OR p.product_id IS NULL) TO '/opt/project/artifacts/gold_orphaned_sales.csv' WITH CSV HEADER;