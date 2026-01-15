# Data Catalog

## Bronze Layer

Raw data ingested from CSV files.

- `bronze.crm_cust_info`: Customer info from CRM.
- `bronze.crm_prd_info`: Product info from CRM.
- `bronze.crm_sales_details`: Sales details from CRM.
- `bronze.erp_loc_a101`: Location data from ERP.
- `bronze.erp_cust_az12`: Customer additional data from ERP.
- `bronze.erp_px_cat_g1v2`: Product category from ERP.

## Silver Layer

Cleansed and transformed data.

- `silver.crm_cust_info`: Deduplicated customers with normalized marital status and gender.
- `silver.crm_prd_info`: Products with extracted category ID, normalized product line, dates as DATE.
- `silver.crm_sales_details`: Sales with converted date columns.
- `silver.erp_loc_a101`: Trimmed location data.
- `silver.erp_cust_az12`: Customer data with normalized gender.
- `silver.erp_px_cat_g1v2`: Trimmed category data.

## Gold Layer

Business-ready views.

- `gold.dim_customers`: Customer dimension with surrogate key.
- `gold.dim_products`: Product dimension with surrogate key.
- `gold.fact_sales`: Sales fact table linking customers and products.