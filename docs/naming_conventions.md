# Naming Conventions

## Schemas
- `bronze`: Raw data layer
- `silver`: Cleansed data layer
- `gold`: Business-ready data layer

## Tables
- CRM tables: `crm_<entity>`
- ERP tables: `erp_<code>`

## Columns
- Snake_case for column names
- Abbreviations: cst (customer), prd (product), sls (sales), etc.

## Views
- Dimensions: `dim_<entity>`
- Facts: `fact_<entity>`