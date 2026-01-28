import pandera as pa
from pandera import Column, DataFrameSchema

# Schema for Bronze layer data (e.g., CRM customer info)
bronze_crm_cust_schema = DataFrameSchema({
    "cst_id": Column(pa.Int64, nullable=False, unique=True),
    "cst_key": Column(pa.String, nullable=True),
    "cst_firstname": Column(pa.String, nullable=True),
    "cst_lastname": Column(pa.String, nullable=True),
    "cst_marital_status": Column(pa.String, checks=pa.Check.isin(["Single", "Married", "n/a"]), nullable=True),
    "cst_gndr": Column(pa.String, checks=pa.Check.isin(["Female", "Male", "n/a"]), nullable=True),
    "cst_create_date": Column(pa.DateTime, nullable=True),
})

# Schema for Silver layer (transformed CRM customer info)
silver_crm_cust_schema = DataFrameSchema({
    "cst_id": Column(pa.Int64, nullable=False, unique=True),
    "cst_key": Column(pa.String, nullable=False, unique=True),
    "cst_firstname": Column(pa.String, nullable=False),
    "cst_lastname": Column(pa.String, nullable=False),
    "cst_marital_status": Column(pa.String, checks=pa.Check.isin(["Single", "Married", "n/a"]), nullable=True),
    "cst_gndr": Column(pa.String, checks=pa.Check.isin(["Female", "Male", "n/a"]), nullable=True),
    "cst_create_date": Column(pa.DateTime, nullable=False),
})

# Schema for Gold layer (dim_customers)
gold_dim_customers_schema = DataFrameSchema({
    "customer_key": Column(pa.String, nullable=False, unique=True),
    "customer_id": Column(pa.Int64, nullable=False, unique=True),
    "first_name": Column(pa.String, nullable=False),
    "last_name": Column(pa.String, nullable=False),
    "marital_status": Column(pa.String, checks=pa.Check.isin(["Single", "Married", "n/a"]), nullable=True),
    "gender": Column(pa.String, checks=pa.Check.isin(["Female", "Male", "n/a"]), nullable=True),
    "create_date": Column(pa.DateTime, nullable=False),
})