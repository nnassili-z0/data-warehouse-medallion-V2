# Data Warehouse Medallion V2

**A modern, production-ready data warehouse implementation using the Medallion Architecture (Bronze → Silver → Gold) with enterprise-grade data quality validation, comprehensive monitoring, and automated orchestration.**

This project demonstrates best practices for building scalable data warehouses with PostgreSQL, Apache Airflow, Great Expectations, and Docker. It includes advanced data quality checks with detailed metrics, percentages, and comprehensive reporting—moving beyond basic row counts to provide actionable insights into data health and lineage.

## Architecture

The Medallion Architecture organizes data into three layers of increasing refinement:

- **Bronze Layer**: Raw data ingestion from source systems (CRM and ERP CSV files). Data is loaded as-is without transformation.
- **Silver Layer**: Cleansed, deduplicated, and standardized data. Includes data validation and basic transformations.
- **Gold Layer**: Business-ready dimensional model with facts and dimensions for analytics and reporting.

### Data Sources
- **CRM Data**: Customer information, product details, and sales transactions.
- **ERP Data**: Customer master data, location information, and product category hierarchies.

## Technologies

- **PostgreSQL 15**: Database engine for all data layers.
- **Apache Airflow 2.8.3**: Workflow orchestration for ETL pipelines.
- **Great Expectations 0.18.12**: Data quality validation framework with expectation suites and checkpoints.
- **Pandera 0.18.0**: DataFrame validation library for runtime schema checks.
- **YData Profiling 4.6.0**: Automated data profiling and exploratory data analysis.
- **Prometheus & Grafana**: Monitoring and visualization of pipeline metrics.
- **Docker & Docker Compose**: Containerized development environment.
- **Python**: Custom quality check logic and data processing.

## Key Improvements from Previous Version

The original version had quality checks that only performed basic `SELECT COUNT(*)` queries, resulting in empty artifacts. This version implements comprehensive quality validation:

- **Detailed Metrics**: Each quality check calculates total records, issues found, clean percentage, and pass/fail status.
- **Multiple Output Formats**: Artifacts now include CSV files with issue details, JSON summaries with metrics, and human-readable text reports.
- **Enhanced Pipeline**: Added database initialization task and improved Docker configuration for proper permissions and volume mounting.
- **Robust Quality Checks**: Implemented in Python for complex logic, including data sampling and detailed issue reporting.
- **Advanced Data Quality**: Added Pandera schema validation and automated profiling with YData Profiling.
- **Monitoring & Alerting**: Integrated Prometheus and Grafana for pipeline observability, with Slack webhook alerts for failures.

## Setup

### Prerequisites
- Docker and Docker Compose installed on your system.
- At least 4GB RAM available for containers.
- Ports 5432 (PostgreSQL) and 8080 (Airflow) available.

### Installation Steps

1. **Clone or Download the Project**
   ```bash
   git clone https://github.com/nnassili-z0/data-warehouse-medallion-V2.git
   cd data-warehouse-medallion-V2
   ```

2. **Start the Environment**
   ```bash
   docker-compose -f docker/docker-compose.yml up --build
   ```

3. **Access Interfaces**
   - **Airflow UI**: http://localhost:8080 (username: `admin`, password: `admin`)
   - **Prometheus**: http://localhost:9090
   - **Grafana**: http://localhost:3000 (username: `admin`, password: `admin`)
   - **PostgreSQL**: localhost:5432 (username: `airflow`, password: `airflow`)

4. **Run the Pipeline**
   - In Airflow UI, enable and trigger the `medallion_pipeline` DAG.
   - Monitor task execution and view logs for each step.

## Monitoring and Alerting

The pipeline includes comprehensive monitoring and alerting capabilities:

- **Prometheus**: Collects metrics from Airflow and pipeline components.
- **Grafana**: Visualizes pipeline performance, DAG run durations, and task statuses.
- **Alerting**: Slack webhooks notify on validation failures (configure `SLACK_WEBHOOK_URL` environment variable).

Access Grafana dashboards at http://localhost:3000 to monitor pipeline health.

## Data Pipeline

The `medallion_pipeline` DAG executes the following tasks in sequence:

1. **init_db**: Initialize database schemas and permissions.
2. **ddl_bronze**: Create Bronze layer tables.
3. **ddl_silver**: Create Silver layer tables and views.
4. **ddl_gold**: Create Gold layer dimensions and facts.
5. **load_bronze**: Load CSV data into Bronze tables.
6. **load_silver**: Transform and load data into Silver layer.
7. **pandera_bronze**: Pandera schema validation on Bronze layer.
8. **pandera_silver**: Pandera schema validation on Silver layer.
9. **pandera_gold**: Pandera schema validation on Gold layer.
10. **profile_silver**: Automated profiling on Silver layer tables.
11. **profile_gold**: Automated profiling on Gold layer tables.
12. **ge_silver_validation**: Great Expectations validation on Silver layer.
13. **ge_gold_validation**: Great Expectations validation on Gold layer.
14. **quality_silver**: Custom quality checks on Silver layer data.
15. **quality_gold**: Custom quality checks on Gold layer data.
16. **alert_failure**: Send alerts if any validation fails.

## Quality Checks

### Great Expectations Framework

Great Expectations provides a comprehensive data quality validation framework integrated into the Airflow pipeline. The framework validates data at both Silver and Gold layers with 50+ built-in expectations.

#### Silver Layer Expectations

**crm_cust_info.json** - Customer data validation:
- Row count between 1,000 and 100,000
- Customer ID (cst_id) never null and unique
- Marital status normalized to [Single, Married, n/a]
- Gender normalized to [Female, Male, n/a]
- Integer data types enforced

**crm_prd_info.json** - Product data validation:
- Row count between 100 and 10,000
- Product ID (prd_id) never null and unique
- Product cost numeric and between 0-10,000
- Integer data types enforced

**crm_sales_details.json** - Sales transaction validation:
- Row count between 10,000 and 1,000,000
- Sales ID (sls_id) never null and unique
- Customer and Product IDs never null
- Sales quantity between 1-1,000 items
- Sales date valid date format

#### Gold Layer Expectations

**dim_customers.json** - Customer dimension table:
- Surrogate keys (customer_key) unique and not null
- Customer ID unique and not null
- Marital status and gender normalized
- Row count between 1,000-100,000

**dim_products.json** - Product dimension table:
- Surrogate keys (product_key) unique and not null
- Product ID unique and not null
- Product cost between 0-10,000
- Row count between 100-10,000

**fact_sales.json** - Sales fact table:
- Product and Customer keys never null (foreign keys)
- Sales quantity between 1-1,000
- Sales amount between 0-100,000
- Valid order dates
- Row count between 10,000-1,000,000

#### Running Great Expectations Validations

**Via Airflow Pipeline:**
```bash
# Trigger the medallion_pipeline DAG in Airflow UI
# GE validation tasks run automatically after data loads
# View results in: artifacts/gx/validations/
```

**Via Command Line:**
```bash
# Run Silver layer checkpoint
docker exec docker-airflow-1 bash -c 'cd /opt/project/great_expectations && great_expectations checkpoint run silver_quality_checkpoint'

# Run Gold layer checkpoint
docker exec docker-airflow-1 bash -c 'cd /opt/project/great_expectations && great_expectations checkpoint run gold_quality_checkpoint'
```

**Validation Results:**
- Stored in: `great_expectations/validations/` directory
- Automatic data documentation: `great_expectations/gx/plugins/gx-docs/`
- Detailed metrics and pass/fail status for each expectation

### Custom Quality Checks

In addition to Great Expectations, the pipeline includes traditional custom quality checks on Silver and Gold layers with detailed metrics and reporting.

#### Silver Layer Checks
- **Duplicate Products**: Identifies products with duplicate SKUs.
- **Invalid Customer Emails**: Finds customers with malformed email addresses.
- **Missing Product Categories**: Products without category assignments.
- **Orphaned Sales Details**: Sales records without matching customers or products.
- **Invalid Sale Amounts**: Transactions with negative or zero amounts.
- **Future Sale Dates**: Sales dated in the future.
- **Invalid Location Codes**: Locations with non-standard codes.

#### Gold Layer Checks
- **Duplicate Dimension Keys**: Ensures unique keys in dimensions.
- **Orphaned Fact Records**: Facts without matching dimension references.
- **Invalid Date Ranges**: Facts with dates outside valid ranges.

### Quality Metrics
Each check generates:
- **Total Records**: Number of records examined.
- **Issues Found**: Number of problematic records.
- **Clean Percentage**: Percentage of clean records.
- **Status**: PASS (≥95% clean) or FAIL (<95% clean).

## Artifacts

Quality check results are exported to the `artifacts/` directory:

### CSV Files
Detailed issue reports for each check:
- `silver_prd_duplicates.csv`: Duplicate product details.
- `silver_invalid_emails.csv`: Invalid customer emails.
- `gold_orphaned_sales.csv`: Orphaned sales fact records.
- etc.

### JSON Summaries
Structured metrics for each layer:
- `silver_quality_summary.json`: Overall Silver layer quality metrics.
- `gold_quality_summary.json`: Overall Gold layer quality metrics.

### Text Reports
Human-readable summaries:
- `silver_quality_report.txt`: Detailed Silver layer report.
- `gold_quality_report.txt`: Detailed Gold layer report.

## Project Structure

```
data-warehouse-medallion-v2/
├── README.md                    # This file
├── artifacts/                   # Quality check outputs
│   ├── *.csv                    # Issue detail files
│   ├── *_summary.json          # Quality metrics
│   └── *_report.txt            # Human-readable reports
├── dags/
│   └── medallion_pipeline.py    # Airflow DAG definition
├── datasets/
│   ├── source_crm/              # CRM CSV files
│   └── source_erp/              # ERP CSV files
├── docker/
│   ├── docker-compose.yml       # Container configuration
│   └── artifacts/               # Mounted artifacts volume
├── docs/
│   ├── data_catalog.md          # Data dictionary
│   └── naming_conventions.md    # Naming standards
├── great_expectations/          # Data quality validation framework
│   ├── great_expectations.yml   # GE configuration
│   ├── checkpoints/             # GE validation checkpoints
│   │   ├── silver_quality_checkpoint.yml
│   │   └── gold_quality_checkpoint.yml
│   └── expectations/            # Expectation suites by layer
│       ├── silver/              # Silver layer expectations
│       │   ├── crm_cust_info.json
│       │   ├── crm_prd_info.json
│       │   └── crm_sales_details.json
│       └── gold/                # Gold layer expectations
│           ├── dim_customers.json
│           ├── dim_products.json
│           └── fact_sales.json
├── migrations/
│   └── postgres/                # SQL migration scripts
│       ├── 00_init.sql          # Schema initialization
│       ├── 01_ddl_bronze.sql    # Bronze layer DDL
│       ├── 02_ddl_silver.sql    # Silver layer DDL
│       ├── 03_ddl_gold.sql      # Gold layer DDL
│       ├── 04_load_bronze.sql   # Bronze data loads
│       └── 05_load_silver.sql   # Silver transformations
├── scripts/
│   ├── generate_quality_summary.py  # Quality report generator
│   ├── init_warehouse.sh        # Warehouse initialization
│   └── load_data.sh             # Data loading script
└── tests/
    ├── gold_quality_checks.sql  # Gold layer SQL checks
    └── silver_quality_checks.sql # Silver layer SQL checks
```

## Usage Examples

### Triggering the Pipeline
```bash
# Access Airflow UI
http://localhost:8080
# Login with admin/admin
# Enable medallion_pipeline DAG
# Click "Trigger DAG" button

# Monitor execution and view logs
# GE validation tasks will execute after data loads
# Check validation results in artifacts/gx/validations/
```

### Running Quality Checks Manually

**Custom Python Checks:**
```python
from dags.medallion_pipeline import run_quality_checks

# Run Silver layer checks
run_quality_checks('silver')

# Run Gold layer checks
run_quality_checks('gold')
```

**Great Expectations Checks:**
```bash
# Run Silver layer expectations
docker exec docker-airflow-1 bash -c 'cd /opt/project/great_expectations && great_expectations checkpoint run silver_quality_checkpoint'

# Run Gold layer expectations
docker exec docker-airflow-1 bash -c 'cd /opt/project/great_expectations && great_expectations checkpoint run gold_quality_checkpoint'
```

### Viewing Quality Results

**Artifacts Directory:**
```bash
# List all artifacts
ls -la artifacts/

# View GE validation results
ls -la artifacts/gx/validations/

# View custom quality summary
cat artifacts/silver_quality_summary.json

# View detailed issues
head artifacts/silver_prd_duplicates.csv
```

**Airflow Logs:**
- Access task logs directly from Airflow UI
- Filter by task name (e.g., ge_silver_validation)
- Check for validation failures and metrics

## Troubleshooting

### Common Issues
- **DAG Fails on ddl_bronze**: Ensure init_db task completed successfully.
- **GE Validation Fails**: Check that PostgreSQL datasource is configured and accessible in great_expectations.yml.
- **Empty Artifacts**: Check Docker volume permissions and paths.
- **Connection Errors**: Verify PostgreSQL container is running and accessible.
- **GE Checkpoint Not Found**: Ensure checkpoints are in great_expectations/checkpoints/ directory with correct naming.

### Logs
- **Airflow task logs**: Available in Airflow UI under task instances.
- **Container logs**: `docker-compose -f docker/docker-compose.yml logs [service_name]`
- **GE validation logs**: Check Airflow task logs for ge_silver_validation and ge_gold_validation tasks.

## Contributing

1. Fork the repository.
2. Create a feature branch.
3. Make changes and test thoroughly.
4. Submit a pull request with detailed description.

## License

This project is provided as-is for educational and demonstration purposes.