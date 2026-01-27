# Data Warehouse Medallion V2

A comprehensive implementation of a data warehouse using the Medallion Architecture (Bronze, Silver, Gold) with PostgreSQL, Apache Airflow for orchestration, and Docker for local development. This version includes enhanced data quality checks with detailed metrics, percentages, and comprehensive reporting.

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
- **Docker & Docker Compose**: Containerized development environment.
- **Python**: Custom quality check logic and data processing.

## Key Improvements from Previous Version

The original version had quality checks that only performed basic `SELECT COUNT(*)` queries, resulting in empty artifacts. This version implements comprehensive quality validation:

- **Detailed Metrics**: Each quality check calculates total records, issues found, clean percentage, and pass/fail status.
- **Multiple Output Formats**: Artifacts now include CSV files with issue details, JSON summaries with metrics, and human-readable text reports.
- **Enhanced Pipeline**: Added database initialization task and improved Docker configuration for proper permissions and volume mounting.
- **Robust Quality Checks**: Implemented in Python for complex logic, including data sampling and detailed issue reporting.

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
   - **PostgreSQL**: localhost:5432 (username: `airflow`, password: `airflow`)

4. **Run the Pipeline**
   - In Airflow UI, enable and trigger the `medallion_pipeline` DAG.
   - Monitor task execution and view logs for each step.

## Data Pipeline

The `medallion_pipeline` DAG executes the following tasks in sequence:

1. **init_db**: Initialize database schemas and permissions.
2. **ddl_bronze**: Create Bronze layer tables.
3. **ddl_silver**: Create Silver layer tables and views.
4. **ddl_gold**: Create Gold layer dimensions and facts.
5. **load_bronze**: Load CSV data into Bronze tables.
6. **load_silver**: Transform and load data into Silver layer.
7. **quality_silver**: Run quality checks on Silver layer data.
8. **quality_gold**: Run quality checks on Gold layer data.

## Quality Checks

Quality validation is performed on Silver and Gold layers with detailed metrics and reporting.

### Silver Layer Checks
- **Duplicate Products**: Identifies products with duplicate SKUs.
- **Invalid Customer Emails**: Finds customers with malformed email addresses.
- **Missing Product Categories**: Products without category assignments.
- **Orphaned Sales Details**: Sales records without matching customers or products.
- **Invalid Sale Amounts**: Transactions with negative or zero amounts.
- **Future Sale Dates**: Sales dated in the future.
- **Invalid Location Codes**: Locations with non-standard codes.

### Gold Layer Checks
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
├── great_expectations/          # Data quality framework
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

### Running Quality Checks Manually
```python
from dags.medallion_pipeline import run_quality_checks

# Run Silver layer checks
run_quality_checks('silver')

# Run Gold layer checks
run_quality_checks('gold')
```

### Viewing Quality Results
```bash
# List all artifacts
ls -la artifacts/

# View summary metrics
cat artifacts/silver_quality_summary.json

# Examine specific issues
head artifacts/silver_prd_duplicates.csv
```

## Troubleshooting

### Common Issues
- **DAG Fails on ddl_bronze**: Ensure init_db task completed successfully.
- **Empty Artifacts**: Check Docker volume permissions and paths.
- **Connection Errors**: Verify PostgreSQL container is running and accessible.

### Logs
- Airflow task logs: Available in Airflow UI under task instances.
- Container logs: `docker-compose -f docker/docker-compose.yml logs`

## Contributing

1. Fork the repository.
2. Create a feature branch.
3. Make changes and test thoroughly.
4. Submit a pull request with detailed description.

## License

This project is provided as-is for educational and demonstration purposes.