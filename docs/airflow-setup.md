# Airflow Setup

This guide explains how to configure Apache Airflow for the Logistic Data Warehouse ETL pipeline.

## Step 1: Copy DAG Files

Copy the DAG files to your Airflow DAGs directory:

```bash
# Find your Airflow DAGs folder (usually ~/airflow/dags or /opt/airflow/dags)
# Copy all DAG files
cp dags/*.py ~/airflow/dags/
```

## Step 2: Configure JDBC Drivers

Ensure the JDBC drivers are accessible to Spark. The project includes:

```
jdbc/
├── postgresql-42.6.0.jar
├── mysql-connector-j-8.3.0.jar
└── sqlite-jdbc-3.51.1.0.jar
```

Add the JDBC path to your Spark configuration or update the DAG files with the correct path.

## Step 3: Update DAG Configuration

Edit the DAG files to match your environment:

### Database Connections

Update connection parameters in each DAG file:

```python
# PostgreSQL (Staging & DWH)
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "your_password"
POSTGRES_DB = "logistics_dwh"

# MySQL (Data Mart)
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_USER = "root"
MYSQL_PASSWORD = "your_password"
MYSQL_DB = "logistics_dm"

# HDFS
HDFS_HOST = "localhost"
HDFS_PORT = "9000"
```

### File Paths

Update source file paths:

```python
# Data source directory
DATA_SOURCE_PATH = "/path/to/logistic/data/data_sources"

# JDBC drivers path
JDBC_PATH = "/path/to/logistic/jdbc"
```

## Step 4: Start Airflow

Start the Airflow services:

Terminal 1: Start the scheduler

```bash
source airflow_venv/bin/activate
airflow scheduler
```

Terminal 2: Start the webserver

```bash
source airflow_venv/bin/activate
airflow webserver --port 8080
```

## Step 5: Access Airflow UI

Open your browser and navigate to:

```
http://localhost:8080
```

Or if running on a remote server:

```
http://<server-ip>:8080
```

Login with your admin credentials.

## DAG Overview

The project includes 6 DAGs that run in sequence:

| DAG | Schedule | Description |
|-----|----------|-------------|
| `logistic_1_1_ingest_csv` | 01:00 AM | Ingest CSV files (customers, drivers) to HDFS |
| `logistic_1_2_ingest_database` | 01:00 AM | Ingest SQLite data (routes, warehouses, deliveries) to HDFS |
| `logistic_1_3_ingest_json` | 01:00 AM | Ingest JSON files (vehicles, packages) to HDFS |
| `logistic_2_hdfs_to_staging` | 02:00 AM | Load HDFS data to PostgreSQL staging tables |
| `logistic_3_staging_to_dwh` | 03:00 AM | Transform staging data to DWH star schema |
| `logistic_4_dwh_to_datamart` | 05:00 AM | Build analytics data mart from DWH |


## Running DAGs Manually

Use the Airflow UI:
1. Navigate to the DAGs page
2. Click on the DAG name
3. Click the "Trigger DAG" button (play icon)

## Monitoring

In the Airflow UI, monitor:
- DAG run status (success/failed/running)
- Task duration
- Task logs for debugging