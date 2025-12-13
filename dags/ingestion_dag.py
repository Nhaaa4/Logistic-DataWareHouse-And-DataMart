"""
Airflow DAG for Logistics Data Ingestion
==========================================
Orchestrates data ingestion from multiple sources into Hive data warehouse using Spark
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Configuration
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPARK_CONN_ID = 'spark_default'
DATA_PATH = os.path.join(BASE_PATH, 'data')
ETL_PATH = os.path.join(BASE_PATH, 'etl')

# Default arguments for DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'logistics_data_ingestion',
    default_args=default_args,
    description='Ingest logistics data from multiple sources to Hive using Spark',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    tags=['logistics', 'ingestion', 'hive', 'spark'],
)

# Start task
start_task = DummyOperator(
    task_id='start_ingestion',
    dag=dag,
)

# Task 1: Ingest CSV sources (customers, drivers)
ingest_csv_sources = SparkSubmitOperator(
    task_id='ingest_csv_sources',
    application=f'{ETL_PATH}/spark_ingest_csv.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--data-path', DATA_PATH,
        '--hive-db', 'logistics_dw',
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Logistics CSV Ingestion',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
    },
    dag=dag,
)

# Task 2: Ingest JSON sources (vehicles, packages)
ingest_json_sources = SparkSubmitOperator(
    task_id='ingest_json_sources',
    application=f'{ETL_PATH}/spark_ingest_json.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--data-path', DATA_PATH,
        '--hive-db', 'logistics_dw',
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Logistics JSON Ingestion',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
    },
    dag=dag,
)

# Task 3: Ingest SQLite database (routes, warehouses, deliveries)
ingest_database_sources = SparkSubmitOperator(
    task_id='ingest_database_sources',
    application=f'{ETL_PATH}/spark_ingest_database.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--data-path', DATA_PATH,
        '--hive-db', 'logistics_dw',
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Logistics Database Ingestion',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
    },
    jars='/path/to/sqlite-jdbc.jar',  # Update with actual path
    dag=dag,
)

# Task 4: Load Date Dimension
load_date_dimension = SparkSubmitOperator(
    task_id='load_date_dimension',
    application=f'{ETL_PATH}/spark_load_dimensions.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--dimension', 'date',
        '--hive-db', 'logistics_dw',
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Load Date Dimension',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
    },
    dag=dag,
)

# Task 5: Load Dimensions (parallel after staging)
load_dimensions = DummyOperator(
    task_id='load_dimensions',
    dag=dag,
)

load_customer_dimension = SparkSubmitOperator(
    task_id='load_customer_dimension',
    application=f'{ETL_PATH}/spark_load_dimensions.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--dimension', 'customer',
        '--hive-db', 'logistics_dw',
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Load Customer Dimension',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
    },
    dag=dag,
)

load_driver_dimension = SparkSubmitOperator(
    task_id='load_driver_dimension',
    application=f'{ETL_PATH}/spark_load_dimensions.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--dimension', 'driver',
        '--hive-db', 'logistics_dw',
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Load Driver Dimension',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
    },
    dag=dag,
)

load_vehicle_dimension = SparkSubmitOperator(
    task_id='load_vehicle_dimension',
    application=f'{ETL_PATH}/spark_load_dimensions.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--dimension', 'vehicle',
        '--hive-db', 'logistics_dw',
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Load Vehicle Dimension',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
    },
    dag=dag,
)

load_route_dimension = SparkSubmitOperator(
    task_id='load_route_dimension',
    application=f'{ETL_PATH}/spark_load_dimensions.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--dimension', 'route',
        '--hive-db', 'logistics_dw',
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Load Route Dimension',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
    },
    dag=dag,
)

load_package_dimension = SparkSubmitOperator(
    task_id='load_package_dimension',
    application=f'{ETL_PATH}/spark_load_dimensions.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--dimension', 'package',
        '--hive-db', 'logistics_dw',
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Load Package Dimension',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
    },
    dag=dag,
)

load_warehouse_dimension = SparkSubmitOperator(
    task_id='load_warehouse_dimension',
    application=f'{ETL_PATH}/spark_load_dimensions.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--dimension', 'warehouse',
        '--hive-db', 'logistics_dw',
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Load Warehouse Dimension',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
    },
    dag=dag,
)

# Task 6: Load Fact Table
load_fact_table = SparkSubmitOperator(
    task_id='load_fact_delivery',
    application=f'{ETL_PATH}/spark_load_fact.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--fact', 'delivery',
        '--hive-db', 'logistics_dw',
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Load Delivery Fact',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
        'spark.sql.shuffle.partitions': '32',  # Match bucketing
    },
    dag=dag,
)

# Task 7: Build Data Marts
build_data_marts = SparkSubmitOperator(
    task_id='build_data_marts',
    application=f'{ETL_PATH}/spark_build_datamart.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--hive-db', 'logistics_dw',
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Build Data Marts',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
    },
    dag=dag,
)

# Task 8: Load Data Marts to MySQL
load_to_mysql = SparkSubmitOperator(
    task_id='load_to_mysql',
    application=f'{ETL_PATH}/spark_hive_to_mysql.py',
    conn_id=SPARK_CONN_ID,
    application_args=[
        '--hive-db', 'logistics_dw',
        '--mysql-host', 'localhost',
        '--mysql-port', '3306',
        '--mysql-database', 'logistics_dm',
        '--mysql-user', 'root',
        '--mysql-password', 'password',  # TODO: Use Airflow Variables or Connections
    ],
    conf={
        'spark.master': 'local[*]',
        'spark.app.name': 'Load to MySQL',
        'spark.sql.warehouse.dir': '/user/hive/warehouse',
        'spark.sql.catalogImplementation': 'hive',
        'spark.jars': '/path/to/mysql-connector-java.jar',  # TODO: Update with actual path
    },
    dag=dag,
)

# End task
end_task = DummyOperator(
    task_id='end_ingestion',
    dag=dag,
)

# Define task dependencies
start_task >> [ingest_csv_sources, ingest_json_sources, ingest_database_sources]

# Date dimension can load immediately after start
start_task >> load_date_dimension

# All staging must complete before dimensions
[ingest_csv_sources, ingest_json_sources, ingest_database_sources] >> load_dimensions

# Dimension tasks run in parallel
load_dimensions >> [
    load_customer_dimension,
    load_driver_dimension,
    load_vehicle_dimension,
    load_route_dimension,
    load_package_dimension,
    load_warehouse_dimension
]

# All dimensions and date must complete before fact
[
    load_date_dimension,
    load_customer_dimension,
    load_driver_dimension,
    load_vehicle_dimension,
    load_route_dimension,
    load_package_dimension,
    load_warehouse_dimension
] >> load_fact_table

# Fact table must complete before data marts
load_fact_table >> build_data_marts

# Data marts must complete before MySQL load
build_data_marts >> load_to_mysql

# MySQL load completes before end
load_to_mysql >> end_task
