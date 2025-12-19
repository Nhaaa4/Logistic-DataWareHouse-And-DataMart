#!/usr/bin/env python3

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# CONFIGURATION
OWNER_DAG = 'hadoop'
ETL_PATH = '/home/hadoop/logistic/etl/spark_staging_to_dwh.py'
POSTGRES_JAR = '/home/hadoop/logistic/jdbc/postgresql-42.6.0.jar'

# PostgreSQL connection details
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5432'
POSTGRES_DB = 'logistics_dwh'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = '172839'

default_args = {
    'owner': OWNER_DAG,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'logistic_staging_to_dwh',
    default_args=default_args,
    description='ETL: Transform and load from Staging to Data Warehouse with SCD Type 2',
    schedule_interval='0 3 * * *',  # Run at 3 AM daily (after staging load)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['logistics', 'etl', 'staging', 'dwh', 'scd-type2']
)

spark_command = f"""
spark-submit \\
--master local[2] \\
--jars "{POSTGRES_JAR}" \\
"{ETL_PATH}" \\
"{POSTGRES_HOST}" \\
"{POSTGRES_PORT}" \\
"{POSTGRES_DB}" \\
"{POSTGRES_USER}" \\
"{POSTGRES_PASSWORD}"
"""

staging_to_dwh_task = BashOperator(
    task_id='staging_to_dwh_etl',
    bash_command=spark_command,
    dag=dag
)
