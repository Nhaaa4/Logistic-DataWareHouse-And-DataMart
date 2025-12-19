#!/usr/bin/env python3

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# CONFIGURATION
OWNER_DAG = 'hadoop'
ETL_PATH = '/home/hadoop/logistic/etl/spark_hdfs_to_postgres.py'
HDFS_SOURCE_PATH = 'hdfs://localhost:9000/logistics'
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
    'logistic_hdfs_to_postgres_staging',
    default_args=default_args,
    description='ETL: Extract from HDFS raw zone and load to PostgreSQL staging',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['logistics', 'etl', 'hdfs', 'postgres', 'staging']
)

spark_command = f"""
spark-submit \\
--master local[2] \\
--jars "{POSTGRES_JAR}" \\
"{ETL_PATH}" \\
"{HDFS_SOURCE_PATH}" \\
"{POSTGRES_HOST}" \\
"{POSTGRES_PORT}" \\
"{POSTGRES_DB}" \\
"{POSTGRES_USER}" \\
"{POSTGRES_PASSWORD}"
"""

extract_transform_load_task = BashOperator(
    task_id='hdfs_to_postgres_etl',
    bash_command=spark_command,
    dag=dag
)
