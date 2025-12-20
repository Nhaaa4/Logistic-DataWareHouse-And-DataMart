#!/usr/bin/env python3

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# CONFIGURATION
OWNER_DAG = 'hadoop'
ETL_PATH = '/home/hadoop/logistic/etl/spark_dwh_to_datamart.py'
POSTGRES_JAR = '/home/hadoop/logistic/jdbc/postgresql-42.6.0.jar'
MYSQL_JAR = '/home/hadoop/logistic/jdbc/mysql-connector-j-8.3.0.jar'

# PostgreSQL DWH connection
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5432'
POSTGRES_DB = 'logistics_dwh'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = '172839'

# MySQL Data Mart connection
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3306'
MYSQL_DB = 'logistics_dm'
MYSQL_USER = 'root'
MYSQL_PASSWORD = '[Panha097]'

default_args = {
    'owner': OWNER_DAG,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'logistic_dwh_to_datamart',
    default_args=default_args,
    description='ETL: Load from Data Warehouse (PostgreSQL) to Data Mart (MySQL)',
    schedule_interval='0 5 * * *',  # Run at 5 AM daily (after DWH load)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['logistics', 'etl', 'dwh', 'datamart', 'mysql']
)

spark_command = f"""
spark-submit \\
--master local[2] \\
--jars "{POSTGRES_JAR},{MYSQL_JAR}" \\
"{ETL_PATH}" \\
"{POSTGRES_HOST}" \\
"{POSTGRES_PORT}" \\
"{POSTGRES_DB}" \\
"{POSTGRES_USER}" \\
"{POSTGRES_PASSWORD}" \\
"{MYSQL_HOST}" \\
"{MYSQL_PORT}" \\
"{MYSQL_DB}" \\
"{MYSQL_USER}" \\
"{MYSQL_PASSWORD}"
"""

dwh_to_datamart_task = BashOperator(
    task_id='dwh_to_datamart_etl',
    bash_command=spark_command,
    dag=dag
)