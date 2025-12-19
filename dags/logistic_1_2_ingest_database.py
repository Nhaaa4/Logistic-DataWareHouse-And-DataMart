#!/usr/bin/env python3

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configuration
OWNER_DAG = 'hadoop'
ETL_PATH = '/home/hadoop/logistic/etl/spark_ingest_database.py'
DATA_SOURCE_PATH = '/home/hadoop/logistic/data/data_sources'
HDFS_TARGET_PATH = 'localhost:9000/logistics'
JARS_PATH = '/home/hadoop/logistic/jdbc/sqlite-jdbc-3.51.1.0.jar'

default_args = {
    'owner': OWNER_DAG,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'logistic_ingest_database',
    default_args=default_args,
    description='Extract database sources to HDFS raw zone',
    schedule_interval='0 1 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

spark_command = f"""
spark-submit \\
--master local[*] \\
--jars "{JARS_PATH}" \\
"{ETL_PATH}" \\
"{DATA_SOURCE_PATH}" \\
"{HDFS_TARGET_PATH}"
"""

extract_database_task = BashOperator(
    task_id='extract_database_sources',
    bash_command=spark_command,
    dag=dag
)
