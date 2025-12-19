#!/usr/bin/env python3

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'hadoop',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ingest_database',
    default_args=default_args,
    description='Extract database sources to HDFS raw zone',
    schedule_interval='0 1 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

spark_command = f"""
spark-submit \\
--master local[*] \\
--jars "/home/hadoop/logistic/jdbc/sqlite-jdbc-3.51.1.0.jar" \\
"/home/hadoop/logistic/etl/spark_ingest_database.py" \\
"/home/hadoop/logistic/data/data_sources" \\
"localhost:9000/logistics/raw/"
"""

extract_database_task = BashOperator(
    task_id='extract_database_sources',
    bash_command=spark_command,
    dag=dag
)
