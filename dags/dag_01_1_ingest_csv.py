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
    'logistic_ingest_csv',
    default_args=default_args,
    description='Extract CSV data sources to HDFS raw zone',
    schedule_interval='0 1 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

spark_command = f"""
spark-submit \\
--master local[*] \\
"/home/hadoop/logistic/etl/spark_ingest_csv.py" \\
"/home/hadoop/logistic/data/data_sources" \\
"localhost:9000/logistics/raw/"
"""

extract_csv_task = BashOperator(
    task_id='extract_csv_sources',
    bash_command=spark_command,
    dag=dag
)
