#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date
import sys
import os

def create_spark_session(app_name="CSV Ingestion"):
    """Create Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

def ingest_customers(spark, data_path, hdfs_output_path, execution_date):
    customers_path = f"{data_path}/customers.csv"
    
    # Read CSV
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(customers_path)
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("CSV")) \
           .withColumn("ingestion_date", lit(execution_date))
    
    # Write to HDFS as Parquet with partition
    output_path = f"{hdfs_output_path}/customers"
    df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .parquet(output_path)
    
    print(f"Ingested {df.count():,} customer records to HDFS: {output_path}")
    return df.count()

def ingest_drivers(spark, data_path, hdfs_output_path, execution_date):
    drivers_path = f"{data_path}/drivers.csv"
    
    # Read CSV
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(drivers_path)
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("CSV")) \
           .withColumn("ingestion_date", lit(execution_date))
    
    # Write to HDFS as Parquet with partition
    output_path = f"{hdfs_output_path}/drivers"
    df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .parquet(output_path)
    
    print(f"Ingested {df.count():,} driver records to HDFS: {output_path}")
    return df.count()

def main():
    # Get values from command-line arguments
    DATA_PATH = sys.argv[1]
    HDFS_OUTPUT_PATH = sys.argv[2]
    
    # Generate execution date
    from datetime import datetime
    execution_date = datetime.now().strftime("%Y-%m-%d") 
    
    # Create Spark session
    spark = create_spark_session("Logistics CSV Ingestion")
    
    try:
        # Ingest sources
        customer_count = ingest_customers(spark, DATA_PATH, HDFS_OUTPUT_PATH, execution_date)
        driver_count = ingest_drivers(spark, DATA_PATH, HDFS_OUTPUT_PATH, execution_date)
        
        print("\n" + "="*60)
        print("CSV to HDFS Complete:")
        print(f"    Customers ingested: {customer_count:,}")
        print(f"    Drivers ingested:   {driver_count:,}")
        print(f"    HDFS Path: {HDFS_OUTPUT_PATH}")
        print(f"    Partition Date: {execution_date}")
        print("="*60)
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
