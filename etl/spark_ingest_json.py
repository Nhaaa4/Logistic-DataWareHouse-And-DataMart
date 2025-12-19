#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, explode, col
import sys
import os

def create_spark_session(app_name="JSON Ingestion"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

def ingest_vehicles(spark, data_path, hdfs_output_path, execution_date):
    vehicles_path = f"{data_path}/vehicles_api.json"
    
    # Read JSON (API response format)
    df_raw = spark.read.option("multiLine", "true").json(vehicles_path)
    
    # Extract data array from API response
    df = df_raw.select(explode(col("data")).alias("vehicle")) \
               .select("vehicle.*")
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("API_JSON")) \
           .withColumn("ingestion_date", lit(execution_date))
    
    # Write to HDFS as Parquet with partition
    output_path = f"{hdfs_output_path}/vehicles"
    df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .parquet(output_path)
    
    print(f"Ingested {df.count():,} vehicle records to HDFS: {output_path}")
    return df.count()

def ingest_packages(spark, data_path, hdfs_output_path, execution_date):
    packages_path = f"{data_path}/packages.json"
    
    # Read JSON
    df_raw = spark.read.option("multiLine", "true").json(packages_path)
    
    # Extract packages array
    df = df_raw.select(explode(col("packages")).alias("package")) \
               .select("package.*")
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("JSON")) \
           .withColumn("ingestion_date", lit(execution_date))
    
    # Write to HDFS as Parquet with partition
    output_path = f"{hdfs_output_path}/packages"
    df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .parquet(output_path)
    
    print(f"Ingested {df.count():,} package records to HDFS: {output_path}")
    return df.count()

def main():
    """Main execution"""
    DATA_PATH = sys.argv[1]
    HDFS_OUTPUT_PATH = sys.argv[2]
    
    # Generate execution date
    from datetime import datetime
    execution_date = datetime.now().strftime("%Y-%m-%d")
    
    # Create Spark session
    spark = create_spark_session("Logistics JSON Ingestion")
    
    try:
        # Ingest sources
        vehicle_count = ingest_vehicles(spark, DATA_PATH, HDFS_OUTPUT_PATH, execution_date)
        package_count = ingest_packages(spark, DATA_PATH, HDFS_OUTPUT_PATH, execution_date)
        
        print("\n" + "="*60)
        print("JSON to HDFS Complete:")
        print(f"Vehicles ingested: {vehicle_count:,}")
        print(f"Packages ingested: {package_count:,}")
        print(f"HDFS Path: {HDFS_OUTPUT_PATH}")
        print(f"Partition Date: {execution_date}")
        print("="*60)
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
