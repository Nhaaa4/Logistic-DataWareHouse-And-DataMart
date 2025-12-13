"""
Spark Job: Ingest CSV Sources to Hive
======================================
Ingests customers and drivers CSV files into Hive staging tables
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import argparse
import os

def create_spark_session(app_name="CSV Ingestion"):
    """Create Spark session with Hive support"""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()

def ingest_customers(spark, data_path, hive_db):
    """Ingest customers CSV to Hive"""
    print("Ingesting customers from CSV...")
    
    customers_path = os.path.join(data_path, "customers.csv")
    
    # Read CSV
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(customers_path)
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("CSV"))
    
    # Write to Hive staging table
    df.write.mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.stg_customers")
    
    print(f"Ingested {df.count():,} customer records")
    return df.count()

def ingest_drivers(spark, data_path, hive_db):
    """Ingest drivers CSV to Hive"""
    print("Ingesting drivers from CSV...")
    
    drivers_path = os.path.join(data_path, "drivers.csv")
    
    # Read CSV
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(drivers_path)
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("CSV"))
    
    # Write to Hive staging table
    df.write.mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.stg_drivers")
    
    print(f"Ingested {df.count():,} driver records")
    return df.count()

def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Ingest CSV sources to Hive')
    parser.add_argument('--data-path', required=True, help='Path to data directory')
    parser.add_argument('--hive-db', required=True, help='Hive database name')
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("Logistics CSV Ingestion")
    
    try:
        # Create database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.hive_db}")
        
        # Ingest sources
        customer_count = ingest_customers(spark, args.data_path, args.hive_db)
        driver_count = ingest_drivers(spark, args.data_path, args.hive_db)
        
        print("\n" + "="*60)
        print("CSV INGESTION COMPLETED")
        print("="*60)
        print(f"Customers ingested: {customer_count:,}")
        print(f"Drivers ingested:   {driver_count:,}")
        print("="*60)
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
