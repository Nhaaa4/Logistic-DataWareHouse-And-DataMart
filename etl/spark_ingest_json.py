"""
Spark Job: Ingest JSON Sources to Hive
=======================================
Ingests vehicles and packages JSON files into Hive staging tables
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, explode, col
import argparse
import os

def create_spark_session(app_name="JSON Ingestion"):
    """Create Spark session with Hive support"""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()

def ingest_vehicles(spark, data_path, hive_db):
    """Ingest vehicles JSON (API format) to Hive"""
    print("Ingesting vehicles from JSON API...")
    
    vehicles_path = os.path.join(data_path, "vehicles_api.json")
    
    # Read JSON (API response format)
    df_raw = spark.read.json(vehicles_path)
    
    # Extract data array from API response
    df = df_raw.select(explode(col("data")).alias("vehicle")) \
               .select("vehicle.*")
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("API_JSON"))
    
    # Write to Hive staging table
    df.write.mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.stg_vehicles")
    
    print(f"Ingested {df.count():,} vehicle records")
    return df.count()

def ingest_packages(spark, data_path, hive_db):
    """Ingest packages JSON to Hive"""
    print("Ingesting packages from JSON...")
    
    packages_path = os.path.join(data_path, "packages.json")
    
    # Read JSON
    df_raw = spark.read.json(packages_path)
    
    # Extract packages array
    df = df_raw.select(explode(col("packages")).alias("package")) \
               .select("package.*")
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("JSON"))
    
    # Write to Hive staging table
    df.write.mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.stg_packages")
    
    print(f"Ingested {df.count():,} package records")
    return df.count()

def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Ingest JSON sources to Hive')
    parser.add_argument('--data-path', required=True, help='Path to data directory')
    parser.add_argument('--hive-db', required=True, help='Hive database name')
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("Logistics JSON Ingestion")
    
    try:
        # Create database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.hive_db}")
        
        # Ingest sources
        vehicle_count = ingest_vehicles(spark, args.data_path, args.hive_db)
        package_count = ingest_packages(spark, args.data_path, args.hive_db)
        
        print("\n" + "="*60)
        print("JSON INGESTION COMPLETED")
        print("="*60)
        print(f"Vehicles ingested: {vehicle_count:,}")
        print(f"Packages ingested: {package_count:,}")
        print("="*60)
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
