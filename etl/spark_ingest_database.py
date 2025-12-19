#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import sys
import os

def create_spark_session(app_name="Database Ingestion"):
    """Create Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

def ingest_routes(spark, db_path, hdfs_output_path, execution_date):
    """Ingest routes from SQLite to HDFS"""
    print("Ingesting routes from SQLite...")
    
    # Read from SQLite
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:sqlite:{db_path}") \
        .option("dbtable", "routes") \
        .option("driver", "org.sqlite.JDBC") \
        .load()
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("SQLITE")) \
           .withColumn("ingestion_date", lit(execution_date))
    
    # Write to HDFS as Parquet with partition
    output_path = f"{hdfs_output_path}/routes"
    df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .parquet(output_path)
    
    print(f"Ingested {df.count():,} route records to HDFS: {output_path}")
    return df.count()

def ingest_warehouses(spark, db_path, hdfs_output_path, execution_date):
    """Ingest warehouses from SQLite to HDFS"""
    print("Ingesting warehouses from SQLite...")
    
    # Read from SQLite
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:sqlite:{db_path}") \
        .option("dbtable", "warehouses") \
        .option("driver", "org.sqlite.JDBC") \
        .load()
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("SQLITE")) \
           .withColumn("ingestion_date", lit(execution_date))
    
    # Write to HDFS as Parquet with partition
    output_path = f"{hdfs_output_path}/warehouses"
    df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .parquet(output_path)
    
    print(f"Ingested {df.count():,} warehouse records to HDFS: {output_path}")
    return df.count()

def ingest_deliveries(spark, db_path, hdfs_output_path, execution_date):
    """Ingest deliveries from SQLite to HDFS"""
    print("Ingesting deliveries from SQLite...")
    
    # Read from SQLite
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:sqlite:{db_path}") \
        .option("dbtable", "deliveries") \
        .option("driver", "org.sqlite.JDBC") \
        .load()
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("SQLITE")) \
           .withColumn("ingestion_date", lit(execution_date))
    
    # Write to HDFS as Parquet with partition
    output_path = f"{hdfs_output_path}/deliveries"
    df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .parquet(output_path)
    
    print(f"Ingested {df.count():,} delivery records to HDFS: {output_path}")
    return df.count()

def main():
    """Main execution"""
    # Get values from command-line arguments
    execution_date = sys.argv[1] if len(sys.argv) > 1 else None
    run_id = sys.argv[2] if len(sys.argv) > 2 else None
    DATA_PATH = sys.argv[3] if len(sys.argv) > 3 else "D:/Y3T1/Data Engineering/Logistic Data Warehouse/data/data_sources"
    HDFS_OUTPUT_PATH = sys.argv[4] if len(sys.argv) > 4 else f"/logistics/raw/db/{execution_date}"
    
    DB_PATH = os.path.join(DATA_PATH, "logistics_source.db")
    
    # Create Spark session
    spark = create_spark_session("Logistics Database Ingestion")
    
    try:
        # Ingest sources
        route_count = ingest_routes(spark, DB_PATH, HDFS_OUTPUT_PATH, execution_date)
        warehouse_count = ingest_warehouses(spark, DB_PATH, HDFS_OUTPUT_PATH, execution_date)
        delivery_count = ingest_deliveries(spark, DB_PATH, HDFS_OUTPUT_PATH, execution_date)
        
        print("\n" + "="*60)
        print("DATABASE INGESTION COMPLETED → HDFS")
        print("="*60)
        print(f"Routes ingested:      {route_count:>10,}")
        print(f"Warehouses ingested:  {warehouse_count:>10,}")
        print(f"Deliveries ingested:  {delivery_count:>10,}")
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
