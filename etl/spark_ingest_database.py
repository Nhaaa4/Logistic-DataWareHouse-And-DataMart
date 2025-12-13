"""
Spark Job: Ingest Database Sources to Hive
===========================================
Ingests routes, warehouses, and deliveries from SQLite into Hive staging tables
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import argparse
import os

def create_spark_session(app_name="Database Ingestion"):
    """Create Spark session with Hive support"""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()

def ingest_routes(spark, db_path, hive_db):
    """Ingest routes from SQLite to Hive"""
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
           .withColumn("source_system", lit("SQLITE"))
    
    # Write to Hive staging table
    df.write.mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.stg_routes")
    
    print(f"Ingested {df.count():,} route records")
    return df.count()

def ingest_warehouses(spark, db_path, hive_db):
    """Ingest warehouses from SQLite to Hive"""
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
           .withColumn("source_system", lit("SQLITE"))
    
    # Write to Hive staging table
    df.write.mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.stg_warehouses")
    
    print(f"Ingested {df.count():,} warehouse records")
    return df.count()

def ingest_deliveries(spark, db_path, hive_db):
    """Ingest deliveries from SQLite to Hive"""
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
           .withColumn("source_system", lit("SQLITE"))
    
    # Write to Hive staging table
    df.write.mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.stg_deliveries")
    
    print(f"Ingested {df.count():,} delivery records")
    return df.count()

def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Ingest database sources to Hive')
    parser.add_argument('--data-path', required=True, help='Path to data directory')
    parser.add_argument('--hive-db', required=True, help='Hive database name')
    args = parser.parse_args()
    
    # Build database file path
    db_path = os.path.join(args.data_path, "operational_db.sqlite")
    
    # Create Spark session
    spark = create_spark_session("Logistics Database Ingestion")
    
    try:
        # Create database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.hive_db}")
        
        # Ingest sources
        route_count = ingest_routes(spark, db_path, args.hive_db)
        warehouse_count = ingest_warehouses(spark, db_path, args.hive_db)
        delivery_count = ingest_deliveries(spark, db_path, args.hive_db)
        
        print("\n" + "="*60)
        print("DATABASE INGESTION COMPLETED")
        print("="*60)
        print(f"Routes ingested:      {route_count:>10,}")
        print(f"Warehouses ingested:  {warehouse_count:>10,}")
        print(f"Deliveries ingested:  {delivery_count:>10,}")
        print("="*60)
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
