"""
Spark Job: Load Fact Delivery Table
====================================
Loads fact_delivery table from staging with dimension key lookups
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, date_format, to_date, current_timestamp, lit
)
from pyspark.sql.types import IntegerType
import argparse

def create_spark_session(app_name="Load Fact Table"):
    """Create Spark session with Hive support"""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

def load_fact_delivery(spark, hive_db):
    """Load fact_delivery table with dimension key lookups"""
    print("Loading fact_delivery table...")
    
    # Read staging deliveries
    stg_deliveries = spark.table(f"{hive_db}.stg_deliveries")
    
    # Read current dimensions
    dim_customer = spark.table(f"{hive_db}.dim_customer").filter(col("is_current") == True)
    dim_driver = spark.table(f"{hive_db}.dim_driver").filter(col("is_current") == True)
    dim_vehicle = spark.table(f"{hive_db}.dim_vehicle").filter(col("is_current") == True)
    dim_route = spark.table(f"{hive_db}.dim_route").filter(col("is_current") == True)
    dim_package = spark.table(f"{hive_db}.dim_package").filter(col("is_current") == True)
    dim_warehouse = spark.table(f"{hive_db}.dim_warehouse").filter(col("is_current") == True)
    dim_date = spark.table(f"{hive_db}.dim_date")
    
    # Join with dimensions to get surrogate keys
    fact = stg_deliveries \
        .join(dim_customer, stg_deliveries.customer_id == dim_customer.customer_id, "left") \
        .join(dim_driver, stg_deliveries.driver_id == dim_driver.driver_id, "left") \
        .join(dim_vehicle, stg_deliveries.vehicle_id == dim_vehicle.vehicle_id, "left") \
        .join(dim_route, stg_deliveries.route_id == dim_route.route_id, "left") \
        .join(dim_package, stg_deliveries.package_id == dim_package.package_id, "left") \
        .join(dim_warehouse, stg_deliveries.warehouse_id == dim_warehouse.warehouse_id, "left") \
        .join(dim_date, 
              date_format(to_date(stg_deliveries.pickup_datetime), "yyyyMMdd").cast(IntegerType()) == dim_date.date_key,
              "left")
    
    # Select fact columns with surrogate keys
    fact = fact.select(
        col("delivery_id"),
        dim_customer.customer_key.alias("customer_key"),
        dim_driver.driver_key.alias("driver_key"),
        dim_vehicle.vehicle_key.alias("vehicle_key"),
        dim_route.route_key.alias("route_key"),
        dim_package.package_key.alias("package_key"),
        dim_warehouse.warehouse_key.alias("warehouse_key"),
        dim_date.date_key.alias("date_key"),
        col("pickup_datetime"),
        col("delivery_datetime"),
        col("actual_distance_km"),
        col("delivery_fee"),
        col("tip_amount"),
        col("total_amount"),
        col("payment_method"),
        col("delivery_status"),
        col("priority_level"),
        col("special_instructions"),
        col("customer_rating"),
        col("driver_rating")
    )
    
    # Add partition columns for writing
    fact = fact.withColumn("year", year(col("pickup_datetime"))) \
               .withColumn("month", month(col("pickup_datetime")))
    
    # Write to Hive with partitioning and bucketing
    fact.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .bucketBy(32, "customer_key") \
        .sortBy("customer_key") \
        .format("parquet") \
        .option("compression", "snappy") \
        .saveAsTable(f"{hive_db}.fact_delivery")
    
    print(f"Loaded {fact.count():,} delivery records")
    return fact.count()

def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Load fact_delivery table')
    parser.add_argument('--hive-db', required=True, help='Hive database name')
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("Load Fact Delivery")
    
    try:
        count = load_fact_delivery(spark, args.hive_db)
        
        print("\n" + "="*60)
        print("FACT TABLE LOAD COMPLETED")
        print("="*60)
        print(f"Delivery records loaded: {count:,}")
        print("="*60)
        
        # Show partition info
        print("\nPartition Statistics:")
        partitions = spark.sql(f"SHOW PARTITIONS {args.hive_db}.fact_delivery")
        partitions.show(20, False)
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
