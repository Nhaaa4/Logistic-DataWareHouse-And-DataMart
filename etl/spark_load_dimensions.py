"""
Spark Job: Load Dimension Tables with SCD Type 2
=================================================
Loads dimension tables from staging with Slowly Changing Dimension Type 2 logic
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_date, lit, col, coalesce, max as spark_max, 
    year, month, dayofmonth, date_format, to_date
)
from pyspark.sql.types import IntegerType
import argparse
from datetime import datetime

def create_spark_session(app_name="Load Dimensions"):
    """Create Spark session with Hive support"""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

def load_date_dimension(spark, hive_db):
    """Generate and load date dimension"""
    print("Loading date dimension...")
    
    # Generate date range (2020-01-01 to 2025-12-31)
    df = spark.sql("""
        SELECT date_add('2020-01-01', seq) as full_date
        FROM (SELECT row_number() OVER (ORDER BY 1) - 1 as seq
              FROM (SELECT 1 FROM (SELECT 1) t1 
                    CROSS JOIN (SELECT 1) t2 
                    CROSS JOIN (SELECT 1) t3 
                    CROSS JOIN (SELECT 1) t4 
                    CROSS JOIN (SELECT 1) t5 
                    CROSS JOIN (SELECT 1) t6 
                    CROSS JOIN (SELECT 1) t7 
                    CROSS JOIN (SELECT 1) t8 
                    CROSS JOIN (SELECT 1) t9 
                    CROSS JOIN (SELECT 1) t10 
                    CROSS JOIN (SELECT 1) t11 
                    CROSS JOIN (SELECT 1) t12))
        WHERE seq < 2192
    """)
    
    # Add date attributes
    df = df.withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast(IntegerType())) \
           .withColumn("year", year(col("full_date"))) \
           .withColumn("month", month(col("full_date"))) \
           .withColumn("day", dayofmonth(col("full_date"))) \
           .withColumn("quarter", ((month(col("full_date")) - 1) / 3 + 1).cast(IntegerType())) \
           .withColumn("day_of_week", date_format(col("full_date"), "u").cast(IntegerType())) \
           .withColumn("day_name", date_format(col("full_date"), "EEEE")) \
           .withColumn("month_name", date_format(col("full_date"), "MMMM")) \
           .withColumn("is_weekend", (date_format(col("full_date"), "u") >= 6).cast("boolean"))
    
    # Write to Hive
    df.write.mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.dim_date")
    
    print(f"Loaded {df.count():,} date records")
    return df.count()

def load_customer_dimension(spark, hive_db):
    """Load customer dimension with SCD Type 2"""
    print("Loading customer dimension...")
    
    # Read staging
    stg = spark.table(f"{hive_db}.stg_customers")
    
    # Check if dimension exists
    try:
        dim = spark.table(f"{hive_db}.dim_customer")
        
        # Get current records
        current = dim.filter(col("is_current") == True)
        
        # Find changes
        changes = stg.join(current, stg.customer_id == current.customer_id, "left") \
                     .where((current.customer_id.isNull()) | 
                            (stg.name != current.name) |
                            (stg.email != current.email) |
                            (stg.phone != current.phone) |
                            (stg.address != current.address) |
                            (stg.city != current.city) |
                            (stg.state != current.state))
        
        # Expire old records
        if changes.count() > 0:
            spark.sql(f"""
                UPDATE {hive_db}.dim_customer
                SET is_current = false, expiry_date = current_date()
                WHERE customer_id IN (SELECT customer_id FROM changes)
                  AND is_current = true
            """)
        
        # Get next key
        next_key = dim.agg(spark_max("customer_key")).collect()[0][0] + 1
        
    except:
        # First load
        next_key = 1
        changes = stg
    
    # Create new records
    from pyspark.sql.window import Window
    window = Window.orderBy("customer_id")
    
    new_records = changes.withColumn("customer_key", 
                                     (col("customer_id").cast(IntegerType()) + lit(next_key - 1))) \
                         .withColumn("effective_date", current_date()) \
                         .withColumn("expiry_date", to_date(lit("9999-12-31"))) \
                         .withColumn("is_current", lit(True)) \
                         .select("customer_key", "customer_id", "name", "email", "phone", 
                                "address", "city", "state", "zip_code", "registration_date",
                                "effective_date", "expiry_date", "is_current")
    
    # Write to Hive (append mode for SCD2)
    new_records.write.mode("append") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.dim_customer")
    
    print(f"Loaded {new_records.count():,} customer records")
    return new_records.count()

def load_driver_dimension(spark, hive_db):
    """Load driver dimension with SCD Type 2"""
    print("Loading driver dimension...")
    
    stg = spark.table(f"{hive_db}.stg_drivers")
    
    try:
        dim = spark.table(f"{hive_db}.dim_driver")
        next_key = dim.agg(spark_max("driver_key")).collect()[0][0] + 1
    except:
        next_key = 1
    
    new_records = stg.withColumn("driver_key", 
                                 (col("driver_id").cast(IntegerType()) + lit(next_key - 1))) \
                     .withColumn("effective_date", current_date()) \
                     .withColumn("expiry_date", to_date(lit("9999-12-31"))) \
                     .withColumn("is_current", lit(True)) \
                     .select("driver_key", "driver_id", "name", "license_number", "phone",
                            "hire_date", "rating", "effective_date", "expiry_date", "is_current")
    
    new_records.write.mode("append") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.dim_driver")
    
    print(f"Loaded {new_records.count():,} driver records")
    return new_records.count()

def load_vehicle_dimension(spark, hive_db):
    """Load vehicle dimension with SCD Type 2"""
    print("Loading vehicle dimension...")
    
    stg = spark.table(f"{hive_db}.stg_vehicles")
    
    try:
        dim = spark.table(f"{hive_db}.dim_vehicle")
        next_key = dim.agg(spark_max("vehicle_key")).collect()[0][0] + 1
    except:
        next_key = 1
    
    new_records = stg.withColumn("vehicle_key", 
                                 (col("vehicle_id").cast(IntegerType()) + lit(next_key - 1))) \
                     .withColumn("effective_date", current_date()) \
                     .withColumn("expiry_date", to_date(lit("9999-12-31"))) \
                     .withColumn("is_current", lit(True)) \
                     .select("vehicle_key", "vehicle_id", "make", "model", "year",
                            "license_plate", "capacity", "fuel_type",
                            "effective_date", "expiry_date", "is_current")
    
    new_records.write.mode("append") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.dim_vehicle")
    
    print(f"Loaded {new_records.count():,} vehicle records")
    return new_records.count()

def load_route_dimension(spark, hive_db):
    """Load route dimension with SCD Type 2"""
    print("Loading route dimension...")
    
    stg = spark.table(f"{hive_db}.stg_routes")
    
    try:
        dim = spark.table(f"{hive_db}.dim_route")
        next_key = dim.agg(spark_max("route_key")).collect()[0][0] + 1
    except:
        next_key = 1
    
    new_records = stg.withColumn("route_key", 
                                 (col("route_id").cast(IntegerType()) + lit(next_key - 1))) \
                     .withColumn("effective_date", current_date()) \
                     .withColumn("expiry_date", to_date(lit("9999-12-31"))) \
                     .withColumn("is_current", lit(True)) \
                     .select("route_key", "route_id", "origin_city", "origin_state",
                            "destination_city", "destination_state", "distance_km",
                            "effective_date", "expiry_date", "is_current")
    
    new_records.write.mode("append") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.dim_route")
    
    print(f"Loaded {new_records.count():,} route records")
    return new_records.count()

def load_package_dimension(spark, hive_db):
    """Load package dimension with SCD Type 2"""
    print("Loading package dimension...")
    
    stg = spark.table(f"{hive_db}.stg_packages")
    
    try:
        dim = spark.table(f"{hive_db}.dim_package")
        next_key = dim.agg(spark_max("package_key")).collect()[0][0] + 1
    except:
        next_key = 1
    
    new_records = stg.withColumn("package_key", 
                                 (col("package_id").cast(IntegerType()) + lit(next_key - 1))) \
                     .withColumn("effective_date", current_date()) \
                     .withColumn("expiry_date", to_date(lit("9999-12-31"))) \
                     .withColumn("is_current", lit(True)) \
                     .select("package_key", "package_id", "package_type", "weight_kg",
                            "length_cm", "width_cm", "height_cm", "is_fragile", "is_hazardous",
                            "effective_date", "expiry_date", "is_current")
    
    new_records.write.mode("append") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.dim_package")
    
    print(f"Loaded {new_records.count():,} package records")
    return new_records.count()

def load_warehouse_dimension(spark, hive_db):
    """Load warehouse dimension with SCD Type 2"""
    print("Loading warehouse dimension...")
    
    stg = spark.table(f"{hive_db}.stg_warehouses")
    
    try:
        dim = spark.table(f"{hive_db}.dim_warehouse")
        next_key = dim.agg(spark_max("warehouse_key")).collect()[0][0] + 1
    except:
        next_key = 1
    
    new_records = stg.withColumn("warehouse_key", 
                                 (col("warehouse_id").cast(IntegerType()) + lit(next_key - 1))) \
                     .withColumn("effective_date", current_date()) \
                     .withColumn("expiry_date", to_date(lit("9999-12-31"))) \
                     .withColumn("is_current", lit(True)) \
                     .select("warehouse_key", "warehouse_id", "name", "address",
                            "city", "state", "zip_code", "capacity", "manager",
                            "effective_date", "expiry_date", "is_current")
    
    new_records.write.mode("append") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.dim_warehouse")
    
    print(f"Loaded {new_records.count():,} warehouse records")
    return new_records.count()

def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Load dimension tables')
    parser.add_argument('--hive-db', required=True, help='Hive database name')
    parser.add_argument('--dimension', required=True, 
                        choices=['date', 'customer', 'driver', 'vehicle', 'route', 'package', 'warehouse'],
                        help='Dimension to load')
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session(f"Load {args.dimension.title()} Dimension")
    
    try:
        # Load dimension based on argument
        if args.dimension == 'date':
            count = load_date_dimension(spark, args.hive_db)
        elif args.dimension == 'customer':
            count = load_customer_dimension(spark, args.hive_db)
        elif args.dimension == 'driver':
            count = load_driver_dimension(spark, args.hive_db)
        elif args.dimension == 'vehicle':
            count = load_vehicle_dimension(spark, args.hive_db)
        elif args.dimension == 'route':
            count = load_route_dimension(spark, args.hive_db)
        elif args.dimension == 'package':
            count = load_package_dimension(spark, args.hive_db)
        elif args.dimension == 'warehouse':
            count = load_warehouse_dimension(spark, args.hive_db)
        
        print("\n" + "="*60)
        print(f"{args.dimension.upper()} DIMENSION LOAD COMPLETED")
        print("="*60)
        print(f"Records loaded: {count:,}")
        print("="*60)
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
