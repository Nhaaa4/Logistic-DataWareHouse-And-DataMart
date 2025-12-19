#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.functions import trim, upper, lower, regexp_replace, col, when, length
import sys
import os

def create_spark_session(app_name="HDFS to PostgreSQL ETL"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "/home/hadoop/logistic/jdbc/postgresql-42.6.0.jar") \
        .getOrCreate()

def clean_and_validate_data(df, entity_name, primary_key):
    
    initial_count = df.count()
    print("=" * 60)
    print(f"  Initial records: {initial_count:,}")
    
    # 1. Remove records with NULL primary keys
    df_clean = df.filter(col(primary_key).isNotNull())
    null_removed = initial_count - df_clean.count()
    if null_removed > 0:
        print(f"Removed {null_removed:,} records with NULL {primary_key}")
    
    # 2. Remove duplicates based on primary key (keep first occurrence)
    df_clean = df_clean.dropDuplicates([primary_key])
    duplicates_removed = initial_count - null_removed - df_clean.count()
    if duplicates_removed > 0:
        print(f"Removed {duplicates_removed:,} duplicate records")
    
    # 3. Clean string columns - trim whitespace
    string_cols = [field.name for field in df_clean.schema.fields 
                   if str(field.dataType) == "StringType"]
    for col_name in string_cols:
        if col_name not in ['source_file']:  # Don't clean metadata columns
            df_clean = df_clean.withColumn(col_name, trim(col(col_name)))
            # Replace empty strings with NULL
            df_clean = df_clean.withColumn(
                col_name, 
                when(length(col(col_name)) == 0, None).otherwise(col(col_name))
            )
    
    # 4. Validate email format (if email column exists)
    if 'email' in [field.name for field in df_clean.schema.fields]:
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        df_clean = df_clean.withColumn(
            'email',
            when(col('email').rlike(email_pattern), col('email')).otherwise(None)
        )
    
    # 5. Validate phone numbers (if phone column exists) - remove invalid characters
    if 'phone' in [field.name for field in df_clean.schema.fields]:
        df_clean = df_clean.withColumn(
            'phone',
            regexp_replace(col('phone'), r'[^0-9+\-\(\)\s]', '')
        )
    
    final_count = df_clean.count()
    print(f"  Final records: {final_count:,}")
    print(f"  Data quality: {(final_count/initial_count*100):.2f}% retained")
    print("=" * 60)
    
    return df_clean

def load_to_postgres(df, table_name, postgres_url, postgres_properties, mode="append"):
    df.write \
        .jdbc(url=postgres_url, 
              table=table_name, 
              mode=mode, 
              properties=postgres_properties)
    
    print(f"Loaded {df.count():,} records to {table_name}")
    return df.count()

def etl_customers(spark, hdfs_path, postgres_url, postgres_properties, execution_date):
    # Read from HDFS - ONLY today's partition
    customers_path = f"{hdfs_path}/customers/ingestion_date={execution_date}"
    df = spark.read.parquet(customers_path)
    
    # Transform: Select and rename columns for staging
    df_transformed = df.select(
        df["customer_id"],
        df["customer_name"],
        df["gender"],
        df["date_of_birth"],
        df["phone"],
        df["email"],
        df["address"],
        df["commune"],
        df["district"],
        df["province"],
        df["country"],
        df["postal_code"],
        df["customer_type"],
        df["registration_date"],
        df["loyalty_level"],
        df["preferred_contact"],
        df["is_active"],
        current_timestamp().alias("load_timestamp"),
        lit(f"HDFS-{execution_date}").alias("source_file")
    )
    
    # Clean and validate data
    df_clean = clean_and_validate_data(df_transformed, "customers", "customer_id")
    
    # Load to PostgreSQL staging
    count = load_to_postgres(df_clean, "staging.stg_customer", 
                             postgres_url, postgres_properties, mode="overwrite")
    
    return count

def etl_drivers(spark, hdfs_path, postgres_url, postgres_properties, execution_date):
    # Read from HDFS - ONLY today's partition
    drivers_path = f"{hdfs_path}/drivers/ingestion_date={execution_date}"
    df = spark.read.parquet(drivers_path)
    
    # Transform: Select and rename columns for staging
    df_transformed = df.select(
        df["driver_id"],
        df["driver_name"],
        df["gender"],
        df["date_of_birth"],
        df["phone"],
        df["license_number"],
        df["license_type"],
        df["license_expiry"],
        df["hire_date"],
        df["experience_years"],
        df["emergency_contact"],
        df["employment_type"],
        df["rating"],
        df["status"],
        df["base_city"],
        current_timestamp().alias("load_timestamp"),
        lit(f"HDFS-{execution_date}").alias("source_file")
    )
    
    # Clean and validate data
    df_clean = clean_and_validate_data(df_transformed, "drivers", "driver_id")
    
    # Load to PostgreSQL staging
    count = load_to_postgres(df_clean, "staging.stg_driver", 
                             postgres_url, postgres_properties, mode="overwrite")
    
    return count

def etl_vehicles(spark, hdfs_path, postgres_url, postgres_properties, execution_date):
    # Read from HDFS - ONLY today's partition
    vehicles_path = f"{hdfs_path}/vehicles/ingestion_date={execution_date}"
    df = spark.read.parquet(vehicles_path)
    
    # Transform: Select and rename columns for staging
    df_transformed = df.select(
        df["vehicle_id"],
        df["plate_number"],
        df["vehicle_type"],
        df["brand"],
        df["model"],
        df["manufacture_year"],
        df["capacity_kg"],
        df["capacity_volume_m3"],
        df["fuel_type"],
        df["fuel_efficiency"],
        df["last_service_date"],
        df["next_service_date"],
        df["insurance_expiry"],
        df["gps_installed"],
        df["status"],
        current_timestamp().alias("load_timestamp"),
        lit(f"HDFS-{execution_date}").alias("source_file")
    )
    
    # Clean and validate data
    df_clean = clean_and_validate_data(df_transformed, "vehicles", "vehicle_id")
    
    # Load to PostgreSQL staging
    count = load_to_postgres(df_clean, "staging.stg_vehicle", 
                             postgres_url, postgres_properties, mode="overwrite")
    
    return count

def etl_packages(spark, hdfs_path, postgres_url, postgres_properties, execution_date):
    # Read from HDFS - ONLY today's partition
    packages_path = f"{hdfs_path}/packages/ingestion_date={execution_date}"
    df = spark.read.parquet(packages_path)
    
    # Transform: Select and rename columns for staging
    df_transformed = df.select(
        df["package_id"],
        df["package_type"],
        df["weight_kg"],
        df["length_cm"],
        df["width_cm"],
        df["height_cm"],
        df["volume_cm3"],
        df["size_category"],
        df["fragile"],
        df["hazardous"],
        df["temperature_control"],
        df["insurance_value"],
        current_timestamp().alias("load_timestamp"),
        lit(f"HDFS-{execution_date}").alias("source_file")
    )
    
    # Clean and validate data
    df_clean = clean_and_validate_data(df_transformed, "packages", "package_id")
    
    # Load to PostgreSQL staging
    count = load_to_postgres(df_clean, "staging.stg_package", 
                             postgres_url, postgres_properties, mode="overwrite")
    
    return count

def etl_routes(spark, hdfs_path, postgres_url, postgres_properties, execution_date):
    # Read from HDFS - ONLY today's partition
    routes_path = f"{hdfs_path}/routes/ingestion_date={execution_date}"
    df = spark.read.parquet(routes_path)
    
    # Transform: Select and rename columns for staging
    df_transformed = df.select(
        df["route_id"],
        df["origin_country"],
        df["origin_province"],
        df["destination_country"],
        df["destination_province"],
        df["distance_km"],
        df["average_time_min"],
        df["road_type"],
        df["traffic_level"],
        df["toll_required"],
        df["region"],
        current_timestamp().alias("load_timestamp"),
        lit(f"HDFS-{execution_date}").alias("source_file")
    )
    
    # Clean and validate data
    df_clean = clean_and_validate_data(df_transformed, "routes", "route_id")
    
    # Load to PostgreSQL staging
    count = load_to_postgres(df_clean, "staging.stg_route", 
                             postgres_url, postgres_properties, mode="overwrite")
    
    return count

def etl_warehouses(spark, hdfs_path, postgres_url, postgres_properties, execution_date):
    # Read from HDFS - ONLY today's partition
    warehouses_path = f"{hdfs_path}/warehouses/ingestion_date={execution_date}"
    df = spark.read.parquet(warehouses_path)
    
    # Transform: Select and rename columns for staging
    df_transformed = df.select(
        df["warehouse_id"],
        df["warehouse_name"],
        df["province"],
        df["country"],
        df["capacity_packages"],
        df["manager_name"],
        df["contact_number"],
        df["operational_status"],
        current_timestamp().alias("load_timestamp"),
        lit(f"HDFS-{execution_date}").alias("source_file")
    )
    
    # Clean and validate data
    df_clean = clean_and_validate_data(df_transformed, "warehouses", "warehouse_id")
    
    # Load to PostgreSQL staging
    count = load_to_postgres(df_clean, "staging.stg_warehouse", 
                             postgres_url, postgres_properties, mode="overwrite")
    
    return count

def etl_deliveries(spark, hdfs_path, postgres_url, postgres_properties, execution_date):
    # Read from HDFS - ONLY today's partition
    deliveries_path = f"{hdfs_path}/deliveries/ingestion_date={execution_date}"
    df = spark.read.parquet(deliveries_path)
    
    # Transform: Select and rename columns for staging
    df_transformed = df.select(
        df["delivery_id"],
        df["customer_id"],
        df["driver_id"],
        df["vehicle_id"],
        df["route_id"],
        df["package_id"],
        df["warehouse_id"],
        df["pickup_time"],
        df["departure_time"],
        df["arrival_time"],
        df["delivery_time"],
        df["distance_km"],
        df["fuel_used_liters"],
        df["base_cost"],
        df["fuel_cost"],
        df["toll_cost"],
        df["insurance_cost"],
        df["total_delivery_cost"],
        df["payment_method"],
        df["payment_status"],
        df["delivery_status"],
        df["on_time_flag"],
        df["damaged_flag"],
        df["returned_flag"],
        current_timestamp().alias("load_timestamp"),
        lit(f"HDFS-{execution_date}").alias("source_file")
    )
    
    # Clean and validate data
    df_clean = clean_and_validate_data(df_transformed, "deliveries", "delivery_id")
    
    # Load to PostgreSQL staging
    count = load_to_postgres(df_clean, "staging.stg_delivery", 
                             postgres_url, postgres_properties, mode="overwrite")
    
    return count

def main():
    # Get command-line arguments
    HDFS_PATH = sys.argv[1] 
    POSTGRES_HOST = sys.argv[2] 
    POSTGRES_PORT = sys.argv[3] 
    POSTGRES_DB = sys.argv[4] 
    POSTGRES_USER = sys.argv[5] 
    POSTGRES_PASSWORD = sys.argv[6] 
    
    # PostgreSQL connection properties
    postgres_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    postgres_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    
    from datetime import datetime
    execution_date = datetime.now().strftime("%Y-%m-%d")
    
    # Create Spark session
    spark = create_spark_session("HDFS to PostgreSQL Staging")
    
    try:
        # ETL process for each entity
        customer_count = etl_customers(spark, HDFS_PATH, postgres_url, postgres_properties, execution_date)
        driver_count = etl_drivers(spark, HDFS_PATH, postgres_url, postgres_properties, execution_date)
        vehicle_count = etl_vehicles(spark, HDFS_PATH, postgres_url, postgres_properties, execution_date)
        package_count = etl_packages(spark, HDFS_PATH, postgres_url, postgres_properties, execution_date)
        route_count = etl_routes(spark, HDFS_PATH, postgres_url, postgres_properties, execution_date)
        warehouse_count = etl_warehouses(spark, HDFS_PATH, postgres_url, postgres_properties, execution_date)
        delivery_count = etl_deliveries(spark, HDFS_PATH, postgres_url, postgres_properties, execution_date)
        
        # Summary
        print("\n" + "="*60)
        print("ETL COMPLETED: HDFS to PostgreSQL Staging")
        print(f" - Customers:  {customer_count:>10,}")
        print(f" - Drivers:    {driver_count:>10,}")
        print(f" - Vehicles:   {vehicle_count:>10,}")
        print(f" - Packages:   {package_count:>10,}")
        print(f" - Routes:     {route_count:>10,}")
        print(f" - Warehouses: {warehouse_count:>10,}")
        print(f" - Deliveries: {delivery_count:>10,}")
        print("-"*60)
        total = customer_count + driver_count + vehicle_count + package_count + route_count + warehouse_count + delivery_count
        print(f"TOTAL:        {total:>10,}")
        print("="*60)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
