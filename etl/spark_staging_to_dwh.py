#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, current_date, lit, col, when,
    year, datediff, coalesce, to_date, unix_timestamp,
    floor, concat_ws
)
import pyspark.sql.functions as F
import sys
from datetime import datetime

def create_spark_session(app_name="Staging to DWH ETL with SCD Type 2"):
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "/home/hadoop/logistic/jdbc/postgresql-42.6.0.jar") \
        .getOrCreate()

def get_postgres_config(host, port, database, user, password):
    """Return PostgreSQL connection configuration"""
    postgres_url = f"jdbc:postgresql://{host}:{port}/{database}"
    postgres_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
    return postgres_url, postgres_properties

def expire_records_via_spark(spark, dimension_name, surrogate_key_col, keys_to_expire,
                             postgres_url, postgres_properties):
    """
    Expire records using Spark by reading, updating, and overwriting.
    This avoids using psycopg2 for UPDATE queries.
    """
    if not keys_to_expire:
        return 0

    # Read all records from the dimension table
    existing_df = spark.read.jdbc(
        url=postgres_url,
        table=f"dwh.{dimension_name}",
        properties=postgres_properties
    )

    # Create a DataFrame with keys to expire
    keys_df = spark.createDataFrame([(k,) for k in keys_to_expire], [surrogate_key_col])

    # Split into records to expire and records to keep unchanged
    records_to_expire = existing_df.join(
        keys_df,
        existing_df[surrogate_key_col] == keys_df[surrogate_key_col],
        "inner"
    ).drop(keys_df[surrogate_key_col]).filter(col("is_current") == True)

    records_unchanged = existing_df.join(
        keys_df,
        existing_df[surrogate_key_col] == keys_df[surrogate_key_col],
        "left_anti"
    )

    # Also keep already expired records (is_current = False) from the keys_to_expire set
    already_expired = existing_df.join(
        keys_df,
        existing_df[surrogate_key_col] == keys_df[surrogate_key_col],
        "inner"
    ).drop(keys_df[surrogate_key_col]).filter(col("is_current") == False)

    # Update the records to expire
    expired_records = records_to_expire \
        .withColumn("expiry_date", current_date()) \
        .withColumn("is_current", lit(False)) \
        .withColumn("updated_at", current_timestamp())

    # Combine all records
    final_df = records_unchanged.unionByName(expired_records).unionByName(already_expired)

    # Overwrite the dimension table
    final_df.write.jdbc(
        url=postgres_url,
        table=f"dwh.{dimension_name}",
        mode="overwrite",
        properties=postgres_properties
    )

    return expired_records.count()

def read_from_staging(spark, table_name, postgres_url, postgres_properties):
    """Read data from staging table"""
    df = spark.read \
        .jdbc(url=postgres_url, 
              table=f"staging.{table_name}", 
              properties=postgres_properties)
    return df

def read_from_dwh(spark, table_name, postgres_url, postgres_properties):
    """Read data from DWH dimension table"""
    try:
        df = spark.read \
            .jdbc(url=postgres_url, 
                  table=f"dwh.{table_name}", 
                  properties=postgres_properties)
        return df
    except:
        return None

def scd_type2_dimension(spark, staging_df, dimension_name, business_key, 
                        postgres_url, postgres_properties, compare_columns):
    """
    Implement SCD Type 2 for dimension tables
    
    Args:
        staging_df: DataFrame from staging
        dimension_name: Name of dimension table (e.g., 'dim_customer')
        business_key: Business key column (e.g., 'customer_id')
        compare_columns: List of columns to compare for changes
    """
    
    print(f"\n{'='*70}")
    print(f"Processing SCD Type 2 for {dimension_name}")
    print(f"{'='*70}")
    
    # Read existing dimension
    existing_dim = read_from_dwh(spark, dimension_name, postgres_url, postgres_properties)
    
    # If dimension table is empty, insert all as new records
    if existing_dim is None or existing_dim.count() == 0:
        print(f"Dimension table {dimension_name} is empty. Inserting all records as new...")
        
        # Prepare new records with SCD Type 2 columns
        new_records = staging_df.withColumn("effective_date", current_date()) \
                                 .withColumn("expiry_date", lit("9999-12-31").cast("date")) \
                                 .withColumn("is_current", lit(True)) \
                                 .withColumn("created_at", current_timestamp()) \
                                 .withColumn("updated_at", current_timestamp())
        
        # Remove staging metadata columns if they exist
        cols_to_drop = ['load_timestamp', 'source_file']
        for col_name in cols_to_drop:
            if col_name in new_records.columns:
                new_records = new_records.drop(col_name)
        
        # Write to dimension table
        new_records.write \
            .jdbc(url=postgres_url, 
                  table=f"dwh.{dimension_name}", 
                  mode="append", 
                  properties=postgres_properties)
        
        print(f"Inserted {new_records.count()} new records into {dimension_name}")
        return new_records.count()
    
    # Get only current records from dimension
    current_dim = existing_dim.filter(col("is_current") == True)
    
    print(f"Current records in dimension: {current_dim.count()}")
    print(f"Staging records: {staging_df.count()}")
    
    # Join staging with current dimension on business key
    joined = staging_df.alias("stg").join(
        current_dim.alias("dim"),
        col(f"stg.{business_key}") == col(f"dim.{business_key}"),
        "left"
    )
    
    # Identify new records (not in dimension)
    new_records = joined.filter(col(f"dim.{business_key}").isNull())
    
    # Identify existing records
    existing_records = joined.filter(col(f"dim.{business_key}").isNotNull())
    
    # Check for changes in compare columns
    change_condition = None
    for compare_col in compare_columns:
        if change_condition is None:
            change_condition = (col(f"stg.{compare_col}") != col(f"dim.{compare_col}")) | \
                               (col(f"stg.{compare_col}").isNull() != col(f"dim.{compare_col}").isNull())
        else:
            change_condition = change_condition | \
                               (col(f"stg.{compare_col}") != col(f"dim.{compare_col}")) | \
                               (col(f"stg.{compare_col}").isNull() != col(f"dim.{compare_col}").isNull())
    
    # Records with changes
    changed_records = existing_records.filter(change_condition)
    
    print(f"New records to insert: {new_records.count()}")
    print(f"Changed records to update: {changed_records.count()}")
    
    # Prepare new records to insert
    if new_records.count() > 0:
        # Select only staging columns and add SCD columns
        stg_columns = [col(f"stg.{c}") for c in staging_df.columns]
        new_inserts = new_records.select(*stg_columns) \
            .withColumn("effective_date", current_date()) \
            .withColumn("expiry_date", lit("9999-12-31").cast("date")) \
            .withColumn("is_current", lit(True)) \
            .withColumn("created_at", current_timestamp()) \
            .withColumn("updated_at", current_timestamp())
        
        # Remove staging metadata columns
        cols_to_drop = ['load_timestamp', 'source_file']
        for col_name in cols_to_drop:
            if col_name in new_inserts.columns:
                new_inserts = new_inserts.drop(col_name)
        
        # Write new records
        new_inserts.write \
            .jdbc(url=postgres_url, 
                  table=f"dwh.{dimension_name}", 
                  mode="append", 
                  properties=postgres_properties)
        
        print(f"Inserted {new_inserts.count()} new records")
    
    # Handle changed records (SCD Type 2)
    if changed_records.count() > 0:
        # Get the surrogate keys of records to expire
        keys_to_expire = changed_records.select(
            col(f"dim.{dimension_name.replace('dim_', '')}_key").alias("surrogate_key")
        ).distinct()

        # Collect keys before any write operations
        keys_list = [row.surrogate_key for row in keys_to_expire.collect()]

        # Prepare new versions of changed records
        stg_columns = [col(f"stg.{c}") for c in staging_df.columns]
        new_versions = changed_records.select(*stg_columns) \
            .withColumn("effective_date", current_date()) \
            .withColumn("expiry_date", lit("9999-12-31").cast("date")) \
            .withColumn("is_current", lit(True)) \
            .withColumn("created_at", current_timestamp()) \
            .withColumn("updated_at", current_timestamp())

        # Remove staging metadata columns
        cols_to_drop = ['load_timestamp', 'source_file']
        for col_name in cols_to_drop:
            if col_name in new_versions.columns:
                new_versions = new_versions.drop(col_name)

        print(f"Creating new versions for {new_versions.count()} changed records")

        # Step 1: Expire old records first (before inserting new versions)
        surrogate_key_col = f"{dimension_name.replace('dim_', '')}_key"
        if keys_list:
            print(f"Expiring {len(keys_list)} old records in {dimension_name}...")
            affected_rows = expire_records_via_spark(
                spark, dimension_name, surrogate_key_col, keys_list,
                postgres_url, postgres_properties
            )
            print(f"Expired {affected_rows} old records")

        # Step 2: Insert new versions
        new_versions.write \
            .jdbc(url=postgres_url,
                  table=f"dwh.{dimension_name}",
                  mode="append",
                  properties=postgres_properties)
        print(f"Inserted {new_versions.count()} new version records")
    
    print(f"{'='*70}\n")
    return new_records.count() + changed_records.count()

def load_dim_customer(spark, postgres_url, postgres_properties):
    """Load customer dimension with SCD Type 2"""
    staging_df = read_from_staging(spark, "stg_customer", postgres_url, postgres_properties)

    if staging_df.count() == 0:
        print("No customer data in staging")
        return 0

    # Cast date columns from string to date type
    staging_df = staging_df.withColumn("date_of_birth", to_date(col("date_of_birth")))

    # Cast boolean columns
    staging_df = staging_df.withColumn("is_active", col("is_active").cast("boolean"))

    # Add calculated columns
    staging_df = staging_df.withColumn(
        "age",
        when(col("date_of_birth").isNotNull(),
             floor(datediff(current_date(), col("date_of_birth")) / 365)
        ).otherwise(None)
    )

    # Define columns to compare for changes (excluding metadata)
    compare_columns = [
        'customer_name', 'gender', 'date_of_birth', 'phone', 'email',
        'address', 'commune', 'district', 'province', 'country', 'postal_code',
        'customer_type', 'loyalty_level', 'preferred_contact', 'is_active'
    ]

    return scd_type2_dimension(
        spark, staging_df, 'dim_customer', 'customer_id', 
        postgres_url, postgres_properties, compare_columns
    )

def load_dim_driver(spark, postgres_url, postgres_properties):
    """Load driver dimension with SCD Type 2"""
    staging_df = read_from_staging(spark, "stg_driver", postgres_url, postgres_properties)

    if staging_df.count() == 0:
        print("No driver data in staging")
        return 0

    # Cast date columns from string to date type
    staging_df = staging_df.withColumn("date_of_birth", to_date(col("date_of_birth"))) \
                           .withColumn("license_expiry", to_date(col("license_expiry")))

    # Add calculated columns
    staging_df = staging_df.withColumn(
        "age",
        when(col("date_of_birth").isNotNull(),
             floor(datediff(current_date(), col("date_of_birth")) / 365)
        ).otherwise(None)
    )
    
    compare_columns = [
        'driver_name', 'gender', 'date_of_birth', 'phone', 'license_number',
        'license_type', 'license_expiry', 'experience_years', 'emergency_contact',
        'employment_type', 'rating', 'status', 'base_city'
    ]
    
    return scd_type2_dimension(
        spark, staging_df, 'dim_driver', 'driver_id', 
        postgres_url, postgres_properties, compare_columns
    )

def load_dim_vehicle(spark, postgres_url, postgres_properties):
    """Load vehicle dimension with SCD Type 2"""
    staging_df = read_from_staging(spark, "stg_vehicle", postgres_url, postgres_properties)

    if staging_df.count() == 0:
        print("No vehicle data in staging")
        return 0

    # Cast date columns from string to date type
    staging_df = staging_df.withColumn("last_service_date", to_date(col("last_service_date"))) \
                           .withColumn("next_service_date", to_date(col("next_service_date"))) \
                           .withColumn("insurance_expiry", to_date(col("insurance_expiry")))

    # Cast boolean columns
    staging_df = staging_df.withColumn("gps_installed", col("gps_installed").cast("boolean"))

    # Add calculated columns
    staging_df = staging_df.withColumn(
        "vehicle_age",
        when(col("manufacture_year").isNotNull(),
             year(current_date()) - col("manufacture_year")
        ).otherwise(None)
    )

    compare_columns = [
        'plate_number', 'vehicle_type', 'brand', 'model', 'manufacture_year',
        'capacity_kg', 'capacity_volume_m3', 'fuel_type', 'fuel_efficiency',
        'last_service_date', 'next_service_date', 'insurance_expiry',
        'gps_installed', 'status'
    ]

    return scd_type2_dimension(
        spark, staging_df, 'dim_vehicle', 'vehicle_id', 
        postgres_url, postgres_properties, compare_columns
    )

def load_dim_route(spark, postgres_url, postgres_properties):
    """Load route dimension with SCD Type 2"""
    staging_df = read_from_staging(spark, "stg_route", postgres_url, postgres_properties)

    if staging_df.count() == 0:
        print("No route data in staging")
        return 0

    # Cast toll_required to boolean (PostgreSQL expects boolean, not integer)
    staging_df = staging_df.withColumn("toll_required", col("toll_required").cast("boolean"))

    compare_columns = [
        'origin_country', 'origin_province', 'destination_country', 
        'destination_province', 'distance_km', 'average_time_min',
        'road_type', 'traffic_level', 'toll_required', 'region'
    ]
    
    return scd_type2_dimension(
        spark, staging_df, 'dim_route', 'route_id', 
        postgres_url, postgres_properties, compare_columns
    )

def load_dim_package(spark, postgres_url, postgres_properties):
    """Load package dimension with SCD Type 2"""
    staging_df = read_from_staging(spark, "stg_package", postgres_url, postgres_properties)

    if staging_df.count() == 0:
        print("No package data in staging")
        return 0

    # Cast boolean columns
    staging_df = staging_df.withColumn("fragile", col("fragile").cast("boolean")) \
                           .withColumn("hazardous", col("hazardous").cast("boolean")) \
                           .withColumn("temperature_control", col("temperature_control").cast("boolean"))

    compare_columns = [
        'package_type', 'weight_kg', 'length_cm', 'width_cm', 'height_cm',
        'volume_cm3', 'size_category', 'fragile', 'hazardous',
        'temperature_control', 'insurance_value'
    ]

    return scd_type2_dimension(
        spark, staging_df, 'dim_package', 'package_id', 
        postgres_url, postgres_properties, compare_columns
    )

def load_dim_warehouse(spark, postgres_url, postgres_properties):
    """Load warehouse dimension with SCD Type 2"""
    staging_df = read_from_staging(spark, "stg_warehouse", postgres_url, postgres_properties)
    
    if staging_df.count() == 0:
        print("No warehouse data in staging")
        return 0
    
    compare_columns = [
        'warehouse_name', 'province', 'country', 'capacity_packages',
        'manager_name', 'contact_number', 'operational_status'
    ]
    
    return scd_type2_dimension(
        spark, staging_df, 'dim_warehouse', 'warehouse_id', 
        postgres_url, postgres_properties, compare_columns
    )

def load_dim_date(spark, postgres_url, postgres_properties):
    """
    Load date dimension
    This should be pre-populated or updated separately
    For this ETL, we'll check if dates exist and insert missing ones
    """
    print("\n" + "="*70)
    print("Processing Date Dimension")
    print("="*70)
    
    # Read staging delivery to get dates
    staging_delivery = read_from_staging(spark, "stg_delivery", postgres_url, postgres_properties)
    
    if staging_delivery.count() == 0:
        print("No delivery data in staging to extract dates")
        return 0
    
    # Extract unique dates from delivery timestamps
    dates_df = staging_delivery.select(
        to_date(col("delivery_time")).alias("full_date")
    ).filter(col("full_date").isNotNull()).distinct()
    
    # Add date attributes
    dates_with_attrs = dates_df.withColumn("date_key", 
                                            F.regexp_replace(F.date_format(col("full_date"), "yyyyMMdd"), "-", "").cast("int")) \
        .withColumn("day", F.dayofmonth(col("full_date"))) \
        .withColumn("day_name", F.date_format(col("full_date"), "EEEE")) \
        .withColumn("day_of_week", F.dayofweek(col("full_date"))) \
        .withColumn("day_of_year", F.dayofyear(col("full_date"))) \
        .withColumn("week_of_year", F.weekofyear(col("full_date"))) \
        .withColumn("month", F.month(col("full_date"))) \
        .withColumn("month_name", F.date_format(col("full_date"), "MMMM")) \
        .withColumn("quarter", F.quarter(col("full_date"))) \
        .withColumn("year", F.year(col("full_date"))) \
        .withColumn("is_weekend", 
                    when(F.dayofweek(col("full_date")).isin([1, 7]), True).otherwise(False)) \
        .withColumn("is_holiday", lit(False)) \
        .withColumn("fiscal_year", F.year(col("full_date"))) \
        .withColumn("fiscal_quarter", F.quarter(col("full_date"))) \
        .withColumn("effective_date", current_date()) \
        .withColumn("expiry_date", lit("9999-12-31").cast("date")) \
        .withColumn("is_current", lit(True)) \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("updated_at", current_timestamp())
    
    # Read existing dates from dimension
    existing_dates = read_from_dwh(spark, "dim_date", postgres_url, postgres_properties)
    
    if existing_dates is None or existing_dates.count() == 0:
        # Insert all dates
        dates_with_attrs.write \
            .jdbc(url=postgres_url, 
                  table="dwh.dim_date", 
                  mode="append", 
                  properties=postgres_properties)
        print(f"Inserted {dates_with_attrs.count()} dates into dim_date")
        return dates_with_attrs.count()
    
    # Insert only new dates
    new_dates = dates_with_attrs.join(
        existing_dates,
        dates_with_attrs.date_key == existing_dates.date_key,
        "left_anti"
    )
    
    if new_dates.count() > 0:
        new_dates.write \
            .jdbc(url=postgres_url, 
                  table="dwh.dim_date", 
                  mode="append", 
                  properties=postgres_properties)
        print(f"Inserted {new_dates.count()} new dates into dim_date")
    else:
        print("No new dates to insert")
    
    print("="*70 + "\n")
    return new_dates.count()

def load_fact_delivery(spark, postgres_url, postgres_properties):
    """Load fact table for deliveries"""
    print("\n" + "="*70)
    print("Processing Fact Delivery")
    print("="*70)
    
    # Read staging delivery
    staging_delivery = read_from_staging(spark, "stg_delivery", postgres_url, postgres_properties)
    
    if staging_delivery.count() == 0:
        print("No delivery data in staging")
        return 0
    
    print(f"Staging delivery records: {staging_delivery.count()}")
    
    # Read current dimensions to get surrogate keys
    dim_customer = read_from_dwh(spark, "dim_customer", postgres_url, postgres_properties) \
        .filter(col("is_current") == True)
    dim_driver = read_from_dwh(spark, "dim_driver", postgres_url, postgres_properties) \
        .filter(col("is_current") == True)
    dim_vehicle = read_from_dwh(spark, "dim_vehicle", postgres_url, postgres_properties) \
        .filter(col("is_current") == True)
    dim_route = read_from_dwh(spark, "dim_route", postgres_url, postgres_properties) \
        .filter(col("is_current") == True)
    dim_package = read_from_dwh(spark, "dim_package", postgres_url, postgres_properties) \
        .filter(col("is_current") == True)
    dim_warehouse = read_from_dwh(spark, "dim_warehouse", postgres_url, postgres_properties) \
        .filter(col("is_current") == True)
    dim_date = read_from_dwh(spark, "dim_date", postgres_url, postgres_properties)
    
    # Join with dimensions to get surrogate keys
    fact_df = staging_delivery.alias("stg") \
        .join(dim_date.select("date_key", "full_date").alias("dd"),
              to_date(col("stg.delivery_time")) == col("dd.full_date"), "left") \
        .join(dim_customer.select("customer_key", "customer_id").alias("dc"),
              col("stg.customer_id") == col("dc.customer_id"), "left") \
        .join(dim_driver.select("driver_key", "driver_id").alias("ddr"),
              col("stg.driver_id") == col("ddr.driver_id"), "left") \
        .join(dim_vehicle.select("vehicle_key", "vehicle_id").alias("dv"),
              col("stg.vehicle_id") == col("dv.vehicle_id"), "left") \
        .join(dim_route.select("route_key", "route_id").alias("dr"),
              col("stg.route_id") == col("dr.route_id"), "left") \
        .join(dim_package.select("package_key", "package_id").alias("dp"),
              col("stg.package_id") == col("dp.package_id"), "left") \
        .join(dim_warehouse.select("warehouse_key", "warehouse_id").alias("dw"),
              col("stg.warehouse_id") == col("dw.warehouse_id"), "left")
    
    # Select and transform columns for fact table
    fact_transformed = fact_df.select(
        col("stg.delivery_id"),
        col("dd.date_key"),
        col("dc.customer_key"),
        col("ddr.driver_key"),
        col("dv.vehicle_key"),
        col("dr.route_key"),
        col("dp.package_key"),
        col("dw.warehouse_key"),
        col("stg.pickup_time").cast("timestamp"),
        col("stg.departure_time").cast("timestamp"),
        col("stg.arrival_time").cast("timestamp"),
        col("stg.delivery_time").cast("timestamp"),
        col("stg.distance_km"),
        col("stg.fuel_used_liters"),
        col("stg.base_cost"),
        col("stg.fuel_cost"),
        col("stg.toll_cost"),
        col("stg.insurance_cost"),
        col("stg.total_delivery_cost"),
        col("stg.payment_method"),
        col("stg.payment_status"),
        col("stg.delivery_status"),
        col("stg.on_time_flag").cast("boolean"),
        col("stg.damaged_flag").cast("boolean"),
        col("stg.returned_flag").cast("boolean")
    ).withColumn("delivery_duration_min",
                 when((col("delivery_time").isNotNull()) & (col("departure_time").isNotNull()),
                      (unix_timestamp(col("delivery_time")) - unix_timestamp(col("departure_time"))) / 60
                 ).otherwise(None).cast("int")
    ).withColumn("year", year(col("delivery_time"))) \
     .withColumn("month", F.month(col("delivery_time"))) \
     .withColumn("created_at", current_timestamp()) \
     .withColumn("updated_at", current_timestamp())
    
    # Filter out records with missing dimension keys
    fact_complete = fact_transformed.filter(
        col("date_key").isNotNull() &
        col("customer_key").isNotNull() &
        col("driver_key").isNotNull() &
        col("vehicle_key").isNotNull() &
        col("route_key").isNotNull() &
        col("package_key").isNotNull() &
        col("warehouse_key").isNotNull()
    )
    
    incomplete_count = fact_transformed.count() - fact_complete.count()
    if incomplete_count > 0:
        print(f"WARNING: {incomplete_count} records skipped due to missing dimension references")
    
    # Check for existing deliveries to avoid duplicates
    existing_facts = read_from_dwh(spark, "fact_delivery", postgres_url, postgres_properties)
    
    if existing_facts is not None and existing_facts.count() > 0:
        # Insert only new deliveries
        new_facts = fact_complete.join(
            existing_facts.select("delivery_id"),
            "delivery_id",
            "left_anti"
        )
        
        if new_facts.count() > 0:
            new_facts.write \
                .jdbc(url=postgres_url, 
                      table="dwh.fact_delivery", 
                      mode="append", 
                      properties=postgres_properties)
            print(f"Inserted {new_facts.count()} new delivery facts")
        else:
            print("No new deliveries to insert")
        
        result_count = new_facts.count()
    else:
        # Insert all facts
        fact_complete.write \
            .jdbc(url=postgres_url, 
                  table="dwh.fact_delivery", 
                  mode="append", 
                  properties=postgres_properties)
        print(f"Inserted {fact_complete.count()} delivery facts")
        result_count = fact_complete.count()
    
    print("="*70 + "\n")
    return result_count

def main():    
    # Get parameters
    postgres_host = sys.argv[1]
    postgres_port = sys.argv[2]
    postgres_db = sys.argv[3]
    postgres_user = sys.argv[4]
    postgres_password = sys.argv[5]
    
    # Create Spark session
    spark = create_spark_session()
    
    # Get PostgreSQL configuration
    postgres_url, postgres_properties = get_postgres_config(
        postgres_host, postgres_port, postgres_db, 
        postgres_user, postgres_password
    )
    
    try:
        # Load dimensions with SCD Type 2
        customer_count = load_dim_customer(spark, postgres_url, postgres_properties)
        driver_count = load_dim_driver(spark, postgres_url, postgres_properties)
        vehicle_count = load_dim_vehicle(spark, postgres_url, postgres_properties)
        route_count = load_dim_route(spark, postgres_url, postgres_properties)
        package_count = load_dim_package(spark, postgres_url, postgres_properties)
        warehouse_count = load_dim_warehouse(spark, postgres_url, postgres_properties)
        date_count = load_dim_date(spark, postgres_url, postgres_properties)
        
        # Load fact table
        fact_count = load_fact_delivery(spark, postgres_url, postgres_properties)
        
        # Summary
        print("\n" + "="*70)
        print("ETL SUMMARY")
        print("="*70)
        print(f"Dimensions loaded:")
        print(f"  - Customers:  {customer_count:,}")
        print(f"  - Drivers:    {driver_count:,}")
        print(f"  - Vehicles:   {vehicle_count:,}")
        print(f"  - Routes:     {route_count:,}")
        print(f"  - Packages:   {package_count:,}")
        print(f"  - Warehouses: {warehouse_count:,}")
        print(f"  - Dates:      {date_count:,}")
        print(f"\nFact table loaded:")
        print(f"  - Deliveries: {fact_count:,}")
        print(f"\nEnd Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70 + "\n")
        
        print("ETL completed successfully!")
        
    except Exception as e:
        print(f"\nETL failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
