#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, count, sum, avg, min, max, when, datediff, current_date,
    round as spark_round, coalesce, concat_ws, to_date, quarter
)
import pyspark.sql.functions as F
import sys
from datetime import datetime


def create_spark_session(app_name="DWH to Data Mart ETL"):
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "/home/hadoop/logistic/jdbc/postgresql-42.6.0.jar,/home/hadoop/logistic/jdbc/mysql-connector-j-8.3.0.jar") \
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


def get_mysql_config(host, port, database, user, password):
    """Return MySQL connection configuration"""
    mysql_url = f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&allowPublicKeyRetrieval=true"
    mysql_properties = {
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    return mysql_url, mysql_properties


def read_from_dwh(spark, table_name, postgres_url, postgres_properties):
    """Read data from DWH table"""
    try:
        df = spark.read.jdbc(
            url=postgres_url,
            table=f"dwh.{table_name}",
            properties=postgres_properties
        )
        return df
    except Exception as e:
        print(f"Error reading {table_name}: {str(e)}")
        return None


def write_to_datamart(df, table_name, mysql_url, mysql_properties, mode="overwrite"):
    """Write data to MySQL data mart"""
    df.write.jdbc(
        url=mysql_url,
        table=table_name,
        mode=mode,
        properties=mysql_properties
    )


def load_mart_customer_analytics(spark, postgres_url, postgres_properties, mysql_url, mysql_properties):
    """Load customer analytics data mart"""
    print("\n" + "="*70)
    print("Loading mart_customer_analytics")
    print("="*70)

    # Read from DWH
    dim_customer = read_from_dwh(spark, "dim_customer", postgres_url, postgres_properties)
    fact_delivery = read_from_dwh(spark, "fact_delivery", postgres_url, postgres_properties)

    if dim_customer is None or fact_delivery is None:
        print("Missing required tables")
        return 0

    # Get current customers only
    current_customers = dim_customer.filter(col("is_current") == True)

    # Aggregate delivery metrics per customer
    customer_metrics = fact_delivery.groupBy("customer_key").agg(
        count("*").alias("total_deliveries"),
        sum(when(col("delivery_status") == "Delivered", 1).otherwise(0)).alias("completed_deliveries"),
        sum(when(col("delivery_status") == "Failed", 1).otherwise(0)).alias("failed_deliveries"),
        sum(when(col("returned_flag") == True, 1).otherwise(0)).alias("returned_deliveries"),
        sum(when(col("on_time_flag") == True, 1).otherwise(0)).alias("on_time_deliveries"),
        sum(when(col("damaged_flag") == True, 1).otherwise(0)).alias("damaged_deliveries"),
        sum("total_delivery_cost").alias("total_spent"),
        avg("total_delivery_cost").alias("avg_order_value"),
        max("total_delivery_cost").alias("max_order_value"),
        min("total_delivery_cost").alias("min_order_value"),
        min(to_date("delivery_time")).alias("first_order_date"),
        max(to_date("delivery_time")).alias("last_order_date")
    )

    # Get preferred payment method using groupBy and max count approach
    payment_counts = fact_delivery.groupBy("customer_key", "payment_method") \
        .agg(count("*").alias("payment_count"))

    # Get max payment count per customer
    max_payment_per_customer = payment_counts.groupBy("customer_key") \
        .agg(max("payment_count").alias("max_payment_count"))

    # Join to get preferred payment (first one with max count)
    preferred_payment = payment_counts.alias("pc") \
        .join(max_payment_per_customer.alias("mpc"),
              (col("pc.customer_key") == col("mpc.customer_key")) &
              (col("pc.payment_count") == col("mpc.max_payment_count")),
              "inner") \
        .select(col("pc.customer_key"), col("pc.payment_method").alias("preferred_payment")) \
        .dropDuplicates(["customer_key"])

    # Join all together
    mart_df = current_customers.alias("c") \
        .join(customer_metrics.alias("m"), col("c.customer_key") == col("m.customer_key"), "left") \
        .join(preferred_payment.alias("p"), col("c.customer_key") == col("p.customer_key"), "left") \
        .select(
            col("c.customer_key"),
            col("c.customer_id"),
            col("c.customer_name"),
            col("c.province"),
            col("c.country"),
            col("c.customer_type"),
            col("c.loyalty_level"),
            coalesce(col("m.total_deliveries"), lit(0)).alias("total_deliveries"),
            coalesce(col("m.completed_deliveries"), lit(0)).alias("completed_deliveries"),
            coalesce(col("m.failed_deliveries"), lit(0)).alias("failed_deliveries"),
            coalesce(col("m.returned_deliveries"), lit(0)).alias("returned_deliveries"),
            coalesce(col("m.on_time_deliveries"), lit(0)).alias("on_time_deliveries"),
            coalesce(col("m.damaged_deliveries"), lit(0)).alias("damaged_deliveries"),
            coalesce(col("m.total_spent"), lit(0)).alias("total_spent"),
            col("m.avg_order_value"),
            col("m.max_order_value"),
            col("m.min_order_value"),
            col("m.first_order_date"),
            col("m.last_order_date"),
            datediff(current_date(), col("m.last_order_date")).alias("days_since_last_order"),
            col("p.preferred_payment"),
            col("c.is_active"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at")
        )

    # Calculate avg_days_between_orders
    mart_df = mart_df.withColumn(
        "avg_days_between_orders",
        when(col("total_deliveries") > 1,
             datediff(col("last_order_date"), col("first_order_date")) / (col("total_deliveries") - 1)
        ).otherwise(None)
    )

    # Write to MySQL
    write_to_datamart(mart_df, "mart_customer_analytics", mysql_url, mysql_properties)

    count_result = mart_df.count()
    print(f"Loaded {count_result} records into mart_customer_analytics")
    return count_result


def load_mart_delivery_performance(spark, postgres_url, postgres_properties, mysql_url, mysql_properties):
    """Load delivery performance data mart (daily aggregations)"""
    print("\n" + "="*70)
    print("Loading mart_delivery_performance")
    print("="*70)

    # Read from DWH
    fact_delivery = read_from_dwh(spark, "fact_delivery", postgres_url, postgres_properties)
    dim_date = read_from_dwh(spark, "dim_date", postgres_url, postgres_properties)

    if fact_delivery is None or dim_date is None:
        print("Missing required tables")
        return 0

    # Join fact with date dimension
    delivery_with_date = fact_delivery.alias("f") \
        .join(dim_date.alias("d"), col("f.date_key") == col("d.date_key"), "left")

    # Daily aggregation
    daily_metrics = delivery_with_date.groupBy(
        col("d.full_date").alias("date_value"),
        col("d.year"),
        col("d.month"),
        col("d.month_name"),
        col("d.quarter"),
        col("d.day_name").alias("day_of_week")
    ).agg(
        count("*").alias("total_deliveries"),
        sum(when(col("f.delivery_status") == "Delivered", 1).otherwise(0)).alias("completed_deliveries"),
        sum(when(col("f.delivery_status") == "Pending", 1).otherwise(0)).alias("pending_deliveries"),
        sum(when(col("f.delivery_status") == "In Transit", 1).otherwise(0)).alias("in_transit_deliveries"),
        sum(when(col("f.delivery_status") == "Failed", 1).otherwise(0)).alias("failed_deliveries"),
        sum(when(col("f.returned_flag") == True, 1).otherwise(0)).alias("returned_deliveries"),
        sum(when(col("f.on_time_flag") == True, 1).otherwise(0)).alias("on_time_deliveries"),
        sum(when(col("f.on_time_flag") == False, 1).otherwise(0)).alias("late_deliveries"),
        sum(when(col("f.damaged_flag") == True, 1).otherwise(0)).alias("damaged_deliveries"),
        sum("f.distance_km").alias("total_distance_km"),
        sum("f.fuel_used_liters").alias("total_fuel_used_liters"),
        avg("f.delivery_duration_min").alias("avg_delivery_time_min"),
        F.countDistinct("f.driver_key").alias("active_drivers"),
        F.countDistinct("f.vehicle_key").alias("active_vehicles"),
        sum("f.total_delivery_cost").alias("total_revenue"),
        sum("f.fuel_cost").alias("total_fuel_cost"),
        sum("f.toll_cost").alias("total_toll_cost"),
        sum("f.insurance_cost").alias("total_insurance_cost")
    ).withColumn("date_type", lit("DAILY"))

    # Calculate derived metrics
    daily_metrics = daily_metrics \
        .withColumn("on_time_percentage",
            spark_round(col("on_time_deliveries") * 100.0 / col("total_deliveries"), 2)) \
        .withColumn("completion_rate",
            spark_round(col("completed_deliveries") * 100.0 / col("total_deliveries"), 2)) \
        .withColumn("damage_rate",
            spark_round(col("damaged_deliveries") * 100.0 / col("total_deliveries"), 2)) \
        .withColumn("avg_fuel_efficiency",
            when(col("total_fuel_used_liters") > 0,
                 spark_round(col("total_distance_km") / col("total_fuel_used_liters"), 2)
            ).otherwise(None)) \
        .withColumn("avg_deliveries_per_driver",
            when(col("active_drivers") > 0,
                 spark_round(col("total_deliveries").cast("double") / col("active_drivers"), 2)
            ).otherwise(None)) \
        .withColumn("avg_deliveries_per_vehicle",
            when(col("active_vehicles") > 0,
                 spark_round(col("total_deliveries").cast("double") / col("active_vehicles"), 2)
            ).otherwise(None)) \
        .withColumn("total_operating_cost",
            col("total_fuel_cost") + col("total_toll_cost") + col("total_insurance_cost")) \
        .withColumn("gross_profit",
            col("total_revenue") - (col("total_fuel_cost") + col("total_toll_cost") + col("total_insurance_cost"))) \
        .withColumn("profit_margin_pct",
            when(col("total_revenue") > 0,
                 spark_round((col("total_revenue") - (col("total_fuel_cost") + col("total_toll_cost") + col("total_insurance_cost"))) * 100.0 / col("total_revenue"), 2)
            ).otherwise(None)) \
        .withColumn("created_at", F.current_timestamp()) \
        .withColumn("updated_at", F.current_timestamp())

    # Select final columns
    final_daily = daily_metrics.select(
        "date_type", "date_value", "year", "month", "month_name", "quarter", "day_of_week",
        "total_deliveries", "completed_deliveries", "pending_deliveries", "in_transit_deliveries",
        "failed_deliveries", "returned_deliveries", "on_time_deliveries", "late_deliveries",
        "damaged_deliveries", "on_time_percentage", "completion_rate", "damage_rate",
        "total_distance_km", "total_fuel_used_liters", "avg_delivery_time_min", "avg_fuel_efficiency",
        "active_drivers", "active_vehicles", "avg_deliveries_per_driver", "avg_deliveries_per_vehicle",
        "total_revenue", "total_fuel_cost", "total_toll_cost", "total_insurance_cost",
        "total_operating_cost", "gross_profit", "profit_margin_pct", "created_at", "updated_at"
    )

    # Write to MySQL
    write_to_datamart(final_daily, "mart_delivery_performance", mysql_url, mysql_properties)

    count_result = final_daily.count()
    print(f"Loaded {count_result} daily records into mart_delivery_performance")
    return count_result


def load_mart_resource_performance(spark, postgres_url, postgres_properties, mysql_url, mysql_properties):
    """Load resource (driver/vehicle) performance data mart"""
    print("\n" + "="*70)
    print("Loading mart_resource_performance")
    print("="*70)

    # Read from DWH
    dim_driver = read_from_dwh(spark, "dim_driver", postgres_url, postgres_properties)
    dim_vehicle = read_from_dwh(spark, "dim_vehicle", postgres_url, postgres_properties)
    fact_delivery = read_from_dwh(spark, "fact_delivery", postgres_url, postgres_properties)

    if dim_driver is None or dim_vehicle is None or fact_delivery is None:
        print("Missing required tables")
        return 0

    # Driver performance
    driver_metrics = fact_delivery.groupBy("driver_key").agg(
        count("*").alias("total_deliveries"),
        sum(when(col("delivery_status") == "Delivered", 1).otherwise(0)).alias("completed_deliveries"),
        sum(when(col("delivery_status") == "Failed", 1).otherwise(0)).alias("failed_deliveries"),
        sum(when(col("on_time_flag") == True, 1).otherwise(0)).alias("on_time_deliveries"),
        sum(when(col("damaged_flag") == True, 1).otherwise(0)).alias("damaged_deliveries"),
        sum("distance_km").alias("total_distance_km"),
        sum("fuel_used_liters").alias("total_fuel_used_liters"),
        sum("total_delivery_cost").alias("total_revenue"),
        sum(col("fuel_cost") + col("toll_cost") + col("insurance_cost")).alias("total_cost")
    )

    current_drivers = dim_driver.filter(col("is_current") == True)

    driver_perf = current_drivers.alias("d") \
        .join(driver_metrics.alias("m"), col("d.driver_key") == col("m.driver_key"), "left") \
        .select(
            lit("DRIVER").alias("resource_type"),
            col("d.driver_key").alias("resource_key"),
            col("d.driver_id").alias("resource_identifier"),
            col("d.driver_name").alias("resource_name"),
            col("d.license_type").alias("category"),
            col("d.status"),
            col("d.base_city").alias("base_location"),
            coalesce(col("m.total_deliveries"), lit(0)).alias("total_deliveries"),
            coalesce(col("m.completed_deliveries"), lit(0)).alias("completed_deliveries"),
            coalesce(col("m.failed_deliveries"), lit(0)).alias("failed_deliveries"),
            coalesce(col("m.on_time_deliveries"), lit(0)).alias("on_time_deliveries"),
            coalesce(col("m.damaged_deliveries"), lit(0)).alias("damaged_deliveries"),
            coalesce(col("m.total_distance_km"), lit(0)).alias("total_distance_km"),
            coalesce(col("m.total_fuel_used_liters"), lit(0)).alias("total_fuel_used_liters"),
            coalesce(col("m.total_revenue"), lit(0)).alias("total_revenue"),
            coalesce(col("m.total_cost"), lit(0)).alias("total_cost"),
            col("d.rating").alias("avg_rating")
        )

    # Vehicle performance
    vehicle_metrics = fact_delivery.groupBy("vehicle_key").agg(
        count("*").alias("total_deliveries"),
        sum(when(col("delivery_status") == "Delivered", 1).otherwise(0)).alias("completed_deliveries"),
        sum(when(col("delivery_status") == "Failed", 1).otherwise(0)).alias("failed_deliveries"),
        sum(when(col("on_time_flag") == True, 1).otherwise(0)).alias("on_time_deliveries"),
        sum(when(col("damaged_flag") == True, 1).otherwise(0)).alias("damaged_deliveries"),
        sum("distance_km").alias("total_distance_km"),
        sum("fuel_used_liters").alias("total_fuel_used_liters"),
        sum("total_delivery_cost").alias("total_revenue"),
        sum(col("fuel_cost") + col("toll_cost") + col("insurance_cost")).alias("total_cost")
    )

    current_vehicles = dim_vehicle.filter(col("is_current") == True)

    vehicle_perf = current_vehicles.alias("v") \
        .join(vehicle_metrics.alias("m"), col("v.vehicle_key") == col("m.vehicle_key"), "left") \
        .select(
            lit("VEHICLE").alias("resource_type"),
            col("v.vehicle_key").alias("resource_key"),
            col("v.vehicle_id").alias("resource_identifier"),
            concat_ws(" ", col("v.brand"), col("v.model")).alias("resource_name"),
            col("v.vehicle_type").alias("category"),
            col("v.status"),
            lit(None).cast("string").alias("base_location"),
            coalesce(col("m.total_deliveries"), lit(0)).alias("total_deliveries"),
            coalesce(col("m.completed_deliveries"), lit(0)).alias("completed_deliveries"),
            coalesce(col("m.failed_deliveries"), lit(0)).alias("failed_deliveries"),
            coalesce(col("m.on_time_deliveries"), lit(0)).alias("on_time_deliveries"),
            coalesce(col("m.damaged_deliveries"), lit(0)).alias("damaged_deliveries"),
            coalesce(col("m.total_distance_km"), lit(0)).alias("total_distance_km"),
            coalesce(col("m.total_fuel_used_liters"), lit(0)).alias("total_fuel_used_liters"),
            coalesce(col("m.total_revenue"), lit(0)).alias("total_revenue"),
            coalesce(col("m.total_cost"), lit(0)).alias("total_cost"),
            lit(None).cast("decimal(3,2)").alias("avg_rating")
        )

    # Union driver and vehicle performance
    all_resources = driver_perf.unionByName(vehicle_perf)

    # Calculate derived metrics (without window functions)
    all_resources = all_resources \
        .withColumn("on_time_percentage",
            when(col("total_deliveries") > 0,
                 spark_round(col("on_time_deliveries") * 100.0 / col("total_deliveries"), 2)
            ).otherwise(None)) \
        .withColumn("completion_rate",
            when(col("total_deliveries") > 0,
                 spark_round(col("completed_deliveries") * 100.0 / col("total_deliveries"), 2)
            ).otherwise(None)) \
        .withColumn("avg_fuel_efficiency",
            when(col("total_fuel_used_liters") > 0,
                 spark_round(col("total_distance_km") / col("total_fuel_used_liters"), 2)
            ).otherwise(None)) \
        .withColumn("profit_contribution",
            col("total_revenue") - col("total_cost")) \
        .withColumn("total_working_hours", lit(None).cast("decimal(10,2)")) \
        .withColumn("utilization_rate", lit(None).cast("decimal(5,2)")) \
        .withColumn("customer_complaints", lit(0)) \
        .withColumn("performance_rank", lit(None).cast("int")) \
        .withColumn("performance_tier",
            when(col("total_deliveries") >= 100, "High Performer")
            .when(col("total_deliveries") >= 50, "Medium Performer")
            .when(col("total_deliveries") >= 1, "Low Performer")
            .otherwise("No Activity")) \
        .withColumn("created_at", F.current_timestamp()) \
        .withColumn("updated_at", F.current_timestamp())

    # Write to MySQL
    write_to_datamart(all_resources, "mart_resource_performance", mysql_url, mysql_properties)

    count_result = all_resources.count()
    print(f"Loaded {count_result} records into mart_resource_performance")
    return count_result


def load_mart_financial_analytics(spark, postgres_url, postgres_properties, mysql_url, mysql_properties):
    """Load financial analytics data mart"""
    print("\n" + "="*70)
    print("Loading mart_financial_analytics")
    print("="*70)

    # Read from DWH
    fact_delivery = read_from_dwh(spark, "fact_delivery", postgres_url, postgres_properties)
    dim_customer = read_from_dwh(spark, "dim_customer", postgres_url, postgres_properties)
    dim_route = read_from_dwh(spark, "dim_route", postgres_url, postgres_properties)

    if fact_delivery is None:
        print("Missing required tables")
        return 0

    # Join with dimensions
    current_customers = dim_customer.filter(col("is_current") == True) if dim_customer else None
    current_routes = dim_route.filter(col("is_current") == True) if dim_route else None

    enriched_fact = fact_delivery.alias("f")
    if current_customers is not None:
        enriched_fact = enriched_fact.join(
            current_customers.select("customer_key", "province", "customer_type").alias("c"),
            col("f.customer_key") == col("c.customer_key"), "left"
        )
    if current_routes is not None:
        enriched_fact = enriched_fact.join(
            current_routes.select("route_key", "region", "origin_province", "destination_province").alias("r"),
            col("f.route_key") == col("r.route_key"), "left"
        )

    all_financials = None

    # By Province (Monthly)
    province_monthly = enriched_fact.groupBy(
        col("c.province").alias("dimension_key"),
        col("f.year"),
        col("f.month")
    ).agg(
        count("*").alias("total_deliveries"),
        sum(when(col("f.delivery_status") == "Delivered", 1).otherwise(0)).alias("completed_deliveries"),
        sum("f.total_delivery_cost").alias("total_revenue"),
        sum("f.base_cost").alias("base_revenue"),
        sum("f.fuel_cost").alias("fuel_cost"),
        sum("f.toll_cost").alias("toll_cost"),
        sum("f.insurance_cost").alias("insurance_cost"),
        F.countDistinct("f.customer_key").alias("customer_count")
    ).withColumn("analysis_dimension", lit("PROVINCE")) \
     .withColumn("dimension_name", col("dimension_key")) \
     .withColumn("quarter", quarter(concat_ws("-", col("year"), F.lpad(col("month"), 2, "0"), lit("01")).cast("date"))) \
     .withColumn("period_type", lit("MONTHLY"))

    all_financials = province_monthly

    # By Customer Type (Monthly)
    customer_type_monthly = enriched_fact.groupBy(
        col("c.customer_type").alias("dimension_key"),
        col("f.year"),
        col("f.month")
    ).agg(
        count("*").alias("total_deliveries"),
        sum(when(col("f.delivery_status") == "Delivered", 1).otherwise(0)).alias("completed_deliveries"),
        sum("f.total_delivery_cost").alias("total_revenue"),
        sum("f.base_cost").alias("base_revenue"),
        sum("f.fuel_cost").alias("fuel_cost"),
        sum("f.toll_cost").alias("toll_cost"),
        sum("f.insurance_cost").alias("insurance_cost"),
        F.countDistinct("f.customer_key").alias("customer_count")
    ).withColumn("analysis_dimension", lit("CUSTOMER_TYPE")) \
     .withColumn("dimension_name", col("dimension_key")) \
     .withColumn("quarter", quarter(concat_ws("-", col("year"), F.lpad(col("month"), 2, "0"), lit("01")).cast("date"))) \
     .withColumn("period_type", lit("MONTHLY"))

    all_financials = all_financials.unionByName(customer_type_monthly)

    # By Region (Monthly)
    if current_routes is not None:
        region_monthly = enriched_fact.groupBy(
            col("r.region").alias("dimension_key"),
            col("f.year"),
            col("f.month")
        ).agg(
            count("*").alias("total_deliveries"),
            sum(when(col("f.delivery_status") == "Delivered", 1).otherwise(0)).alias("completed_deliveries"),
            sum("f.total_delivery_cost").alias("total_revenue"),
            sum("f.base_cost").alias("base_revenue"),
            sum("f.fuel_cost").alias("fuel_cost"),
            sum("f.toll_cost").alias("toll_cost"),
            sum("f.insurance_cost").alias("insurance_cost"),
            F.countDistinct("f.customer_key").alias("customer_count")
        ).withColumn("analysis_dimension", lit("REGION")) \
         .withColumn("dimension_name", col("dimension_key")) \
         .withColumn("quarter", quarter(concat_ws("-", col("year"), F.lpad(col("month"), 2, "0"), lit("01")).cast("date"))) \
         .withColumn("period_type", lit("MONTHLY"))

        all_financials = all_financials.unionByName(region_monthly)

    # Filter out null dimension keys
    all_financials = all_financials.filter(col("dimension_key").isNotNull())

    # Calculate derived metrics
    all_financials = all_financials \
        .withColumn("additional_charges",
            col("total_revenue") - col("base_revenue")) \
        .withColumn("total_cost",
            col("fuel_cost") + col("toll_cost") + col("insurance_cost")) \
        .withColumn("maintenance_cost", lit(0).cast("decimal(18,2)")) \
        .withColumn("labor_cost", lit(0).cast("decimal(18,2)")) \
        .withColumn("overhead_cost", lit(0).cast("decimal(18,2)")) \
        .withColumn("revenue_per_delivery",
            when(col("total_deliveries") > 0,
                 spark_round(col("total_revenue") / col("total_deliveries"), 2)
            ).otherwise(None)) \
        .withColumn("cost_per_delivery",
            when(col("total_deliveries") > 0,
                 spark_round((col("fuel_cost") + col("toll_cost") + col("insurance_cost")) / col("total_deliveries"), 2)
            ).otherwise(None)) \
        .withColumn("gross_profit",
            col("total_revenue") - (col("fuel_cost") + col("toll_cost") + col("insurance_cost"))) \
        .withColumn("gross_margin_pct",
            when(col("total_revenue") > 0,
                 spark_round((col("total_revenue") - (col("fuel_cost") + col("toll_cost") + col("insurance_cost"))) * 100.0 / col("total_revenue"), 2)
            ).otherwise(None)) \
        .withColumn("operating_profit",
            col("total_revenue") - (col("fuel_cost") + col("toll_cost") + col("insurance_cost"))) \
        .withColumn("operating_margin_pct",
            when(col("total_revenue") > 0,
                 spark_round((col("total_revenue") - (col("fuel_cost") + col("toll_cost") + col("insurance_cost"))) * 100.0 / col("total_revenue"), 2)
            ).otherwise(None)) \
        .withColumn("ebitda",
            col("total_revenue") - (col("fuel_cost") + col("toll_cost") + col("insurance_cost"))) \
        .withColumn("ebitda_margin_pct",
            when(col("total_revenue") > 0,
                 spark_round((col("total_revenue") - (col("fuel_cost") + col("toll_cost") + col("insurance_cost"))) * 100.0 / col("total_revenue"), 2)
            ).otherwise(None)) \
        .withColumn("revenue_growth_pct", lit(None).cast("decimal(5,2)")) \
        .withColumn("volume_growth_pct", lit(None).cast("decimal(5,2)")) \
        .withColumn("cost_growth_pct", lit(None).cast("decimal(5,2)")) \
        .withColumn("market_share_pct", lit(None).cast("decimal(5,2)")) \
        .withColumn("avg_customer_value",
            when(col("customer_count") > 0,
                 spark_round(col("total_revenue") / col("customer_count"), 2)
            ).otherwise(None)) \
        .withColumn("created_at", F.current_timestamp()) \
        .withColumn("updated_at", F.current_timestamp())

    # Select final columns
    final_financials = all_financials.select(
        "analysis_dimension", "dimension_key", "dimension_name",
        "year", "month", "quarter", "period_type",
        "total_deliveries", "completed_deliveries",
        "total_revenue", "base_revenue", "additional_charges", "revenue_per_delivery",
        "total_cost", "fuel_cost", "toll_cost", "insurance_cost",
        "maintenance_cost", "labor_cost", "overhead_cost", "cost_per_delivery",
        "gross_profit", "gross_margin_pct", "operating_profit", "operating_margin_pct",
        "ebitda", "ebitda_margin_pct",
        "revenue_growth_pct", "volume_growth_pct", "cost_growth_pct",
        "market_share_pct", "customer_count", "avg_customer_value",
        "created_at", "updated_at"
    )

    # Write to MySQL
    write_to_datamart(final_financials, "mart_financial_analytics", mysql_url, mysql_properties)

    count_result = final_financials.count()
    print(f"Loaded {count_result} records into mart_financial_analytics")
    return count_result


def main():
    if len(sys.argv) != 11:
        print("Usage: spark_dwh_to_datamart.py <pg_host> <pg_port> <pg_db> <pg_user> <pg_password> <mysql_host> <mysql_port> <mysql_db> <mysql_user> <mysql_password>")
        sys.exit(1)

    # Get parameters
    pg_host = sys.argv[1]
    pg_port = sys.argv[2]
    pg_db = sys.argv[3]
    pg_user = sys.argv[4]
    pg_password = sys.argv[5]
    mysql_host = sys.argv[6]
    mysql_port = sys.argv[7]
    mysql_db = sys.argv[8]
    mysql_user = sys.argv[9]
    mysql_password = sys.argv[10]

    print("\n" + "="*70)
    print("DWH TO DATA MART ETL")
    print("="*70)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"PostgreSQL DWH: {pg_host}:{pg_port}/{pg_db}")
    print(f"MySQL Data Mart: {mysql_host}:{mysql_port}/{mysql_db}")
    print("="*70 + "\n")

    # Create Spark session
    spark = create_spark_session()

    # Get connection configurations
    postgres_url, postgres_properties = get_postgres_config(pg_host, pg_port, pg_db, pg_user, pg_password)
    mysql_url, mysql_properties = get_mysql_config(mysql_host, mysql_port, mysql_db, mysql_user, mysql_password)

    try:
        # Load data marts
        customer_count = load_mart_customer_analytics(spark, postgres_url, postgres_properties, mysql_url, mysql_properties)
        delivery_count = load_mart_delivery_performance(spark, postgres_url, postgres_properties, mysql_url, mysql_properties)
        resource_count = load_mart_resource_performance(spark, postgres_url, postgres_properties, mysql_url, mysql_properties)
        financial_count = load_mart_financial_analytics(spark, postgres_url, postgres_properties, mysql_url, mysql_properties)

        # Summary
        print("\n" + "="*70)
        print("ETL SUMMARY")
        print("="*70)
        print(f"Data marts loaded:")
        print(f"  - Customer Analytics:     {customer_count:,}")
        print(f"  - Delivery Performance:   {delivery_count:,}")
        print(f"  - Resource Performance:   {resource_count:,}")
        print(f"  - Financial Analytics:    {financial_count:,}")
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