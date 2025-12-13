"""
Spark Job: Build Data Mart Tables
==================================
Builds aggregated data mart tables from data warehouse
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, 
    min as spark_min, round as spark_round, when, lit
)
import argparse

def create_spark_session(app_name="Build Data Mart"):
    """Create Spark session with Hive support"""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()

def build_delivery_performance(spark, hive_db):
    """Build dm_delivery_performance mart"""
    print("Building dm_delivery_performance...")
    
    # Read fact and dimensions
    fact = spark.table(f"{hive_db}.fact_delivery")
    dim_date = spark.table(f"{hive_db}.dim_date")
    dim_route = spark.table(f"{hive_db}.dim_route").filter(col("is_current") == True)
    dim_driver = spark.table(f"{hive_db}.dim_driver").filter(col("is_current") == True)
    
    # Join and aggregate
    dm = fact \
        .join(dim_date, fact.date_key == dim_date.date_key) \
        .join(dim_route, fact.route_key == dim_route.route_key) \
        .join(dim_driver, fact.driver_key == dim_driver.driver_key) \
        .groupBy(
            dim_date.full_date.alias("delivery_date"),
            dim_date.year,
            dim_date.month,
            dim_date.day,
            dim_date.day_name,
            dim_route.origin_city,
            dim_route.destination_city,
            fact.delivery_status
        ) \
        .agg(
            count("*").alias("total_deliveries"),
            count(when(col("delivery_status") == "Delivered", 1)).alias("successful_deliveries"),
            count(when(col("delivery_status") == "Failed", 1)).alias("failed_deliveries"),
            count(when(col("delivery_status") == "Cancelled", 1)).alias("cancelled_deliveries"),
            spark_round(avg("actual_distance_km"), 2).alias("avg_distance_km"),
            spark_round(avg("delivery_fee"), 2).alias("avg_delivery_fee"),
            spark_round(avg("tip_amount"), 2).alias("avg_tip_amount"),
            spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
            spark_round(avg("customer_rating"), 2).alias("avg_customer_rating"),
            spark_round(avg("driver_rating"), 2).alias("avg_driver_rating"),
            spark_sum("total_amount").alias("total_revenue")
        ) \
        .withColumn("success_rate", 
                    spark_round(col("successful_deliveries") / col("total_deliveries") * 100, 2))
    
    # Write to Hive data mart
    dm.write.mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.dm_delivery_performance")
    
    print(f"Built dm_delivery_performance with {dm.count():,} records")
    return dm.count()

def build_driver_performance(spark, hive_db):
    """Build dm_driver_performance mart"""
    print("Building dm_driver_performance...")
    
    # Read fact and dimensions
    fact = spark.table(f"{hive_db}.fact_delivery")
    dim_driver = spark.table(f"{hive_db}.dim_driver").filter(col("is_current") == True)
    dim_date = spark.table(f"{hive_db}.dim_date")
    
    # Join and aggregate
    dm = fact \
        .join(dim_driver, fact.driver_key == dim_driver.driver_key) \
        .join(dim_date, fact.date_key == dim_date.date_key) \
        .groupBy(
            dim_driver.driver_id,
            dim_driver.name.alias("driver_name"),
            dim_driver.license_number,
            dim_date.year,
            dim_date.month
        ) \
        .agg(
            count("*").alias("total_deliveries"),
            count(when(col("delivery_status") == "Delivered", 1)).alias("successful_deliveries"),
            spark_sum("actual_distance_km").alias("total_distance_km"),
            spark_sum("total_amount").alias("total_revenue_generated"),
            spark_sum("tip_amount").alias("total_tips_earned"),
            spark_round(avg("customer_rating"), 2).alias("avg_customer_rating"),
            spark_round(avg("driver_rating"), 2).alias("avg_driver_rating"),
            spark_max("driver_rating").alias("max_driver_rating"),
            spark_min("driver_rating").alias("min_driver_rating")
        ) \
        .withColumn("success_rate", 
                    spark_round(col("successful_deliveries") / col("total_deliveries") * 100, 2)) \
        .withColumn("avg_revenue_per_delivery",
                    spark_round(col("total_revenue_generated") / col("total_deliveries"), 2))
    
    # Write to Hive data mart
    dm.write.mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.dm_driver_performance")
    
    print(f"Built dm_driver_performance with {dm.count():,} records")
    return dm.count()

def build_customer_insights(spark, hive_db):
    """Build dm_customer_insights mart"""
    print("Building dm_customer_insights...")
    
    # Read fact and dimensions
    fact = spark.table(f"{hive_db}.fact_delivery")
    dim_customer = spark.table(f"{hive_db}.dim_customer").filter(col("is_current") == True)
    dim_date = spark.table(f"{hive_db}.dim_date")
    
    # Join and aggregate
    dm = fact \
        .join(dim_customer, fact.customer_key == dim_customer.customer_key) \
        .join(dim_date, fact.date_key == dim_date.date_key) \
        .groupBy(
            dim_customer.customer_id,
            dim_customer.name.alias("customer_name"),
            dim_customer.city,
            dim_customer.state,
            dim_date.year,
            dim_date.month
        ) \
        .agg(
            count("*").alias("total_orders"),
            count(when(col("delivery_status") == "Delivered", 1)).alias("successful_deliveries"),
            count(when(col("delivery_status") == "Failed", 1)).alias("failed_deliveries"),
            spark_sum("total_amount").alias("total_spent"),
            spark_sum("tip_amount").alias("total_tips_given"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
            spark_round(avg("customer_rating"), 2).alias("avg_rating_given"),
            spark_max(dim_date.full_date).alias("last_order_date"),
            count(when(col("priority_level") == "Express", 1)).alias("express_orders"),
            count(when(col("priority_level") == "Standard", 1)).alias("standard_orders")
        ) \
        .withColumn("success_rate",
                    spark_round(col("successful_deliveries") / col("total_orders") * 100, 2)) \
        .withColumn("avg_tip_per_order",
                    spark_round(col("total_tips_given") / col("total_orders"), 2))
    
    # Write to Hive data mart
    dm.write.mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{hive_db}.dm_customer_insights")
    
    print(f"Built dm_customer_insights with {dm.count():,} records")
    return dm.count()

def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Build data mart tables')
    parser.add_argument('--hive-db', required=True, help='Hive database name')
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("Build Data Marts")
    
    try:
        # Build all data marts
        delivery_count = build_delivery_performance(spark, args.hive_db)
        driver_count = build_driver_performance(spark, args.hive_db)
        customer_count = build_customer_insights(spark, args.hive_db)
        
        print("\n" + "="*70)
        print("DATA MART BUILD COMPLETED")
        print("="*70)
        print(f"dm_delivery_performance:  {delivery_count:>10,} records")
        print(f"dm_driver_performance:    {driver_count:>10,} records")
        print(f"dm_customer_insights:     {customer_count:>10,} records")
        print("="*70)
        
        # Show sample from each mart
        print("\nSample from dm_delivery_performance:")
        spark.table(f"{args.hive_db}.dm_delivery_performance").show(5, False)
        
        print("\nSample from dm_driver_performance:")
        spark.table(f"{args.hive_db}.dm_driver_performance").show(5, False)
        
        print("\nSample from dm_customer_insights:")
        spark.table(f"{args.hive_db}.dm_customer_insights").show(5, False)
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
