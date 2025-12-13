"""
Spark Job: Load Data Marts from Hive to MySQL
==============================================
Transfers data mart tables from Hive to MySQL for BI tool consumption
"""

from pyspark.sql import SparkSession
import argparse

def create_spark_session(app_name="Hive to MySQL"):
    """Create Spark session with Hive support"""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()

def load_table_to_mysql(spark, hive_db, mysql_config, table_name):
    """Load a single table from Hive to MySQL"""
    print(f"Loading {table_name} to MySQL...")
    
    # Read from Hive
    df = spark.table(f"{hive_db}.{table_name}")
    
    # Write to MySQL
    df.write \
        .format("jdbc") \
        .option("url", mysql_config['url']) \
        .option("dbtable", f"{mysql_config['database']}.{table_name}") \
        .option("user", mysql_config['user']) \
        .option("password", mysql_config['password']) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()
    
    record_count = df.count()
    print(f"Loaded {record_count:,} records to MySQL table: {table_name}")
    return record_count

def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Load data marts from Hive to MySQL')
    parser.add_argument('--hive-db', required=True, help='Hive database name')
    parser.add_argument('--mysql-host', required=True, help='MySQL host')
    parser.add_argument('--mysql-port', default='3306', help='MySQL port')
    parser.add_argument('--mysql-database', required=True, help='MySQL database name')
    parser.add_argument('--mysql-user', required=True, help='MySQL username')
    parser.add_argument('--mysql-password', required=True, help='MySQL password')
    args = parser.parse_args()
    
    # MySQL configuration
    mysql_config = {
        'url': f"jdbc:mysql://{args.mysql_host}:{args.mysql_port}",
        'database': args.mysql_database,
        'user': args.mysql_user,
        'password': args.mysql_password
    }
    
    # Create Spark session
    spark = create_spark_session("Load Data Marts to MySQL")
    
    try:
        # Data mart tables to transfer
        tables = [
            'dm_delivery_performance',
            'dm_driver_performance',
            'dm_customer_insights'
        ]
        
        results = {}
        for table in tables:
            count = load_table_to_mysql(spark, args.hive_db, mysql_config, table)
            results[table] = count
        
        print("\n" + "="*70)
        print("HIVE TO MYSQL TRANSFER COMPLETED")
        print("="*70)
        for table, count in results.items():
            print(f"{table:<30} {count:>10,} records")
        print("="*70)
        print(f"\nData marts available in MySQL: {mysql_config['database']}")
        print(f"Connect with: mysql -h {args.mysql_host} -u {args.mysql_user} -p {args.mysql_database}")
        print("="*70)
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
