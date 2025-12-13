# MySQL JDBC Connector Setup

To enable Spark to MySQL connectivity, download the MySQL JDBC connector:

## Download

1. Visit: https://dev.mysql.com/downloads/connector/j/
2. Download MySQL Connector/J (JDBC driver)
3. Extract the JAR file: `mysql-connector-java-X.X.XX.jar`

## Installation Options

### Option 1: Add to Spark JARs directory
```bash
cp mysql-connector-java-*.jar $SPARK_HOME/jars/
```

### Option 2: Specify in spark-submit
```bash
spark-submit --jars /path/to/mysql-connector-java.jar script.py
```

### Option 3: Maven coordinates (Airflow DAG)
Update the DAG configuration:
```python
conf={
    'spark.jars.packages': 'mysql:mysql-connector-java:8.0.33',
}
```

## Update DAG Configuration

In `dags/ingestion_dag.py`, update the `load_to_mysql` task:

1. **Replace JAR path** with actual location:
   ```python
   'spark.jars': '/path/to/mysql-connector-java.jar',
   ```

2. **Update MySQL credentials** using Airflow Variables:
   ```python
   from airflow.models import Variable
   
   application_args=[
       '--mysql-host', Variable.get('MYSQL_HOST', 'localhost'),
       '--mysql-user', Variable.get('MYSQL_USER', 'root'),
       '--mysql-password', Variable.get('MYSQL_PASSWORD'),
   ]
   ```

3. **Or use Airflow Connection**:
   ```python
   from airflow.hooks.base import BaseHook
   
   mysql_conn = BaseHook.get_connection('mysql_default')
   ```

## MySQL Database Setup

Create the MySQL database and tables:

```bash
# Create database
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS logistics_dm;"

# Run schema
mysql -u root -p logistics_dm < sql/mysql/2_create_dm_mysql.sql
```

## Test Connection

Test Spark to MySQL connection:

```bash
spark-submit \
  --jars /path/to/mysql-connector-java.jar \
  etl/spark_hive_to_mysql.py \
  --hive-db logistics_dw \
  --mysql-host localhost \
  --mysql-port 3306 \
  --mysql-database logistics_dm \
  --mysql-user root \
  --mysql-password yourpassword
```
