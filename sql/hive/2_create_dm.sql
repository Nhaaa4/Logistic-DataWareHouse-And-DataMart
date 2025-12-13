CREATE DATABASE IF NOT EXISTS logistics_dm;
CREATE TABLE logistics_dm.dm_delivery_performance
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY')
AS
SELECT
    r.region,
    d.year,
    d.month_name,
    COUNT(f.delivery_id) AS total_deliveries,
    SUM(CASE WHEN f.on_time_flag THEN 1 ELSE 0 END) AS on_time_deliveries,
    ROUND(
        SUM(CASE WHEN f.on_time_flag THEN 1 ELSE 0 END) / COUNT(f.delivery_id) * 100,
        2
    ) AS on_time_rate,
    AVG((UNIX_TIMESTAMP(f.delivery_time) - UNIX_TIMESTAMP(f.pickup_time)) / 60) AS avg_delivery_time,
    SUM(f.total_delivery_cost) AS total_cost
FROM logistics_dw.fact_delivery f
JOIN logistics_dw.dim_route r ON f.route_key = r.route_key AND r.is_current = TRUE
JOIN logistics_dw.dim_date d ON f.date_key = d.date_key AND d.is_current = TRUE
GROUP BY r.region, d.year, d.month_name;
CREATE TABLE logistics_dm.dm_driver_performance
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY')
AS
SELECT
    d.driver_name,
    d.employment_type,
    d.base_city,
    COUNT(f.delivery_id) AS total_deliveries,
    AVG((UNIX_TIMESTAMP(f.delivery_time) - UNIX_TIMESTAMP(f.pickup_time)) / 60) AS avg_delivery_time,
    SUM(f.total_delivery_cost) AS total_revenue,
    AVG(d.rating) AS avg_driver_rating
FROM logistics_dw.fact_delivery f
JOIN logistics_dw.dim_driver d
ON f.driver_key = d.driver_key AND d.is_current = TRUE
GROUP BY d.driver_name, d.employment_type, d.base_city;
CREATE TABLE logistics_dm.dm_customer_insights
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY')
AS
SELECT
    c.customer_type,
    c.loyalty_level,
    c.province,
    COUNT(f.delivery_id) AS total_orders,
    SUM(f.total_delivery_cost) AS total_spent,
    AVG(CASE WHEN f.on_time_flag = FALSE 
        THEN (UNIX_TIMESTAMP(f.delivery_time) - UNIX_TIMESTAMP(f.arrival_time)) / 60 
        ELSE 0 END) AS avg_delay
FROM logistics_dw.fact_delivery f
JOIN logistics_dw.dim_customer c
ON f.customer_key = c.customer_key AND c.is_current = TRUE
GROUP BY c.customer_type, c.loyalty_level, c.province;
