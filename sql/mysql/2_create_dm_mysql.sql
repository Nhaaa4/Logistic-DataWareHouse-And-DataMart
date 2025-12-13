CREATE DATABASE IF NOT EXISTS logistics_dm
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE logistics_dm;

DROP TABLE IF EXISTS dm_delivery_performance;

CREATE TABLE dm_delivery_performance (
    region VARCHAR(50) NOT NULL,
    year INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    total_deliveries BIGINT NOT NULL DEFAULT 0,
    on_time_deliveries BIGINT NOT NULL DEFAULT 0,
    on_time_rate DECIMAL(5, 2) DEFAULT 0.00,
    avg_delivery_time DECIMAL(10, 2) DEFAULT 0.00,
    total_cost DECIMAL(15, 2) DEFAULT 0.00,
    PRIMARY KEY (region, year, month_name),
    INDEX idx_region (region),
    INDEX idx_year_month (year, month_name)
) ENGINE=InnoDB 
COMMENT='Delivery performance by region and time period';

DROP TABLE IF EXISTS dm_driver_performance;

CREATE TABLE dm_driver_performance (
    driver_name VARCHAR(100) NOT NULL,
    employment_type VARCHAR(20) DEFAULT NULL,
    base_city VARCHAR(50) DEFAULT NULL,
    total_deliveries BIGINT NOT NULL DEFAULT 0,
    avg_delivery_time DECIMAL(10, 2) DEFAULT 0.00,
    total_revenue DECIMAL(15, 2) DEFAULT 0.00,
    avg_driver_rating DECIMAL(3, 2) DEFAULT 0.00,
    PRIMARY KEY (driver_name, employment_type, base_city),
    INDEX idx_driver_name (driver_name),
    INDEX idx_employment_type (employment_type),
    INDEX idx_base_city (base_city)
) ENGINE=InnoDB 
COMMENT='Driver performance metrics and KPIs';

DROP TABLE IF EXISTS dm_customer_insights;

CREATE TABLE dm_customer_insights (
    customer_type VARCHAR(50) NOT NULL,
    loyalty_level VARCHAR(20) NOT NULL,
    province VARCHAR(50) NOT NULL,
    total_orders BIGINT NOT NULL DEFAULT 0,
    total_spent DECIMAL(15, 2) DEFAULT 0.00,
    avg_delay DECIMAL(10, 2) DEFAULT 0.00,
    PRIMARY KEY (customer_type, loyalty_level, province),
    INDEX idx_customer_type (customer_type),
    INDEX idx_loyalty_level (loyalty_level),
    INDEX idx_province (province)
) ENGINE=InnoDB 
COMMENT='Customer segmentation and spending patterns';