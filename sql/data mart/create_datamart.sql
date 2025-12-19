-- ==========================================
-- DATA MART - LOGISTICS ANALYTICS
-- MySQL Implementation
-- ==========================================
-- Three focused data marts for business intelligence

-- Create Data Mart database
CREATE DATABASE IF NOT EXISTS logistics_datamart;
USE logistics_datamart;

-- ==========================================
-- DATA MART 1: CUSTOMER ANALYTICS
-- ==========================================
-- Purpose: Customer behavior, segmentation, and performance analysis

CREATE TABLE mart_customer_analytics (
    customer_key            BIGINT PRIMARY KEY,
    customer_id             VARCHAR(50) NOT NULL,
    customer_name           VARCHAR(200) NOT NULL,
    province                VARCHAR(100),
    country                 VARCHAR(100),
    customer_type           VARCHAR(50),
    loyalty_level           VARCHAR(50),
    
    -- Delivery metrics
    total_deliveries        INT DEFAULT 0,
    completed_deliveries    INT DEFAULT 0,
    failed_deliveries       INT DEFAULT 0,
    returned_deliveries     INT DEFAULT 0,
    on_time_deliveries      INT DEFAULT 0,
    damaged_deliveries      INT DEFAULT 0,
    
    -- Financial metrics
    total_spent             DECIMAL(18,2) DEFAULT 0,
    avg_order_value         DECIMAL(18,2),
    max_order_value         DECIMAL(18,2),
    min_order_value         DECIMAL(18,2),
    
    -- Behavioral metrics
    first_order_date        DATE,
    last_order_date         DATE,
    days_since_last_order   INT,
    avg_days_between_orders DECIMAL(10,2),
    preferred_payment       VARCHAR(50),
    
    -- Status
    is_active               BOOLEAN DEFAULT TRUE,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_customer_id (customer_id),
    INDEX idx_province (province),
    INDEX idx_customer_type (customer_type),
    INDEX idx_loyalty_level (loyalty_level),
    INDEX idx_last_order_date (last_order_date)
) ENGINE=InnoDB
COMMENT = 'Customer analytics and performance metrics';

-- ==========================================
-- DATA MART 2: OPERATIONAL PERFORMANCE
-- ==========================================
-- Purpose: Daily/monthly delivery operations, driver, vehicle, and route performance

-- Delivery Performance Summary (Time-based)
CREATE TABLE mart_delivery_performance (
    performance_id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_type               VARCHAR(20) NOT NULL,
    date_value              DATE NOT NULL,
    year                    INT NOT NULL,
    month                   INT,
    month_name              VARCHAR(20),
    quarter                 INT,
    day_of_week             VARCHAR(20),
    
    -- Volume metrics
    total_deliveries        INT DEFAULT 0,
    completed_deliveries    INT DEFAULT 0,
    pending_deliveries      INT DEFAULT 0,
    in_transit_deliveries   INT DEFAULT 0,
    failed_deliveries       INT DEFAULT 0,
    returned_deliveries     INT DEFAULT 0,
    
    -- Quality metrics
    on_time_deliveries      INT DEFAULT 0,
    late_deliveries         INT DEFAULT 0,
    damaged_deliveries      INT DEFAULT 0,
    on_time_percentage      DECIMAL(5,2),
    completion_rate         DECIMAL(5,2),
    damage_rate             DECIMAL(5,2),
    
    -- Operational metrics
    total_distance_km       DECIMAL(15,2) DEFAULT 0,
    total_fuel_used_liters  DECIMAL(15,2) DEFAULT 0,
    avg_delivery_time_min   DECIMAL(10,2),
    avg_fuel_efficiency     DECIMAL(10,2),
    
    -- Resource utilization
    active_drivers          INT DEFAULT 0,
    active_vehicles         INT DEFAULT 0,
    avg_deliveries_per_driver DECIMAL(10,2),
    avg_deliveries_per_vehicle DECIMAL(10,2),
    
    -- Financial metrics
    total_revenue           DECIMAL(18,2) DEFAULT 0,
    total_fuel_cost         DECIMAL(18,2) DEFAULT 0,
    total_toll_cost         DECIMAL(18,2) DEFAULT 0,
    total_insurance_cost    DECIMAL(18,2) DEFAULT 0,
    total_operating_cost    DECIMAL(18,2) DEFAULT 0,
    gross_profit            DECIMAL(18,2) DEFAULT 0,
    profit_margin_pct       DECIMAL(5,2),
    
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_date_type_value (date_type, date_value),
    INDEX idx_date_type (date_type),
    INDEX idx_date_value (date_value),
    INDEX idx_year_month (year, month),
    INDEX idx_quarter (quarter)
) ENGINE=InnoDB
COMMENT = 'Delivery performance metrics by day, month, and quarter';

-- Driver and Vehicle Performance
CREATE TABLE mart_resource_performance (
    resource_id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    resource_type           VARCHAR(20) NOT NULL,
    resource_key            BIGINT NOT NULL,
    resource_identifier     VARCHAR(50) NOT NULL,
    resource_name           VARCHAR(200),
    
    -- Classification
    category                VARCHAR(50),
    status                  VARCHAR(50),
    base_location           VARCHAR(100),
    
    -- Performance metrics
    total_deliveries        INT DEFAULT 0,
    completed_deliveries    INT DEFAULT 0,
    failed_deliveries       INT DEFAULT 0,
    on_time_deliveries      INT DEFAULT 0,
    on_time_percentage      DECIMAL(5,2),
    completion_rate         DECIMAL(5,2),
    
    -- Operational metrics
    total_distance_km       DECIMAL(15,2) DEFAULT 0,
    total_fuel_used_liters  DECIMAL(15,2) DEFAULT 0,
    avg_fuel_efficiency     DECIMAL(10,2),
    total_working_hours     DECIMAL(10,2),
    utilization_rate        DECIMAL(5,2),
    
    -- Financial metrics
    total_revenue           DECIMAL(18,2) DEFAULT 0,
    total_cost              DECIMAL(18,2) DEFAULT 0,
    profit_contribution     DECIMAL(18,2) DEFAULT 0,
    
    -- Quality metrics
    avg_rating              DECIMAL(3,2),
    damaged_deliveries      INT DEFAULT 0,
    customer_complaints     INT DEFAULT 0,
    
    -- Ranking
    performance_rank        INT,
    performance_tier        VARCHAR(20),
    
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_resource_type_key (resource_type, resource_key),
    INDEX idx_resource_type (resource_type),
    INDEX idx_resource_identifier (resource_identifier),
    INDEX idx_status (status),
    INDEX idx_performance_rank (performance_rank),
    INDEX idx_performance_tier (performance_tier)
) ENGINE=InnoDB
COMMENT = 'Unified performance metrics for drivers, vehicles, and routes';

-- ==========================================
-- DATA MART 3: FINANCIAL ANALYTICS
-- ==========================================
-- Purpose: Revenue, cost, profitability analysis by various dimensions

CREATE TABLE mart_financial_analytics (
    financial_id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    analysis_dimension      VARCHAR(50) NOT NULL,
    dimension_key           VARCHAR(100) NOT NULL,
    dimension_name          VARCHAR(200),
    
    -- Time period
    year                    INT NOT NULL,
    month                   INT,
    quarter                 INT,
    period_type             VARCHAR(20),
    
    -- Volume
    total_deliveries        INT DEFAULT 0,
    completed_deliveries    INT DEFAULT 0,
    
    -- Revenue breakdown
    total_revenue           DECIMAL(18,2) DEFAULT 0,
    base_revenue            DECIMAL(18,2) DEFAULT 0,
    additional_charges      DECIMAL(18,2) DEFAULT 0,
    revenue_per_delivery    DECIMAL(15,2),
    
    -- Cost breakdown
    total_cost              DECIMAL(18,2) DEFAULT 0,
    fuel_cost               DECIMAL(18,2) DEFAULT 0,
    toll_cost               DECIMAL(18,2) DEFAULT 0,
    insurance_cost          DECIMAL(18,2) DEFAULT 0,
    maintenance_cost        DECIMAL(18,2) DEFAULT 0,
    labor_cost              DECIMAL(18,2) DEFAULT 0,
    overhead_cost           DECIMAL(18,2) DEFAULT 0,
    cost_per_delivery       DECIMAL(15,2),
    
    -- Profitability
    gross_profit            DECIMAL(18,2) DEFAULT 0,
    gross_margin_pct        DECIMAL(5,2),
    operating_profit        DECIMAL(18,2) DEFAULT 0,
    operating_margin_pct    DECIMAL(5,2),
    ebitda                  DECIMAL(18,2) DEFAULT 0,
    ebitda_margin_pct       DECIMAL(5,2),
    
    -- Growth metrics
    revenue_growth_pct      DECIMAL(5,2),
    volume_growth_pct       DECIMAL(5,2),
    cost_growth_pct         DECIMAL(5,2),
    
    -- Market metrics
    market_share_pct        DECIMAL(5,2),
    customer_count          INT DEFAULT 0,
    avg_customer_value      DECIMAL(18,2),
    
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_dimension_period (analysis_dimension, dimension_key, year, month),
    INDEX idx_analysis_dimension (analysis_dimension),
    INDEX idx_dimension_key (dimension_key),
    INDEX idx_year_month (year, month),
    INDEX idx_quarter (quarter),
    INDEX idx_gross_margin (gross_margin_pct),
    INDEX idx_revenue (total_revenue)
) ENGINE=InnoDB
COMMENT = 'Financial analytics by province, route, customer type, and time period';

-- ==========================================
-- SUMMARY VIEWS FOR DASHBOARDS
-- ==========================================

-- Executive Dashboard
CREATE OR REPLACE VIEW v_executive_summary AS
SELECT 
    CURDATE() AS report_date,
    
    -- Today's metrics
    (SELECT SUM(total_deliveries) FROM mart_delivery_performance 
     WHERE date_type = 'DAILY' AND date_value = CURDATE()) AS today_deliveries,
    
    (SELECT SUM(total_revenue) FROM mart_delivery_performance 
     WHERE date_type = 'DAILY' AND date_value = CURDATE()) AS today_revenue,
    
    (SELECT AVG(on_time_percentage) FROM mart_delivery_performance 
     WHERE date_type = 'DAILY' AND date_value >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)) AS monthly_on_time_rate,
    
    -- Resource counts
    (SELECT COUNT(*) FROM mart_resource_performance 
     WHERE resource_type = 'DRIVER' AND status = 'Active') AS active_drivers,
    
    (SELECT COUNT(*) FROM mart_resource_performance 
     WHERE resource_type = 'VEHICLE' AND status = 'Available') AS available_vehicles,
    
    -- Customer metrics
    (SELECT COUNT(*) FROM mart_customer_analytics WHERE is_active = TRUE) AS active_customers,
    
    (SELECT COUNT(*) FROM mart_customer_analytics WHERE days_since_last_order > 90) AS inactive_customers;

-- Top Performers
CREATE OR REPLACE VIEW v_top_performers AS
SELECT 
    resource_type,
    resource_name,
    total_deliveries,
    on_time_percentage,
    total_revenue,
    avg_rating,
    performance_tier
FROM mart_resource_performance
WHERE status IN ('Active', 'Available')
ORDER BY total_deliveries DESC, on_time_percentage DESC
LIMIT 20;

-- Financial Summary
CREATE OR REPLACE VIEW v_financial_summary AS
SELECT 
    analysis_dimension,
    dimension_name,
    year,
    month,
    total_revenue,
    total_cost,
    gross_profit,
    gross_margin_pct,
    revenue_per_delivery
FROM mart_financial_analytics
WHERE period_type = 'MONTHLY'
  AND year = YEAR(CURDATE())
ORDER BY year DESC, month DESC, total_revenue DESC;

-- Comments
ALTER TABLE mart_customer_analytics COMMENT = 'Customer analytics and performance metrics';
ALTER TABLE mart_delivery_performance COMMENT = 'Delivery operations performance by time period';
ALTER TABLE mart_resource_performance COMMENT = 'Unified performance metrics for drivers, vehicles, and routes';
ALTER TABLE mart_financial_analytics COMMENT = 'Financial performance by dimension (province, route, customer type)';
