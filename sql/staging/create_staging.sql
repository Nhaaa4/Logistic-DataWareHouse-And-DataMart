-- ==========================================
-- STAGING LAYER - LOGISTICS DATA WAREHOUSE
-- ==========================================

-- Create Staging schema
CREATE SCHEMA IF NOT EXISTS staging;

-- STAGING TABLES (TEMPORARY)

-- STG_CUSTOMER: Staging for customer data from CSV
CREATE TABLE staging.stg_customer (
    customer_id         VARCHAR(50),
    customer_name       VARCHAR(200),
    gender              VARCHAR(20),
    date_of_birth       DATE,
    phone               VARCHAR(50),
    email               VARCHAR(200),
    address             TEXT,
    commune             VARCHAR(100),
    district            VARCHAR(100),
    province            VARCHAR(100),
    country             VARCHAR(100),
    postal_code         VARCHAR(20),
    customer_type       VARCHAR(50),
    registration_date   DATE,
    loyalty_level       VARCHAR(50),
    preferred_contact   VARCHAR(50),
    is_active           BOOLEAN,
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(200)
);

-- STG_DRIVER: Staging for driver data from CSV
CREATE TABLE staging.stg_driver (
    driver_id           VARCHAR(50),
    driver_name         VARCHAR(200),
    gender              VARCHAR(20),
    date_of_birth       DATE,
    phone               VARCHAR(50),
    license_number      VARCHAR(50),
    license_type        VARCHAR(20),
    license_expiry      DATE,
    hire_date           DATE,
    experience_years    INTEGER,
    emergency_contact   VARCHAR(200),
    employment_type     VARCHAR(50),
    rating              DECIMAL(3,2),
    status              VARCHAR(50),
    base_city           VARCHAR(100),
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(200)
);

-- STG_VEHICLE: Staging for vehicle data from JSON API
CREATE TABLE staging.stg_vehicle (
    vehicle_id          VARCHAR(50),
    plate_number        VARCHAR(50),
    vehicle_type        VARCHAR(50),
    brand               VARCHAR(50),
    model               VARCHAR(100),
    manufacture_year    INTEGER,
    capacity_kg         DECIMAL(10,2),
    capacity_volume_m3  DECIMAL(10,2),
    fuel_type           VARCHAR(50),
    fuel_efficiency     DECIMAL(10,2),
    last_service_date   DATE,
    next_service_date   DATE,
    insurance_expiry    DATE,
    gps_installed       BOOLEAN,
    status              VARCHAR(50),
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(200)
);

-- STG_PACKAGE: Staging for package data from JSON
CREATE TABLE staging.stg_package (
    package_id          VARCHAR(50),
    package_type        VARCHAR(50),
    weight_kg           DECIMAL(10,2),
    length_cm           DECIMAL(10,2),
    width_cm            DECIMAL(10,2),
    height_cm           DECIMAL(10,2),
    volume_cm3          DECIMAL(10,2),
    size_category       VARCHAR(50),
    fragile             BOOLEAN,
    hazardous           BOOLEAN,
    temperature_control BOOLEAN,
    insurance_value     DECIMAL(15,2),
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(200)
);

-- STG_ROUTE: Staging for route data
CREATE TABLE staging.stg_route (
    route_id            VARCHAR(50),
    origin_country      VARCHAR(100),
    origin_province     VARCHAR(100),
    destination_country VARCHAR(100),
    destination_province VARCHAR(100),
    distance_km         DECIMAL(10,2),
    average_time_min    DECIMAL(10,2),
    road_type           VARCHAR(50),
    traffic_level       VARCHAR(50),
    toll_required       BOOLEAN,
    region              VARCHAR(100),
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(200)
);

-- STG_WAREHOUSE: Staging for warehouse data
CREATE TABLE staging.stg_warehouse (
    warehouse_id        VARCHAR(50),
    warehouse_name      VARCHAR(200),
    province            VARCHAR(100),
    country             VARCHAR(100),
    capacity_packages   INTEGER,
    manager_name        VARCHAR(200),
    contact_number      VARCHAR(50),
    operational_status  VARCHAR(50),
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(200)
);

-- STG_DELIVERY: Staging for delivery transactions
CREATE TABLE staging.stg_delivery (
    delivery_id             VARCHAR(50),
    customer_id             VARCHAR(50),
    driver_id               VARCHAR(50),
    vehicle_id              VARCHAR(50),
    route_id                VARCHAR(50),
    package_id              VARCHAR(50),
    warehouse_id            VARCHAR(50),
    pickup_time             TIMESTAMP,
    departure_time          TIMESTAMP,
    arrival_time            TIMESTAMP,
    delivery_time           TIMESTAMP,
    distance_km             DECIMAL(10,2),
    fuel_used_liters        DECIMAL(10,2),
    base_cost               DECIMAL(15,2),
    fuel_cost               DECIMAL(15,2),
    toll_cost               DECIMAL(15,2),
    insurance_cost          DECIMAL(15,2),
    total_delivery_cost     DECIMAL(15,2),
    payment_method          VARCHAR(50),
    payment_status          VARCHAR(50),
    delivery_status         VARCHAR(50),
    on_time_flag            BOOLEAN,
    damaged_flag            BOOLEAN,
    returned_flag           BOOLEAN,
    load_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file             VARCHAR(200)
);