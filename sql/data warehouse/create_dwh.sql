-- ==========================================
-- DATA WAREHOUSE - LOGISTICS 
-- ==========================================

-- Create DWH schema
CREATE SCHEMA IF NOT EXISTS dwh;

-- DIMENSION TABLES

-- DIM_DATE: Time dimension
CREATE TABLE dwh.dim_date (
    date_key            INTEGER PRIMARY KEY,
    full_date           DATE NOT NULL,
    day                 INTEGER NOT NULL,
    day_name            VARCHAR(20) NOT NULL,
    day_of_week         INTEGER NOT NULL,
    day_of_year         INTEGER NOT NULL,
    week_of_year        INTEGER NOT NULL,
    month               INTEGER NOT NULL,
    month_name          VARCHAR(20) NOT NULL,
    quarter             INTEGER NOT NULL,
    year                INTEGER NOT NULL,
    is_weekend          BOOLEAN DEFAULT FALSE,
    is_holiday          BOOLEAN DEFAULT FALSE,
    fiscal_year         INTEGER,
    fiscal_quarter      INTEGER,
    effective_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date         DATE NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dwh_dim_date_full_date ON dwh.dim_date(full_date);
CREATE INDEX idx_dwh_dim_date_year_month ON dwh.dim_date(year, month);
CREATE INDEX idx_dwh_dim_date_is_current ON dwh.dim_date(is_current);

-- DIM_CUSTOMER: Customer dimension
CREATE TABLE dwh.dim_customer (
    customer_key        BIGSERIAL PRIMARY KEY,
    customer_id         VARCHAR(50) NOT NULL,
    customer_name       VARCHAR(200) NOT NULL,
    gender              VARCHAR(20),
    date_of_birth       DATE,
    age                 INTEGER,
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
    is_active           BOOLEAN DEFAULT TRUE,
    effective_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date         DATE NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dwh_dim_customer_id ON dwh.dim_customer(customer_id);
CREATE INDEX idx_dwh_dim_customer_is_current ON dwh.dim_customer(is_current);
CREATE INDEX idx_dwh_dim_customer_province ON dwh.dim_customer(province);
CREATE INDEX idx_dwh_dim_customer_type ON dwh.dim_customer(customer_type);

-- DIM_DRIVER: Driver dimension
CREATE TABLE dwh.dim_driver (
    driver_key          BIGSERIAL PRIMARY KEY,
    driver_id           VARCHAR(50) NOT NULL,
    driver_name         VARCHAR(200) NOT NULL,
    gender              VARCHAR(20),
    date_of_birth       DATE,
    age                 INTEGER,
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
    effective_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date         DATE NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dwh_dim_driver_id ON dwh.dim_driver(driver_id);
CREATE INDEX idx_dwh_dim_driver_is_current ON dwh.dim_driver(is_current);
CREATE INDEX idx_dwh_dim_driver_status ON dwh.dim_driver(status);
CREATE INDEX idx_dwh_dim_driver_base_city ON dwh.dim_driver(base_city);

-- DIM_VEHICLE: Vehicle dimension
CREATE TABLE dwh.dim_vehicle (
    vehicle_key         BIGSERIAL PRIMARY KEY,
    vehicle_id          VARCHAR(50) NOT NULL,
    plate_number        VARCHAR(50),
    vehicle_type        VARCHAR(50),
    brand               VARCHAR(50),
    model               VARCHAR(100),
    manufacture_year    INTEGER,
    vehicle_age         INTEGER,
    capacity_kg         DECIMAL(10,2),
    capacity_volume_m3  DECIMAL(10,2),
    fuel_type           VARCHAR(50),
    fuel_efficiency     DECIMAL(10,2),
    last_service_date   DATE,
    next_service_date   DATE,
    insurance_expiry    DATE,
    gps_installed       BOOLEAN DEFAULT FALSE,
    status              VARCHAR(50),
    effective_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date         DATE NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dwh_dim_vehicle_id ON dwh.dim_vehicle(vehicle_id);
CREATE INDEX idx_dwh_dim_vehicle_is_current ON dwh.dim_vehicle(is_current);
CREATE INDEX idx_dwh_dim_vehicle_status ON dwh.dim_vehicle(status);
CREATE INDEX idx_dwh_dim_vehicle_type ON dwh.dim_vehicle(vehicle_type);

-- DIM_ROUTE: Route dimension
CREATE TABLE dwh.dim_route (
    route_key           BIGSERIAL PRIMARY KEY,
    route_id            VARCHAR(50) NOT NULL,
    origin_country      VARCHAR(100),
    origin_province     VARCHAR(100),
    destination_country VARCHAR(100),
    destination_province VARCHAR(100),
    distance_km         DECIMAL(10,2),
    average_time_min    DECIMAL(10,2),
    road_type           VARCHAR(50),
    traffic_level       VARCHAR(50),
    toll_required       BOOLEAN DEFAULT FALSE,
    region              VARCHAR(100),
    effective_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date         DATE NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dwh_dim_route_id ON dwh.dim_route(route_id);
CREATE INDEX idx_dwh_dim_route_is_current ON dwh.dim_route(is_current);
CREATE INDEX idx_dwh_dim_route_provinces ON dwh.dim_route(origin_province, destination_province);
CREATE INDEX idx_dwh_dim_route_region ON dwh.dim_route(region);

-- DIM_PACKAGE: Package dimension
CREATE TABLE dwh.dim_package (
    package_key         BIGSERIAL PRIMARY KEY,
    package_id          VARCHAR(50) NOT NULL,
    package_type        VARCHAR(50),
    weight_kg           DECIMAL(10,2),
    length_cm           DECIMAL(10,2),
    width_cm            DECIMAL(10,2),
    height_cm           DECIMAL(10,2),
    volume_cm3          DECIMAL(10,2),
    size_category       VARCHAR(50),
    fragile             BOOLEAN DEFAULT FALSE,
    hazardous           BOOLEAN DEFAULT FALSE,
    temperature_control BOOLEAN DEFAULT FALSE,
    insurance_value     DECIMAL(15,2),
    effective_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date         DATE NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dwh_dim_package_id ON dwh.dim_package(package_id);
CREATE INDEX idx_dwh_dim_package_is_current ON dwh.dim_package(is_current);
CREATE INDEX idx_dwh_dim_package_type ON dwh.dim_package(package_type);
CREATE INDEX idx_dwh_dim_package_size ON dwh.dim_package(size_category);

-- DIM_WAREHOUSE: Warehouse dimension
CREATE TABLE dwh.dim_warehouse (
    warehouse_key       BIGSERIAL PRIMARY KEY,
    warehouse_id        VARCHAR(50) NOT NULL,
    warehouse_name      VARCHAR(200) NOT NULL,
    province            VARCHAR(100),
    country             VARCHAR(100),
    capacity_packages   INTEGER,
    manager_name        VARCHAR(200),
    contact_number      VARCHAR(50),
    operational_status  VARCHAR(50),
    effective_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date         DATE NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dwh_dim_warehouse_id ON dwh.dim_warehouse(warehouse_id);
CREATE INDEX idx_dwh_dim_warehouse_is_current ON dwh.dim_warehouse(is_current);
CREATE INDEX idx_dwh_dim_warehouse_province ON dwh.dim_warehouse(province);
CREATE INDEX idx_dwh_dim_warehouse_status ON dwh.dim_warehouse(operational_status);

-- FACT TABLE

-- FACT_DELIVERY: Main delivery transactions
CREATE TABLE dwh.fact_delivery (
    fact_delivery_id        BIGSERIAL PRIMARY KEY,
    delivery_id             VARCHAR(50) NOT NULL,
    date_key                INTEGER NOT NULL,
    customer_key            BIGINT NOT NULL,
    driver_key              BIGINT NOT NULL,
    vehicle_key             BIGINT NOT NULL,
    route_key               BIGINT NOT NULL,
    package_key             BIGINT NOT NULL,
    warehouse_key           BIGINT NOT NULL,
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
    on_time_flag            BOOLEAN DEFAULT FALSE,
    damaged_flag            BOOLEAN DEFAULT FALSE,
    returned_flag           BOOLEAN DEFAULT FALSE,
    delivery_duration_min   INTEGER,
    year                    INTEGER,
    month                   INTEGER,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fact_delivery_date FOREIGN KEY (date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_fact_delivery_customer FOREIGN KEY (customer_key) REFERENCES dwh.dim_customer(customer_key),
    CONSTRAINT fk_fact_delivery_driver FOREIGN KEY (driver_key) REFERENCES dwh.dim_driver(driver_key),
    CONSTRAINT fk_fact_delivery_vehicle FOREIGN KEY (vehicle_key) REFERENCES dwh.dim_vehicle(vehicle_key),
    CONSTRAINT fk_fact_delivery_route FOREIGN KEY (route_key) REFERENCES dwh.dim_route(route_key),
    CONSTRAINT fk_fact_delivery_package FOREIGN KEY (package_key) REFERENCES dwh.dim_package(package_key),
    CONSTRAINT fk_fact_delivery_warehouse FOREIGN KEY (warehouse_key) REFERENCES dwh.dim_warehouse(warehouse_key)
);

-- Partitioning by year and month
ALTER TABLE dwh.fact_delivery PARTITION BY RANGE (year, month);

CREATE INDEX idx_dwh_fact_delivery_date ON dwh.fact_delivery(date_key);
CREATE INDEX idx_dwh_fact_delivery_customer ON dwh.fact_delivery(customer_key);
CREATE INDEX idx_dwh_fact_delivery_driver ON dwh.fact_delivery(driver_key);
CREATE INDEX idx_dwh_fact_delivery_vehicle ON dwh.fact_delivery(vehicle_key);
CREATE INDEX idx_dwh_fact_delivery_route ON dwh.fact_delivery(route_key);
CREATE INDEX idx_dwh_fact_delivery_status ON dwh.fact_delivery(delivery_status);
CREATE INDEX idx_dwh_fact_delivery_payment ON dwh.fact_delivery(payment_status);
CREATE INDEX idx_dwh_fact_delivery_id ON dwh.fact_delivery(delivery_id);
CREATE INDEX idx_dwh_fact_delivery_year_month ON dwh.fact_delivery(year, month);
CREATE INDEX idx_dwh_fact_delivery_time ON dwh.fact_delivery(delivery_time);
