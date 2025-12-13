CREATE DATABASE IF NOT EXISTS logistics_dw;
CREATE TABLE logistics_dw.dim_date (
    date_key            INT,
    day                 INT,
    day_name            STRING,
    week_of_year        INT,
    month               INT,
    month_name          STRING,
    quarter             INT,
    year                INT,
    is_weekend          BOOLEAN,
    is_holiday          BOOLEAN,
    effective_date      DATE,
    expiry_date         DATE,
    is_current          BOOLEAN
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
CREATE TABLE logistics_dw.dim_customer (
    customer_key        BIGINT,
    customer_id         STRING,
    customer_name       STRING,
    gender              STRING,
    date_of_birth       DATE,
    phone               STRING,
    email               STRING,
    address             STRING,
    commune             STRING,
    district            STRING,
    province            STRING,
    country             STRING,
    postal_code         STRING,
    customer_type       STRING,
    registration_date   DATE,
    loyalty_level       STRING,
    preferred_contact   STRING,
    is_active           BOOLEAN,
    effective_date      DATE,
    expiry_date         DATE,
    is_current          BOOLEAN
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
CREATE TABLE logistics_dw.dim_driver (
    driver_key          BIGINT,
    driver_id           STRING,
    driver_name         STRING,
    gender              STRING,
    date_of_birth       DATE,
    phone               STRING,
    license_number      STRING,
    license_type        STRING,
    license_expiry      DATE,
    hire_date           DATE,
    experience_years    INT,
    emergency_contact   STRING,
    employment_type     STRING,
    rating              DOUBLE,
    status              STRING,
    base_city           STRING,
    effective_date      DATE,
    expiry_date         DATE,
    is_current          BOOLEAN
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
CREATE TABLE logistics_dw.dim_vehicle (
    vehicle_key         BIGINT,
    vehicle_id          STRING,
    plate_number        STRING,
    vehicle_type        STRING,
    brand               STRING,
    model               STRING,
    manufacture_year    INT,
    capacity_kg         DOUBLE,
    capacity_volume_m3  DOUBLE,
    fuel_type           STRING,
    fuel_efficiency     DOUBLE,
    last_service_date   DATE,
    next_service_date   DATE,
    insurance_expiry    DATE,
    gps_installed       BOOLEAN,
    status              STRING,
    effective_date      DATE,
    expiry_date         DATE,
    is_current          BOOLEAN
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
CREATE TABLE logistics_dw.dim_route (
    route_key           BIGINT,
    route_id            STRING,
    origin_country      STRING,
    origin_province     STRING,
    destination_country STRING,
    destination_province STRING,
    distance_km         DOUBLE,
    average_time_min    DOUBLE,
    road_type           STRING,
    traffic_level       STRING,
    toll_required       BOOLEAN,
    region              STRING,
    effective_date      DATE,
    expiry_date         DATE,
    is_current          BOOLEAN
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
CREATE TABLE logistics_dw.dim_package (
    package_key         BIGINT,
    package_id          STRING,
    package_type        STRING,
    weight_kg           DOUBLE,
    length_cm           DOUBLE,
    width_cm            DOUBLE,
    height_cm           DOUBLE,
    volume_cm3          DOUBLE,
    size_category       STRING,
    fragile             BOOLEAN,
    hazardous           BOOLEAN,
    temperature_control BOOLEAN,
    insurance_value     DOUBLE,
    effective_date      DATE,
    expiry_date         DATE,
    is_current          BOOLEAN
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
CREATE TABLE logistics_dw.dim_warehouse (
    warehouse_key       BIGINT,
    warehouse_id        STRING,
    warehouse_name      STRING,
    province            STRING,
    country             STRING,
    capacity_packages   INT,
    manager_name        STRING,
    contact_number      STRING,
    operational_status  STRING,
    effective_date      DATE,
    expiry_date         DATE,
    is_current          BOOLEAN
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
CREATE TABLE logistics_dw.fact_delivery (
    delivery_id             STRING,
    date_key                INT,
    customer_key            BIGINT,
    driver_key              BIGINT,
    vehicle_key             BIGINT,
    route_key               BIGINT,
    package_key             BIGINT,
    warehouse_key           BIGINT,
    pickup_time             TIMESTAMP,
    departure_time          TIMESTAMP,
    arrival_time            TIMESTAMP,
    delivery_time           TIMESTAMP,
    distance_km             DOUBLE,
    fuel_used_liters        DOUBLE,
    base_cost               DOUBLE,
    fuel_cost               DOUBLE,
    toll_cost               DOUBLE,
    insurance_cost          DOUBLE,
    total_delivery_cost     DOUBLE,
    payment_method          STRING,
    payment_status          STRING,
    delivery_status         STRING,
    on_time_flag            BOOLEAN,
    damaged_flag            BOOLEAN,
    returned_flag           BOOLEAN
)
PARTITIONED BY (year INT, month INT)
CLUSTERED BY (customer_key) INTO 32 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'hive.exec.dynamic.partition'='true',
    'hive.exec.dynamic.partition.mode'='nonstrict'
);
