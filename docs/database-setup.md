# Database Setup

This guide explains how to create the required databases for the Logistic Data Warehouse project.

## Overview

The project uses three database layers:

| Layer | Database | Schema/Database Name | Purpose |
|-------|----------|---------------------|---------|
| Staging | PostgreSQL | `staging` | Raw data landing zone |
| Data Warehouse | PostgreSQL | `dwh` | Star schema with dimensions and facts |
| Data Mart | MySQL | `logistics_dm` | Aggregated analytics tables |

## Prerequisites

- PostgreSQL installed and running ([Install Guide](https://www.postgresql.org/download/))
- MySQL installed and running ([Install Guide](https://dev.mysql.com/downloads/mysql/))

## Step 1: Create PostgreSQL Database

Connect to PostgreSQL and create the main database:

```bash
sudo -u postgres psql
```

```sql
-- Create database
CREATE DATABASE logistics_dwh;

-- Connect to the database
\c logistics_dwh
```

## Step 2: Create Staging Layer (PostgreSQL)

Run the staging schema script:

```bash
psql -U postgres -d logistics_dwh -f sql/staging/create_staging.sql
```

Or execute manually in psql:

```sql
\c logistics_dwh
\i sql/staging/create_staging.sql
```

This creates 7 staging tables:
- `staging.stg_customer`
- `staging.stg_driver`
- `staging.stg_vehicle`
- `staging.stg_package`
- `staging.stg_route`
- `staging.stg_warehouse`
- `staging.stg_delivery`

## Step 3: Create Data Warehouse Layer (PostgreSQL)

Run the DWH schema script:

```bash
psql -U postgres -d logistics_dwh -f sql/data\ warehouse/create_dwh.sql
```

Or execute manually:

```sql
\c logistics_dwh
\i 'sql/data warehouse/create_dwh.sql'
```

This creates:

**Dimension Tables (7):**
- `dwh.dim_date` - Time dimension
- `dwh.dim_customer` - Customer dimension (SCD Type 2)
- `dwh.dim_driver` - Driver dimension (SCD Type 2)
- `dwh.dim_vehicle` - Vehicle dimension (SCD Type 2)
- `dwh.dim_route` - Route dimension (SCD Type 2)
- `dwh.dim_package` - Package dimension (SCD Type 2)
- `dwh.dim_warehouse` - Warehouse dimension (SCD Type 2)

**Fact Table:**
- `dwh.fact_delivery` - Delivery transactions (partitioned by year/month)

## Step 4: Create Data Mart Layer (MySQL)

Connect to MySQL:

```bash
mysql -u root -p
```

Run the data mart script:

```sql
SOURCE sql/data mart/create_dm.sql;
```

Or from command line:

```bash
mysql -u root -p < sql/data\ mart/create_dm.sql
```

This creates:
- `mart_customer_analytics` - Customer KPIs and metrics
- `mart_delivery_performance` - Time-based performance summaries
- `mart_resource_performance` - Driver and vehicle performance
- `mart_financial_analytics` - Financial analysis tables
- Dashboard views (`v_executive_summary`, `v_top_performers`, `v_financial_summary`)

## Verification

### Verify PostgreSQL Schemas

```sql
-- Connect to PostgreSQL
psql -U postgres -d logistics_dwh

-- Check staging tables
\dt staging.*

-- Check DWH tables
\dt dwh.*
```

### Verify MySQL Tables

```sql
-- Connect to MySQL
mysql -u root -p logistics_dm

-- Check tables
SHOW TABLES;
```

## Database Connection Configuration

Update the database connections in your DAG files or environment variables:

| Database | Host | Port | Default User | Database |
|----------|------|------|--------------|----------|
| PostgreSQL | localhost | 5432 | postgres | logistics_dwh |
| MySQL | localhost | 3306 | root | logistics_dm |

## Troubleshooting

### PostgreSQL Permission Issues

```sql
-- Grant permissions if needed
GRANT ALL PRIVILEGES ON DATABASE logistics_dwh TO your_user;
GRANT ALL PRIVILEGES ON SCHEMA staging TO your_user;
GRANT ALL PRIVILEGES ON SCHEMA dwh TO your_user;
```

### MySQL Access Issues

```sql
-- Grant permissions if needed
GRANT ALL PRIVILEGES ON logistics_dm.* TO 'your_user'@'localhost';
FLUSH PRIVILEGES;
```
