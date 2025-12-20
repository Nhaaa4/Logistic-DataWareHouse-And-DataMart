# Generate Data Sources

This guide explains how to generate sample data for the Logistic Data Warehouse project.

## Overview

The data generation script creates realistic logistics data across multiple formats:

| File | Format | Records | Description |
|------|--------|---------|-------------|
| `customers.csv` | CSV | 50 | Customer master data |
| `drivers.csv` | CSV | 5 | Driver information |
| `vehicles_api.json` | JSON | 30 | Vehicle fleet (simulated API response) |
| `packages.json` | JSON | 100 | Package specifications |
| `logistics_source.db` | SQLite | - | Database containing routes, warehouses, and deliveries |

## Prerequisites

Install the required Python packages:

```bash
pip install faker
```

Or install all project dependencies:

```bash
pip install -r requirements.txt
```

## Running the Script

1. Navigate to the project directory:

```bash
cd /path/to/logistic
```

2. Run the data generation script:

```bash
python data/generate_data_sources.py
```

3. The script will create a `data/data_sources/` directory containing all generated files.

## Output

After running the script, you will see:

```
data/
└── data_sources/
    ├── customers.csv
    ├── drivers.csv
    ├── vehicles_api.json
    ├── packages.json
    └── logistics_source.db
```

## Data Configuration

You can modify the data volume by editing the constants in `generate_data_sources.py`:

```python
NUM_CUSTOMERS = 50
NUM_DRIVERS = 5
NUM_VEHICLES = 30
NUM_ROUTES = 5
NUM_PACKAGES = 100
NUM_WAREHOUSES = 5
NUM_DELIVERIES = 100
DAYS_OF_DATA = 365  # Date range for historical data
```

## Generated Data Details

### CSV Files

**customers.csv** - Customer information including:
- Customer ID, name, contact details
- Address (commune, district, province, country)
- Customer type (Individual, Business, Corporate, Government)
- Loyalty level (Bronze, Silver, Gold, Platinum)

**drivers.csv** - Driver information including:
- Driver ID, name, contact details
- License information (number, type, expiry)
- Employment details (hire date, experience, type)
- Performance rating and status

### JSON Files

**vehicles_api.json** - Simulated API response containing:
- Vehicle specifications (type, brand, model, capacity)
- Service and insurance dates
- GPS installation status

**packages.json** - Package details including:
- Dimensions and weight
- Special handling flags (fragile, hazardous, temperature control)
- Insurance value

### SQLite Database (logistics_source.db)

Contains three tables:

| Table | Description |
|-------|-------------|
| `routes` | Route definitions with origin/destination, distance, traffic level |
| `warehouses` | Warehouse locations, capacity, manager info |
| `deliveries` | Delivery transactions with timestamps, costs, and status |
