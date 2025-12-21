import csv
import json
import sqlite3
import random
from datetime import datetime, timedelta
from faker import Faker
import os

fake = Faker()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_DIR = f"{SCRIPT_DIR}/data_sources"

if not os.path.exists(SCRIPT_DIR):
    os.makedirs(SCRIPT_DIR)

NUM_CUSTOMERS = 50
NUM_DRIVERS = 5         
NUM_VEHICLES = 30         
NUM_ROUTES = 5            
NUM_PACKAGES = 100        
NUM_WAREHOUSES = 5         
NUM_DELIVERIES = 100   

# Date range
DAYS_OF_DATA = 365
START_DATE = datetime.now() - timedelta(days=DAYS_OF_DATA)

PROVINCES = ["Phnom Penh", "Siem Reap", "Battambang", "Preah Sihanouk", "Kampong Cham", 
             "Kandal", "Banteay Meanchey", "Takeo", "Kampot", "Prey Veng"]
CUSTOMER_TYPES = ["Individual", "Business", "Corporate", "Government"]
LOYALTY_LEVELS = ["Bronze", "Silver", "Gold", "Platinum"]
VEHICLE_TYPES = ["Van", "Truck", "Motorcycle", "Pickup"]
FUEL_TYPES = ["Diesel", "Gasoline", "Electric", "Hybrid"]
PACKAGE_TYPES = ["Document", "Parcel", "Box", "Pallet", "Container"]
PAYMENT_METHODS = ["Cash", "Credit Card", "Bank Transfer", "Mobile Money"]
DELIVERY_STATUSES = ["Completed", "In Transit", "Pending", "Cancelled"]


def generate_customers_csv():
    # Generate customer data as single CSV file
    print("Generating customers CSV...")
    
    filename = os.path.join(SCRIPT_DIR, "customers.csv")
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['customer_id', 'customer_name', 'gender', 'date_of_birth', 'phone', 
                        'email', 'address', 'commune', 'district', 'province', 'country', 
                        'postal_code', 'customer_type', 'registration_date', 'loyalty_level', 
                        'preferred_contact', 'is_active'])
        
        for i in range(1, NUM_CUSTOMERS + 1):
            gender = random.choice(['M', 'F'])
            dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
            
            # early adopters (20%) throughout year, majority (80%) in recent 6 months
            if random.random() < 0.2:
                reg_date = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA))
            else:
                reg_date = START_DATE + timedelta(days=random.randint(DAYS_OF_DATA - 180, DAYS_OF_DATA))
            
            writer.writerow([
                f"CUST{i:07d}",
                fake.name(),
                gender,
                dob,
                fake.phone_number(),
                fake.email(),
                fake.street_address(),
                fake.city(),
                fake.city(),
                random.choice(PROVINCES),
                "Cambodia",
                fake.postcode(),
                random.choice(CUSTOMER_TYPES),
                reg_date.strftime('%Y-%m-%d'),
                random.choice(LOYALTY_LEVELS),
                random.choice(['Email', 'Phone', 'SMS']),
                random.choice([True, False]) if random.random() > 0.1 else True
            ])
            
            if i % 10000 == 0:
                print(f"  ... {i:,}/{NUM_CUSTOMERS:,} customers")
    
def generate_drivers_csv():
    # Generate driver data as single CSV file
    print("Generating drivers CSV...")
    
    filename = os.path.join(SCRIPT_DIR, "drivers.csv")
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['driver_id', 'driver_name', 'gender', 'date_of_birth', 'phone', 
                        'license_number', 'license_type', 'license_expiry', 'hire_date', 
                        'experience_years', 'emergency_contact', 'employment_type', 
                        'rating', 'status', 'base_city'])
        
        for i in range(1, NUM_DRIVERS + 1):
            gender = random.choice(['M', 'F'])
            dob = fake.date_of_birth(minimum_age=25, maximum_age=65)
            
            # Early hires (30%) spread across first 9 months, recent hires (70%) in last 3 months
            if random.random() < 0.3:
                hire_date = START_DATE + timedelta(days=random.randint(0, 270))
            else:
                hire_date = START_DATE + timedelta(days=random.randint(DAYS_OF_DATA - 90, DAYS_OF_DATA))
            
            # License expiry: 1-5 years from hire date
            license_expiry = hire_date + timedelta(days=random.randint(365, 1825))
            
            writer.writerow([
                f"DRV{i:06d}",
                fake.name(),
                gender,
                dob,
                fake.phone_number(),
                f"LIC{random.randint(100000, 999999)}",
                random.choice(['A', 'B', 'C', 'D', 'E']),
                license_expiry.strftime('%Y-%m-%d'),
                hire_date.strftime('%Y-%m-%d'),
                random.randint(1, 20),
                fake.phone_number(),
                random.choice(['Full-time', 'Part-time', 'Contract']),
                round(random.uniform(3.5, 5.0), 2),
                random.choice(['Active', 'On Leave', 'Inactive']),
                random.choice(PROVINCES)
            ])

def generate_vehicles_json():
    # Generate vehicle data as single JSON file (API response)
    print("Generating vehicles JSON (API response)...")
    
    vehicles = []
    for i in range(1, NUM_VEHICLES + 1):
        # initial fleet (40%) first 8 months, expansion (60%) last 4 months
        if random.random() < 0.4:
            purchase_date = START_DATE + timedelta(days=random.randint(0, 240))
        else:
            purchase_date = START_DATE + timedelta(days=random.randint(DAYS_OF_DATA - 120, DAYS_OF_DATA))
        
        vehicle = {
            "vehicle_id": f"VEH{i:06d}",
            "plate_number": f"{random.choice(['PP', 'SR', 'BB', 'KM', 'KP'])}-{random.randint(1000, 9999)}",
            "vehicle_type": random.choice(VEHICLE_TYPES),
            "brand": random.choice(['Toyota', 'Isuzu', 'Mitsubishi', 'Honda', 'Ford', 'Hino', 'Fuso']),
            "model": fake.word().capitalize(),
            "manufacture_year": random.randint(2015, 2024),
            "capacity_kg": random.randint(500, 5000),
            "capacity_volume_m3": round(random.uniform(5, 50), 2),
            "fuel_type": random.choice(FUEL_TYPES),
            "fuel_efficiency": round(random.uniform(8, 15), 2),
            "last_service_date": (purchase_date + timedelta(days=random.randint(0, max(0, (datetime.now() - purchase_date).days - 90)))).strftime('%Y-%m-%d'),
            "next_service_date": (datetime.now() + timedelta(days=random.randint(30, 90))).strftime('%Y-%m-%d'),
            "insurance_expiry": (purchase_date + timedelta(days=365)).strftime('%Y-%m-%d'),
            "gps_installed": random.choice([True, False]) if random.random() > 0.2 else True,
            "status": "Available",
            "purchase_date": purchase_date.strftime('%Y-%m-%d')
        }
        vehicles.append(vehicle)
    
    # Simulate API response
    api_response = {
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "total_records": NUM_VEHICLES,
        "data": vehicles
    }
    
    filename = os.path.join(SCRIPT_DIR, "vehicles_api.json")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(api_response, f, indent=2)
    
def generate_packages_json():
    # Generate package data as single JSON file
    print("Generating packages JSON...")
    
    packages = []
    for i in range(1, NUM_PACKAGES + 1):
        # 1-7 days before use (just-in-time logistics)
        created_date = START_DATE + timedelta(days=random.randint(max(0, DAYS_OF_DATA - 7), DAYS_OF_DATA))
        length = round(random.uniform(10, 100), 2)
        width = round(random.uniform(10, 80), 2)
        height = round(random.uniform(10, 80), 2)
        volume = round(length * width * height, 2)
        
        package = {
            "package_id": f"PKG{i:07d}",
            "package_type": random.choice(PACKAGE_TYPES),
            "weight_kg": round(random.uniform(0.5, 500), 2),
            "length_cm": length,
            "width_cm": width,
            "height_cm": height,
            "volume_cm3": volume,
            "size_category": random.choice(['Small', 'Medium', 'Large', 'Extra Large']),
            "fragile": random.choice([True, False]) if random.random() > 0.7 else False,
            "hazardous": random.choice([True, False]) if random.random() > 0.9 else False,
            "temperature_control": random.choice([True, False]) if random.random() > 0.85 else False,
            "insurance_value": round(random.uniform(10, 10000), 2),
            "created_date": created_date.strftime('%Y-%m-%d')
        }
        packages.append(package)
    
    filename = os.path.join(SCRIPT_DIR, "packages.json")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump({"packages": packages, "total": NUM_PACKAGES}, f, indent=2)

def generate_routes_database(cursor, conn):
    # Generate route data in SQLite database
    print("Generating routes in database...")
    
    cursor.execute('DROP TABLE IF EXISTS routes')
    cursor.execute('''
        CREATE TABLE routes (
            route_id TEXT PRIMARY KEY,
            origin_country TEXT,
            origin_province TEXT,
            destination_country TEXT,
            destination_province TEXT,
            distance_km REAL,
            average_time_min REAL,
            road_type TEXT,
            traffic_level TEXT,
            toll_required INTEGER,
            region TEXT
        )
    ''')
    
    for i in range(1, NUM_ROUTES + 1):
        origin = random.choice(PROVINCES)
        destination = random.choice([p for p in PROVINCES if p != origin])
        
        cursor.execute('''
            INSERT INTO routes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            f"RT{i:05d}",
            "Cambodia",
            origin,
            "Cambodia",
            destination,
            round(random.uniform(10, 500), 2),
            round(random.uniform(30, 600), 2),
            random.choice(['Highway', 'National Road', 'Provincial Road', 'Urban']),
            random.choice(['Low', 'Medium', 'High']),
            1 if random.random() > 0.5 else 0,
            random.choice(['North', 'South', 'East', 'West', 'Central'])
        ))
    
    conn.commit()

def generate_warehouses_database(cursor, conn):
    # Generate warehouse data in SQLite database
    print("Generating warehouses in database...")
    
    cursor.execute('DROP TABLE IF EXISTS warehouses')
    cursor.execute('''
        CREATE TABLE warehouses (
            warehouse_id TEXT PRIMARY KEY,
            warehouse_name TEXT,
            province TEXT,
            country TEXT,
            capacity_packages INTEGER,
            manager_name TEXT,
            contact_number TEXT,
            operational_status TEXT
        )
    ''')
    
    for i in range(1, NUM_WAREHOUSES + 1):
        cursor.execute('''
            INSERT INTO warehouses VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            f"WH{i:04d}",
            f"Warehouse {fake.city()}",
            random.choice(PROVINCES),
            "Cambodia",
            random.randint(1000, 10000),
            fake.name(),
            fake.phone_number(),
            random.choice(['Operational', 'Under Maintenance', 'Closed'])
        ))
    
    conn.commit()

def generate_deliveries_database(cursor, conn):
    # Generate delivery fact data in same database
    print("Generating deliveries in database...")
    
    cursor.execute('DROP TABLE IF EXISTS deliveries')
    cursor.execute('''
        CREATE TABLE deliveries (
            delivery_id TEXT PRIMARY KEY,
            customer_id TEXT,
            driver_id TEXT,
            vehicle_id TEXT,
            route_id TEXT,
            package_id TEXT,
            warehouse_id TEXT,
            pickup_time TEXT,
            departure_time TEXT,
            arrival_time TEXT,
            delivery_time TEXT,
            distance_km REAL,
            fuel_used_liters REAL,
            base_cost REAL,
            fuel_cost REAL,
            toll_cost REAL,
            insurance_cost REAL,
            total_delivery_cost REAL,
            payment_method TEXT,
            payment_status TEXT,
            delivery_status TEXT,
            on_time_flag INTEGER,
            damaged_flag INTEGER,
            returned_flag INTEGER,
            delivery_date DATE
        )
    ''')
    
    for i in range(1, NUM_DELIVERIES + 1):
        # Business day (Monday-Saturday, excluding 10% for Sundays)
        pickup_date = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA))
        while pickup_date.weekday() == 6 and random.random() < 0.9: 
            pickup_date = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA))
        
        # 24 hours = 0-6AM:none(0), 6AM-12PM:peak(3), 12PM-6PM:med(2), 6PM-12AM:low(1)
        hour_weights = [0]*6 + [3]*6 + [2]*6 + [1]*6
        pickup_hour = random.choices(range(24), weights=hour_weights)[0]
        
        pickup_time = pickup_date.replace(
            hour=pickup_hour,
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )
        
        # Preparation time at warehouse: 15-45 minutes
        departure_time = pickup_time + timedelta(minutes=random.randint(15, 45))
        
        # Calculate travel time based on distance
        distance = round(random.uniform(10, 500), 2)
        
        # Speed varies: urban (20-40 km/h), highway (60-80 km/h)
        if distance < 50:  # Urban delivery
            avg_speed = random.uniform(20, 40)
        elif distance < 150:  # Mixed
            avg_speed = random.uniform(40, 60)
        else:  # Long distance
            avg_speed = random.uniform(60, 80)
        
        travel_minutes = int((distance / avg_speed) * 60)
        # Add traffic variance: ±20%
        travel_minutes = int(travel_minutes * random.uniform(0.8, 1.2))
        
        arrival_time = departure_time + timedelta(minutes=travel_minutes)
        
        # Delivery completion time: unloading, verification, signature
        # On-time: 5-20 minutes, Delayed: 20-90 minutes (waiting, issues)
        on_time = random.random() > 0.15  # 85% on-time delivery rate
        if on_time:
            completion_minutes = random.randint(5, 20)
        else:
            completion_minutes = random.randint(20, 90)
        
        delivery_time = arrival_time + timedelta(minutes=completion_minutes)
        
        # Ensure delivery doesn't go past midnight (next day deliveries are separate orders)
        if delivery_time.date() > pickup_date.date():
            # Cap at 11:59 PM same day
            delivery_time = pickup_date.replace(hour=23, minute=59, second=59)
            arrival_time = delivery_time - timedelta(minutes=completion_minutes)
        
        # Calculate costs based on distance and realistic fuel consumption
        fuel_efficiency = random.uniform(8, 15)  # km per liter
        fuel_used = round(distance / fuel_efficiency, 2)
        
        # Pricing structure
        base_cost = round(10 + (distance * 0.3), 2)  # Base + distance rate
        fuel_cost = round(fuel_used * random.uniform(1.2, 1.6), 2)  # Fuel price per liter
        toll_cost = round(random.uniform(5, 25), 2) if distance > 100 and random.random() > 0.4 else 0
        insurance_cost = round(random.uniform(5, 50), 2)
        total_cost = base_cost + fuel_cost + toll_cost + insurance_cost
        
        cursor.execute('''
            INSERT INTO deliveries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            f"DEL{i:09d}",
            f"CUST{random.randint(1, NUM_CUSTOMERS):07d}",
            f"DRV{random.randint(1, NUM_DRIVERS):06d}",
            f"VEH{random.randint(1, NUM_VEHICLES):06d}",
            f"RT{random.randint(1, NUM_ROUTES):05d}",
            f"PKG{random.randint(1, NUM_PACKAGES):07d}",
            f"WH{random.randint(1, NUM_WAREHOUSES):04d}",
            pickup_time.strftime('%Y-%m-%d %H:%M:%S'),
            departure_time.strftime('%Y-%m-%d %H:%M:%S'),
            arrival_time.strftime('%Y-%m-%d %H:%M:%S'),
            delivery_time.strftime('%Y-%m-%d %H:%M:%S'),
            distance,
            fuel_used,
            base_cost,
            fuel_cost,
            toll_cost,
            insurance_cost,
            total_cost,
            random.choice(PAYMENT_METHODS),
            random.choice(['Paid', 'Pending', 'Overdue']),
            random.choice(DELIVERY_STATUSES),
            1 if on_time else 0,
            1 if random.random() > 0.95 else 0,
            1 if random.random() > 0.97 else 0,
            pickup_time.strftime('%Y-%m-%d')
        ))
        
        # Commit every 10k records and show progress
        if i % 10000 == 0:
            conn.commit()
            print(f"  ... {i:,}/{NUM_DELIVERIES:,} deliveries")
    
    conn.commit()

def get_directory_size(path):
    # Calculate total size of directory in bytes
    total = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if os.path.exists(filepath):
                total += os.path.getsize(filepath)
    return total


def format_size(bytes):
    # Format bytes to human readable format
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} TB"


def main():
    # Main function to generate all full load data sources
    start_time = datetime.now()
    
    # CSV sources
    print("Generating CSV files...")
    generate_customers_csv()
    generate_drivers_csv()
    print()
    
    # JSON/API sources
    print("Generating JSON/API files...")
    generate_vehicles_json()
    generate_packages_json()
    print()
    
    # Database sources
    print("Generating database files...")
    db_path = os.path.join(SCRIPT_DIR, "logistics_source.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    generate_routes_database(cursor, conn)
    generate_warehouses_database(cursor, conn)
    generate_deliveries_database(cursor, conn)
    conn.close()
    print()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Calculate total size
    total_size = get_directory_size(SCRIPT_DIR)
    
    print("=" * 80)
    print("FULL LOAD DATA GENERATION COMPLETE!")
    print("=" * 80)
    print(f"\nGeneration time: {duration:.1f} seconds ({duration/60:.1f} minutes)")
    print(f"Total data size: {format_size(total_size)}")
    print()
    print(f"Generated files in {SCRIPT_DIR} directory:")
    print(f"  customers.csv           - {NUM_CUSTOMERS:,} customer records")
    print(f"  drivers.csv             - {NUM_DRIVERS:,} driver records")
    print(f"  vehicles_api.json       - {NUM_VEHICLES:,} vehicle records (API response)")
    print(f"  packages.json           - {NUM_PACKAGES:,} package records")
    print(f"  logistics_source.db     - SQLite database with:")
    print(f"     - routes table          ({NUM_ROUTES:,} records)")
    print(f"     - warehouses table      ({NUM_WAREHOUSES:,} records)")
    print(f"     - deliveries table      ({NUM_DELIVERIES:,} records)")
    print()

if __name__ == "__main__":
    main()
