import pandas as pd
import sqlite3

# Read your current data
df = pd.read_csv('data/csv/parts_master.csv')

# Group by vehicle to see the pattern
vehicles = df['vehicle_name'].unique()
print("Vehicles found:", len(vehicles))

# Check if data for vehicle N actually belongs to vehicle N-1
for i, vehicle in enumerate(vehicles[1:], 1):  # Skip first vehicle
    current_data = df[df['vehicle_name'] == vehicle]
    prev_vehicle = vehicles[i - 1]

    print(f"\nVehicle {i}: {vehicle}")
    print(f"Previous: {prev_vehicle}")
    print(f"Group codes in current: {current_data['group_code'].unique()[:5]}")
    # You should see group codes that don't match the current vehicle
