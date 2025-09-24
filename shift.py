import pandas as pd


def verify_corrections():
    original = pd.read_csv('data/csv/parts_master.csv')
    corrected = pd.read_csv('data/csv/parts_master_corrected.csv')

    print("Original vehicles:", original['vehicle_name'].nunique())
    print("Corrected vehicles:", corrected['vehicle_name'].nunique())

    # Check if group_codes now make sense for their assigned vehicles
    for vehicle in corrected['vehicle_name'].unique()[:3]:
        data = corrected[corrected['vehicle_name'] == vehicle]
        print(f"\n{vehicle}:")
        print(f"  Model code: {data['model_code'].iloc[0]}")
        print(f"  Sample group codes: {data['group_code'].unique()[:3]}")
        # Group codes should now logically match the vehicle name

if __name__ == "__main__":
    verify_corrections()
