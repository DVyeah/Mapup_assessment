import pandas as pd
import argparse
from datetime import timedelta

def extract_trips(df, output_dir):
    # Convert 'timestamp' column to datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Define the time threshold for trip identification (7 hours)
    time_threshold = timedelta(hours=7)
    
    # Check if 'unit' column exists and the DataFrame is not empty
    if 'unit' not in df.columns or df.empty:
        print("DataFrame is empty or 'unit' column is missing.")
        return
    
    # Initialize variables for trip numbering and unit
    trip_number = 0
    unit = df['unit'].iloc[0]
    trip_data = pd.DataFrame(columns=['latitude', 'longitude', 'timestamp'])
    trip_start_time = df['timestamp'].iloc[0]
    
    # Iterate through the rows to identify trips
    for index, row in df.iterrows():
        current_time = row['timestamp']
        time_diff = current_time - trip_start_time
        
        # Check if a new trip should start based on time difference
        if time_diff > time_threshold:
            # Save trip data to a CSV file
            trip_data.to_csv(f'{output_dir}/{unit}_{trip_number}.csv', index=False)
            
            # Increment trip number and reset trip data
            trip_number += 1
            trip_data = pd.DataFrame(columns=['latitude', 'longitude', 'timestamp'])
            trip_start_time = current_time
        
        # Append current row to the trip data
        trip_data = trip_data.append({'latitude': row['latitude'], 'longitude': row['longitude'], 'timestamp': row['timestamp']}, ignore_index=True)
    
    # Save the last trip data to a CSV file
    trip_data.to_csv(f'{output_dir}/{unit}_{trip_number}.csv', index=False)

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Process GPS data and extract trips')
    parser.add_argument('--to_process', type=str, help='Path to the Parquet file to be processed')
    parser.add_argument('--output_dir', type=str, help='The folder to store the resulting CSV files')
    args = parser.parse_args()
    
    # Read the Parquet file
    try:
        data = pd.read_parquet(args.to_process)
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return
    
    # Group data by 'unit' and extract trips for each unit
    for unit, df_unit in data.groupby('unit'):
        extract_trips(df_unit, args.output_dir)


##if _name_ == "_main_":
run_main = True
if run_main:
    print("success")
