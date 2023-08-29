# Python script to concatenate CSV files with a specific naming pattern in a given directory.

import os
import pandas as pd
import logging
import argparse
import glob

def concatenate_csv_files(station_id):
    directory = f"../data/{station_id}"
    
    directory_path = os.path.join("../data", str(station_id))

    # Pattern to match the CSV files

    pattern = os.path.join(directory_path, "en_climate_daily_BC_*_P1D.csv")
    assert len(glob.glob(pattern)) > 0, f"No files found for station {station_id}"

    climate_id = glob.glob(pattern)[0].split('_')[4]
    logging.info(f"Climate ID: {climate_id}")
    output_file = f"../data/{station_id}/{climate_id}_daily_combined.csv"
    
    # Check if the directory exists
    if not os.path.exists(directory):
        logging.error(f"The directory {directory} does not exist.")
        return

    # List all the CSV files in the directory
    csv_files = [f for f in os.listdir(directory) if f.startswith("en_climate_daily_BC_") and f.endswith("_P1D.csv")]
    
    # Check if there are any matching CSV files
    if len(csv_files) == 0:
        logging.error("No matching CSV files found.")
        return
    
    # Initialize an empty DataFrame to hold the concatenated data
    concatenated_df = pd.DataFrame()
    
    # Loop through each CSV file and append its contents to the DataFrame
    for csv_file in csv_files:
        file_path = os.path.join(directory, csv_file)
        df = pd.read_csv(file_path)
        concatenated_df = pd.concat([concatenated_df, df], ignore_index=True)
        
    # Write the concatenated DataFrame to a new CSV file
    concatenated_df.to_csv(output_file, index=False)
    logging.info(f"Concatenated {len(csv_files)} CSV files and saved to {output_file}.")

def main(args):
    concatenate_csv_files(args.station_id)

def parse_arguments():
    parser = argparse.ArgumentParser(description="Retrieve daily data years for a given station ID.")
    parser.add_argument('station_id', type=int, help="Station ID to look up")
    return parser.parse_args()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parse_arguments()
    main(args)