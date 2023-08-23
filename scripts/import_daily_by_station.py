import sqlite3
import pandas as pd
import os
import glob
import logging
import argparse

def import_csv_files(station_id):
    # Connect to the SQLite database
    conn = sqlite3.connect('../data/database.db')

    # Directory path containing the CSV files for the given station ID
    directory_path = os.path.join("../data", str(station_id))

    # Pattern to match the CSV files
    pattern = os.path.join(directory_path, "en_climate_daily_BC_*_P1D.csv")

    # Iterate over all matching CSV files
    for file_path in glob.glob(pattern):
        # Extract Climate ID from the file name
        file_name = os.path.basename(file_path)
        climate_id = file_name.split('_')[4]
        # logging.info(f"Climate ID: {climate_id}")

        # Table name for the corresponding Climate ID
        table_name = str(climate_id)+"_"+str(station_id)+"_daily"
        # logging.info(f"Table name: {table_name}")

        # Read CSV file into a Pandas DataFrame
        df = pd.read_csv(file_path)

        # Import the DataFrame into the SQLite database, creating or appending to the table named after the Climate ID
        df.to_sql(table_name, conn, if_exists='append', index=False)

        logging.info(f"Imported '{file_path}' into table '{table_name}'.")

    # Close the connection
    conn.close()

def main(args):
    station_id = args.station_id # Replace with the desired station ID
    import_csv_files(station_id)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Retrieve hourly data years for a given station ID.")
    parser.add_argument('station_id', type=int, help="Station ID to look up")
    parser.add_argument('--database_path', type=str, default='data/database.db', help="Path to the SQLite database")
    return parser.parse_args()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parse_arguments()
    main(args)