import sqlite3
import pandas as pd
import os
import glob
import logging
import argparse
import shutil
import requests
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
from sqlalchemy import text
import sqlalchemy
import sqlalchemy.types as types
from sqlalchemy.exc import ProgrammingError
from import_stations import test_mysql_connection, test_engine_connection
import numpy as np

logger = logging.getLogger(__name__)

debug = False
def import_csv_files(station_id):

    # if not debug:
    #     # ----------------------------------------
    #     # Connect to the SQLite database
    #     conn = sqlite3.connect('../data/database.db')

    #     # Directory path containing the CSV files for the given station ID
    #     directory_path = os.path.join("../data", str(station_id))

    #     # Pattern to match the CSV files
    #     pattern = os.path.join(directory_path, "en_climate_daily_BC_*_P1D.csv")

    #     # Iterate over all matching CSV files
    #     for file_path in glob.glob(pattern):
    #         # Extract Climate ID from the file name
    #         file_name = os.path.basename(file_path)
    #         climate_id = file_name.split('_')[4]
    #         # logging.info(f"Climate ID: {climate_id}")

    #         # Table name for the corresponding Climate ID
    #         table_name = str(climate_id)+"_"+str(station_id)+"_daily"
    #         # logging.info(f"Table name: {table_name}")

    #         # Read CSV file into a Pandas DataFrame
    #         df = pd.read_csv(file_path)

    #         # Import the DataFrame into the SQLite database, creating or appending to the table named after the Climate ID
    #         df.to_sql(table_name, conn, if_exists='append', index=False)

    #         # logging.info(f"Imported '{file_path}' into table '{table_name}'.")

    #     # Close the connection
    #     conn.close()
    # ----------------------------------------

    # ----------------------------------------
    # Connect to the MySQL database

    # Get credentials from environment variables
    user = os.environ.get('MYSQL_USER')
    password = os.environ.get('MYSQL_PASSWORD')
    host = os.environ.get('MYSQL_HOST')
    host_port = os.environ.get('MYSQL_PORT')

    # Database connection parameters
    database_name = "canada-climate"
    
    # Using sqlalchemy
    connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{host_port}/{database_name}"
    engine = create_engine(connection_string)

    if test_engine_connection(engine):
        logging.debug("Engine is set up correctly.")
        # Directory path containing the CSV files for the given station ID
        directory_path = os.path.join("../data", str(station_id))

        # Pattern to match the CSV files
        pattern = os.path.join(directory_path, "en_climate_daily_BC_*_P1D.csv")
        assert len(glob.glob(pattern)) > 0, f"No files found for station {station_id}"

        # we're going to DROP the table if it exists, and then recreate it
        # this is so we can re-run this script over and over again
        # without violating the UNIQUE constraint on the Date/Time column

        file_name = glob.glob(pattern)[0]
        climate_id = file_name.split('_')[4]
        assert climate_id is not None, f"Climate ID not found for station {station_id}"

        # Table name for the corresponding Climate ID
        table_name = str(climate_id)+"_"+str(station_id)+"_daily"
        assert table_name is not None, f"Table name not found for station {station_id}"

        # table_name = str(climate_id)+"_"+str(station_id)+"_daily"
        # Define the query to drop the table
        query = text(f"DROP TABLE IF EXISTS {table_name}")

        # Execute the query
        with engine.connect() as connection:
            connection.execute(query)

        logging.debug(f"Table '{table_name}' has been dropped.")

        # total number of files to process
        total_files = len(glob.glob(pattern))
        logging.info(f"Total files: {total_files}")
        file_number = 0

        # Iterate over all matching CSV files
        for file_path in glob.glob(pattern):
            # Extract Climate ID from the file name
            file_name = os.path.basename(file_path)
            climate_id = file_name.split('_')[4]

            # Read CSV file into a Pandas DataFrame
            df = pd.read_csv(file_path)

            # Define the data type for the "Climate ID" column
            dtype = {
                'Data Quality': types.VARCHAR(length=16),
                'Max Temp Flag': types.VARCHAR(length=16),
                'Min Temp Flag': types.VARCHAR(length=16),
                'Mean Temp Flag': types.VARCHAR(length=16),
                'Heat Deg Days Flag': types.VARCHAR(length=16),
                'Cool Deg Days Flag': types.VARCHAR(length=16),
                'Total Rain Flag': types.VARCHAR(length=16),
                'Total Snow Flag': types.VARCHAR(length=16),
                'Total Precip Flag': types.VARCHAR(length=16),
                'Snow on Grnd Flag': types.VARCHAR(length=16),
                'Dir of Max Gust Flag': types.VARCHAR(length=16),
                'Spd of Max Gust Flag': types.VARCHAR(length=16),
                'Spd of Max Gust (km/h)': types.VARCHAR(length=8)
            }

            df['Date/Time'] = pd.to_datetime(df['Date/Time'], format='%Y-%m-%d')

            # Import the DataFrame into the SQLite database, creating or appending to the table named after the Climate ID
            # Note: added chunksize=100 to avoid the error "payload string too large"
            df.to_sql(table_name, con=engine, if_exists='append', index=True, chunksize=100, dtype=dtype)
            # df.to_sql('stations', con=engine, index=False, if_exists='replace', chunksize=100)

            # logging.debug(f"Imported '{file_path}' into table '{table_name}'.")

            # Define the query to count the number of records
            query = text(f"SELECT COUNT(*) FROM {table_name}")

            # Execute the query and fetch the result
            with engine.connect() as connection:
                result = connection.execute(query)
                count = result.scalar()

            # Calculate the percentage of completion
            percentage_completion = (file_number / total_files) * 100

            # Determine the number of "#" symbols to include
            num_hashes = int(percentage_completion)

            # Build the progress bar string with "#" symbols and "-" symbols for the remainder
            progress_bar = "#" * num_hashes + "-" * (100 - num_hashes)

            # Ensure the string is exactly 100 characters long
            progress_bar = progress_bar[:100]

            # logging.info(progress_bar)
            # log the progress bar without scrolling the terminal
            # logging.info(f"\rProgress: {progress_bar}"
            logger.info(f"\rProgress: {progress_bar}")
            file_number += 1
    else:
        logging.error("Engine setup failed.")
    
    logging.info(f"The number of records in the table '{table_name}' is: {count}")
    # ----------------------------------------

def main(args):
    station_id = args.station_id # Replace with the desired station ID
    import_csv_files(station_id)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Retrieve hourly data years for a given station ID.")
    parser.add_argument('station_id', type=int, help="Station ID to look up")
    parser.add_argument('--database_path', type=str, default='data/database.db', help="Path to the SQLite database")
    return parser.parse_args()

if __name__ == '__main__':
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    args = parse_arguments()
    main(args)