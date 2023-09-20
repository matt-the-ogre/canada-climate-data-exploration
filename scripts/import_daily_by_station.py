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
# from import_stations import test_mysql_connection, test_engine_connection
import sys
sys.path.append('../common')  # Add common directory to Python path
from utils import test_engine_connection
import numpy as np

import sys
sys.path.append('../update-database-daily')

from clean_daily_data import clean_daily_data

logger = logging.getLogger(__name__)

debug = True

def import_csv_files(station_id, combined=False):

    # if not debug:
    #     # ----------------------------------------
    #     # Connect to the SQLite database
    #     conn = sqlite3.connect('../data/database.db')

    #     # Directory path containing the CSV files for the given station ID
    #     directory_path = os.path.join("../data", str(station_id))

    #     # Pattern to match the CSV files
    #     pattern = os.path.join(directory_path, "en_climate_daily_*_P1D.csv")

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
        if combined:
            pattern = os.path.join(directory_path, "*_daily_combined.csv")
        else:
            pattern = os.path.join(directory_path, "en_climate_daily_*_P1D.csv")
        assert len(glob.glob(pattern)) > 0, f"No files found for station {station_id}"

        # we're going to DROP the table if it exists, and then recreate it
        # this is so we can re-run this script over and over again
        # without violating the UNIQUE constraint on the Date/Time column

        file_name = glob.glob(pattern)[0]
        logging.debug(f"File name: {file_name}")
        if combined:
            climate_id = file_name.split('/')[-1].split('_')[0]
            logging.debug(f"Climate ID: {climate_id}")
        else:
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
            # climate_id = file_name.split('_')[4]

            # clean the data and make a backup csv file
            clean_daily_data(file_path)

            # Read CSV file into a Pandas DataFrame
            df = pd.read_csv(file_path)

            #             #   Column                     Non-Null Count  Dtype  
            # ---  ------                     --------------  -----  
            #  0   Date/Time                  178 non-null    object 
            #  1   Year                       178 non-null    int64  
            #  2   Month                      178 non-null    int64  
            #  3   Day                        178 non-null    int64  
            #  4   Data Quality               178 non-null    object 
            #  5   Max Temp (°C)              178 non-null    float64
            #  6   Max Temp Flag              0 non-null      float64
            #  7   Min Temp (°C)              178 non-null    float64
            #  8   Min Temp Flag              0 non-null      float64
            #  9   Mean Temp (°C)             178 non-null    float64
            #  10  Mean Temp Flag             0 non-null      float64
            #  11  Heat Deg Days (°C)         178 non-null    float64
            #  12  Heat Deg Days Flag         0 non-null      float64
            #  13  Cool Deg Days (°C)         178 non-null    float64
            #  14  Cool Deg Days Flag         0 non-null      float64
            #  15  Total Rain (mm)            178 non-null    float64
            #  16  Total Rain Flag            7 non-null      object 
            #  17  Total Snow (cm)            178 non-null    float64
            #  18  Total Snow Flag            0 non-null      float64
            #  19  Total Precip (mm)          178 non-null    float64
            #  20  Total Precip Flag          7 non-null      object 
            #  21  Snow on Grnd (cm)          178 non-null    float64
            #  22  Snow on Grnd Flag          3 non-null      object 
            #  23  Dir of Max Gust (10s deg)  0 non-null      float64
            #  24  Dir of Max Gust Flag       0 non-null      float64
            #  25  Spd of Max Gust (km/h)     0 non-null      float64
            #  26  Spd of Max Gust Flag       0 non-null      float64

            # Define the data type for the columns
            # Note that we removed the flag columns in the clean function
            dtype = {
                'Data Quality': types.BOOLEAN,
                'Max Temp (°C)': types.FLOAT,
                # 'Max Temp Flag': types.VARCHAR(length=16),
                'Min Temp (°C)': types.FLOAT,
                # 'Min Temp Flag': types.VARCHAR(length=16),
                'Mean Temp (°C)': types.FLOAT,
                # 'Mean Temp Flag': types.VARCHAR(length=16),
                'Heat Deg Days (°C)': types.FLOAT,
                # 'Heat Deg Days Flag': types.VARCHAR(length=16),
                'Cool Deg Days (°C)': types.FLOAT,
                # 'Cool Deg Days Flag': types.VARCHAR(length=16),
                'Total Rain (mm)': types.FLOAT,
                # 'Total Rain Flag': types.VARCHAR(length=16),
                'Total Snow (cm)': types.FLOAT,
                # 'Total Snow Flag': types.VARCHAR(length=16),
                'Total Precip (mm)': types.FLOAT,
                # 'Total Precip Flag': types.VARCHAR(length=16),
                'Snow on Grnd (cm)': types.FLOAT,
                # 'Snow on Grnd Flag': types.VARCHAR(length=16),
                'Dir of Max Gust (10s deg)': types.FLOAT,
                # 'Dir of Max Gust Flag': types.VARCHAR(length=16),
                # 'Spd of Max Gust Flag': types.VARCHAR(length=16),
                'Spd of Max Gust (km/h)': types.FLOAT
            }

            df['Date/Time'] = pd.to_datetime(df['Date/Time'], format='%Y-%m-%d')

            # Import the DataFrame into the SQLite database, creating or appending to the table named after the Climate ID
            # Note: added chunksize=100 to avoid the error "payload string too large"
            df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=100, dtype=dtype)
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