import sqlite3
import logging
import argparse
import os
import requests

from sqlalchemy import create_engine
from sqlalchemy import text

import sys
sys.path.append('../scripts')

from import_stations import test_mysql_connection, test_engine_connection

# This program takes a station_id and downloads all the available daily data for that station_id
# It uses the database to determine the first and last years available for the station_id

debug = True
database_to_use = "mysql"

def get_daily_data_years(database_path, station_id):
    if database_to_use == "sqlite":
        # ----------------------------------------
        # Connect to the SQLite database
        conn = sqlite3.connect(database_path)

        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()

        # SQL query to retrieve "DLY First Year" and "DLY Last Year" for the given station ID
        query = """
        SELECT "Name", "Province", "DLY First Year", "DLY Last Year"
        FROM stations
        WHERE "Station ID" = ? AND "DLY First Year" IS NOT NULL
        """

        # Execute the query with the provided station ID
        cursor.execute(query, (station_id,))

        # Fetch the result
        result = cursor.fetchone()
        logging.info(f"Result: {result}")

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # If a result was found, return it as a tuple; otherwise, return (0, 0)
        if result:
            return result
        else:
            return ("Unknown station name", "Unknown station province", 0, 0)
        # ----------------------------------------
    elif database_to_use == "mysql":
        # ----------------------------------------
        # Connect to the MySQL database

        # Get credentials from environment variables
        user = os.environ.get('MYSQL_USER')
        password = os.environ.get('MYSQL_PASSWORD')
        host = os.environ.get('MYSQL_HOST')
        host_port = os.environ.get('MYSQL_PORT')

        # assert that the credentials are not None
        assert user is not None, "MYSQL_USER environment variable not set"
        assert password is not None, "MYSQL_PASSWORD environment variable not set"
        assert host is not None, "MYSQL_HOST environment variable not set"
        assert host_port is not None, "MYSQL_PORT environment variable not set"
        
        # Database connection parameters
        database_name = "canada-climate"
        
        # Using sqlalchemy
        connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{host_port}/{database_name}"
        engine = create_engine(connection_string)

        if test_engine_connection(engine):
            logging.debug("Engine is set up correctly.")

            # Define the query to retrieve "DLY First Year" and "DLY Last Year" for the given station ID
            query = f'SELECT `Name`, `Province`, `DLY First Year`, `DLY Last Year` FROM stations WHERE `Station ID` = {station_id} AND `DLY First Year` IS NOT NULL'
            logging.debug(f"Query: {query}")
            query = text(query)

            # Execute the query with the provided station ID
            # Execute the query and fetch the result
            with engine.connect() as connection:
                result = connection.execute(query)
                result = result.fetchone()
                logging.info(f"Result: {result}")
                return result
        else:
            logging.error("Engine setup failed.")
        # ----------------------------------------

def download_station_data(station_id, year, month, timeframe):
    # This function takes a station_id, year, month and data time frame and downloads the corresponding data file into a directory named after the station_id
    # Define the URL with the given parameters
    url = f"https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={station_id}&Year={year}&Month={month}&Day=14&timeframe={timeframe}&submit=Download+Data"

    # Define the directory path
    directory_path = os.path.join("../data", str(station_id))

    # Ensure the directory exists
    os.makedirs(directory_path, exist_ok=True)

    # Make a GET request to the URL
    response = requests.get(url)
    response.raise_for_status()

    # Get the filename from the content disposition header
    content_disposition = response.headers.get("content-disposition")
    filename = content_disposition.split("filename=")[-1].strip("\"")

    # Define the file path
    file_path = os.path.join(directory_path, filename)

    # Write the content to the file
    with open(file_path, 'wb') as file:
        file.write(response.content)

    logging.info(f"Downloaded file for station {station_id}, year {year}, month {month} to '{file_path}'.")
    return file_path

def main(args):
    # need to handle case where years are not available (0,0)
    # How do you do this with variable return values?
    name, province, first_year, last_year = get_daily_data_years(args.database_path, args.station_id)
    logging.info(f"daily data available for station {args.station_id} ({name}, {province}): {first_year} to {last_year}")
    
    # Example usage
    station_id = args.station_id
    timeframe = 2 # 1 = daily, 2 = daily, 3 = monthly
    month = 1 # doesn't matter for daily data

    for year in range(first_year, last_year + 1):
        download_station_data(station_id, year, month, timeframe)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Retrieve daily data years for a given station ID.")
    parser.add_argument('station_id', type=int, help="Station ID to look up")
    parser.add_argument('--database_path', type=str, default='../data/database.db', help="Path to the SQLite database")
    return parser.parse_args()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parse_arguments()
    main(args)