import sqlite3
import logging
import argparse
import os
import requests
import time

from sqlalchemy import create_engine
from sqlalchemy import text

from import_stations import test_engine_connection
from get_climate_id_from_station_id import get_climate_id_for_station

def get_daily_data_years_sqlite(database_path, station_id):
    # Check if the database file is present
    if not os.path.exists(database_path):
        logging.error("Error: '{database_path}' file not found. Aborting update.")
        return

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
        return (0, 0)
    
def get_daily_data_years(station_id):
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

    logging.info(f"Engine: {engine}")

    if test_engine_connection(engine):
        logging.debug("Engine is set up correctly.")
        table_name = 'stations'

        # SQL query to retrieve "DLY First Year" and "DLY Last Year" for the given station ID
        query = text(f"SELECT `Name`, `Province`, `DLY First Year`, `DLY Last Year` FROM {table_name} WHERE `Station ID` = {station_id} AND `DLY First Year` IS NOT NULL")
        logging.debug(f"Query: {query}")

        # Execute the query 
        with engine.connect() as connection:
            result = connection.execute(query)
            result = result.fetchall()
            logging.debug(f"Result: {result[0]}")
            # If a result was found, return it as a tuple; otherwise, return (0, 0)
            if result:
                return result[0]
            else:
                return (0,0,0,0)
    else:
        logging.error("Engine setup failed.")
    # ----------------------------------------

def download_station_data(station_id, year, month, timeframe):
    # check if we have already downloaded the CSV file for this station, year, month, and timeframe
    climate_id = get_climate_id_for_station(station_id)
    logging.info(f"Climate ID: {climate_id}")
    filename = f"en_climate_daily_BC_{climate_id}_{year}_P1D.csv"
    directory_path = os.path.join("../data", str(station_id))
    file_path = os.path.join(directory_path, filename)
    if os.path.exists(file_path):
        logging.info(f"File '{file_path}' already exists. Skipping download.")
        return file_path
    else:
        logging.info(f"File '{file_path}' does not exist. Downloading.")
        # Define the URL with the given parameters
        url = f"https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={station_id}&Year={year}&Month={month}&Day=14&timeframe={timeframe}&submit=Download+Data"

        # Define the directory path
        directory_path = os.path.join("../data", str(station_id))

        # Ensure the directory exists
        os.makedirs(directory_path, exist_ok=True)

        # Make a GET request to the URL
        st_get_url = time.process_time()
        response = requests.get(url)
        response.raise_for_status()
        et_get_url = time.process_time()
        logging.info(f"Time to get URL: {et_get_url - st_get_url} seconds")

        # Get the filename from the content disposition header
        content_disposition = response.headers.get("content-disposition")
        filename = content_disposition.split("filename=")[-1].strip("\"")

        # Define the file path
        file_path = os.path.join(directory_path, filename)

        # Write the content to the file
        with open(file_path, 'wb') as file:
            file.write(response.content)

        logging.info(f"Downloaded file for station {station_id}, year {year}, month {month} to '{file_path}'.")
        time.sleep(0.2) # sleep for 1 second to avoid overloading the server
        return file_path

def download_all_station_data(station_id, debug=False):
    name, province, first_year, last_year = get_daily_data_years(station_id)
    logging.info(f"daily data available for station {station_id} ({name}, {province}): {first_year} to {last_year}")
    
    assert first_year != 0, f"Error: no daily data available for station {station_id} ({name}, {province})."  
    assert last_year != 0, f"Error: no daily data available for station {station_id} ({name}, {province})."
    assert province == "BRITISH COLUMBIA", f"Error: no daily data available for station {station_id} ({name}, {province})."

    if first_year == 0 or last_year == 0:
        logging.error(f"Error: no daily data available for station {station_id} ({name}, {province}).")
        return
    
    # Example usage
    timeframe = 2 # 1 = hourly, 2 = daily, 3 = monthly
    month = 12 # doesn't matter for daily data

    for year in range(first_year, last_year + 1):
        download_station_data(station_id, year, month, timeframe)

def main(args):
    if args.debug:
        debug = True
        logging.basicConfig(level=logging.DEBUG)
        logging.info("Debug logging enabled.")
    else:
        debug = False
        logging.basicConfig(level=logging.INFO)
    download_all_station_data(args.station_id)



def parse_arguments():
    parser = argparse.ArgumentParser(description="Retrieve daily data years for a given station ID.")
    parser.add_argument('station_id', type=int, help="Station ID to look up")
    # parser.add_argument('--database_path', type=str, default='../data/database.db', help="Path to the SQLite database")
    parser.add_argument('--debug', type=bool, default=False, help="Enable debug logging")
    return parser.parse_args()

if __name__ == '__main__':
    # logging.basicConfig(level=logging.INFO)
    args = parse_arguments()
    main(args)