import sqlite3
import pandas as pd
import logging
import shutil
import requests
import os
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
from sqlalchemy import text
import sqlalchemy
from sqlalchemy.exc import ProgrammingError

import sys
sys.path.append('../common')  # Add common directory to Python path
from utils import test_engine_connection

# Function to download the stations file
def download_stations_file(url, directory='../data'):
    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)

    # Download the file from the URL
    response = requests.get(url)
    response.raise_for_status()

    # Paths for the downloaded file, backup file, and final file
    download_path = os.path.join(directory, 'stations.csv')
    backup_path = os.path.join(directory, 'stations.csv.bak')

    # Save the downloaded file
    with open(download_path, 'wb') as file:
        file.write(response.content)

    # Backup the downloaded file
    shutil.copy(download_path, backup_path)

    # Read the file, skipping the first three lines
    # Note: We skip the first three lines because they have header / metadata information
    # and the CSV import won't work correctly without the column names being at the first line of the file
    df = pd.read_csv(download_path, skiprows=3)

    # Save the modified file back to 'stations.csv'
    df.to_csv(download_path, index=False)

    logging.info(f"Downloaded and processed 'stations.csv' in the '{directory}' directory.")

# Function to get the count of stations from the database
def get_station_count(conn):
    # This is for the SQLite database
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stations")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except sqlite3.OperationalError:
        # Return 0 if the "stations" table doesn't exist
        return 0

# Function to get the count of stations from the database
def get_station_count3(engine):
    # This is for the MySQL database
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT COUNT(*) FROM stations"))
            count = result.scalar()
        return count
    except ProgrammingError:
        # Return 0 if the "stations" table doesn't exist
        return 0

def convert_zero_to_null(conn):
    # This is for the SQLite database
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE stations SET \"HLY First Year\" = NULL WHERE \"HLY First Year\" = 0")
        cursor.execute("UPDATE stations SET \"HLY Last Year\" = NULL WHERE \"HLY Last Year\" = 0")
        cursor.execute("UPDATE stations SET \"DLY First Year\" = NULL WHERE \"DLY First Year\" = 0")
        cursor.execute("UPDATE stations SET \"DLY Last Year\" = NULL WHERE \"DLY Last Year\" = 0")
        cursor.execute("UPDATE stations SET \"MLY First Year\" = NULL WHERE \"MLY First Year\" = 0")
        cursor.execute("UPDATE stations SET \"MLY Last Year\" = NULL WHERE \"MLY Last Year\" = 0")
        conn.commit()
        cursor.close()
    except sqlite3.OperationalError:
        pass

def convert_zero_to_null2(engine):
    # This is for the MySQL database
    try:
        with engine.connect() as connection:
            connection.execute(text("UPDATE stations SET `HLY First Year` = NULL WHERE `HLY First Year` = 0"))
            connection.execute(text("UPDATE stations SET `HLY Last Year` = NULL WHERE `HLY Last Year` = 0"))
            connection.execute(text("UPDATE stations SET `DLY First Year` = NULL WHERE `DLY First Year` = 0"))
            connection.execute(text("UPDATE stations SET `DLY Last Year` = NULL WHERE `DLY Last Year` = 0"))
            connection.execute(text("UPDATE stations SET `MLY First Year` = NULL WHERE `MLY First Year` = 0"))
            connection.execute(text("UPDATE stations SET `MLY Last Year` = NULL WHERE `MLY Last Year` = 0"))
            connection.commit()
    except Exception as e:
        logging.error(f"Error: {str(e)}")


def test_mysql_connection():
    # Get credentials from environment variables
    user = os.environ.get('MYSQL_USER')
    password = os.environ.get('MYSQL_PASSWORD')
    host = os.environ.get('MYSQL_HOST')
    host_port = os.environ.get('MYSQL_PORT')

    logging.debug(f"MYSQL_USER: {user}, MYSQL_PASSWORD: {password}, MYSQL_HOST: {host}, MYSQL_PORT: {host_port}")
    # assert that the credentials are not None
    assert user is not None, "MYSQL_USER environment variable not set"
    assert password is not None, "MYSQL_PASSWORD environment variable not set"
    assert host is not None, "MYSQL_HOST environment variable not set"
    assert host_port is not None, "MYSQL_PORT environment variable not set"
    
    # Database connection parameters
    database_name = "canada-climate"
    
    try:
        # Establish the connection
        connection = mysql.connector.connect(
            host=host,
            user=user,
            passwd=password,
            port=host_port,
            database=database_name
        )
        if connection.is_connected():
            logging.debug(f"Successfully connected to {database_name} at {host}")
            cursor = connection.cursor()
            # You can execute any queries using cursor.execute() here
            # cursor.execute("SHOW TABLES;")
        else:
            logging.error(f"Failed to connect to {database_name} at {host}")

    except Error as e:
        logging.error(f"Error: {str(e)}")

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            logging.info("MySQL connection is closed")

def main(args):
    logging.basicConfig(level=logging.DEBUG)
    # Read the CSV file into a Pandas DataFrame
    # Note that some rows have missing values for the "Latitude" and "Longitude" columns
    # I'm going to delete those rows after importing the CSV data into the dataframe
    # because I want to map all the stations later
    
    # Call the function to download and process the file
    # URL of the CSV file
    url = 'https://collaboration.cmc.ec.gc.ca/cmc/climate/Get_More_Data_Plus_de_donnees/Station%20Inventory%20EN.csv'
    download_stations_file(url)

    csv_file = '../data/stations.csv'
    df = pd.read_csv(csv_file)

    # delete the rows that have missing values for the "Latitude" and "Longitude" columns
    df = df.dropna(subset=['Latitude', 'Longitude'])

    # convert the "Latitude" and "Longitude" columns to integers
    df['Latitude'] = df['Latitude'].astype(int)
    df['Longitude'] = df['Longitude'].astype(int)

    # fill NA values with 0 for the "HLY First Year", "HLY Last Year", "DLY First Year", "DLY Last Year", "MLY First Year", and "MLY Last Year" columns
    df['HLY First Year'].fillna(0, inplace=True)
    df['HLY Last Year'].fillna(0, inplace=True)
    df['DLY First Year'].fillna(0, inplace=True)
    df['DLY Last Year'].fillna(0, inplace=True)
    df['MLY First Year'].fillna(0, inplace=True)
    df['MLY Last Year'].fillna(0, inplace=True)

    # convert the "HLY First Year", "HLY Last Year", "DLY First Year", "DLY Last Year", "MLY First Year", and "MLY Last Year" columns to integers
    df['HLY First Year'] = df['HLY First Year'].astype(int)
    df['HLY Last Year'] = df['HLY Last Year'].astype(int)
    df['DLY First Year'] = df['DLY First Year'].astype(int)
    df['DLY Last Year'] = df['DLY Last Year'].astype(int)
    df['MLY First Year'] = df['MLY First Year'].astype(int)
    df['MLY Last Year'] = df['MLY Last Year'].astype(int)

    # Define the data type for the "Climate ID" column
    # dtype = {
    #     'Climate ID': sqlalchemy.types.VARCHAR(length=16)
    # }

    # Set the "Climate ID" column as the DataFrame's index
    # df.set_index('Climate ID', inplace=True)

    # ----------------------------------------
    # Connect to the SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect('../data/database.db')

    # Get the number of stations before the import
    before_count = get_station_count(conn)

    # Create a table and import the CSV data into it
    df.to_sql('stations', conn, if_exists='replace', index=True)

    # Get the number of stations after the import
    after_count = get_station_count(conn)

    # Print the results
    logging.info(f"SQLite: Number of stations before the import: {before_count}")
    logging.info(f"SQLite: Number of new stations added: {after_count - before_count}")

    # Convert 0 values to NULL
    convert_zero_to_null(conn)

    # Close the connection
    conn.close()
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

    logging.info(f"Connecting to {database_name} at {host}...")
    # logging.info(f"Using user '{user}' and password '{password}'")
    logging.info(f"Using port {host_port}")

    # Using sqlalchemy
    connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{host_port}/{database_name}"
    engine = create_engine(connection_string)

    if test_engine_connection(engine):
        logging.info("Engine is set up correctly.")
        # Get the number of stations before the import
        before_count = get_station_count3(engine)
        # df.to_sql('stations', con=connection, if_exists='replace', index=False)
        # Note: added chucksize=100 to avoid the error "payload string too large"
        df.to_sql('stations', con=engine, index=False, if_exists='replace', chunksize=100)

        # Get the number of stations after the import
        after_count = get_station_count3(engine)

        # Print the results
        logging.info(f"MySQL: Number of stations before the import: {before_count}")
        logging.info(f"MySQL: Number of new stations added: {after_count - before_count}") 

        # Convert 0 values to NULL
        convert_zero_to_null2(engine)           
    else:
        logging.error("Engine setup failed.")
    # ----------------------------------------

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Import stations into the database.')
    args = parser.parse_args()
    main(args)
