import sqlite3
import pandas as pd
import logging
import shutil
import requests
import os

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
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stations")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except sqlite3.OperationalError:
        # Return 0 if the "stations" table doesn't exist
        return 0

def convert_zero_to_null(conn):
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


def main(args):
    logging.basicConfig(level=logging.INFO)
    # Read the CSV file into a Pandas DataFrame
    # Note that some rows have missing values for the "Latitude" and "Longitude" columns
    # I'm going to delete those rows after importing the CSV data into the dataframe
    # because I want to map all the stations later
    
    # Call the function to download and process the file
    # URL of the CSV file
    url = 'https://collaboration.cmc.ec.gc.ca/cmc/climate/Get_More_Data_Plus_de_donnees/Station%20Inventory%20EN.csv'
    download_stations_file(url)

    csv_file = 'data/stations.csv'
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

    # Connect to the SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect('../data/database.db')

    # Get the number of stations before the import
    before_count = get_station_count(conn)

    # Create a table and import the CSV data into it
    df.to_sql('stations', conn, if_exists='replace', index=False)

    # Get the number of stations after the import
    after_count = get_station_count(conn)

    # Print the results
    logging.info(f"Number of stations before the import: {before_count}")
    logging.info(f"Number of new stations added: {after_count - before_count}")

    # Convert 0 values to NULL
    convert_zero_to_null(conn)

    # Close the connection
    conn.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Import stations into the database.')
    args = parser.parse_args()
    main(args)
