import sqlite3
import logging
import argparse
import os
import requests

def get_hourly_data_years(database_path, station_id):
    # Connect to the SQLite database
    conn = sqlite3.connect(database_path)

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    # SQL query to retrieve "HLY First Year" and "HLY Last Year" for the given station ID
    query = """
    SELECT "Name", "Province", "HLY First Year", "HLY Last Year"
    FROM stations
    WHERE "Station ID" = ? AND "HLY First Year" IS NOT NULL
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

def download_station_data(station_id, year, month, timeframe):
    # Define the URL with the given parameters
    url = f"https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={station_id}&Year={year}&Month={month}&Day=14&timeframe={timeframe}&submit=Download+Data"

    # Define the directory path
    directory_path = os.path.join("data", str(station_id), str(year))

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


def main(args):
    name, province, first_year, last_year = get_hourly_data_years(args.database_path, args.station_id)
    logging.info(f"Hourly data available for station {args.station_id} ({name}, {province}): {first_year} to {last_year}")
    
    # Example usage
    station_id = args.station_id
    # year = first_year
    # month = 1
    timeframe = 1 # 1 = hourly, 2 = daily, 3 = monthly

    for year in range(first_year, last_year + 1):
        for month in range(1, 13):
            download_station_data(station_id, year, month, timeframe)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Retrieve hourly data years for a given station ID.")
    parser.add_argument('station_id', type=int, help="Station ID to look up")
    parser.add_argument('--database_path', type=str, default='data/database.db', help="Path to the SQLite database")
    return parser.parse_args()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parse_arguments()
    main(args)