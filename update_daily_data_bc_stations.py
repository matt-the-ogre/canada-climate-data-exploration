import sqlite3
import pandas as pd
import requests
import os
from datetime import datetime
import logging

from get_daily_data_by_station import download_station_data


def get_climate_id_for_station(conn,station_id):
    cursor = conn.cursor()
    query = f"SELECT \"Climate ID\" FROM stations WHERE \"Station ID\" = {station_id}"
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    return result[0]

def get_most_recent_date(conn, table_name):
    cursor = conn.cursor()
    query = f"SELECT MAX(date_column) FROM {table_name}"
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    return result[0]

def download_new_data(climate_id, station_id, start_date):
    # Construct the URL based on the climate_id, station_id, and start_date
    url = f"https://example.com/path/to/data?climateID={climate_id}&stationID={station_id}&startDate={start_date}"
    
    # Download the CSV file
    response = requests.get(url)
    response.raise_for_status()
    
    # Path to the downloaded CSV file
    file_path = os.path.join("data", f"{climate_id}_{station_id}_new_data.csv")
    
    # Save the CSV file
    with open(file_path, 'wb') as file:
        file.write(response.content)
    
    return file_path

def update_daily_data_table(conn, table_name, csv_file_path):
    # Read the new CSV data
    df = pd.read_csv(csv_file_path)
    
    # Append the new data to the corresponding table
    df.to_sql(table_name, conn, if_exists='replace', index=False)

def main():
    station_ids = [26, 40, 52, 65]

    # Connect to the SQLite database
    conn = sqlite3.connect('database.db')

    for station_id in station_ids:
        # Assume a function to get the corresponding Climate ID for the given station ID
        climate_id = get_climate_id_for_station(station_id)
        logging.info(f"Climate ID: {climate_id}")

        # Table name for the corresponding Climate ID
        table_name = str(climate_id)+"_"+str(station_id)+"_daily"

        # Get the most recent date in the table
        most_recent_date = get_most_recent_date(conn, table_name)

        # Download the new CSV data from the most recent date to today
        csv_file_path = download_new_data(climate_id, station_id, most_recent_date)
        year = datetime.now().year
        month = datetime.now().month
        timeframe = 2 # Daily data
        csv_file_path = download_station_data(station_id, year, month, timeframe)

        # Update the Climate ID daily table with the new CSV data
        update_daily_data_table(conn, table_name, csv_file_path)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
