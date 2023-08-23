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
    # query = f"SELECT MAX(date_column) FROM {table_name}"
    query1 = f"""
    SELECT "Year", "Month", "Day"
    FROM \"{table_name}\" WHERE "Data Quality" IS NOT NULL
    ORDER BY "Date/Time" DESC
    LIMIT 1;
    """
    cursor.execute(query1)
    result = cursor.fetchone()
    if result is None:
        query2 = f"""
        SELECT "Year", "Month", "Day"
        FROM \"{table_name}\" WHERE "Max Temp (°C)" IS NOT NULL
        ORDER BY "Date/Time" DESC
        LIMIT 1;
        """
        cursor.execute(query2)
        result = cursor.fetchone()
    # logging.info(f"Most recent date: {result[0]}-{result[1]}-{result[2]}")
    logging.debug(f"Most recent date: {result}")
    cursor.close()
    return (result)

def download_new_data(climate_id, station_id, start_date):
    # Construct the URL based on the climate_id, station_id, and start_date
    url = f"https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={station_id}&Year={year}&Month={month}&Day=14&timeframe={timeframe}&submit=Download+Data"
    
    # Download the CSV file
    response = requests.get(url)
    response.raise_for_status()
    
    # Path to the downloaded CSV file
    file_path = os.path.join("../data", f"{climate_id}_{station_id}_new_data.csv")
    
    # Save the CSV file
    with open(file_path, 'wb') as file:
        file.write(response.content)
    
    return file_path

def update_daily_data_table(conn, table_name, csv_file_path):
    # Read the new CSV data
    df = pd.read_csv(csv_file_path)
    
    # Append the new data to the corresponding table
    df.to_sql(table_name, conn, if_exists='replace', index=False)

def update_database_daily():
    station_ids = [26, 40, 52, 65]
    # station_ids = [26,40,27226,52,10853,65,68,76,46728,77,10943,87,54641,78,6810,96,97,6811,6812,114,51337,53479,138,145,52979,152,153,155,29411,29733,181,52941,189,51319,45627,208,209,6813,132,225,235,244,8040,257,260,261,262,271,272,8045,8261,8260,276,277,52960,283,48888,6816,309,27449,320,6817,323,45267,327,45807,52018,45788,336,348,10901,8062,358,6819,52940,8064,50108,48648,374,49388,54338,52978,383,384,8265,8264,6822,387,389,393,8258,6823,395,396,54978,6824,409,48693,424,53438,442,51037,446,27121,470,48628,51658,450,46747,536,8092,30494,568,574,51538,55363,617,48370,48248,50169,48688,48108,650,50820,50308,54238,706,707,43443,43500,747,50368,49088,766,43723,810,823,833,6830,844,853,834,837,6831,870,50228,51357,51442,888,6833,924,925,49588,50517,951,27388,51598,44927,27122,48369,51117,49408,1022,1041,1046,50269,1056,52958,46987,1070,6834,979,1091,1100,1105,1106,52938,1115,6840,1137,1142,6839,31067,6838,1095,50818,51818,1180,1186,1194,6842,52959,48708,1237,53424,8214,52398,51558,53638,51423,42203,1309,1340,1216,6843,1364,52980,54318,1367,49492,49548,6844,1400,52939,48208,50837,55198,48870,51517,54818,49628,50819,54098,6845]

    # Connect to the SQLite database
    conn = sqlite3.connect('../data/database.db')

    for station_id in station_ids:
        # Assume a function to get the corresponding Climate ID for the given station ID
        climate_id = get_climate_id_for_station(conn, station_id)
        logging.info(f"Climate ID: {climate_id}")

        # Table name for the corresponding Climate ID
        table_name = str(climate_id)+"_"+str(station_id)+"_daily"
        logging.debug(f"Table name: {table_name}")

        # Get the most recent date in the table
        most_recent_date = get_most_recent_date(conn, table_name)
        logging.info(f"Most recent date: {most_recent_date}")
        # Download the new CSV data from the most recent date to today
        # csv_file_path = download_new_data(climate_id, station_id, most_recent_date)
        year = most_recent_date[0]
        month = most_recent_date[1]
        # year = datetime.now().year
        # month = datetime.now().month
        timeframe = 2 # Daily data
        csv_file_path = download_station_data(station_id, year, month, timeframe)
        logging.info(f"CSV file path: {csv_file_path}")
        # Update the Climate ID daily table with the new CSV data
        update_daily_data_table(conn, table_name, csv_file_path)

    # Close the connection
    conn.close()

def main():
    update_database_daily()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    # logging.basicConfig(level=logging.INFO)
    main()
