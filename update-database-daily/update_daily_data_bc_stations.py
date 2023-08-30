import sqlite3
import pandas as pd
import requests
import os
from datetime import datetime
import logging
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime

from get_daily_data_by_station import download_station_data

# import sys
# sys.path.append('../scripts')

# from import_stations import test_mysql_connection, test_engine_connection
import sys
sys.path.append('../common')  # Add common directory to Python path
from utils import test_engine_connection

debug = False
database_to_use = "mysql"

def get_climate_id_for_station(conn_engine, station_id):
    if database_to_use == "sqlite":
        cursor = conn_engine.cursor()
        query = f"SELECT \"Climate ID\" FROM stations WHERE \"Station ID\" = {station_id}"
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        return result[0]
    elif database_to_use == "mysql":
        # Define the query to retrieve "DLY First Year" and "DLY Last Year" for the given station ID
        query = f"SELECT `Climate ID` FROM stations WHERE `Station ID` = {station_id}"
        logging.debug(f"Query: {query}")
        query = text(query)

        # Execute the query with the provided station ID
        with conn_engine.connect() as connection:
            result = connection.execute(query)
            result = result.fetchone()
            logging.debug(f"Result: {result[0]}")
            return result[0]

def get_most_recent_date(conn_engine, table_name):
    if database_to_use == "mysql":
        # Define the query to retrieve "DLY First Year" and "DLY Last Year" for the given station ID
        # query = f"SELECT `Climate ID` FROM stations WHERE `Station ID` = {station_id}"
        # trying to find the most recent date in the table for which there is data quality information
        query1 = f"SELECT `Year`, `Month`, `Day` FROM {table_name} WHERE `Data Quality` IS NOT NULL ORDER BY `Date/Time` DESC LIMIT 1;"

        logging.debug(f"Query1: {query1}")
        query1 = text(query1)

        # Execute the query with the provided station ID
        # Execute the query and fetch the result
        with conn_engine.connect() as connection:
            result = connection.execute(query1)
            result = result.fetchone()
            logging.debug(f"Result: {result}")
            if result is None:
                query2 = f"SELECT `Year`, `Month`, `Day` FROM {table_name} WHERE `Max Temp (°C)` IS NOT NULL ORDER BY `Date/Time` DESC LIMIT 1;"
                logging.debug(f"Query2: {query2}")
                query2 = text(query2)
                
                result = connection.execute(query2)
                result = result.fetchone()
                logging.debug(f"Result: {result}")
            logging.debug(f"Most recent date: {result}")
            return result

    elif database_to_use == "sqlite":
        cursor = conn_engine.cursor()
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
        logging.debug(f"Most recent date: {result}")
        cursor.close()
        return (result)

def update_daily_data_table(conn_engine, table_name, csv_file_path):
    logging.info(f"Updating table {table_name} with data from {csv_file_path}")
    # Read the new CSV data
    df = pd.read_csv(csv_file_path)
    if database_to_use == "mysql":
        # Note: added chunksize=100 to avoid the error "payload string too large"
        # df.to_sql(table_name, con=conn_engine, if_exists='append', index=True, chunksize=100, dtype=dtype)
        df.to_sql(table_name, con=conn_engine, if_exists='append', index=True, chunksize=100)

    elif database_to_use == "sqlite":
        # Append the new data to the corresponding table
        # Note I think we want append, not replace
        # I'll need to deal with duplicate dates though
        df.to_sql(table_name, conn_engine, if_exists='append', index=False)

def update_database_daily(conn_engine):
    if debug:
        station_ids = [26, 40, 52, 65, 10853]
        # station_ids = [10853]
    else:
        station_ids = [26,40,27226,52,10853,65,68,76,46728,77,10943,87,54641,78,6810,96,97,6811,6812,114,51337,53479,138,145,52979,152,153,155,29411,29733,181,52941,189,51319,45627,208,209,6813,132,225,235,244,8040,257,260,261,262,271,272,8045,8261,8260,276,277,52960,283,48888,6816,309,27449,320,6817,323,45267,327,45807,52018,45788,336,348,10901,8062,358,6819,52940,8064,50108,48648,374,49388,54338,52978,383,384,8265,8264,6822,387,389,393,8258,6823,395,396,54978,6824,409,48693,424,53438,442,51037,446,27121,470,48628,51658,450,46747,536,8092,30494,568,574,51538,55363,617,48370,48248,50169,48688,48108,650,50820,50308,54238,706,707,43443,43500,747,50368,49088,766,43723,810,823,833,6830,844,853,834,837,6831,870,50228,51357,51442,888,6833,924,925,49588,50517,951,27388,51598,44927,27122,48369,51117,49408,1022,1041,1046,50269,1056,52958,46987,1070,6834,979,1091,1100,1105,1106,52938,1115,6840,1137,1142,6839,31067,6838,1095,50818,51818,1180,1186,1194,6842,52959,48708,1237,53424,8214,52398,51558,53638,51423,42203,1309,1340,1216,6843,1364,52980,54318,1367,49492,49548,6844,1400,52939,48208,50837,55198,48870,51517,54818,49628,50819,54098,6845]

    logging.info(f"Station IDs: {station_ids}")

    for station_id in station_ids:
        # Assume a function to get the corresponding Climate ID for the given station ID
        climate_id = get_climate_id_for_station(conn_engine, station_id)
        logging.debug(f"Climate ID: {climate_id}")

        # Table name for the corresponding Climate ID
        table_name = str(climate_id)+"_"+str(station_id)+"_daily"
        logging.debug(f"Table name: {table_name}")

        # Get the most recent date in the table
        most_recent_date = get_most_recent_date(conn_engine, table_name)
        logging.debug(f"Most recent date: {most_recent_date}")
        # Sometimes we get a table with no temperature data, so we need to check for that
        # example is station_id 10853
        # for now, just skip it
        if most_recent_date is None:
            most_recent_date = (datetime.now().year,datetime.now().month)
            logging.debug(f"Most recent date defaults to now: {most_recent_date}")
        if most_recent_date is not None:
            assert most_recent_date is not None, "Most recent date is None"
            year = most_recent_date[0]
            month = most_recent_date[1]
            # year = datetime.now().year
            # month = datetime.now().month
            timeframe = 2 # Daily data
            csv_file_path = download_station_data(station_id, year, month, timeframe)
            logging.debug(f"CSV file path: {csv_file_path}")
            # Update the Climate ID daily table with the new CSV data
            update_daily_data_table(conn_engine, table_name, csv_file_path)
        else:
            logging.error("Most recent date is None")

def main():
    if database_to_use == "mysql":
        # Connect to the MySQL database

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
        
        # Using sqlalchemy
        connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}:{host_port}/{database_name}"
        logging.debug(f"Connection string: {connection_string}")
        # engine = create_engine(connection_string)
        try:
            engine = create_engine(connection_string)
            # assert test_mysql_connection(engine), "Engine setup failed."
            # assert engine isn't None, "Engine setup failed."
            assert engine is not None, "Engine setup failed."
            
            if test_engine_connection(engine):
                update_database_daily(engine)
            else:
                logging.error("Engine setup failed.")
        except Exception as e:
            logging.error(f"Connection error: {e}")
        engine.dispose()
    elif database_to_use == "sqlite":
        # Connect to the SQLite database
        conn = sqlite3.connect('../data/database.db')
        update_database_daily(conn)
        conn.close()

if __name__ == "__main__":
    logging_format = "%(asctime)s - %(levelname)s - %(message)s"
    if debug:
        logging.basicConfig(format=logging_format, level=logging.DEBUG)
    else:
        logging.basicConfig(format=logging_format, level=logging.INFO)
    main()
