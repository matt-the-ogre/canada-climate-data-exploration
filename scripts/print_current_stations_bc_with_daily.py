import argparse
import logging
import os
import glob
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime

from import_stations import test_engine_connection

def get_stations_with_current_daily_data():
    # returns a list of stations in BC with data for the current year
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
        table_name = 'stations'
        field_name = 'DLY Last Year'
        return_field = 'Station ID'
        province = 'BRITISH COLUMBIA'
        current_year = datetime.now().year

        # Define the query to drop the table
        # query = text(f"DROP TABLE IF EXISTS {table_name}")
        # example query
        # SELECT * FROM `stations` WHERE `Last Year` = 2023 AND `Province` = "British Columbia";
        query = text(f"SELECT `{return_field}` FROM {table_name} WHERE `{field_name}` = {current_year} AND `Province` = \"{province}\";")
        logging.debug(f"Query: {query}")
        # SELECT * FROM stations 
        # WHERE \"Last Year\" = 2023 
        # AND \"Province\" = 'BRITISH COLUMBIA'
        # """
        # Execute the query 
        with engine.connect() as connection:
            result = connection.execute(query)
            result = result.fetchall()
            flattened_result = [item[0] for item in result]
            logging.debug(f"Result: {flattened_result}")
            # return result[
        # # Execute the query
        # with engine.connect() as connection:
        #     connection.execute(query)
        #     result = connection.fetchone()
        #     logging.debug(f"Result: {result}")
        if flattened_result:
            return flattened_result
    else:
        logging.error("Engine setup failed.")
    # ----------------------------------------


def main(args):
    debug = args.debug
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    stations_with_current_daily_data = get_stations_with_current_daily_data()
    assert stations_with_current_daily_data is not None, "No stations with current daily data found"
    assert len(stations_with_current_daily_data) > 0, "No stations with current daily data found"
    assert 53478 not in stations_with_current_daily_data, "Station 53478 should not be in the list"
    return stations_with_current_daily_data

def parse_arguments():
    parser = argparse.ArgumentParser(description="Print all stations in BC with current data")
    # parser.add_argument('station_id', type=int, help="Station ID to look up")
    # parser.add_argument('--database_path', type=str, default='data/database.db', help="Path to the SQLite database")
    parser.add_argument('--debug', type=bool, default=False, help="Debug flag")
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    main(args)