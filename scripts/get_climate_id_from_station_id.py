import argparse
import logging
import os
from sqlalchemy import create_engine
from sqlalchemy import text

# from import_stations import test_engine_connection
import sys
sys.path.append('../common')  # Add common directory to Python path
from utils import test_engine_connection

def get_climate_id_for_station(station_id):
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
        field_name = 'Station ID'
        return_field = 'Climate ID'

        # Define the query to drop the table
        # query = text(f"DROP TABLE IF EXISTS {table_name}")
        # example query
        # SELECT * FROM `stations` WHERE `Last Year` = 2023 AND `Province` = "British Columbia";
        query = text(f"SELECT `{return_field}` FROM {table_name} WHERE `{field_name}` = {station_id};")
        logging.debug(f"Query: {query}")

        # Execute the query 
        with engine.connect() as connection:
            result = connection.execute(query)
            result = result.fetchall()
            flattened_result = [item[0] for item in result]
            flattened_result = flattened_result[0]
            logging.debug(f"Result: {flattened_result}")
        if flattened_result:
            return flattened_result
    else:
        logging.error("Engine setup failed.")
    # ----------------------------------------




def main(args):
    get_climate_id_for_station(args.station_id)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Retrieve the Climate ID for a given station ID.")
    parser.add_argument('station_id', type=int, help="Station ID to look up")
    parser.add_argument('--debug', type=bool, default=False, help="Debug flag")
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    if args.debug:
        debug = True
        logging.basicConfig(level=logging.DEBUG)
    else:
        debug = False
        logging.basicConfig(level=logging.INFO)
    main(args)