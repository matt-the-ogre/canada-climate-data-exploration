# Utility functions used across multiple services

# put these lines at the top of another file that needs to use these functions

# import sys
# sys.path.append('../common')  # Add common directory to Python path
# from utils import test_engine_connection

import logging
from sqlalchemy import text

def my_utils_function():
    pass

# Path: common/utils.py

# Pass this function an engine connection and it will return True if the connection is successful, False otherwise
def test_engine_connection(engine):
    # This is for the MySQL database
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT VERSION()"))
            version = result.scalar()
            logging.debug(f"MySQL version: {version}")
            return True
    except Exception as e:
        logging.error(f"Connection error: {str(e)}")
        return False
