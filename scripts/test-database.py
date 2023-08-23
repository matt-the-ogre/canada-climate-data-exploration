import os
import mysql.connector
from mysql.connector import Error
import logging

# Get credentials from environment variables
user = os.environ.get('MYSQL_USER')
password = os.environ.get('MYSQL_PASSWORD')
host = os.environ.get('MYSQL_HOST')
host_port = os.environ.get('MYSQL_PORT')

# Database connection parameters
database_name = "canada-climate"

logging.basicConfig(level=logging.DEBUG)
logging.info(f"Connecting to {database_name} at {host}...")
logging.info(f"Using user '{user}' and password '{password}'")
logging.info(f"Using port {host_port}")

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
        print(f"Successfully connected to {database_name} at {host}")
        cursor = connection.cursor()
        # You can execute any queries using cursor.execute() here
        # Example: cursor.execute("SHOW TABLES;")
    else:
        print(f"Failed to connect to {database_name} at {host}")

except Error as e:
    print(f"Error: {str(e)}")

finally:
    if connection and connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
