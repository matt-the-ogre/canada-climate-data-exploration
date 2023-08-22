import sqlite3
import pandas as pd
import logging

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

def delete_stations_without_location(conn):
    try:
        cursor = conn.cursor()
        # count the stations that will be deleted before deleting them
        cursor.execute("SELECT COUNT(*) FROM stations WHERE Latitude = -99999 AND Longitude = -99999")
        count = cursor.fetchone()[0]
        logging.info(f"Number of stations without location data: {count}")
        # delete the stations that have missing location data
        cursor.execute("DELETE FROM stations WHERE Latitude = -99999 AND Longitude = -99999")
        cursor.close()
    except sqlite3.OperationalError:
        pass

def convert_zero_to_null(conn):
    try:
        cursor = conn.cursor()
        # count the stations that will be deleted before deleting them
        # cursor.execute("SELECT COUNT(*) FROM stations WHERE \"HLY First Year\" = 0")
        # logging.info(f"Number of stations with \"HLY First Year\" = 0: {cursor.fetchone()[0]}")
        cursor.execute("UPDATE stations SET \"HLY First Year\" = NULL WHERE \"HLY First Year\" = 0")
        cursor.execute("UPDATE stations SET \"HLY Last Year\" = NULL WHERE \"HLY Last Year\" = 0")
        cursor.execute("UPDATE stations SET \"DLY First Year\" = NULL WHERE \"DLY First Year\" = 0")
        cursor.execute("UPDATE stations SET \"DLY Last Year\" = NULL WHERE \"DLY Last Year\" = 0")
        cursor.execute("UPDATE stations SET \"MLY First Year\" = NULL WHERE \"MLY First Year\" = 0")
        cursor.execute("UPDATE stations SET \"MLY Last Year\" = NULL WHERE \"MLY Last Year\" = 0")
        # cursor.execute("SELECT COUNT(*) FROM stations WHERE \"HLY First Year\" IS NULL")
        # logging.info(f"Number of stations with \"HLY First Year\" = NULL: {cursor.fetchone()[0]}")
        conn.commit()
        cursor.close()
    except sqlite3.OperationalError:
        pass

logging.basicConfig(level=logging.INFO)

# Read the CSV file into a Pandas DataFrame
# Note that some rows have missing values for the "Latitude" and "Longitude" columns
# I'm going to delete those rows after importing the CSV data into the database
# because I want to map all the stations later
csv_file = 'data/stations.csv'
df = pd.read_csv(csv_file)

# fill NA values with -99999 for the "Latitude" and "Longitude" columns
df['Latitude'].fillna(-99999, inplace=True)
df['Longitude'].fillna(-99999, inplace=True)

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
conn = sqlite3.connect('database.db')

# Get the number of stations before the import
before_count = get_station_count(conn)

# Create a table and import the CSV data into it
df.to_sql('stations', conn, if_exists='replace', index=False)

# Get the number of stations after the import
after_count = get_station_count(conn)

# Print the results
logging.info(f"Number of stations before the import: {before_count}")
logging.info(f"Number of new stations added: {after_count - before_count}")

# Delete the stations that have missing location data
delete_stations_without_location(conn)

logging.info(f"Number of stations after deleting stations without location data: {get_station_count(conn)}")

# Convert 0 values to NULL
convert_zero_to_null(conn)

# Close the connection
conn.close()
