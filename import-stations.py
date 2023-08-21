import sqlite3
import pandas as pd

# Function to get the count of stations from the database
def get_station_count(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stations")
    count = cursor.fetchone()[0]
    cursor.close()
    return count

# Read the CSV file into a Pandas DataFrame
csv_file = 'data/stations.csv'
df = pd.read_csv(csv_file)

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('database.db')

# Get the number of stations before the import
before_count = get_station_count(conn)

# Create a table and import the CSV data into it
df.to_sql('stations', conn, if_exists='replace', index=False)

# Get the number of stations after the import
after_count = get_station_count(conn)

# Print the results
print(f"Number of stations before the import: {before_count}")
print(f"Number of new stations added: {after_count - before_count}")

# Close the connection
conn.close()
