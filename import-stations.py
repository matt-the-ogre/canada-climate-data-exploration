import sqlite3
import pandas as pd

# Read the CSV file into a Pandas DataFrame
csv_file = 'data/stations.csv'
df = pd.read_csv(csv_file)

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('database.db')

# Create a table and import the CSV data into it
df.to_sql('stations', conn, if_exists='replace', index=False)

# Close the connection
conn.close()
