import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('../data/database.db')

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Query to retrieve all records from the "stations" table where "Last Year" is 2023 and "Province" is "BRITISH COLUMBIA"
query = """
SELECT * FROM stations 
WHERE \"HLY Last Year\" = 2023 
AND \"Province\" = 'BRITISH COLUMBIA'
"""

# Execute the query
cursor.execute(query)

# Get column names
column_names = [description[0] for description in cursor.description]

# Print the column names as a comma-separated string
print(','.join(column_names))

# Fetch all the matching records
records = cursor.fetchall()

# Print the records as comma-separated values
for record in records:
    print(','.join(map(str, record)))

# Close the cursor and connection
cursor.close()
conn.close()
