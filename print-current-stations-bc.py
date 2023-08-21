import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('database.db')

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Query to retrieve all records from the "stations" table where "Last Year" is 2023 and "Province" is "BRITISH COLUMBIA"
query = """
SELECT * FROM stations 
WHERE \"Last Year\" = 2023 
AND \"Province\" = 'BRITISH COLUMBIA'
"""

# Execute the query
cursor.execute(query)

# Fetch all the matching records
records = cursor.fetchall()

# Print the records
for record in records:
    print(record)

# Close the cursor and connection
cursor.close()
conn.close()
