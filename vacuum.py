import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('database.db')

# Execute the VACUUM command
conn.execute('VACUUM')

# Commit the changes and close the connection
conn.commit()
conn.close()
