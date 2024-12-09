import sqlite3
import pymonetdb
import pandas as pd
import random
import time

num_rows = 500000  # Adjust size as needed!

# Crime table (with state_id as a foreign key)
crime_table = pd.DataFrame({
    "crime_id": range(1, num_rows + 1),
    "crime_type": [random.choice(["Theft", "Assault", "Fraud", "burglary", "Vandalism"]) for _ in range(num_rows)],
    "criminal_id": [random.randint(1, 10000) for _ in range(num_rows)],
    "severity": [random.uniform(1.0, 10.0) for _ in range(num_rows)],
    "state_id": [random.randint(1, 50) for _ in range(num_rows)]  # Foreign key to states
})

# State table
state_table = pd.DataFrame({
    "state_id": range(1, 51),
    "state_name": [random.choice(["New York", "Florida", "California", "Texas", "Boston"]) for _ in range(1, 51)],
    "region": [random.choice(["North", "South", "East", "West"]) for _ in range(1, 51)]
})

"""
------------------------------
        1.SQLite 
------------------------------
"""

# Load data into SQLite
sqlite_conn = sqlite3.connect(":memory:")
crime_table.to_sql("crime", sqlite_conn, index=False, if_exists="replace")
state_table.to_sql("states", sqlite_conn, index=False, if_exists="replace")

# Perform Join Query
sqlite_start_time = time.time()
sqlite_join_query = """
SELECT c.crime_type, c.severity, s.state_name
FROM crime c
JOIN states s ON c.state_id = s.state_id
WHERE s.state_name = 'New York' AND c.severity > 8.0;
"""
sqlite_cursor = sqlite_conn.cursor()
sqlite_cursor.execute(sqlite_join_query)
sqlite_result = sqlite_cursor.fetchall()
sqlite_query_time = time.time() - sqlite_start_time

"""
------------------------------
        2. MONETDB
------------------------------
"""

# Connect to MonetDB (ensure the server is running)
monetdb_conn = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="mydb")
monetdb_cursor = monetdb_conn.cursor()

# Drop tables if they exist, with rollback handling
tables_to_drop = ["crime", "states"]
for tbl in tables_to_drop:
    try:
        monetdb_cursor.execute(f"DROP TABLE {tbl}")
        monetdb_conn.commit()
    except:
        monetdb_conn.rollback()

# Create tables in MonetDB
try:
    monetdb_cursor.execute("""
        CREATE TABLE crime (
            crime_id INT, 
            crime_type TEXT, 
            criminal_id INT, 
            severity FLOAT, 
            state_id INT
        )
    """)
    monetdb_cursor.execute("""
        CREATE TABLE states (
            state_id INT, 
            state_name TEXT, 
            region TEXT
        )
    """)
    monetdb_conn.commit()
except:
    monetdb_conn.rollback()
    raise  # Terminate on failure

# Insert data into MonetDB tables
try:
    monetdb_cursor.executemany(
        "INSERT INTO crime VALUES (%s, %s, %s, %s, %s)", 
        crime_table.values.tolist()
    )
    monetdb_cursor.executemany(
        "INSERT INTO states VALUES (%s, %s, %s)", 
        state_table.values.tolist()
    )
    monetdb_conn.commit()
except:
    monetdb_conn.rollback()
    raise  # Terminate on failure

# Perform Join Query
try:
    monetdb_start_time = time.time()
    monetdb_join_query = """
    SELECT c.crime_type, c.severity, s.state_name
    FROM crime c
    JOIN states s ON c.state_id = s.state_id
    WHERE s.state_name = 'New York' AND c.severity > 8.0;
    """
    monetdb_cursor.execute(monetdb_join_query)
    monetdb_result = monetdb_cursor.fetchall()
    monetdb_query_time = time.time() - monetdb_start_time
    monetdb_conn.commit()
except:
    monetdb_conn.rollback()
    raise  # Terminate on failure

"""
------------------------------
        3.RESULT
------------------------------
"""

print("SQLite Join Query Result (First 5 Rows):")
print(sqlite_result[:5])
print(f"SQLite Join Query Time: {sqlite_query_time:.5f} seconds\n")

print("MonetDB Join Query Result (First 5 Rows):")
print(monetdb_result[:5])
print(f"MonetDB Join Query Time: {monetdb_query_time:.5f} seconds")

# Close SQLite connection
sqlite_conn.close()

# Cleanup MonetDB
monetdb_cursor.close()
monetdb_conn.close()
