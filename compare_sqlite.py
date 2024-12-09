import sqlite3
import time
import pymonetdb
import pandas as pd
import random

num_rows = 100000  # Adjust size as needed!

# Create a Crime-like dataset
crime_table = pd.DataFrame({
    "Crime_id": range(1, num_rows + 1),
    "Category_name": [random.choice(["Theft", "Assault", "Fraud", "burglary", "Vandalism"]) for _ in range(num_rows)],
    "Criminal_id": [random.randint(1, 10000) for _ in range(num_rows)],
    "State": [random.choice(["New York", "California", "Texas", "Florida", "Boston"]) for _ in range(num_rows)],
    "Severity": [random.uniform(1.0, 10.0) for _ in range(num_rows)]
})

"""
------------------------------
        1.SQLite 
------------------------------
"""

# Load data into SQLite
sqlite_conn = sqlite3.connect(":memory:")
sqlite_conn.execute("""
    CREATE TABLE crime (
        Crime_id INTEGER, 
        Category_name TEXT, 
        Criminal_id INTEGER, 
        State TEXT, 
        Severity FLOAT
    )
""")
crime_table.to_sql("crime", sqlite_conn, index=False, if_exists="replace")

# Measure query time in SQLite
sqlite_start_time = time.time()
sqlite_query = "SELECT State, AVG(Severity) AS Avg_Severity FROM crime GROUP BY State"
sqlite_cursor = sqlite_conn.cursor()
sqlite_cursor.execute(sqlite_query)
sqlite_result = sqlite_cursor.fetchall()
sqlite_query_time = time.time() - sqlite_start_time

"""
------------------------------
       2.MONETDB 
------------------------------
"""

# Connect to MonetDB (ensure MonetDB server is running)
monetdb_conn = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="mydb")
monetdb_cursor = monetdb_conn.cursor()

# Drop the table if it exists with rollback handling
try:
    monetdb_cursor.execute("DROP TABLE crime")
    monetdb_conn.commit()
except pymonetdb.exceptions.OperationalError:
    monetdb_conn.rollback()

# Create table in MonetDB
try:
    monetdb_cursor.execute("""
        CREATE TABLE crime (
            Crime_id INTEGER, 
            Category_name TEXT, 
            Criminal_id INTEGER, 
            State TEXT, 
            Severity FLOAT
        )
    """)
    monetdb_conn.commit()
except pymonetdb.exceptions.OperationalError:
    monetdb_conn.rollback()
    raise

# Insert data into MonetDB with rollback handling
try:
    monetdb_cursor.executemany(
        "INSERT INTO crime VALUES (%s, %s, %s, %s, %s)",
        crime_table.values.tolist()
    )
    monetdb_conn.commit()
except pymonetdb.exceptions.OperationalError:
    monetdb_conn.rollback()
    raise

# Measure query time in MonetDB
try:
    monetdb_start_time = time.time()
    monetdb_query = "SELECT State, AVG(Severity) AS Avg_Severity FROM crime GROUP BY State"
    monetdb_cursor.execute(monetdb_query)
    monetdb_result = monetdb_cursor.fetchall()
    monetdb_query_time = time.time() - monetdb_start_time
    monetdb_conn.commit()
except pymonetdb.exceptions.OperationalError:
    monetdb_conn.rollback()
    raise

"""
------------------------------
       3. RESULT
------------------------------
"""

print("SQLite Query Result:")
print(sqlite_result)
print(f"SQLite Query Time: {sqlite_query_time:.5f} seconds\n")

print("MonetDB Query Result:")
print(monetdb_result)
print(f"MonetDB Query Time: {monetdb_query_time:.5f} seconds")

# Close connections
sqlite_conn.close()
monetdb_cursor.close()
monetdb_conn.close()
