import duckdb
import time
import pymonetdb
import pandas as pd
import random

num_rows = 100000   # Adjust size as needed!
E_QUERY = "SELECT State, AVG(Severity) AS Avg_Severity FROM crime GROUP BY State"

# Crime table data
crime_table = pd.DataFrame({
    "Crime_id": range(1, num_rows + 1),
    "Crime_type": [random.choice(["Theft", "Assault", "Fraud", "burglary", "Vandalism"]) for _ in range(num_rows)],
    "Criminal_id": [random.randint(1, 10000) for _ in range(num_rows)],
    "Severity": [random.uniform(1.0, 10.0) for _ in range(num_rows)],
    "State": [random.choice(["New York", "California", "Texas", "Florida", "Boston"]) for _ in range(num_rows)],
    "Reported_by": [random.choice(["Officer_A", "Officer_B", "Officer_C", "Officer_D"]) for _ in range(num_rows)]
})

"""
------------------------------
      1. Duck DB
------------------------------
"""

# Load data into DuckDB
duckdb_conn = duckdb.connect(":memory:")
duckdb_conn.execute("""
    CREATE TABLE crime (
        Crime_id INTEGER, 
        Crime_type TEXT, 
        Criminal_id INTEGER, 
        Severity FLOAT, 
        State TEXT, 
        Reported_by TEXT
    )
""")
duckdb_conn.register("crime_table", crime_table)
duckdb_conn.execute("INSERT INTO crime SELECT * FROM crime_table")

# Measure query time in DuckDB
duckdb_start_time = time.time()
duckdb_result = duckdb_conn.execute(E_QUERY).fetchdf()
duckdb_query_time = time.time() - duckdb_start_time

"""
------------------------------
       2. Monet DB
------------------------------
"""

# Connect to MonetDB (ensure MonetDB server is running)
monetdb_conn = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="mydb")
monetdb_cursor = monetdb_conn.cursor()

# Drop the table if it exists
try:
    monetdb_cursor.execute("DROP TABLE crime")
except pymonetdb.exceptions.OperationalError:
    # Ignore error if table does not exist
    pass

# Create table in MonetDB
monetdb_cursor.execute("""
    CREATE TABLE crime (
        Crime_id INTEGER, 
        Crime_type TEXT, 
        Criminal_id INTEGER, 
        Severity FLOAT, 
        State TEXT, 
        Reported_by TEXT
    )
""")

# Insert data into MonetDB
for index, row in crime_table.iterrows():
    monetdb_cursor.execute("INSERT INTO crime VALUES (%s, %s, %s, %s, %s, %s)", tuple(row))

# Measure query time in MonetDB
monetdb_start_time = time.time()
monetdb_cursor.execute(E_QUERY)
monetdb_result = monetdb_cursor.fetchall()
monetdb_query_time = time.time() - monetdb_start_time

"""
------------------------------
       3. RESULT
------------------------------
"""

print("DuckDB Query Result:")
print(duckdb_result)
print(f"DuckDB Query Time: {duckdb_query_time:.4f} seconds\n")

print("MonetDB Query Result:")
print(monetdb_result)
print(f"MonetDB Query Time: {monetdb_query_time:.4f} seconds")

# Close MonetDB connection
monetdb_cursor.close()
monetdb_conn.close()
