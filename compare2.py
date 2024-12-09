import duckdb
import time
import pymonetdb
import pandas as pd
import random

num_rows = 100000   # Adjust size as needed!
E_QUERY = "SELECT State, AVG(Severity) AS Avg_Severity FROM crime GROUP BY State"
# Create a Crime-like dataset
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
        1.DUCKDB 
------------------------------
"""
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
       2. MONETDB
------------------------------
"""
monetdb_conn = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="mydb")
monetdb_cursor = monetdb_conn.cursor()

# Drop tables if they exist
tables_to_drop = ["crime"]
for tbl in tables_to_drop:
    try:
        monetdb_cursor.execute(f"DROP TABLE {tbl}")
        monetdb_conn.commit()
    except:
        monetdb_conn.rollback()

# Create table and insert data in MonetDB
try:
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
    monetdb_conn.commit()
except:
    monetdb_conn.rollback()
    raise  # Terminate on failure

# Insert data into MonetDB
try:
    monetdb_cursor.executemany(
        "INSERT INTO crime VALUES (%s, %s, %s, %s, %s, %s)",
        crime_table.values.tolist()
    )
    monetdb_conn.commit()
except:
    monetdb_conn.rollback()
    raise

# Measure query time in MonetDB
try:
    monetdb_start_time = time.time()
    monetdb_cursor.execute(E_QUERY)
    monetdb_result = monetdb_cursor.fetchall()
    monetdb_query_time = time.time() - monetdb_start_time
    monetdb_conn.commit()
except:
    monetdb_conn.rollback()
    raise

"""
------------------------------
       3. RESULT
------------------------------
"""
print("DuckDB Query Result:")
print(duckdb_result)
print(f"DuckDB Query Time: {duckdb_query_time:.5f} seconds\n")

print("MonetDB Query Result:")
print(monetdb_result)
print(f"MonetDB Query Time: {monetdb_query_time:.5f} seconds")

# Close MonetDB connection
monetdb_cursor.close()
monetdb_conn.close()
