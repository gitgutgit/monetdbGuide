import duckdb
import time
import pymonetdb
import pandas as pd
import random


num_rows = 100000  # feel free to change
example_table = pd.DataFrame({
    "ID": range(1, num_rows + 1),
    "Name": [f"Employee_{i}" for i in range(1, num_rows + 1)],
    "Age": [random.randint(20, 60) for _ in range(num_rows)],
    "Salary": [random.randint(30000, 120000) for _ in range(num_rows)],
    "Department": [random.choice(["HR", "Engineering", "Marketing", "Sales"]) for _ in range(num_rows)],
    "Performance_Score": [random.uniform(1.0, 5.0) for _ in range(num_rows)]
})

"""
------------------------------
      1. Duck DB
------------------------------

"""
# Load data into DuckDB
duckdb_conn = duckdb.connect(":memory:")
duckdb_conn.execute("""
    CREATE TABLE employees (
        ID INTEGER, 
        Name TEXT, 
        Age INTEGER, 
        Salary INTEGER, 
        Department TEXT, 
        Performance_Score FLOAT
    )
""")
duckdb_conn.register("example_table", example_table)
duckdb_conn.execute("INSERT INTO employees SELECT * FROM example_table")

# Measure query time in DuckDB
duckdb_start_time = time.time()
duckdb_query = "SELECT Department, AVG(Salary) AS Avg_Salary FROM employees GROUP BY Department"
duckdb_result = duckdb_conn.execute(duckdb_query).fetchdf()
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
    monetdb_cursor.execute("DROP TABLE employees")
except pymonetdb.exceptions.OperationalError:
    # Ignore error if table does not exist
    pass

# Create table in MonetDB
monetdb_cursor.execute("""
    CREATE TABLE employees (
        ID INTEGER, 
        Name TEXT, 
        Age INTEGER, 
        Salary INTEGER, 
        Department TEXT, 
        Performance_Score FLOAT
    )
""")

# Insert data into MonetDB
for index, row in example_table.iterrows():
    monetdb_cursor.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", tuple(row))

# Measure query time in MonetDB
monetdb_start_time = time.time()
monetdb_query = "SELECT Department, AVG(Salary) AS Avg_Salary FROM employees GROUP BY Department"
monetdb_cursor.execute(monetdb_query)
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
