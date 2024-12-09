import sqlite3
import time
import pymonetdb
import pandas as pd
import random

# Prepare data for SQLite and MonetDB
num_rows = 100000  # Number of rows
example_table = pd.DataFrame({
    "ID": range(1, num_rows + 1),
    "Name": [f"Employee_{i}" for i in range(1, num_rows + 1)],
    "Age": [random.randint(20, 65) for _ in range(num_rows)],
    "Salary": [random.randint(40000, 110000) for _ in range(num_rows)],
    "Department": [random.choice(["HR", "Engineering", "Marketing", "Sales"]) for _ in range(num_rows)],
    "Performance_Star": [random.uniform(1.0, 5.0) for _ in range(num_rows)]
})

"""
------------------------------
        1.SQLite 
------------------------------

"""


sqlite_conn = sqlite3.connect(":memory:")
sqlite_conn.execute("""
    CREATE TABLE employees (
        ID INTEGER, 
        Name TEXT, 
        Age INTEGER, 
        Salary INTEGER, 
        Department TEXT, 
        Performance_Star FLOAT
    )
""")
example_table.to_sql("employees", sqlite_conn, index=False, if_exists="replace")

# Measure query time in SQLite
sqlite_start_time = time.time()
sqlite_query = "SELECT Department, AVG(Salary) AS Avg_Salary FROM employees GROUP BY Department"
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
        Performance_Star FLOAT
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

print("SQLite Query Result:")
print(sqlite_result)
print(f"SQLite Query Time: {sqlite_query_time:.4f} seconds\n")

print("MonetDB Query Result:")
print(monetdb_result)
print(f"MonetDB Query Time: {monetdb_query_time:.4f} seconds")

# Close connections
sqlite_conn.close()
monetdb_cursor.close()
monetdb_conn.close()
