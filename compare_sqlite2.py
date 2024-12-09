import sqlite3
import pymonetdb
import pandas as pd
import random
import time



num_rows = 500000  # feel free to change size!

# Employee table (with department_id as foreign key)
employee_table = pd.DataFrame({
    "emp_id": range(1, num_rows + 1),
    "emp_name": [f"Employee_{i}" for i in range(1, num_rows + 1)],
    "age": [random.randint(20, 65) for _ in range(num_rows)],
    "salary": [random.randint(40000, 110000) for _ in range(num_rows)],
    "department_id": [random.randint(1, 100) for _ in range(num_rows)]  # Foreign key to departments
})

# Department table
department_table = pd.DataFrame({
    "dept_id": range(1, 101),
    "dept_name": [random.choice(["HR", "Engineering", "Marketing", "Sales"]) for _ in range(1, 101)],
    "location": [random.choice(["New York", "Florida", "Seoul", "Boston"]) for _ in range(1, 101)]
})



"""
------------------------------
        1.SQLite 
------------------------------

"""
# Load data into SQLite
sqlite_conn = sqlite3.connect(":memory:")
employee_table.to_sql("employees", sqlite_conn, index=False, if_exists="replace")
department_table.to_sql("departments", sqlite_conn, index=False, if_exists="replace")

# Perform Join Query
sqlite_start_time = time.time()
sqlite_join_query = """
SELECT e.emp_name, e.salary, d.dept_name
FROM employees e
JOIN departments d ON e.department_id = d.dept_id
WHERE d.location = 'New York' AND e.salary > 70000;
"""
sqlite_cursor = sqlite_conn.cursor()
sqlite_cursor.execute(sqlite_join_query)
sqlite_result = sqlite_cursor.fetchall()
sqlite_query_time = time.time() - sqlite_start_time

"""
------------------------------
        2. MONET DB
------------------------------

"""
# Connect to MonetDB (ensure the server is running)
monetdb_conn = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="mydb")
monetdb_cursor = monetdb_conn.cursor()

# Drop tables if they exist, with rollback handling
tables_to_drop = ["employees", "departments"]
for tbl in tables_to_drop:
    try:
        monetdb_cursor.execute(f"DROP TABLE {tbl}")
        monetdb_conn.commit()
    except:
        monetdb_conn.rollback()

# Create tables in MonetDB
try:
    monetdb_cursor.execute("""
        CREATE TABLE employees (
            emp_id INT, 
            emp_name TEXT, 
            age INT, 
            salary INT, 
            department_id INT
        )
    """)
    monetdb_cursor.execute("""
        CREATE TABLE departments (
            dept_id INT, 
            dept_name TEXT, 
            location TEXT
        )
    """)
    monetdb_conn.commit()
except:
    monetdb_conn.rollback()
    raise  # Terminate on failure

# Insert data into MonetDB tables
try:
    monetdb_cursor.executemany(
        "INSERT INTO employees VALUES (%s, %s, %s, %s, %s)", 
        employee_table.values.tolist()
    )
    monetdb_cursor.executemany(
        "INSERT INTO departments VALUES (%s, %s, %s)", 
        department_table.values.tolist()
    )
    monetdb_conn.commit()
except:
    monetdb_conn.rollback()
    raise  # Terminate on failure

# Perform Join Query
try:
    monetdb_start_time = time.time()
    monetdb_join_query = """
    SELECT e.emp_name, e.salary, d.dept_name
    FROM employees e
    JOIN departments d ON e.department_id = d.dept_id
    WHERE d.location = 'New York' AND e.salary > 80000;
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

# Close MonetDB and sqlite connection
sqlite_conn.close()

# Cleanup MonetDB
monetdb_cursor.close()
monetdb_conn.close()
