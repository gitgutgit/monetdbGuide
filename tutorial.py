import pymonetdb

# datbase connect
connection = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="mydb")

# cursor
cursor = connection.cursor()

# create table and insert
cursor.execute('CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR(50))')
cursor.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")

# print data
cursor.execute('SELECT * FROM test_table')
rows = cursor.fetchall()
print(rows)  # [(1, 'Alice'), (2, 'Bob')]

# close
cursor.close()
connection.close()
