import pymonetdb

# 데이터베이스 연결
connection = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="mydb")

# 커서 생성
cursor = connection.cursor()

# 테이블 생성 및 데이터 삽입
cursor.execute('CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR(50))')
cursor.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")

# 데이터 조회
cursor.execute('SELECT * FROM test_table')
rows = cursor.fetchall()
print(rows)  # [(1, 'Alice'), (2, 'Bob')]

# 자원 해제
cursor.close()
connection.close()
