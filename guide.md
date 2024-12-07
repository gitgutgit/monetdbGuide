1. Start new farm
shell> monetdbd create /path/to/mydbfarm

shell> monetdbd start /path/to/mydbfarm

2. Create and release a Database (default in maintenance mode)

 shell> monetdb create yourdbname
shell> monetdb release yourdbname

3. Connect to the database with the following credentials:

shell> mclient -u monetdb -d yourdbname


default user: monetdb

default password: monetdb



test query : sql> SELECT 'hello world'; 
to quit: \q