# connect to the sqlite database at /home/builder-love/dagster_logs/schedules/schedules.db

import sqlite3

conn = sqlite3.connect('/home/builder-love/dagster_logs/schedules/schedules.db')

# print all the tables in the database
print("Tables in the database:")
print(conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall())

# print the first 10 rows of the jobs table
print("First 10 rows of the jobs table:")
print(conn.execute("SELECT * FROM jobs LIMIT 10;").fetchall())

# print the first 10 rows of the instigators table
print("First 10 rows of the instigators table:")
print(conn.execute("SELECT * FROM instigators LIMIT 10;").fetchall())

