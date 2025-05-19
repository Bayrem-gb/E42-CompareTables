import duckdb

# Connect to an in-memory DuckDB database
# To persist the database, replace ':memory:' with a file path e.g., 'my_duckdb.db'
conn = duckdb.connect(database=':memory:', read_only=False)

print("Setting up DuckDB tables: table_A and table_B...")

# Create table_A
conn.execute("""
CREATE TABLE table_A (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    value INTEGER,
    last_seen TIMESTAMP
);
""")
conn.execute("INSERT INTO table_A VALUES (1, 'Alice', 100, '2023-01-01 10:00:00');")
conn.execute("INSERT INTO table_A VALUES (2, 'Bob', 200, '2023-01-02 11:00:00');")
conn.execute("INSERT INTO table_A VALUES (3, 'Charlie', 300, '2023-01-03 12:00:00');")
conn.execute("INSERT INTO table_A VALUES (5, 'Eve_Old', 500, '2023-01-05 14:00:00');") # Row only in table_A

print("table_A created and populated.")

# Create table_B (with some differences)
conn.execute("""
CREATE TABLE table_B (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    value INTEGER,
    last_seen TIMESTAMP
);
""")
conn.execute("INSERT INTO table_B VALUES (1, 'Alice', 100, '2023-01-01 10:00:00');") # Identical to table_A
conn.execute("INSERT INTO table_B VALUES (2, 'Bob', 250, '2023-01-02 11:30:00');") # Different value and last_seen
conn.execute("INSERT INTO table_B VALUES (4, 'David', 400, '2023-01-04 13:00:00');") # Row only in table_B
conn.execute("INSERT INTO table_B VALUES (5, 'Eve_New', 550, '2023-01-05 14:30:00');") # Different name, value, last_seen for same ID

print("table_B created and populated.")

print("\nContents of table_A:")
print(conn.execute("SELECT * FROM table_A;").fetchdf())
for row in conn.execute("SELECT * FROM table_A;").fetchall():
    print(row)

print("\nContents of table_B:")
print(conn.execute("SELECT * FROM table_B;").fetchdf())
for row in conn.execute("SELECT * FROM table_B;").fetchall():
    print(row)

conn.close()
print("\nDuckDB setup complete. Tables were created in an in-memory database.")
print("You can now run compare_tables.py against 'table_A' and 'table_B'.") 