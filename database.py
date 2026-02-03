import duckdb

conn = duckdb.connect('olist.db')

# Show all tables
print("=== All Tables in Database ===")
tables = conn.execute("SHOW TABLES").fetchdf()
print(tables)