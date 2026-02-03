import duckdb

conn = duckdb.connect('olist.db')

print("="*60)
print("ALL TABLES IN DATABASE")
print("="*60)

# Show all tables
tables = conn.execute("SHOW TABLES").fetchdf()
print(tables)

print("\n" + "="*60)
print("TABLE COUNTS")
print("="*60)

# Count rows in each table
for table_name in tables['name']:
    count = conn.execute(f"SELECT COUNT(*) as count FROM {table_name}").fetchone()[0]
    print(f"{table_name}: {count:,} rows")

conn.close()