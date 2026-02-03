import duckdb
import os

conn = duckdb.connect('olist.db')

# Create export folder
os.makedirs('powerbi_export', exist_ok=True)

print("="*60)
print("EXPORTING TABLES FOR POWER BI")
print("="*60)

# Export dimension and fact tables
tables = ['dim_customers', 'dim_products', 'dim_sellers', 'dim_dates', 'fct_order_items']

for table in tables:
    print(f"\nExporting {table}...")
    
    conn.execute(f"""
        COPY (SELECT * FROM {table}) 
        TO 'powerbi_export/{table}.csv' 
        (HEADER, DELIMITER ',')
    """)
    
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"{table}: {count:,} rows exported")

conn.close()

print("\n" + "="*60)
print("ALL TABLES EXPORTED SUCCESSFULLY!")
print("="*60)
print(f"\nLocation: C:\\Users\\saket\\olist_analytics\\powerbi_export\\")
print("\nFiles ready for Power BI:")
for table in tables:
    print(f"  - {table}.csv")