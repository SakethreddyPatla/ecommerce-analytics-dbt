import duckdb

conn = duckdb.connect('olist.db')
# Exploring Data
#print("=== All Tables in Database ===")

all_tables = conn.execute("""
                          SHOW TABLES""").fetchdf()
#print(all_tables)

#print("=== Customer Count by State ===")
results = conn.execute("""
                       SELECT
                        customer_state,
                        customer_city,
                        COUNT(DISTINCT customer_id) AS customer_count
                       FROM raw_customers
                       GROUP BY customer_state, customer_city
                       ORDER BY customer_count DESC
                       LIMIT 5""").fetchdf()
#print(results)

#print("=== Tables Info ===")
 
for table in all_tables['name']:

    #print(f'table : {table}')
    results = conn.execute(f'DESCRIBE {table}').fetchdf()
    #print(results)

#print("=== Total Count of Order Status ===")

results = conn.execute("""
                       SELECT
                        order_status,
                        COUNT(DISTINCT customer_id) AS total_customers
                       FROM raw_orders
                       GROUP BY order_status
                       ORDER BY total_customers DESC""").fetchdf()
#print(results)

print("=== Top 10 Category by Revenue===")

results = conn.execute("""
                       SELECT
                        p.product_category_name,
                        SUM(oi.price) as total_revenue
                       FROM raw_order_items oi
                       LEFT JOIN raw_products p ON oi.product_id = p.product_id
                       LEFT JOIN raw_orders o ON oi.order_id = o.order_id
                       WHERE order_status = 'delivered'
                       GROUP BY p.product_category_name
                       ORDER BY total_revenue DESC
                       LIMIT 10
                       """).fetchdf()
print(results)

conn.close()