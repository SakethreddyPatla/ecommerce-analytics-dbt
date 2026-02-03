import duckdb

conn = duckdb.connect('olist.db')

print("="*60)
print("DIMENSION TABLES")
print("="*60)

# Customer Segments
print("\n Customer Segments:")
segments = conn.execute("""
    SELECT 
        customer_segment,
        COUNT(*) as customers,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct
    FROM dim_customers
    GROUP BY customer_segment
    ORDER BY customers DESC
""").fetchdf()
print(segments)

# Top Product Categories
print("\n Top 10 Product Categories:")
categories = conn.execute("""
    SELECT 
        product_category_name_english as category,
        COUNT(*) as products
    FROM dim_products
    GROUP BY product_category_name_english
    ORDER BY products DESC
    LIMIT 10
""").fetchdf()
print(categories)

# Top Cities
print("\n Top 10 Cities by Customer Count:")
cities = conn.execute("""
    SELECT 
        customer_city,
        customer_state,
        COUNT(*) as customers
    FROM dim_customers
    GROUP BY customer_city, customer_state
    ORDER BY customers DESC
    LIMIT 10
""").fetchdf()
print(cities)

conn.close()