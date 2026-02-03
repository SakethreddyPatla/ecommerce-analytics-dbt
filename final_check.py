import duckdb
conn = duckdb.connect('olist.db')

print("== Complete Data Warehouse ==")
print("= All Tables =")
tables = conn.execute("SHOW TABLES").fetchdf()
for table in tables['name']:
    if table.startswith('dim_') or table.startswith('fct_'):
        count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        print(f'{table}: {count:,} rows')

print("= Star Schema =")

# Monthly Revenue
print("== Montly Revenue ==")
monthly_revenue = conn.execute("""
                               SELECT
                                d.year,
                                d.month,
                                d.month_name,
                                COUNT(DISTINCT f.order_id) AS orders,
                                ROUND(SUM(f.line_total), 2) AS revenue
                               FROM fct_order_items f
                               JOIN dim_dates d ON f.order_date = d.date_day
                               GROUP BY d.year, d.month, d.month_name
                                ORDER BY d.year, d.month
                               LIMIT 10
                                """).fetchdf()

print(monthly_revenue)

# Revenue by Category

print("== Revenue By Category ==")

rev_by_categ = conn.execute("""
                            SELECT
                                p.product_category_name_english AS product_category,
                                COUNT(*) AS item_sold,
                                ROUND(SUM(f.line_total), 2) AS revenue
                            FROM fct_order_items f
                            JOIN dim_products p ON f.product_id = p.product_id
                            GROUP BY product_category
                            ORDER BY revenue DESC
                            LIMIT 10
                            """).fetchdf()
print(rev_by_categ)

# Revenue by City
print("== Revenue by City ==")

rev_by_city = conn.execute("""
                           SELECT
                            c.customer_city,
                            c.customer_state,
                            COUNT(DISTINCT f.order_id) as orders,
                            ROUND(SUM(line_total), 2) as revenue
                           FROM fct_order_items f
                           JOIN dim_customers c ON f.customer_id = c.customer_id
                           GROUP BY c.customer_city, c.customer_state
                           ORDER BY revenue DESC
                           LIMIT 10
                           """).fetchdf()
print(rev_by_city)

# Products Sold the Most

print("== Product Sold the Most ==")
top_products = conn.execute("""
                            SELECT
                                
                                p.product_category_name_english AS category,
                                COUNT(*) AS times_sold,
                                ROUND(AVG(f.price), 2) AS avg_price
                            FROM fct_order_items f
                            JOIN dim_products p ON f.product_id = p.product_id
                            GROUP BY category
                            ORDER BY times_sold DESC
                            LIMIT 10""").fetchdf()
print(top_products)

# Top Sellers

print("== Top Sellers ==")
top_sellers = conn.execute("""
                           SELECT
                            s.seller_id,
                            s.seller_state,
                            s.seller_performance_tier,
                            COUNT(DISTINCT f.order_id) AS orders,
                            ROUND(SUM(f.line_total), 2) AS revenue
                           FROM fct_order_items f
                           JOIN dim_sellers s ON f.seller_id = s.seller_id
                           GROUP BY s.seller_id, s.seller_state, s.seller_performance_tier
                           ORDER BY revenue DESC
                           LIMIT 10 
                           """).fetchdf()
print(top_sellers)

# On Time Delivery

print("== ON-Time Delivery ==")
delivery = conn.execute("""
                        SELECT
                            delivered_on_time,
                            COUNT(*) as orders,
                            ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(), 1) as pct,
                        FROM fct_order_items
                        WHERE delivered_on_time is NOT NULL
                        GROUP by delivered_on_time
                        ORDER BY pct DESC
                        """).fetchdf()
print(delivery)

# Customer Segments
print("== Customer Segments ==")
customer_revenue = conn.execute("""
                                SELECT
                                    c.customer_segment,
                                    COUNT(DISTINCT c.customer_id) AS customers,
                                    COUNT(DISTINCT f.order_id) AS orders,
                                    ROUND(SUM(f.line_total), 2) AS total_revenue,
                                    ROUND(AVG(line_total), 2) AS avg_order_value
                                FROM fct_order_items f
                                JOIN dim_customers c ON f.customer_id = c.customer_id
                                GROUP BY c.customer_segment
                                ORDER by total_revenue
                                
                                """).fetchdf()
print(customer_revenue)

