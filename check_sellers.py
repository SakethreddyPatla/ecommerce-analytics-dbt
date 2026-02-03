import duckdb

conn = duckdb.connect('olist.db')
print("=== Seller Dimension Analysis ===")

print("== Seller Performance ==")
tier = conn.execute("""
                     SELECT
                        seller_performance_tier,
                        COUNT(*) AS seller_count,
                        ROUND(AVG(total_orders), 2) AS avg_orders,
                        ROUND(AVG(total_revenue), 2) AS avg_revenue
                     FROM dim_sellers
                     GROUP BY seller_performance_tier
                     ORDER BY
                        CASE seller_performance_tier
                            WHEN 'Top Performer' THEN 1
                            WHEN 'Good' THEN 2
                            WHEN 'Average' THEN 3
                            WHEN 'New' THEN 4
                            WHEN 'Inactive' THEN 5
                        END
                     """).fetchdf()
print(tier)

print("== Top 10 Sellers by Revenue ==")

top_rev = conn.execute("""
                       SELECT
                        
                        seller_city,
                        seller_state,
                        total_orders,
                        ROUND(total_revenue, 2) as total_revenue,
                        seller_performance_tier
                       FROM dim_sellers
                       ORDER BY total_revenue DESC
                       LIMIT 10
                       """).fetchdf()
print(top_rev)

print("== Top 10 states by seller count ==")

top_state = conn.execute("""
                        SELECT
                            seller_state,
                            COUNT(*) as sellers,
                            SUM(total_orders) as total_orders,
                            ROUND(SUM(total_revenue),2) AS total_revenue
                        FROM dim_sellers
                        GROUP BY seller_state
                        ORDER by sellers DESC
                        LIMIT 10
                         """
).fetchdf()
print(top_state)
conn.close()