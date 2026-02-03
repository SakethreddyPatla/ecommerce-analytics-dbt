import duckdb
conn = duckdb.connect('olist.db')

dates  = conn.execute("""
                      SELECT
                        MIN(order_purchase_timestamp)::DATE AS earliest,
                        MAX(order_purchase_timestamp)::DATE AS latest
                      FROM raw_orders
                      """).fetchdf()
print(dates)