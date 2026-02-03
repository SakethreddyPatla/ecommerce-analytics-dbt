import duckdb
import pandas as pd
conn = duckdb.connect('olist.db')
# Loading data to database
print("loading data into database")

tables = ['customers', 'geolocation', 'order_items', 'order_payments', 'order_reviews', 'orders', 'products', 'sellers']
for table in tables:
    df = pd.read_csv(f'olist_data/olist_{table}_dataset.csv')
    df = df.convert_dtypes()
    conn.execute(f'CREATE OR REPLACE TABLE raw_{table} AS SELECT * FROM df')
    print(f'loaded {table} : {len(df)} rows')

df = pd.read_csv('olist_data/product_category_name_translation.csv')
df = df.convert_dtypes()
conn.execute(f'CREATE OR REPLACE TABLE raw_product_category_name_translation AS SELECT * FROM df')
print(f'loaded raw_product_category_name_translation : {len(df)} rows')
