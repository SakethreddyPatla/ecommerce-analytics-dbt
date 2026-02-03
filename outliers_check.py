import duckdb
import pandas as pd
conn = duckdb.connect('olist.db')

print("="*60)
print("DATA QUALITY ANALYSIS - OLIST DATASET")
print("="*60)

print("\n" + "="*60)
print("CHECK 1: NULL VALUES")
print("="*60)
print("=== Null Value CHeck in raw_orders table ===")

null_check = conn.execute("""
                          SELECT
                            COUNT(*) AS total_rows,
                            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_id,
                            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_id,
                            SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END) as null_order_status,
                            SUM(CASE WHEN order_purchase_timestamp IS NULL THEN 1 ELSE 0 END) as null_purchase_date
                          FROM raw_orders
                          """).fetchdf()
print(null_check)

print("=== Null Value CHeck in raw_customers table ===")

null_check = conn.execute("""
                          SELECT
                            COUNT(*) AS total_rows,
                            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_id,
                            SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END) as null_customer_unique_id,
                            SUM(CASE WHEN customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END) as null_zipcode,
                            SUM(CASE WHEN customer_city IS NULL THEN 1 ELSE 0 END) as null_city,
                            SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END) as null_state
                          FROM raw_customers
                          """).fetchdf()
print(null_check)


print("=== Null Value Check in raw_order_items table ===")
null_check = conn.execute("""
                          SELECT
                            COUNT(*) AS total_rows,
                            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_id,
                            SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_product_id,
                            SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) as null_seller_id,
                            SUM(CASE WHEN shipping_limit_date IS NULL THEN 1 ELSE 0 END) as null_shipping_limit_date,
                            SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) as null_price
                          FROM raw_order_items
                          """).fetchdf()
print(null_check)

print("=== Null Value Check in raw_products table ===")
null_check = conn.execute("""
                          SELECT
                            COUNT(*) AS total_rows,
                            SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_product_id,
                            SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) as null_category,
                            SUM(CASE WHEN product_weight_g IS NULL THEN 1 ELSE 0 END) as null_product_weight,
                            SUM(CASE WHEN product_length_cm IS NULL THEN 1 ELSE 0 END) as null_product_length,
                            SUM(CASE WHEN product_height_cm IS NULL THEN 1 ELSE 0 END) as null_product_height,
                            SUM(CASE WHEN product_width_cm IS NULL THEN 1 ELSE 0 END) as null_product_width
                          FROM raw_products
                          """).fetchdf()
print(null_check)


print("=== Null Value Check in raw_sellers table ===")
null_check = conn.execute("""
                          SELECT
                            COUNT(*) AS total_rows,
                            SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) as null_seller_id,
                            SUM(CASE WHEN seller_city IS NULL THEN 1 ELSE 0 END) as null_seller_city,
                            SUM(CASE WHEN seller_state IS NULL THEN 1 ELSE 0 END) as null_seller_state
                            FROM raw_sellers
                          """).fetchdf()
print(null_check)

print("\n" + "="*60)
print("CHECK 2: DUPLICATE RECORDS")
print("="*60)

print("=== Checking for Duplicates in olist_orders_table ===")
duplicates = conn.execute("""
                          SELECT
                            order_id,
                            COUNT(*) as duplicate_count
                          FROM raw_orders
                          GROUP BY order_id
                          HAVING COUNT(*) > 1""").fetchdf()

print(f"Duplicate order_ids found: {len(duplicates)}")

if len(duplicates) > 0:
    print(duplicates.head())
else:
    print("No duplicates found!")


print("=== Checking for Duplicates in olist_customers_table ===")

duplicates = conn.execute("""
                          SELECT
                            customer_id,
                            customer_unique_id,
                            COUNT(*) as duplicate_count
                          FROM raw_customers
                          GROUP BY customer_id, customer_unique_id
                          HAVING duplicate_count > 1""").fetchdf()

if len(duplicates) > 0:
    print(duplicates.head())
else:
    print("No duplicates found!")

print("=== Checking for Duplicates in raw_products ===")

duplicates = conn.execute("""
                          SELECT
                            product_id,
                            COUNT(*) as duplicate_count
                          FROM raw_products
                          GROUP BY product_id
                          HAVING duplicate_count > 1""").fetchdf()

if len(duplicates) > 0:
    print(duplicates.head())
else:
    print("No duplicates found!")

print("=== Checking for Duplicates in raw_order_items ===")

duplicates = conn.execute("""
                          SELECT
                            order_id,
                            product_id,
                            COUNT(*) as duplicate_count
                          FROM raw_order_items
                          GROUP BY order_id, product_id
                          HAVING duplicate_count > 1""").fetchdf()

if len(duplicates) > 0:
    print(f"Number of Duplicate Records {len(duplicates)}")
else:
    print("No duplicates found!")

print("\n" + "="*60)
print("CHECK 3: ORPHANED RECORDS")
print("="*60)

print("=== Orphaned Order Items ===")

orphaned = conn.execute("""
                        SELECT
                            oi.order_id,
                            COUNT(*) AS item_count
                        FROM raw_order_items oi
                        LEFT JOIN raw_orders o ON oi.order_id = o.order_id
                        WHERE o.order_id IS NULL 
                        GROUP BY oi.order_id
                        """).fetchdf()
print(f"Order items without matching orders: {len(orphaned)}")
if len(orphaned) > 0:
    print(orphaned.head())

print("=== Do all Orders have Matching Customer ===")

orphaned = conn.execute("""
                        SELECT
                            o.customer_id,
                            COUNT(*) as orphaned_count
                        FROM raw_orders o
                        LEFT JOIN raw_customers c ON o.customer_id = c.customer_id
                        WHERE c.customer_id IS NULL
                        GROUP BY o.customer_id
                        """).fetchdf()
print(f"Order items without matching orders: {len(orphaned)}")
if len(orphaned) > 0:
    print(orphaned.head())

print("=== Do all Order Items have Matching Product ===")

orphaned = conn.execute("""
                        SELECT
                            oi.product_id,
                            COUNT(*) as orphaned_count
                        FROM raw_order_items oi
                        LEFT JOIN raw_products p ON oi.product_id = p.product_id
                        WHERE p.product_id IS NULL
                        GROUP BY oi.product_id
                        """).fetchdf()
print(f"Order items without matching orders: {len(orphaned)}")
if len(orphaned) > 0:
    print(orphaned.head())

print("=== Do all Reviews have Matching order ===")

orphaned = conn.execute("""
                        SELECT
                            r.order_id,
                            COUNT(*) as orphaned_count
                        FROM raw_order_reviews r
                        LEFT JOIN raw_orders o ON r.order_id = o.order_id
                        WHERE o.order_id IS NULL
                        GROUP BY r.order_id
                        """).fetchdf()
print(f"Order items without matching orders: {len(orphaned)}")
if len(orphaned) > 0:
    print(orphaned.head())

print("=== Checking Date Ranges ===")
date_range = conn.execute("""
                          SELECT
                            MIN(order_purchase_timestamp) AS earliest_order,
                            MAX(order_purchase_timestamp)::DATE - MIN(order_purchase_timestamp)::DATE AS days_of_data
                          FROM raw_orders
                          """).fetchdf()
print(date_range)

future_dates = conn.execute("""
    SELECT COUNT(*) as future_orders
    FROM raw_orders
    WHERE order_purchase_timestamp::DATE > CURRENT_TIMESTAMP
""").fetchone()[0]

print(f"\nOrders in the future: {future_dates}")

error_order_dates = conn.execute("""
    SELECT COUNT(*) as error_orders
    FROM raw_orders
    WHERE order_purchase_timestamp::DATE > order_delivered_customer_date::DATE
""").fetchdf()

print(error_order_dates)

print("=== Price Range Check in Order Items ===")
price_range = conn.execute("""
                           SELECT
                            MAX(price) as max_price,
                            MIN(price) as min_price,
                            ROUND(AVG(price),2) as avg_price,
                            stddev(price) as stddev_price,
                            mode(price) as mode_price,
                            median(price) as median_price
                            FROM raw_order_items
                            
                           """).fetchdf()
print(price_range)

print("=== Negative Prices ===")
neg_prices = conn.execute("""
                          SELECT
                            price,
                            
                          FROM raw_order_items
                          WHERE price<0
                          """).fetchdf()
print(f"Number of Negative Values: {len(neg_prices)}")

print("\n" + "="*60)
print("CHECK 4: DATA RANGES & OUTLIERS")
print("="*60)

print("=== Price Range Check in Order Payments ===")
price_range = conn.execute("""
                           SELECT
                            MAX(payment_value) as max_payment,
                            MIN(payment_value) as min_payment,
                            ROUND(AVG(payment_value),2) as avg_payment,
                            stddev(payment_value) as stddev_payment,
                            mode(payment_value) as mode_payment,
                            median(payment_value) as median_payment
                            FROM raw_order_payments
                            
                           """).fetchdf()
print(price_range)

print("=== Negative Prices ===")
neg_prices = conn.execute("""
                          SELECT
                            payment_value,
                            
                          FROM raw_order_payments
                          WHERE payment_value<0
                          """).fetchdf()
print(f"Number of Negative Values: {len(neg_prices)}")

print("=== Orders Without Items ===")

orders_no_items = conn.execute("""
                              SELECT
                                o.order_id,
                                oi.order_id,
                                o.order_status
                              FROM raw_orders o
                              LEFT JOIN raw_order_items oi ON o.order_id = oi.order_id
                              WHERE oi.order_id IS NULL
                              """).fetchdf()
print(f"Orders with no items: {len(orders_no_items)}")
print(orders_no_items.head())

print("=== Orders Delivered But Null Dates ===")

orders_del = conn.execute("""
                          SELECT
                            order_id,
                            order_status,
                            order_delivered_customer_date,
                            
                          FROM raw_orders
                          WHERE order_status = 'delivered'
                            AND order_delivered_customer_date IS NULL
                          
                          """).fetchdf()
print(f'Delivered orders without dates: {len(orders_del)}')
if len(orders_del) > 0:
    print(orders_del.head())

print("=== Any Products with Price 0 ===")

products_with_price_zero = conn.execute("""
                                        SELECT
                                            COUNT(*) as count
                                        FROM raw_order_items
                                        WHERE price = 0
                                        """).fetchdf()
print(products_with_price_zero)

print("\n" + "="*60)
print("SUMMARY")
print("="*60)


# Initialize issues tracking
issues_found = []

print("="*60)
print("DATA QUALITY ANALYSIS - OLIST DATASET")
print("="*60)


print("\n" + "="*60)
print("SUMMARY OF FINDINGS")
print("="*60)

# Collect all issues (manually from findings)

# 1. NULL product categories
null_product_category = 610  
if null_product_category > 0:
    issues_found.append({
        'Check': 'NULL Values',
        'Table': 'raw_products',
        'Issue': 'NULL product_category_name',
        'Count': null_product_category,
        'Severity': 'Medium',
        'Recommendation': 'Classify as "Unknown" category or exclude from category analysis'
    })

# 2. NULL product dimensions
null_product_dimensions = 2  # From your output
if null_product_dimensions > 0:
    issues_found.append({
        'Check': 'NULL Values',
        'Table': 'raw_products',
        'Issue': 'NULL product dimensions (weight/length/height/width)',
        'Count': null_product_dimensions,
        'Severity': 'Low',
        'Recommendation': 'Use average dimensions or mark as estimated'
    })

# 3. Duplicate order items
duplicate_order_items = 0  

# 4. Orders without items
orders_no_items_count = len(orders_no_items)
if orders_no_items_count > 0:
    issues_found.append({
        'Check': 'Business Logic',
        'Table': 'raw_orders',
        'Issue': 'Orders with no order_items',
        'Count': orders_no_items_count,
        'Severity': 'High',
        'Recommendation': 'Investigate if these are cancelled orders or data loading issues'
    })

# 5. Delivered orders without delivery dates
orders_del_count = len(orders_del)
if orders_del_count > 0:
    issues_found.append({
        'Check': 'Business Logic',
        'Table': 'raw_orders',
        'Issue': 'Delivered orders without delivery date',
        'Count': orders_del_count,
        'Severity': 'High',
        'Recommendation': 'Mark these as "status error" or use estimated delivery date'
    })

# 6. Products with price 0
price_zero_count = products_with_price_zero['count'].iloc[0]
if price_zero_count > 0:
    issues_found.append({
        'Check': 'Data Range',
        'Table': 'raw_order_items',
        'Issue': 'Products with price = 0',
        'Count': price_zero_count,
        'Severity': 'Medium',
        'Recommendation': 'Investigate if these are free items or data errors'
    })

# Display Summary
if len(issues_found) == 0:
    print("\n No critical data quality issues found! Data is relatively clean.")
else:
    print(f"\n Total Issues Found: {len(issues_found)}\n")
    
    # Convert to DataFrame
    df_summary = pd.DataFrame(issues_found)
    
    # Display full table
    print("="*100)
    print("DETAILED ISSUES:")
    print("="*100)
    print(df_summary.to_string(index=False))
    
    # Summary by severity
    print("\n" + "="*60)
    print("BY SEVERITY:")
    print("="*60)
    severity_counts = df_summary['Severity'].value_counts()
    for severity, count in severity_counts.items():
        icon = "🔴" if severity == "High" else "🟡" if severity == "Medium" else "🟢"
        print(f"{icon} {severity}: {count} issues")
    
    # Summary by check type
    print("\n" + "="*60)
    print("BY CHECK TYPE:")
    print("="*60)
    check_counts = df_summary['Check'].value_counts()
    for check, count in check_counts.items():
        print(f"• {check}: {count} issues")
    
    # Summary by table
    print("\n" + "="*60)
    print("BY TABLE:")
    print("="*60)
    table_counts = df_summary['Table'].value_counts()
    for table, count in table_counts.items():
        print(f"• {table}: {count} issues")
    
    # Save to CSV
    df_summary.to_csv('data_quality_issues.csv', index=False)
    print("\n💾 Full report saved to: data_quality_issues.csv")
    
    # Key Recommendations
    print("\n" + "="*60)
    print("🎯 KEY RECOMMENDATIONS:")
    print("="*60)
    high_priority = df_summary[df_summary['Severity'] == 'High']
    if len(high_priority) > 0:
        for idx, row in high_priority.iterrows():
            print(f"\n{idx+1}. {row['Issue']}")
            print(f"   → {row['Recommendation']}")

print("\n" + "="*60)
print("ANALYSIS COMPLETE ✅")
print("="*60)

conn.close()
