import duckdb

conn = duckdb.connect('olist.db')


print("DATE DIMENSION CHECK")


# Date range
print("Date Range:")
range_check = conn.execute("""
    SELECT 
        MIN(date_day) as earliest,
        MAX(date_day) as latest,
        COUNT(*) as total_days
    FROM dim_dates
""").fetchdf()
print(range_check)

# Sample dates
print("Sample Dates:")
sample = conn.execute("""
    SELECT 
        date_day,
        year,
        month,
        month_name,
        day_name,
        is_weekend
    FROM dim_dates
    WHERE date_day BETWEEN '2016-09-01' AND '2016-09-10'
    ORDER BY date_day
""").fetchdf()
print(sample)

# Weekend vs Weekday count
print("Weekend vs Weekday:")
weekend = conn.execute("""
    SELECT 
        is_weekend,
        COUNT(*) as days
    FROM dim_dates
    GROUP BY is_weekend
""").fetchdf()
print(weekend)

conn.close()