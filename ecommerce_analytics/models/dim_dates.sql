WITH date_spine AS (
    SELECT
        DATE '2016-01-01' + INTERVAL (n) DAY AS date_day
    FROM (
        SELECT UNNEST(GENERATE_SERIES(0, 1460)) AS n
    )
),

final AS (
    SELECT
        date_day,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        
        -- Day name
        CASE EXTRACT(DAYOFWEEK FROM date_day)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_name,
        
        -- Month name
        CASE EXTRACT(MONTH FROM date_day)
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name,
        
        -- Is weekend
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend
        
    FROM date_spine
)

SELECT * FROM final
