WITH sellers AS (
    SELECT * FROM raw_sellers
),
seller_performance AS (
    SELECT 
        oi.seller_id,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        SUM(oi.price) AS total_revenue,
        COUNT(*) AS total_items_sold,
        ROUND(AVG(oi.price), 2) AS avg_item_price
    FROM raw_order_items oi
    JOIN raw_orders o ON oi.order_id = o.order_id
    WHERE o.order_status NOT IN ('unavailable', 'canceled')
    GROUP BY oi.seller_id
),

final AS (
    SELECT
        s.seller_id,
        s.seller_zip_code_prefix AS seller_zipcode,
        s.seller_city,
        s.seller_state,

        COALESCE(sp.total_orders, 0) as total_orders,
        COALESCE(sp.total_items_sold, 0) as total_items_sold,
        COALESCE(sp.total_revenue, 0) as total_revenue,
        COALESCE(sp.avg_item_price, 0) as avg_item_price,
        
        CASE 
            WHEN COALESCE(sp.total_orders, 0) = 0 THEN 'Inactive'
            WHEN sp.total_orders < 10 THEN 'New'
            WHEN sp.total_orders BETWEEN 10 AND 50 THEN 'Average'
            WHEN sp.total_orders BETWEEN 51 AND 200 THEN 'Good'
            WHEN sp.total_orders > 200 THEN 'Top Performer'
        END as seller_performance_tier
    FROM sellers s
    LEFT JOIN seller_performance sp ON s.seller_id = sp.seller_id
)

SELECT * FROM final