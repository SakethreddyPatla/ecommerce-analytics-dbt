WITH customer_order AS (
SELECT
    customer_id,
    COUNT(DISTINCT order_id) as total_orders,
    MIN(order_purchase_timestamp) as first_order_date,
    MAX(order_purchase_timestamp) as last_order_date
FROM raw_orders
WHERE order_status NOT IN ('unavailable', 'canceled')
GROUP BY customer_id
),

final AS (
    SELECT  
        c.customer_id,
        c.customer_zip_code_prefix as customer_zipcode,
        c.customer_city,
        c.customer_state,
        COALESCE(co.total_orders, 0) as lifetime_orders,
        co.first_order_date,
        co.last_order_date,
        CASE
            WHEN COALESCE(co.total_orders, 0) = 0 THEN 'New'
            WHEN co.total_orders = 1 THEN 'One-time'
            WHEN co.total_orders BETWEEN 2 AND 5 THEN 'Regular'
            WHEN co.total_orders > 5 THEN 'VIP'
        END AS customer_segment
    FROM raw_customers c
    LEFT JOIN customer_order co 
        ON c.customer_id = co.customer_id
)

SELECT * FROM final