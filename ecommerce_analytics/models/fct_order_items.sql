WITH order_items AS (
    SELECT * FROM raw_order_items
),

orders AS (
    SELECT * FROM raw_orders
),

final AS (
    SELECT 
        -- Primary key
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        
        -- Foreign keys to dimensions
        o.customer_id,
        DATE(o.order_purchase_timestamp) AS order_date,
        
        -- Order attributes
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        
        -- Item attributes
        oi.shipping_limit_date,
        
        -- Measures
        oi.price,
        oi.freight_value,
        oi.price + oi.freight_value AS line_total,
        
        -- Delivery performance (calculated)
        CASE 
            WHEN o.order_delivered_customer_date IS NOT NULL 
                 AND o.order_estimated_delivery_date IS NOT NULL
            THEN CASE 
                WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date 
                THEN TRUE 
                ELSE FALSE 
            END
            ELSE NULL
        END AS delivered_on_time
        
    FROM order_items oi
    INNER JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_status NOT IN ('unavailable', 'canceled')  
)

SELECT * FROM final