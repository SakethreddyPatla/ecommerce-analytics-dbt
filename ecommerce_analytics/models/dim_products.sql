WITH products AS (
    SELECT * FROM raw_products
),

translations AS (
    SELECT * FROM raw_product_category_name_translation
),

final AS (
    SELECT
    p.product_id,
    COALESCE(p.product_category_name, 'Uncategorized') AS product_category_name,
    COALESCE(t.product_category_name_english, 'Uncategorized') AS product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
FROM products p
LEFT JOIN translations t
    ON p.product_category_name = t.product_category_name
)

SELECT * FROM final