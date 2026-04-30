SELECT
    YEAR(order_purchase_timestamp) AS year,
    QUARTER(order_purchase_timestamp) AS quarter,
    COUNT(DISTINCT o.customer_id) AS new_customer_count,
    ROUND(SUM(oi.price + oi.freight_value), 2) AS new_customer_gmv,
    ROUND(SUM(oi.price + oi.freight_value) / COUNT(DISTINCT o.customer_id), 2) AS avg_revenue_per_new_customer
FROM olist_orders_dataset o
JOIN olist_order_items_dataset oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
GROUP BY year, quarter
ORDER BY year, quarter;

SELECT
    YEAR(o.order_purchase_timestamp) AS year,
    QUARTER(o.order_purchase_timestamp) AS quarter,
    p.product_category_name,
    COUNT(DISTINCT o.order_id) AS orders,
    ROUND(SUM(oi.price + oi.freight_value), 2) AS category_gmv
FROM olist_orders_dataset o
JOIN olist_order_items_dataset oi ON o.order_id = oi.order_id
JOIN olist_products_dataset p ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered'
  AND (YEAR(o.order_purchase_timestamp) = 2018 AND QUARTER(o.order_purchase_timestamp) IN (2,3))
GROUP BY year, quarter, product_category_name
ORDER BY year, quarter, category_gmv DESC;

SELECT
    YEAR(o.order_purchase_timestamp) AS year,
    QUARTER(o.order_purchase_timestamp) AS quarter,
    AVG(r.review_score) AS avg_review_score,
    SUM(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END) / COUNT(*) * 100 AS delay_rate_pct
FROM olist_orders_dataset o
JOIN olist_order_reviews_dataset r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY year, quarter
ORDER BY year, quarter;

SELECT
    YEAR(o.order_purchase_timestamp) AS year,
    QUARTER(o.order_purchase_timestamp) AS quarter,
    c.customer_state,
    COUNT(DISTINCT o.customer_id) AS new_customers,
    ROUND(SUM(oi.price + oi.freight_value), 2) AS state_gmv
FROM olist_orders_dataset o
JOIN olist_customers_dataset c ON o.customer_id = c.customer_id
JOIN olist_order_items_dataset oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
  AND (YEAR(o.order_purchase_timestamp) = 2018 AND QUARTER(o.order_purchase_timestamp) IN (2,3))
GROUP BY year, quarter, customer_state
ORDER BY year, quarter, new_customers DESC;