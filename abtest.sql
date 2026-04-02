-- 1.数据处理与清洗
-- 1.1字段名更正
CREATE TABLE olist_orders_dataset_right AS
SELECT
    C1 AS order_id,
    C2 AS customer_id,
    C3 AS order_status,
    C4 AS order_purchase_timestamp,
    C5 AS order_approved_at,
    C6 AS order_delivered_carrier_date,
    C7 AS order_delivered_customer_date,
    C8 AS order_estimated_delivery_date
FROM olist_orders_dataset;


-- 1.2订单数据清洗（去NULL + 只保留已完成订单）
CREATE TABLE orders_clean AS
SELECT *
FROM olist_orders_dataset_right
WHERE
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND order_status = 'delivered';


-- 1.3支付表清洗（去异常金额）
CREATE TABLE payments_clean AS
SELECT *
FROM olist_order_payments_dataset
WHERE
    order_id IS NOT NULL
    AND payment_value > 0;


-- 1.4用户表清洗（去空值）
CREATE TABLE customers_clean AS
SELECT *
FROM olist_customers_dataset
WHERE
    customer_id IS NOT NULL
    AND customer_unique_id IS NOT NULL;



-- 2.1计算核心指标：GMV、订单数、付费用户、客单价
SELECT
    COUNT(DISTINCT o.order_id) AS 有效订单数,
    COUNT(DISTINCT c.customer_unique_id) AS 付费用户数,
    ROUND(SUM(p.payment_value), 2) AS 总GMV,
    ROUND(SUM(p.payment_value) / COUNT(DISTINCT o.order_id), 2) AS 客单价
FROM orders_clean o
JOIN payments_clean p
    ON o.order_id = p.order_id
JOIN customers_clean c
    ON o.customer_id = c.customer_id;
-- GMV：15422461.77 客单价：159.86

-- 2.2.计算复购率（干净、正常、无异常）
SELECT
    COUNT(*) AS 总付费用户,
    SUM(CASE WHEN order_times >= 2 THEN 1 ELSE 0 END) AS 复购用户数,
    ROUND(
        SUM(CASE WHEN order_times >= 2 THEN 1 ELSE 0 END) / COUNT(*),
        4
    ) AS 复购率
FROM (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS order_times
    FROM orders_clean o
    JOIN customers_clean c
        ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
) a;
-- 复购率：3%

-- 复购率较低，根据ABComm 2024 巴西电商报告，互补品类优惠券可以使平均复购率提升幅度达绝对2%

-- 找出需要优惠券的用户
WITH user_buy_times AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS total_orders
    FROM orders_clean o
    JOIN customers_clean c
        ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
),
order_with_category AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_delivered_customer_date,
        i.product_id,
        p.product_category_name
    FROM orders_clean o
    JOIN olist_order_items_dataset i
        ON o.order_id = i.order_id
    JOIN olist_products_dataset p
        ON i.product_id = p.product_id
    WHERE
        o.order_delivered_customer_date IS NOT NULL
        AND p.product_category_name IS NOT NULL
)

SELECT DISTINCT
    c.customer_unique_id AS user_id,
    c.customer_city AS city,
    c.customer_state AS state,
    o.order_id,
    o.order_delivered_customer_date AS receive_time,
    o.product_category_name AS last_buy_category,
    '互补品类定向复购券' AS coupon_type,
    '5% 复购提升目标' AS target
FROM order_with_category o
JOIN customers_clean c
    ON o.customer_id = c.customer_id
JOIN user_buy_times u
    ON c.customer_unique_id = u.customer_unique_id
WHERE
    u.total_orders = 1  
ORDER BY 1;
