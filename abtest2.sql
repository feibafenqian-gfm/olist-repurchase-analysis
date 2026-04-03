-- 1. 数据清洗
CREATE TABLE orders_clean AS
SELECT
    C1 AS order_id,
    C2 AS customer_id,
    C3 AS order_status,
    C4 AS order_purchase_timestamp,
    C5 AS order_approved_at,
    C6 AS order_delivered_carrier_date,
    C7 AS order_delivered_customer_date,
    C8 AS order_estimated_delivery_date
FROM olist_orders_dataset
WHERE C1 <> 'order_id'
  AND C3 = 'delivered';


CREATE TABLE customers_clean AS
SELECT *
FROM olist_customers_dataset
WHERE customer_unique_id IS NOT NULL;


CREATE TABLE items_clean AS
SELECT
    order_id,
    product_id,
    seller_id,
    CAST(price AS DECIMAL(10,2)) AS price,
    CAST(freight_value AS DECIMAL(10,2)) AS freight_value
FROM olist_order_items_dataset
WHERE price IS NOT NULL;


CREATE TABLE payments_clean AS
SELECT
    order_id,
    payment_type,
    CAST(payment_value AS DECIMAL(10,2)) AS payment_value
FROM olist_order_payments_dataset
WHERE payment_value IS NOT NULL;

-- 2.用户画像构建RFM
WITH last_order AS (
    SELECT MAX(order_purchase_timestamp) AS max_date FROM orders_clean
),
user_rfm AS (
    SELECT
        c.customer_unique_id AS user_id,
        DATEDIFF((SELECT max_date FROM last_order), MAX(o.order_purchase_timestamp)) AS recency,
        COUNT(DISTINCT o.order_id) AS frequency,
        SUM(p.payment_value) AS monetary
    FROM orders_clean o
    JOIN customers_clean c ON o.customer_id = c.customer_id
    JOIN payments_clean p ON o.order_id = p.order_id
    GROUP BY c.customer_unique_id
),
rfm_score AS (
    SELECT
        user_id,
        100 - PERCENT_RANK() OVER (ORDER BY recency) * 100 AS r_score,
        PERCENT_RANK() OVER (ORDER BY frequency) * 100 AS f_score,
        PERCENT_RANK() OVER (ORDER BY monetary) * 100 AS m_score
    FROM user_rfm
)
SELECT
    user_id,
    ROUND(r_score, 2) AS r_score,
    ROUND(f_score, 2) AS f_score,
    ROUND(m_score, 2) AS m_score,
    CASE
        WHEN r_score >= 70 AND f_score >= 70 AND m_score >= 70 THEN '重要价值用户'
        WHEN r_score < 30 AND f_score >= 70 AND m_score >= 70 THEN '重要唤回用户'
        WHEN r_score >= 70 AND f_score < 30 AND m_score >= 70 THEN '重要发展用户'
        WHEN r_score < 30 AND f_score < 30 AND m_score >= 70 THEN '重要挽留用户'
        WHEN r_score >= 70 AND f_score >= 70 AND m_score < 30 THEN '忠实用户'
        WHEN r_score >= 70 AND f_score < 30 AND m_score < 30 THEN '新用户'
        WHEN r_score < 30 AND f_score >= 70 AND m_score < 30 THEN '一般用户'
        ELSE '流失用户'
    END AS user_type
FROM rfm_score;

-- 3.算复购率
-- 3.1 整体复购率
WITH user_order AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS order_cnt
    FROM orders_clean o
    JOIN customers_clean c ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
)
SELECT
    COUNT(*) AS total_users,
    SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) AS repurchase_users,
    ROUND(
        SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) * 100,
        2
    ) AS repurchase_rate
FROM user_order;
-- 93358用户 2801复购 3.00%复购率

-- 3.2 城市拆分复购率
WITH user_order_city AS (
    SELECT
        c.customer_unique_id,
        c.customer_city,
        COUNT(DISTINCT o.order_id) AS order_cnt
    FROM orders_clean o
    JOIN customers_clean c ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id, c.customer_city
)
SELECT
    customer_city,
    COUNT(*) AS user_count,
    SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) AS repurchase_user,
    ROUND(
        SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) * 100,
        2
    ) AS repurchase_rate
FROM user_order_city
GROUP BY customer_city
ORDER BY repurchase_rate;
-- 数据太多

-- 3.3 按州拆分复购率
WITH user_order_state AS (
    SELECT
        c.customer_unique_id,
        c.customer_state,
        COUNT(DISTINCT o.order_id) AS order_cnt
    FROM orders_clean o
    JOIN customers_clean c ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id, c.customer_state
)
SELECT
    customer_state AS 州_地区,
    COUNT(*) AS 该地区总用户数,
    SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) AS 该地区复购用户数,
    ROUND(
        SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) * 100,
        2
    ) AS 该地区复购率_百分比
FROM user_order_state
GROUP BY customer_state
ORDER BY 该地区复购率_百分比 DESC;
-- AC最高5.26% PB SE CE AP复购率在2%以下，AP最低1.52%

-- 3.4 按支付方式拆分复购率
WITH user_order_pay AS (
    SELECT
        c.customer_unique_id,
        p.payment_type,
        COUNT(DISTINCT o.order_id) AS order_cnt
    FROM orders_clean o
    JOIN customers_clean c ON o.customer_id = c.customer_id
    JOIN payments_clean p ON o.order_id = p.order_id
    GROUP BY c.customer_unique_id, p.payment_type
)
SELECT
    payment_type,
    COUNT(*) AS user_count,
    SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) AS repurchase_user,
    ROUND(
        SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) * 100,
        2
    ) AS repurchase_rate
FROM user_order_pay
GROUP BY payment_type
ORDER BY repurchase_rate DESC;
-- 信用卡最高2.86% 借记卡最低1.02% 存在辛普森悖论

-- 3.5.1 商品类别拆分复购率
WITH user_order_category AS (
    SELECT
        c.customer_unique_id,
        p.product_category_name,
        COUNT(DISTINCT o.order_id) AS order_cnt
    FROM orders_clean o
    JOIN olist_order_items_dataset oi ON o.order_id = oi.order_id
    JOIN olist_products_dataset p ON oi.product_id = p.product_id
    JOIN customers_clean c ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id, p.product_category_name
)
SELECT
    product_category_name,
    COUNT(*) AS user_count,
    SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) AS repurchase_user,
    ROUND(
        SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) * 100,
        2
    ) AS repurchase_rate
FROM user_order_category
GROUP BY product_category_name
ORDER BY repurchase_rate DESC;

-- 3.5.2 找复购率低于3%商品类别
WITH user_order_category AS (
    SELECT
        c.customer_unique_id,
        p.product_category_name,
        COUNT(DISTINCT o.order_id) AS order_cnt
    FROM orders_clean o
    JOIN olist_order_items_dataset oi ON o.order_id = oi.order_id
    JOIN olist_products_dataset p ON oi.product_id = p.product_id
    JOIN customers_clean c ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id, p.product_category_name
),
category_rate AS (
    SELECT
        product_category_name,
        COUNT(*) AS user_count,
        SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) AS repurchase_user,
        ROUND(
            SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) * 100,
            2
        ) AS repurchase_rate
    FROM user_order_category
    GROUP BY product_category_name
)
SELECT
    product_category_name,
    user_count,
    repurchase_user,
    repurchase_rate
FROM category_rate
WHERE repurchase_rate < 3
ORDER BY repurchase_rate;


WITH category_type AS (
    SELECT
        product_category_name,
        CASE
            WHEN product_category_name IN (
                'moveis_decoracao',        -- 家具装饰
                'eletronicos',             -- 电子产品
                'informatica_acessorios',  -- 电脑配件
                'celulares_acessorios',    -- 手机配件
                'eletrodomesticos',        -- 家用电器
                'eletroportateis',         -- 便携电器
                'relogios_presentes',      -- 钟表礼品
                'esporte_lazer',           -- 运动休闲
                'automotivo',              -- 汽车用品
                'móveis_colchões_e_estofados', -- 家具床垫
                'instrumentos_musicais',   -- 乐器
                'casa_conforto',           -- 家居舒适品
                'casa_construcao',         -- 家居建材
                'ferramentas_jardim',      -- 工具园艺
                'livros_interesse_geral',  -- 书籍
                'malas_acessorios'          -- 箱包配件
            ) THEN '耐用品'
            ELSE '日常品'
        END AS product_type
    FROM result_16
),
type_count AS (
    SELECT
        product_type,
        COUNT(DISTINCT product_category_name) AS category_num
    FROM category_type
    GROUP BY product_type
),
total_count AS (
    SELECT COUNT(DISTINCT product_category_name) AS total_num
    FROM category_type
)
SELECT
    tc.product_type,
    tc.category_num AS 品类数量,
    ROUND(tc.category_num * 1.0 / tt.total_num * 100, 2) AS 品类占比_百分比
FROM type_count tc
CROSS JOIN total_count tt
ORDER BY tc.category_num DESC;
-- 低于平均复购率占比中，日常品占比80.88% 耐用品占比19.12%

-- AB实验设计，每组最小样本量574人

-- 4.1 计算最近90天平均每日独立用户数 & AB实验周期
WITH
max_order_date AS (
    SELECT MAX(order_purchase_timestamp) AS last_day
    FROM orders_clean
),

recent_orders AS (
    SELECT
        o.customer_id,
        DATE(o.order_purchase_timestamp) AS order_dt
    FROM orders_clean o
    CROSS JOIN max_order_date m
    WHERE o.order_purchase_timestamp >= DATE_SUB(m.last_day, INTERVAL 90 DAY)
),

daily_users AS (
    SELECT
        order_dt,
        COUNT(DISTINCT customer_id) AS daily_customer
    FROM recent_orders
    GROUP BY order_dt
),

avg_daily AS (
    SELECT ROUND(AVG(daily_customer), 1) AS avg_daily_users
    FROM daily_users
),

ab_test_calc AS (
    SELECT
        avg_daily_users,
        1148 AS ab_total_sample_size,
        CEIL(1148 / avg_daily_users) AS ab_test_duration_days
    FROM avg_daily
)
SELECT
    avg_daily_users AS 近90天日均独立用户,
    ab_total_sample_size AS AB实验总样本量,
    ab_test_duration_days AS 预计实验周期_天
FROM ab_test_calc;
-- 6天
