-- ============================================================
-- 01_analytics_queries.sql
-- Project : Retail Sales Analytics Pipeline
-- Purpose : Advanced SQL analytics on the star schema
-- Author  : Manohar Marepally
-- ============================================================

USE retail_dw;


-- ============================================================
-- QUERY 1: MONTHLY REVENUE TREND WITH MoM GROWTH %
-- Concepts : CTE, Window function LAG(), NULLIF()
-- Business : Track revenue momentum month over month
-- ============================================================

WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month_num,
        d.month_name,
        d.year_month,
        ROUND(SUM(f.gross_revenue), 2)      AS revenue,
        COUNT(DISTINCT f.order_id)           AS total_orders,
        COUNT(DISTINCT f.customer_key)       AS unique_customers,
        ROUND(AVG(f.gross_revenue), 2)       AS avg_order_value
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.status_category = 'Completed'
    GROUP BY d.year, d.month_num, d.month_name, d.year_month
)
SELECT
    `year_month`,
    month_name,
    year,
    revenue,
    total_orders,
    unique_customers,
    avg_order_value,
    LAG(revenue) OVER (
        ORDER BY year, month_num
    )                                        AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY year, month_num))
        / NULLIF(LAG(revenue) OVER (ORDER BY year, month_num), 0)
        * 100
    , 2)                                     AS mom_growth_pct,
    ROUND(SUM(revenue) OVER (
        PARTITION BY year
        ORDER BY month_num
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                    AS ytd_revenue
FROM monthly_revenue
ORDER BY year, month_num;

-- ============================================================
-- QUERY 2: YEAR OVER YEAR REVENUE COMPARISON
-- Concepts : CTE, LAG() partitioned by month
-- Business : Compare same month across years
-- ============================================================

WITH monthly AS (
    SELECT
        d.year,
        d.month_num,
        d.month_name,
        ROUND(SUM(f.gross_revenue), 2)  AS revenue,
        COUNT(DISTINCT f.order_id)       AS orders
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.status_category = 'Completed'
    GROUP BY d.year, d.month_num, d.month_name
)
SELECT
    month_name,
    month_num,
    MAX(CASE WHEN year = 2016 THEN revenue END)  AS rev_2016,
    MAX(CASE WHEN year = 2017 THEN revenue END)  AS rev_2017,
    MAX(CASE WHEN year = 2018 THEN revenue END)  AS rev_2018,
    MAX(CASE WHEN year = 2017 THEN orders  END)  AS orders_2017,
    MAX(CASE WHEN year = 2018 THEN orders  END)  AS orders_2018,
    ROUND(
        (MAX(CASE WHEN year = 2018 THEN revenue END) -
         MAX(CASE WHEN year = 2017 THEN revenue END))
        / NULLIF(MAX(CASE WHEN year = 2017 THEN revenue END), 0)
        * 100
    , 2)                                         AS yoy_growth_pct
FROM monthly
GROUP BY month_name, month_num
ORDER BY month_num;


-- ============================================================
-- QUERY 3: RFM CUSTOMER SEGMENTATION
-- Concepts : CTE, NTILE(), CASE WHEN, multiple aggregations
-- Business : Identify Champions, Loyal, At Risk, Lost customers
-- ============================================================

WITH rfm_base AS (
    SELECT
        f.customer_key,
        c.customer_city,
        c.customer_state,
        -- Recency: days since last order (lower = better)
        DATEDIFF(
            '2018-10-17',          -- dataset end date
            MAX(d.full_date)
        )                               AS recency_days,
        -- Frequency: number of distinct orders
        COUNT(DISTINCT f.order_id)      AS frequency,
        -- Monetary: total spend
        ROUND(SUM(f.gross_revenue), 2)  AS monetary
    FROM fact_sales f
    JOIN dim_date d     ON f.date_key     = d.date_key
    JOIN dim_customer c ON f.customer_key = c.customer_key
    WHERE f.status_category = 'Completed'
    AND   c.is_current = TRUE
    GROUP BY
        f.customer_key,
        c.customer_city,
        c.customer_state
),
rfm_scored AS (
    SELECT *,
        -- Score 1-5: higher is better for all three
        NTILE(5) OVER (ORDER BY recency_days ASC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency    DESC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary     DESC) AS m_score
    FROM rfm_base
),
rfm_segmented AS (
    SELECT *,
        (r_score + f_score + m_score)   AS rfm_total,
        CASE
            WHEN (r_score + f_score + m_score) >= 13 THEN 'Champions'
            WHEN (r_score + f_score + m_score) >= 10 THEN 'Loyal Customers'
            WHEN (r_score + f_score + m_score) >= 7  THEN 'Potential Loyalists'
            WHEN (r_score + f_score + m_score) >= 5  THEN 'At Risk'
            ELSE                                          'Lost'
        END                             AS customer_segment
    FROM rfm_scored
)
SELECT
    customer_segment,
    COUNT(*)                            AS customer_count,
    ROUND(AVG(recency_days), 0)         AS avg_recency_days,
    ROUND(AVG(frequency), 2)            AS avg_frequency,
    ROUND(AVG(monetary), 2)             AS avg_monetary,
    ROUND(SUM(monetary), 2)             AS total_segment_revenue,
    ROUND(SUM(monetary) / SUM(SUM(monetary)) OVER () * 100, 2)
                                        AS revenue_pct
FROM rfm_segmented
GROUP BY customer_segment
ORDER BY total_segment_revenue DESC;


-- ============================================================
-- QUERY 4: PRODUCT PERFORMANCE RANKING PER CATEGORY
-- Concepts : RANK(), DENSE_RANK(), PARTITION BY, PCT of total
-- Business : Find top products within each category
-- ============================================================

WITH product_sales AS (
    SELECT
        p.product_category,
        p.product_key,
        p.price_band,
        COUNT(DISTINCT f.order_id)          AS order_count,
        SUM(f.gross_revenue)                AS total_revenue,
        ROUND(AVG(f.unit_price), 2)         AS avg_selling_price,
        ROUND(AVG(f.freight_value), 2)      AS avg_freight
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key
    WHERE f.status_category = 'Completed'
    GROUP BY
        p.product_category,
        p.product_key,
        p.price_band
)
SELECT
    product_category,
    product_key,
    price_band,
    order_count,
    ROUND(total_revenue, 2)                 AS total_revenue,
    avg_selling_price,
    -- Rank within category by revenue
    RANK() OVER (
        PARTITION BY product_category
        ORDER BY total_revenue DESC
    )                                       AS revenue_rank,
    -- Rank within category by volume
    DENSE_RANK() OVER (
        PARTITION BY product_category
        ORDER BY order_count DESC
    )                                       AS volume_rank,
    -- Revenue share within category
    ROUND(
        total_revenue
        / SUM(total_revenue) OVER (PARTITION BY product_category)
        * 100
    , 2)                                    AS pct_of_category,
    -- Running total within category
    ROUND(SUM(total_revenue) OVER (
        PARTITION BY product_category
        ORDER BY total_revenue DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                   AS cumulative_revenue
FROM product_sales
ORDER BY product_category, revenue_rank
LIMIT 50;


-- ============================================================
-- QUERY 5: SELLER PERFORMANCE ANALYSIS
-- Concepts : Multiple joins, aggregation, ranking
-- Business : Identify top and underperforming sellers
-- ============================================================

WITH seller_metrics AS (
    SELECT
        s.seller_key,
        s.seller_city,
        s.seller_state,
        COUNT(DISTINCT f.order_id)           AS total_orders,
        COUNT(DISTINCT f.customer_key)       AS unique_customers,
        ROUND(SUM(f.gross_revenue), 2)       AS total_revenue,
        ROUND(AVG(f.gross_revenue), 2)       AS avg_order_value,
        ROUND(AVG(f.delivery_days), 1)       AS avg_delivery_days,
        ROUND(
            SUM(f.is_on_time) / COUNT(*) * 100
        , 2)                                 AS on_time_pct,
        COUNT(DISTINCT d.year_month)         AS active_months
    FROM fact_sales f
    JOIN dim_seller   s ON f.seller_key = s.seller_key
    JOIN dim_date     d ON f.date_key   = d.date_key
    WHERE f.status_category = 'Completed'
    GROUP BY
        s.seller_key,
        s.seller_city,
        s.seller_state
)
SELECT
    seller_key,
    seller_city,
    seller_state,
    total_orders,
    unique_customers,
    total_revenue,
    avg_order_value,
    avg_delivery_days,
    on_time_pct,
    active_months,
    RANK() OVER (ORDER BY total_revenue DESC)    AS revenue_rank,
    RANK() OVER (ORDER BY on_time_pct   DESC)    AS delivery_rank,
    NTILE(4) OVER (ORDER BY total_revenue DESC)  AS revenue_quartile
FROM seller_metrics
ORDER BY total_revenue DESC
LIMIT 20;


-- ============================================================
-- QUERY 6: CUSTOMER COHORT RETENTION ANALYSIS
-- Concepts : CTE chaining, date math, cohort grouping
-- Business : Track how many customers return after first purchase
-- ============================================================

WITH first_purchase AS (
    -- Find each customer's first order month
    SELECT
        f.customer_key,
        MIN(d.year_month)   AS cohort_month,
        MIN(d.full_date)    AS first_order_date
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.status_category = 'Completed'
    GROUP BY f.customer_key
),
customer_activity AS (
    -- Find all months each customer was active
    SELECT
        f.customer_key,
        d.year_month        AS activity_month,
        d.full_date         AS activity_date
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.status_category = 'Completed'
),
cohort_data AS (
    SELECT
        fp.cohort_month,
        -- Months since first purchase
        TIMESTAMPDIFF(
            MONTH,
            STR_TO_DATE(CONCAT(fp.cohort_month, '-01'), '%Y-%m-%d'),
            STR_TO_DATE(CONCAT(ca.activity_month, '-01'), '%Y-%m-%d')
        )                   AS months_since_first,
        COUNT(DISTINCT ca.customer_key) AS active_customers
    FROM first_purchase fp
    JOIN customer_activity ca
        ON fp.customer_key = ca.customer_key
    GROUP BY fp.cohort_month, months_since_first
)
SELECT
    cohort_month,
    months_since_first,
    active_customers,
    -- Retention rate vs month 0 (first purchase month)
    ROUND(
        active_customers
        / MAX(CASE WHEN months_since_first = 0
              THEN active_customers END)
          OVER (PARTITION BY cohort_month)
        * 100
    , 2)                    AS retention_pct
FROM cohort_data
WHERE months_since_first <= 6    -- show first 6 months retention
ORDER BY cohort_month, months_since_first;


-- ============================================================
-- QUERY 7: DELIVERY PERFORMANCE ANALYSIS
-- Concepts : CASE WHEN buckets, conditional aggregation
-- Business : Understand fulfillment quality by region/seller
-- ============================================================

SELECT
    s.seller_state,
    COUNT(*)                                     AS total_deliveries,
    -- Bucket delivery days into bands
    SUM(CASE WHEN f.delivery_days <= 5
             THEN 1 ELSE 0 END)                  AS delivered_within_5d,
    SUM(CASE WHEN f.delivery_days BETWEEN 6 AND 10
             THEN 1 ELSE 0 END)                  AS delivered_6_to_10d,
    SUM(CASE WHEN f.delivery_days BETWEEN 11 AND 20
             THEN 1 ELSE 0 END)                  AS delivered_11_to_20d,
    SUM(CASE WHEN f.delivery_days > 20
             THEN 1 ELSE 0 END)                  AS delivered_over_20d,
    ROUND(AVG(f.delivery_days), 1)               AS avg_delivery_days,
    ROUND(AVG(f.delivery_delay_days), 1)         AS avg_delay_days,
    ROUND(SUM(f.is_on_time) / COUNT(*) * 100, 2) AS on_time_pct
FROM fact_sales f
JOIN dim_seller s ON f.seller_key = s.seller_key
WHERE f.status_category = 'Completed'
AND   f.delivery_days IS NOT NULL
GROUP BY s.seller_state
ORDER BY on_time_pct DESC;


-- ============================================================
-- QUERY 8: REVENUE CONTRIBUTION (80/20 PARETO ANALYSIS)
-- Concepts : Running totals, cumulative percentage, CASE WHEN
-- Business : Which customers drive 80% of revenue?
-- ============================================================

WITH customer_revenue AS (
    SELECT
        f.customer_key,
        c.customer_state,
        ROUND(SUM(f.gross_revenue), 2)   AS total_revenue,
        COUNT(DISTINCT f.order_id)        AS total_orders
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_key = c.customer_key
    WHERE f.status_category = 'Completed'
    AND   c.is_current = TRUE
    GROUP BY f.customer_key, c.customer_state
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY total_revenue DESC)   AS revenue_rank,
        ROUND(
            SUM(total_revenue) OVER (
                ORDER BY total_revenue DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            / SUM(total_revenue) OVER () * 100
        , 2)                                        AS cumulative_pct
    FROM customer_revenue
)
SELECT
    revenue_rank,
    customer_key,
    customer_state,
    total_revenue,
    total_orders,
    cumulative_pct,
    CASE
        WHEN cumulative_pct <= 80 THEN 'Top 80% Revenue Drivers'
        ELSE 'Remaining 20%'
    END                                             AS pareto_segment
FROM ranked
ORDER BY revenue_rank
LIMIT 100;


-- ============================================================
-- QUERY 9: QUARTERLY BUSINESS SUMMARY (EXECUTIVE KPI VIEW)
-- Concepts : Multi-level grouping, multiple metrics in one query
-- Business : Board-level quarterly reporting view
-- ============================================================

SELECT
    d.year,
    d.quarter_name,
    CONCAT(d.year, ' ', d.quarter_name)              AS period,
    COUNT(DISTINCT f.order_id)                        AS total_orders,
    COUNT(DISTINCT f.customer_key)                    AS unique_customers,
    ROUND(SUM(f.gross_revenue), 2)                    AS gross_revenue,
    ROUND(AVG(f.gross_revenue), 2)                    AS avg_order_value,
    ROUND(SUM(f.freight_value), 2)                    AS total_freight,
    ROUND(
        SUM(f.freight_value)
        / NULLIF(SUM(f.gross_revenue), 0) * 100
    , 2)                                              AS freight_pct_of_revenue,
    ROUND(AVG(f.delivery_days), 1)                    AS avg_delivery_days,
    ROUND(
        SUM(f.is_on_time) / COUNT(*) * 100
    , 2)                                              AS on_time_delivery_pct,
    -- QoQ Growth
    ROUND(
        (SUM(f.gross_revenue) - LAG(SUM(f.gross_revenue)) OVER (
            ORDER BY d.year, d.quarter
        ))
        / NULLIF(LAG(SUM(f.gross_revenue)) OVER (
            ORDER BY d.year, d.quarter
        ), 0) * 100
    , 2)                                              AS qoq_growth_pct
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.status_category = 'Completed'
GROUP BY
    d.year,
    d.quarter,
    d.quarter_name
ORDER BY
    d.year,
    d.quarter;