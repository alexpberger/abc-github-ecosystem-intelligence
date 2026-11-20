-- T-SQL for the lh_retail SQL analytics endpoint.
-- Run these after the Phase 1 pipeline has loaded Gold.
-- This is not Spark SQL: use TOP, not LIMIT.
-- Spark booleans usually arrive here as bit. Comparisons use 1 and 0.
-- The database context is the lakehouse endpoint, so two-part names are enough.

-- Row counts by year
SELECT
    YEAR(d.calendar_date) AS sales_year,
    COUNT(*) AS line_count
FROM gold.fact_sales AS f
INNER JOIN gold.dim_date AS d
    ON f.date_key = d.date_key
GROUP BY YEAR(d.calendar_date)
ORDER BY sales_year;

-- Net sales across all line kinds, including cancellations and adjustments
SELECT
    SUM(f.net_amount) AS net_sales_amount
FROM gold.fact_sales AS f;

-- Share of lines that are cancellations
SELECT
    CAST(SUM(CASE WHEN f.is_cancellation = 1 THEN 1 ELSE 0 END) AS DECIMAL(18, 4))
        / NULLIF(COUNT(*), 0) AS cancellation_line_rate
FROM gold.fact_sales AS f;

-- Share of lines with no source customer id
SELECT
    CAST(SUM(CASE WHEN f.customer_key = -1 THEN 1 ELSE 0 END) AS DECIMAL(18, 4))
        / NULLIF(COUNT(*), 0) AS unknown_customer_line_share
FROM gold.fact_sales AS f;

-- Ten countries by signed net amount. UNKNOWN is the missing-country member.
SELECT TOP (10)
    g.country_name,
    SUM(f.net_amount) AS net_sales_amount
FROM gold.fact_sales AS f
INNER JOIN gold.dim_geography AS g
    ON f.geography_key = g.geography_key
GROUP BY g.country_name
ORDER BY net_sales_amount DESC;

-- Known customers with more than one non-cancellation invoice
SELECT
    COUNT(*) AS repeat_purchase_customers
FROM (
    SELECT f.customer_key
    FROM gold.fact_sales AS f
    WHERE f.customer_key <> -1
        AND f.is_cancellation = 0
    GROUP BY f.customer_key
    HAVING COUNT(DISTINCT f.invoice_number) > 1
) AS repeat_customers;
