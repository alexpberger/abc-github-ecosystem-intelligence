# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Build the Gold star schema with Spark SQL.
# Full overwrite from silver.sales_line. Surrogate keys are assigned for this build.
# Customer key -1 and geography key -1 are the unknown members.

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

if not spark.catalog.tableExists("silver.sales_line"):
    raise RuntimeError("silver.sales_line does not exist. Run nb_silver_clean first.")
if spark.table("silver.sales_line").count() == 0:
    raise RuntimeError("silver.sales_line is empty. Refusing to build Gold.")

spark.sql(
    """
    CREATE OR REPLACE TABLE gold.dim_geography AS
    SELECT
        CAST(-1 AS INT) AS geography_key,
        'UNKNOWN' AS country_name
    UNION ALL
    SELECT
        CAST(row_number() OVER (ORDER BY country) AS INT) AS geography_key,
        country AS country_name
    FROM (
        SELECT DISTINCT country
        FROM silver.sales_line
        WHERE country IS NOT NULL
            AND country <> 'UNKNOWN'
    ) AS countries
    """
)

spark.sql(
    """
    CREATE OR REPLACE TABLE gold.dim_customer AS
    SELECT
        CAST(-1 AS INT) AS customer_key,
        'UNKNOWN' AS customer_id,
        CAST(NULL AS TIMESTAMP) AS first_invoice_date,
        CAST(NULL AS TIMESTAMP) AS last_invoice_date,
        CAST(NULL AS STRING) AS latest_country
    UNION ALL
    SELECT
        CAST(customer_key AS INT) AS customer_key,
        customer_id,
        first_invoice_date,
        last_invoice_date,
        latest_country
    FROM (
        SELECT
            row_number() OVER (ORDER BY customer_id) AS customer_key,
            customer_id,
            min(invoice_date) AS first_invoice_date,
            max(invoice_date) AS last_invoice_date,
            max_by(country, invoice_date) AS latest_country
        FROM silver.sales_line
        WHERE customer_id IS NOT NULL
            AND customer_id <> 'UNKNOWN'
        GROUP BY customer_id
    ) AS customers
    """
)

spark.sql(
    """
    CREATE OR REPLACE TABLE gold.dim_product AS
    SELECT
        CAST(row_number() OVER (ORDER BY stock_code) AS INT) AS product_key,
        stock_code,
        description,
        item_type,
        CASE WHEN item_type = 'product' THEN true ELSE false END AS is_merchandise
    FROM (
        SELECT
            stock_code,
            min(item_type) AS item_type,
            max_by(
                description,
                named_struct(
                    'has_description', CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END,
                    'invoice_date', invoice_date,
                    'description_sort', coalesce(description, '')
                )
            ) AS description
        FROM silver.sales_line
        GROUP BY stock_code
    ) AS products
    """
)

spark.sql(
    """
    CREATE OR REPLACE TABLE gold.dim_date AS
    WITH bounds AS (
        SELECT
            to_date(min(invoice_date)) AS start_date,
            to_date(max(invoice_date)) AS end_date
        FROM silver.sales_line
    ),
    days AS (
        SELECT explode(sequence(start_date, end_date, INTERVAL 1 DAY)) AS calendar_date
        FROM bounds
    )
    SELECT
        CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
        calendar_date,
        year(calendar_date) AS calendar_year,
        quarter(calendar_date) AS calendar_quarter,
        month(calendar_date) AS month_of_year,
        date_format(calendar_date, 'MMMM') AS month_name,
        day(calendar_date) AS day_of_month,
        date_format(calendar_date, 'EEEE') AS day_name,
        date_format(calendar_date, 'yyyy-MM') AS year_month
    FROM days
    """
)

spark.sql(
    """
    CREATE OR REPLACE TABLE gold.fact_sales AS
    SELECT
        sha2(
            concat_ws(
                '||',
                s.invoice,
                s.stock_code,
                coalesce(s.description, ''),
                cast(s.quantity AS STRING),
                cast(s.unit_price AS STRING),
                date_format(s.invoice_date, 'yyyy-MM-dd HH:mm:ss'),
                coalesce(s.customer_id, ''),
                coalesce(s.country, '')
            ),
            256
        ) AS sales_line_key,
        s.invoice AS invoice_number,
        CAST(date_format(s.invoice_date, 'yyyyMMdd') AS INT) AS date_key,
        p.product_key,
        coalesce(c.customer_key, -1) AS customer_key,
        coalesce(g.geography_key, -1) AS geography_key,
        s.quantity,
        s.unit_price,
        s.net_amount,
        s.is_cancellation,
        s.line_kind,
        s.item_type
    FROM silver.sales_line AS s
    INNER JOIN gold.dim_product AS p
        ON s.stock_code = p.stock_code
    LEFT JOIN gold.dim_customer AS c
        ON s.customer_id = c.customer_id
    LEFT JOIN gold.dim_geography AS g
        ON s.country = g.country_name
    """
)

for table_name in (
    "gold.dim_date",
    "gold.dim_product",
    "gold.dim_customer",
    "gold.dim_geography",
    "gold.fact_sales",
):
    row_count = spark.table(table_name).count()
    print("gold_table=" + table_name + " rows=" + str(row_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
