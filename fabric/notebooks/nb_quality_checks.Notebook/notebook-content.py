# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Recompute the Silver expectations and check the Gold model.
# Critical failures are written to quality.check_result and then raised,
# so the pipeline activity stops after the results are saved.

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

# RULES_START
PRODUCT_CODE_PATTERN = r"^\d{5}[A-Za-z]?$"
ITEM_TYPE_BY_CODE = {
    "POST": "postage",
    "DOT": "postage",
    "C2": "carriage",
    "C3": "carriage",
    "M": "manual",
    "D": "discount",
    "BANK CHARGES": "fee",
    "AMAZONFEE": "fee",
    "CRUK": "fee",
    "S": "sample",
    "B": "adjustment",
    "ADJUST": "adjustment",
    "ADJUST2": "adjustment",
}
GIFT_PREFIXES = ("GIFT",)
# RULES_END

# TRANSFORM_START
def prepare_sales_lines(bronze_df):
    """Return typed, deduplicated, and valid frames for one Bronze batch."""
    from pyspark.sql import functions as F

    invoice = F.trim(F.col("Invoice").cast("string"))
    stock = F.upper(F.trim(F.col("StockCode").cast("string")))
    description = F.trim(F.col("Description").cast("string"))
    quantity_text = F.trim(F.col("Quantity").cast("string"))
    price_text = F.trim(F.col("Price").cast("string"))
    date_text = F.trim(F.col("InvoiceDate").cast("string"))
    customer_text = F.trim(F.col("Customer ID").cast("string"))
    country = F.trim(F.col("Country").cast("string"))

    invoice = F.when((invoice == "") | invoice.isNull(), F.lit(None)).otherwise(invoice)
    stock = F.when((stock == "") | stock.isNull(), F.lit(None)).otherwise(stock)
    description = F.when((description == "") | description.isNull(), F.lit(None)).otherwise(description)
    country = F.when((country == "") | country.isNull(), F.lit(None)).otherwise(country)

    customer_text = F.when(
        customer_text.isNull()
        | (customer_text == "")
        | customer_text.rlike("(?i)^(nan|none|null)$"),
        F.lit(None),
    ).otherwise(customer_text)
    customer_id = F.when(
        customer_text.rlike(r"^\d+\.0+$"),
        F.regexp_replace(customer_text, r"\.0+$", ""),
    ).otherwise(customer_text)

    quantity = F.when(
        quantity_text.rlike(r"^-?\d+(\.0+)?$"),
        quantity_text.cast("double").cast("long"),
    )
    unit_price = F.when(
        price_text.rlike(r"^-?\d+(\.\d+)?$"),
        price_text.cast("decimal(18,4)"),
    )

    serial = date_text.cast("double")
    excel_timestamp = F.to_timestamp(F.from_unixtime((serial - F.lit(25569.0)) * F.lit(86400.0)))
    is_excel_serial = date_text.rlike(r"^\d+(\.\d+)?$") & serial.between(F.lit(35000), F.lit(60000))
    invoice_date = F.coalesce(
        F.when(is_excel_serial, excel_timestamp),
        F.to_timestamp(date_text),
        F.to_timestamp(date_text, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(date_text, "yyyy-MM-dd HH:mm"),
        F.to_timestamp(date_text, "M/d/yyyy H:mm"),
        F.to_timestamp(date_text, "d/M/yyyy H:mm"),
        F.to_timestamp(date_text, "yyyy-MM-dd"),
    )

    invoice_key = F.upper(invoice)
    line_kind = (
        F.when(invoice_key.startswith("C"), F.lit("cancellation"))
        .when(invoice_key.startswith("A"), F.lit("adjustment"))
        .otherwise(F.lit("sale"))
    )
    is_cancellation = invoice_key.startswith("C")

    item_type = F.when(stock.isNull(), F.lit("non_standard"))
    for code, name in ITEM_TYPE_BY_CODE.items():
        item_type = item_type.when(stock == F.lit(code), F.lit(name))
    gift_match = None
    for prefix in GIFT_PREFIXES:
        prefix_match = stock.startswith(prefix)
        gift_match = prefix_match if gift_match is None else (gift_match | prefix_match)
    item_type = item_type.when(gift_match, F.lit("gift_voucher"))
    item_type = item_type.when(stock.rlike(PRODUCT_CODE_PATTERN), F.lit("product"))
    item_type = item_type.otherwise(F.lit("non_standard"))

    typed = bronze_df.select(
        invoice.alias("invoice"),
        stock.alias("stock_code"),
        description.alias("description"),
        quantity.alias("quantity"),
        invoice_date.alias("invoice_date"),
        unit_price.alias("unit_price"),
        customer_id.alias("customer_id"),
        country.alias("country"),
        line_kind.alias("line_kind"),
        is_cancellation.alias("is_cancellation"),
        item_type.alias("item_type"),
    )
    deduped = typed.dropDuplicates(
        [
            "invoice",
            "stock_code",
            "description",
            "quantity",
            "invoice_date",
            "unit_price",
            "customer_id",
            "country",
        ]
    )
    valid = deduped.filter(
        F.col("invoice").isNotNull()
        & F.col("stock_code").isNotNull()
        & F.col("invoice_date").isNotNull()
        & F.col("quantity").isNotNull()
        & F.col("unit_price").isNotNull()
    )
    valid = valid.withColumn(
        "net_amount",
        (F.col("quantity").cast("decimal(18,4)") * F.col("unit_price")).cast("decimal(18,4)"),
    )
    valid = valid.select(
        "invoice",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
        "line_kind",
        "is_cancellation",
        "item_type",
        "net_amount",
    )
    return typed, deduped, valid
# TRANSFORM_END

for schema_name in ("bronze", "silver", "gold", "quality"):
    spark.sql("CREATE SCHEMA IF NOT EXISTS " + schema_name)


def require_table(table_name):
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(table_name + " does not exist.")


for table_name in (
    "bronze.online_retail",
    "silver.sales_line",
    "gold.fact_sales",
    "gold.dim_date",
    "gold.dim_product",
    "gold.dim_customer",
    "gold.dim_geography",
):
    require_table(table_name)

latest_rows = spark.sql(
    """
    SELECT ingestion_batch_id
    FROM bronze.online_retail
    GROUP BY ingestion_batch_id
    ORDER BY max(ingestion_timestamp) DESC
    LIMIT 1
    """
).collect()
if not latest_rows:
    raise RuntimeError("bronze.online_retail has no batches.")

latest_batch_id = latest_rows[0]["ingestion_batch_id"]
bronze_batch = spark.table("bronze.online_retail").filter(
    F.col("ingestion_batch_id") == F.lit(latest_batch_id)
)
typed, deduped, valid = prepare_sales_lines(bronze_batch)

bronze_count = bronze_batch.count()
typed_count = typed.count()
deduped_count = deduped.count()
expected_silver = valid.count()
silver_count = spark.table("silver.sales_line").count()
fact_count = spark.table("gold.fact_sales").count()


def orphan_count(fact_column, dimension_table, dimension_column):
    query = (
        "SELECT COUNT(*) AS orphan_rows "
        "FROM gold.fact_sales AS f "
        "LEFT JOIN " + dimension_table + " AS d "
        "ON f." + fact_column + " = d." + dimension_column + " "
        "WHERE d." + dimension_column + " IS NULL"
    )
    return spark.sql(query).collect()[0]["orphan_rows"]


date_orphans = orphan_count("date_key", "gold.dim_date", "date_key")
product_orphans = orphan_count("product_key", "gold.dim_product", "product_key")
customer_orphans = orphan_count("customer_key", "gold.dim_customer", "customer_key")
geography_orphans = orphan_count("geography_key", "gold.dim_geography", "geography_key")

product = spark.table("gold.dim_product")
product_rows = product.count()
unique_product_keys = product.select("product_key").distinct().count()
unique_stock_codes = product.select("stock_code").distinct().count()
product_unique = product_rows == unique_product_keys == unique_stock_codes

customer = spark.table("gold.dim_customer")
customer_rows = customer.count()
unique_customer_keys = customer.select("customer_key").distinct().count()
unique_customer_ids = customer.select("customer_id").distinct().count()
customer_unique = customer_rows == unique_customer_keys == unique_customer_ids

unknown_customers = (
    spark.table("gold.dim_customer")
    .filter((F.col("customer_key") == F.lit(-1)) & (F.col("customer_id") == F.lit("UNKNOWN")))
    .count()
)
mismatch = spark.sql(
    """
    SELECT COUNT(*) AS mismatch_count
    FROM gold.fact_sales
    WHERE abs(net_amount - CAST(quantity AS DECIMAL(18, 4)) * unit_price)
        > CAST(0.0001 AS DECIMAL(18, 4))
    """
).collect()[0]["mismatch_count"]
sheet_count = bronze_batch.select("source_sheet").distinct().count()

checks = [
    ("bronze_batch_not_empty", "critical", bronze_count > 0, str(bronze_count), "> 0"),
    (
        "silver_exclusions_accounted",
        "critical",
        expected_silver == silver_count,
        "silver="
        + str(silver_count)
        + "; recomputed_valid="
        + str(expected_silver)
        + "; bronze="
        + str(bronze_count)
        + "; typed="
        + str(typed_count)
        + "; deduped="
        + str(deduped_count),
        "silver count equals deduplicated valid bronze rows",
    ),
    (
        "fact_matches_silver",
        "critical",
        fact_count == silver_count,
        "fact=" + str(fact_count) + "; silver=" + str(silver_count),
        "equal counts",
    ),
    ("no_orphan_date_keys", "critical", date_orphans == 0, str(date_orphans), "0"),
    ("no_orphan_product_keys", "critical", product_orphans == 0, str(product_orphans), "0"),
    ("no_orphan_customer_keys", "critical", customer_orphans == 0, str(customer_orphans), "0"),
    ("no_orphan_geography_keys", "critical", geography_orphans == 0, str(geography_orphans), "0"),
    (
        "unique_product_keys",
        "critical",
        product_unique,
        "rows="
        + str(product_rows)
        + "; product_keys="
        + str(unique_product_keys)
        + "; stock_codes="
        + str(unique_stock_codes),
        "one row per product key and stock code",
    ),
    (
        "unique_customer_keys",
        "critical",
        customer_unique,
        "rows="
        + str(customer_rows)
        + "; customer_keys="
        + str(unique_customer_keys)
        + "; customer_ids="
        + str(unique_customer_ids),
        "one row per customer key and customer id",
    ),
    ("net_amount_matches_inputs", "critical", mismatch == 0, str(mismatch), "0"),
    ("unknown_customer_present", "critical", unknown_customers == 1, str(unknown_customers), "1"),
    ("both_source_sheets_present", "warning", sheet_count == 2, str(sheet_count), "2"),
]

schema = StructType(
    [
        StructField("check_name", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("passed", BooleanType(), False),
        StructField("observed_value", StringType(), False),
        StructField("expected_value", StringType(), False),
    ]
)
checked_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
result = spark.createDataFrame(checks, schema).withColumn(
    "checked_at", F.to_timestamp(F.lit(checked_at))
)
(
    result.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("quality.check_result")
)

failed = [name for name, severity, passed, _observed, _expected in checks if severity == "critical" and not passed]
if failed:
    raise RuntimeError("Critical data quality checks failed: " + ", ".join(failed))
print("quality_status=passed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
