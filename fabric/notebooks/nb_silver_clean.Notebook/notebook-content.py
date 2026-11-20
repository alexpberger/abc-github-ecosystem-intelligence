# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Rebuild silver.sales_line from the latest Bronze batch.
# Uses the DataFrame API. Exact duplicate business rows are removed.
# Lines missing invoice, stock code, invoice date, quantity, or unit price are excluded.
# Cancellations stay in the table with a negative quantity and a cancellation flag.

from pyspark.sql import functions as F

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

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

if not spark.catalog.tableExists("bronze.online_retail"):
    raise RuntimeError("bronze.online_retail does not exist. Run nb_bronze_ingest first.")

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
print("silver_source_batch=" + str(latest_batch_id))
bronze_batch = spark.table("bronze.online_retail").filter(
    F.col("ingestion_batch_id") == F.lit(latest_batch_id)
)
typed, deduped, valid = prepare_sales_lines(bronze_batch)
(
    valid.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.sales_line")
)
print(
    "silver_status=overwritten typed="
    + str(typed.count())
    + " deduped="
    + str(deduped.count())
    + " valid="
    + str(valid.count())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
