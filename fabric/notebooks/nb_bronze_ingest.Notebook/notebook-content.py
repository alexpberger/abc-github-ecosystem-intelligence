# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Parameters. Fabric overwrites these values when a pipeline activity supplies them.
ingestion_batch_id = "online_retail_ii_full"
source_relative_path = "Files/raw/online_retail_II.xlsx"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "tags": [
# META     "parameters"
# META   ]
# META }

# CELL ********************

# Land both Online Retail II sheets in bronze.online_retail.
# The original workbook stays in OneLake Files. This notebook appends one batch
# and skips a batch id that is already present. It does not change the file.
# Attach lh_retail before running. Do not put a lakehouse GUID in this file
# until Fabric writes it back after that attachment.

from datetime import datetime, timezone

from pyspark.sql import functions as F

# CONTRACT_START
SOURCE_FILE_NAME = "online_retail_II.xlsx"
SOURCE_RELATIVE_PATH = "Files/raw/online_retail_II.xlsx"
SHEET_NAMES = ("Year 2009-2010", "Year 2010-2011")
SOURCE_COLUMNS = (
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country",
)
DEFAULT_BATCH_ID = "online_retail_ii_full"
BRONZE_TABLE = "bronze.online_retail"
# CONTRACT_END


def read_sheet(relative_path, sheet_name, ingested_at):
    """Read one worksheet, preserving source values as strings."""
    address = "'" + sheet_name + "'!A1"
    spark_error = None
    try:
        spark_frame = (
            spark.read.format("excel")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("dataAddress", address)
            .load(relative_path)
        )
        spark_frame.limit(1).collect()
        print("excel_reader=spark sheet=" + sheet_name)
        frame = spark_frame
    except Exception as exc:
        spark_error = exc
        print(
            "excel_reader=pandas sheet="
            + sheet_name
            + " reason="
            + type(exc).__name__
            + ": "
            + str(exc)
        )
        import pandas as pd

        local_path = "/lakehouse/default/" + relative_path
        pdf = pd.read_excel(local_path, sheet_name=sheet_name, dtype=str)
        pdf = pdf.fillna("")
        frame = spark.createDataFrame(pdf)

    renamed = frame
    for column_name in list(renamed.columns):
        stripped = column_name.strip()
        if stripped != column_name:
            renamed = renamed.withColumnRenamed(column_name, stripped)

    missing = [column for column in SOURCE_COLUMNS if column not in renamed.columns]
    if missing:
        detail = ""
        if spark_error is not None:
            detail = " spark_error=" + type(spark_error).__name__
        raise RuntimeError(
            "Sheet "
            + sheet_name
            + " is missing columns "
            + str(missing)
            + ". Found "
            + str(renamed.columns)
            + "."
            + detail
        )

    selected = renamed.select(
        [F.col(column).cast("string").alias(column) for column in SOURCE_COLUMNS]
    )
    return (
        selected.withColumn("source_file", F.lit(SOURCE_FILE_NAME))
        .withColumn("source_sheet", F.lit(sheet_name))
        .withColumn("ingestion_timestamp", F.to_timestamp(F.lit(ingested_at)))
        .withColumn("ingestion_batch_id", F.lit(ingestion_batch_id))
    )


spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
safe_batch_id = ingestion_batch_id.replace("'", "''")
already_loaded = False
if spark.catalog.tableExists(BRONZE_TABLE):
    already_loaded = (
        spark.sql(
            "SELECT 1 AS present FROM "
            + BRONZE_TABLE
            + " WHERE ingestion_batch_id = '"
            + safe_batch_id
            + "' LIMIT 1"
        ).count()
        > 0
    )

if already_loaded:
    print("batch_status=skipped ingestion_batch_id=" + ingestion_batch_id)
else:
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    frames = [
        read_sheet(source_relative_path, sheet_name, ingested_at) for sheet_name in SHEET_NAMES
    ]
    landed = frames[0]
    for extra in frames[1:]:
        landed = landed.unionByName(extra)
    landed.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    print(
        "batch_status=appended ingestion_batch_id="
        + ingestion_batch_id
        + " rows="
        + str(landed.count())
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
