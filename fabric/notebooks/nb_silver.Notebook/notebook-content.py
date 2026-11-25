# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# CONTRACT_START
LAKEHOUSE_NAME = "lh_ecosystem"
SCHEMAS = ("bronze", "silver", "gold", "quality")

SEARCH_RESPONSE_TABLE = "bronze.search_response"
DISCOVERED_REPOSITORY_TABLE = "bronze.discovered_repository"
REPOSITORY_RESPONSE_TABLE = "bronze.repository_response"

REPOSITORY_SNAPSHOT_TABLE = "silver.repository_snapshot"
DISCOVERY_MATCH_TABLE = "silver.discovery_match"
TRACKED_REPOSITORY_TABLE = "silver.tracked_repository"

DIM_REPOSITORY_TABLE = "gold.dim_repository"
DIM_TECHNOLOGY_TABLE = "gold.dim_technology"
FACT_REPOSITORY_SNAPSHOT_TABLE = "gold.fact_repository_snapshot"
FACT_DISCOVERY_TABLE = "gold.fact_discovery"

QUALITY_TABLE = "quality.check_result"

DEFAULT_LOOKBACK_DAYS = 14
MAX_TRACKED_REPOSITORIES = 500
DEFAULT_SECRET_NAME = "github-token"
USER_AGENT = "abc-github-ecosystem-intelligence"
# CONTRACT_END

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Rebuild Silver from every Bronze batch with the DataFrame API.
# 200 stores the new payload. 304 copies the latest earlier 200 for that repository.
# 404 keeps the requested name and clears metrics. This matches resolve_snapshot_series:
# snapshot status is refreshed, unchanged, or missing.

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


def github_timestamp(column):
    return F.to_timestamp(F.substr(column, 1, 19), "yyyy-MM-dd'T'HH:mm:ss")


def write_table(frame, table_name):
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
for required in (SEARCH_RESPONSE_TABLE, DISCOVERED_REPOSITORY_TABLE):
    if not spark.catalog.tableExists(required):
        raise RuntimeError(required + " does not exist. Run nb_discover first.")

repository_schema = StructType(
    [
        StructField("id", LongType()),
        StructField("full_name", StringType()),
        StructField("description", StringType()),
        StructField("language", StringType()),
        StructField("topics", ArrayType(StringType())),
        StructField("created_at", StringType()),
        StructField("updated_at", StringType()),
        StructField("pushed_at", StringType()),
        StructField("stargazers_count", LongType()),
        StructField("forks_count", LongType()),
        StructField("subscribers_count", LongType()),
        StructField("open_issues_count", LongType()),
        StructField("fork", BooleanType()),
        StructField("archived", BooleanType()),
        StructField("owner", StructType([StructField("login", StringType())])),
    ]
)
response_schema = StructType(
    [
        StructField("github_repo_id", LongType(), True),
        StructField("collection_run_id", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("http_status", IntegerType(), True),
        StructField("etag", StringType(), True),
        StructField("response_json", StringType(), True),
        StructField("collected_at", TimestampType(), True),
    ]
)
snapshot_schema = StructType(
    [
        StructField("github_repo_id", LongType(), True),
        StructField("collection_run_id", StringType(), True),
        StructField("observed_at", TimestampType(), True),
        StructField("full_name", StringType(), True),
        StructField("owner_login", StringType(), True),
        StructField("description", StringType(), True),
        StructField("primary_language", StringType(), True),
        StructField("topics_json", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("pushed_at", TimestampType(), True),
        StructField("stargazers_count", LongType(), True),
        StructField("forks_count", LongType(), True),
        StructField("subscribers_count", LongType(), True),
        StructField("open_issues_count", LongType(), True),
        StructField("is_fork", BooleanType(), True),
        StructField("is_archived", BooleanType(), True),
        StructField("snapshot_status", StringType(), True),
        StructField("etag", StringType(), True),
    ]
)

if spark.catalog.tableExists(REPOSITORY_RESPONSE_TABLE):
    responses = spark.table(REPOSITORY_RESPONSE_TABLE)
else:
    responses = spark.createDataFrame([], response_schema)

match_window = Window.partitionBy(
    "github_repo_id", "query_id", "collection_run_id"
).orderBy(F.col("search_rank").asc())
discovery_match = (
    spark.table(DISCOVERED_REPOSITORY_TABLE)
    .withColumn("rn", F.row_number().over(match_window))
    .filter(F.col("rn") == 1)
    .select(
        F.col("github_repo_id").cast("long").alias("github_repo_id"),
        F.col("query_id"),
        F.col("technology_id"),
        F.col("collection_run_id"),
        F.col("full_name"),
        F.col("stargazers_count").cast("long").alias("stargazers_count"),
        github_timestamp(F.col("pushed_at")).alias("pushed_at"),
        F.col("collected_at").alias("discovered_at"),
        F.col("search_rank").cast("int").alias("search_rank"),
        F.col("result_cap_hit").cast("boolean").alias("result_cap_hit"),
        F.col("incomplete_results").cast("boolean").alias("incomplete_results"),
    )
)

if responses.limit(1).count() == 0:
    snapshots = spark.createDataFrame([], snapshot_schema)
else:
    unexpected = responses.filter(~F.col("http_status").isin(200, 304, 404))
    if unexpected.limit(1).count() > 0:
        raise RuntimeError(
            "bronze.repository_response has a status other than 200, 304, or 404."
        )
    running = (
        Window.partitionBy("github_repo_id")
        .orderBy(F.col("collected_at").asc(), F.col("collection_run_id").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    with_source = responses.withColumn(
        "source_json",
        F.last(
            F.when(F.col("http_status") == F.lit(200), F.col("response_json")),
            ignorenulls=True,
        ).over(running),
    )
    missing_prior = with_source.filter(
        (F.col("http_status") == F.lit(304)) & F.col("source_json").isNull()
    )
    if missing_prior.limit(1).count() > 0:
        raise RuntimeError("304 response without a previous repository body")
    parsed = with_source.withColumn(
        "parsed", F.from_json(F.col("source_json"), repository_schema)
    )
    description = F.col("parsed.description")
    snapshots = parsed.select(
        F.col("github_repo_id").cast("long").alias("github_repo_id"),
        F.col("collection_run_id"),
        F.col("collected_at").alias("observed_at"),
        F.coalesce(F.col("parsed.full_name"), F.col("full_name")).alias("full_name"),
        F.col("parsed.owner.login").alias("owner_login"),
        F.when(F.col("http_status") == F.lit(404), F.lit(None).cast("string"))
        .when(description.isNull() | (F.trim(description) == ""), F.lit(None).cast("string"))
        .otherwise(description)
        .alias("description"),
        F.col("parsed.language").alias("primary_language"),
        F.when(F.col("http_status") == F.lit(404), F.lit(None).cast("string"))
        .otherwise(F.to_json(F.col("parsed.topics")))
        .alias("topics_json"),
        github_timestamp(F.col("parsed.created_at")).alias("created_at"),
        github_timestamp(F.col("parsed.updated_at")).alias("updated_at"),
        github_timestamp(F.col("parsed.pushed_at")).alias("pushed_at"),
        F.col("parsed.stargazers_count").cast("long").alias("stargazers_count"),
        F.col("parsed.forks_count").cast("long").alias("forks_count"),
        F.col("parsed.subscribers_count").cast("long").alias("subscribers_count"),
        F.col("parsed.open_issues_count").cast("long").alias("open_issues_count"),
        F.col("parsed.fork").cast("boolean").alias("is_fork"),
        F.col("parsed.archived").cast("boolean").alias("is_archived"),
        F.when(F.col("http_status") == F.lit(200), F.lit("refreshed"))
        .when(F.col("http_status") == F.lit(304), F.lit("unchanged"))
        .otherwise(F.lit("missing"))
        .alias("snapshot_status"),
        F.col("etag"),
    )

latest_snapshot_name = (
    snapshots.filter(F.col("snapshot_status") != F.lit("missing"))
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("github_repo_id").orderBy(
                F.col("observed_at").desc(), F.col("collection_run_id").desc()
            )
        ),
    )
    .filter(F.col("rn") == 1)
    .select("github_repo_id", F.col("full_name").alias("snapshot_name"))
)
if spark.catalog.tableExists(REPOSITORY_RESPONSE_TABLE):
    latest_response = (
        spark.table(REPOSITORY_RESPONSE_TABLE)
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("github_repo_id").orderBy(F.col("collected_at").desc())
            ),
        )
        .filter(F.col("rn") == 1)
        .select(
            F.col("github_repo_id").cast("long").alias("github_repo_id"),
            F.col("full_name").alias("response_name"),
            F.col("etag"),
        )
        .withColumn("was_fetched", F.lit(True))
    )
else:
    latest_response = spark.createDataFrame(
        [],
        StructType(
            [
                StructField("github_repo_id", LongType(), True),
                StructField("response_name", StringType(), True),
                StructField("etag", StringType(), True),
                StructField("was_fetched", BooleanType(), True),
            ]
        ),
    )

first_seen = discovery_match.groupBy("github_repo_id").agg(
    F.min("discovered_at").alias("first_discovered_at")
)
latest_discovery = (
    discovery_match.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("github_repo_id").orderBy(
                F.col("discovered_at").desc(), F.col("search_rank").asc()
            )
        ),
    )
    .filter(F.col("rn") == 1)
    .select("github_repo_id", F.col("full_name").alias("discovered_name"))
)
repo_ids = discovery_match.select("github_repo_id").union(
    latest_response.select("github_repo_id")
).distinct()
tracked = (
    repo_ids.join(first_seen, "github_repo_id", "left")
    .join(latest_discovery, "github_repo_id", "left")
    .join(latest_snapshot_name, "github_repo_id", "left")
    .join(latest_response, "github_repo_id", "left")
    .select(
        F.col("github_repo_id"),
        F.coalesce(
            F.col("snapshot_name"), F.col("response_name"), F.col("discovered_name")
        ).alias("full_name"),
        F.col("first_discovered_at"),
        F.when(F.col("was_fetched") == F.lit(True), F.lit("tracked"))
        .otherwise(F.lit("discovered_only"))
        .alias("tracking_status"),
        F.col("etag"),
    )
)

write_table(snapshots, REPOSITORY_SNAPSHOT_TABLE)
write_table(discovery_match, DISCOVERY_MATCH_TABLE)
write_table(tracked, TRACKED_REPOSITORY_TABLE)
print(
    "silver_status=replaced snapshots="
    + str(snapshots.count())
    + " discovery_rows="
    + str(discovery_match.count())
    + " repositories="
    + str(tracked.count())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
