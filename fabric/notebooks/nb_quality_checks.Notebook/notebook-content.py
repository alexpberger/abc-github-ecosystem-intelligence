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

# Recompute expectations for the latest collection run.
# Critical failures are written to quality.check_result and then raised.

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

spark.sql("CREATE SCHEMA IF NOT EXISTS quality")

required_tables = (
    SEARCH_RESPONSE_TABLE,
    DISCOVERED_REPOSITORY_TABLE,
    REPOSITORY_SNAPSHOT_TABLE,
    DISCOVERY_MATCH_TABLE,
    TRACKED_REPOSITORY_TABLE,
    DIM_REPOSITORY_TABLE,
    DIM_TECHNOLOGY_TABLE,
    FACT_REPOSITORY_SNAPSHOT_TABLE,
    FACT_DISCOVERY_TABLE,
)
missing_tables = [name for name in required_tables if not spark.catalog.tableExists(name)]
if missing_tables:
    raise RuntimeError("Missing tables: " + ", ".join(missing_tables))

latest = (
    spark.table(SEARCH_RESPONSE_TABLE)
    .agg(F.max(F.struct("collected_at", "collection_run_id")).alias("latest"))
    .collect()[0]["latest"]
)
if latest is None:
    raise RuntimeError("bronze.search_response has no collection run.")
run_id = latest["collection_run_id"]

search = spark.table(SEARCH_RESPONSE_TABLE).filter(F.col("collection_run_id") == F.lit(run_id))
discovered = spark.table(DISCOVERED_REPOSITORY_TABLE).filter(
    F.col("collection_run_id") == F.lit(run_id)
)
cap = search.agg(F.max("max_tracked_repositories").alias("cap")).collect()[0]["cap"]
discovered_count = discovered.count()
if spark.catalog.tableExists(REPOSITORY_RESPONSE_TABLE):
    responses = spark.table(REPOSITORY_RESPONSE_TABLE)
    prior_tracked = (
        responses.filter(F.col("collection_run_id") != F.lit(run_id))
        .select("github_repo_id")
        .distinct()
        .count()
    )
    response_count = (
        responses.filter(F.col("collection_run_id") == F.lit(run_id))
        .select("github_repo_id")
        .distinct()
        .count()
    )
else:
    prior_tracked = 0
    response_count = 0
expects_rows = prior_tracked > 0 or (
    discovered_count > 0 and cap is not None and int(cap) > 0
)
if expects_rows:
    responses_complete = response_count > 0
    responses_observed = (
        "responses="
        + str(response_count)
        + "; prior_tracked="
        + str(prior_tracked)
        + "; discovered="
        + str(discovered_count)
    )
else:
    responses_complete = True
    responses_observed = "tracked=0"


def orphan_count(fact_table, fact_column, dimension_table, dimension_column):
    query = (
        "SELECT COUNT(*) AS orphan_rows FROM "
        + fact_table
        + " AS f LEFT JOIN "
        + dimension_table
        + " AS d ON f."
        + fact_column
        + " = d."
        + dimension_column
        + " WHERE d."
        + dimension_column
        + " IS NULL"
    )
    return spark.sql(query).collect()[0]["orphan_rows"]


snapshots = spark.table(REPOSITORY_SNAPSHOT_TABLE)
snapshot_rows = snapshots.count()
snapshot_distinct = snapshots.select("github_repo_id", "collection_run_id").distinct().count()
null_repo_ids = snapshots.filter(F.col("github_repo_id").isNull()).count()
fact_rows = spark.table(FACT_REPOSITORY_SNAPSHOT_TABLE).count()
negative_metrics = spark.sql(
    "SELECT COUNT(*) AS negative_rows FROM "
    + FACT_REPOSITORY_SNAPSHOT_TABLE
    + " WHERE stargazers_count < 0 OR forks_count < 0"
).collect()[0]["negative_rows"]
tracked = spark.table(TRACKED_REPOSITORY_TABLE).filter(F.col("tracking_status") == F.lit("tracked"))
tracked_count = tracked.count()
snapped_ids = snapshots.filter(F.col("collection_run_id") == F.lit(run_id)).select("github_repo_id")
missing_snapshots = tracked.join(snapped_ids, "github_repo_id", "left_anti").count()
discovered_only = (
    spark.table(TRACKED_REPOSITORY_TABLE)
    .filter(F.col("tracking_status") == F.lit("discovered_only"))
    .count()
)
kept_search = search.filter(F.col("keep_items") == F.lit(True))
cap_hits = kept_search.filter(F.col("result_cap_hit") == F.lit(True)).count()
incomplete_pages = kept_search.filter(F.col("incomplete_results") == F.lit(True)).count()
within_cap = cap is not None and tracked_count <= int(cap)

checks = [
    (
        "repository_responses_complete",
        "critical",
        responses_complete,
        responses_observed,
        "snapshot responses exist when the run has repositories to refresh",
    ),
    (
        "unique_snapshot_grain",
        "critical",
        snapshot_rows == snapshot_distinct,
        "rows=" + str(snapshot_rows) + "; distinct=" + str(snapshot_distinct),
        "one row per repository and collection run",
    ),
    (
        "snapshot_repo_id_present",
        "critical",
        null_repo_ids == 0,
        str(null_repo_ids),
        "0",
    ),
    (
        "fact_matches_silver",
        "critical",
        fact_rows == snapshot_rows,
        "fact=" + str(fact_rows) + "; silver=" + str(snapshot_rows),
        "equal counts",
    ),
    (
        "no_orphan_snapshot_repository",
        "critical",
        orphan_count(
            FACT_REPOSITORY_SNAPSHOT_TABLE,
            "github_repo_id",
            DIM_REPOSITORY_TABLE,
            "github_repo_id",
        )
        == 0,
        str(
            orphan_count(
                FACT_REPOSITORY_SNAPSHOT_TABLE,
                "github_repo_id",
                DIM_REPOSITORY_TABLE,
                "github_repo_id",
            )
        ),
        "0",
    ),
    (
        "no_orphan_discovery_technology",
        "critical",
        orphan_count(
            FACT_DISCOVERY_TABLE,
            "technology_id",
            DIM_TECHNOLOGY_TABLE,
            "technology_id",
        )
        == 0,
        str(
            orphan_count(
                FACT_DISCOVERY_TABLE,
                "technology_id",
                DIM_TECHNOLOGY_TABLE,
                "technology_id",
            )
        ),
        "0",
    ),
    (
        "no_orphan_discovery_repository",
        "critical",
        orphan_count(
            FACT_DISCOVERY_TABLE,
            "github_repo_id",
            DIM_REPOSITORY_TABLE,
            "github_repo_id",
        )
        == 0,
        str(
            orphan_count(
                FACT_DISCOVERY_TABLE,
                "github_repo_id",
                DIM_REPOSITORY_TABLE,
                "github_repo_id",
            )
        ),
        "0",
    ),
    (
        "stars_and_forks_non_negative",
        "critical",
        negative_metrics == 0,
        str(negative_metrics),
        "0",
    ),
    (
        "tracked_within_cap",
        "critical",
        within_cap,
        "tracked=" + str(tracked_count) + "; cap=" + str(cap),
        "tracked repositories do not exceed the run cap",
    ),
    (
        "tracked_repos_snapshotted",
        "critical",
        missing_snapshots == 0,
        str(missing_snapshots),
        "0",
    ),
    (
        "search_result_cap_hit",
        "warning",
        cap_hits == 0,
        str(cap_hits),
        "0",
    ),
    (
        "search_incomplete_results",
        "warning",
        incomplete_pages == 0,
        str(incomplete_pages),
        "0",
    ),
    (
        "discovered_not_tracked",
        "warning",
        discovered_only == 0,
        str(discovered_only),
        "0",
    ),
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
    .saveAsTable(QUALITY_TABLE)
)

failed = [
    name
    for name, severity, passed, _observed, _expected in checks
    if severity == "critical" and not passed
]
if failed:
    raise RuntimeError("Critical data quality checks failed: " + ", ".join(failed))
print("quality_status=passed collection_run_id=" + run_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
