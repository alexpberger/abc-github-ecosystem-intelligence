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

# TECHNOLOGIES_START
TECHNOLOGIES = (
    ("agentic_ai", "Agentic AI"),
    ("llm_frameworks", "LLM frameworks"),
    ("rag", "RAG"),
    ("mcp", "MCP"),
    ("ai_infrastructure", "AI infrastructure"),
    ("data_engineering", "Data engineering"),
    ("developer_tooling", "Developer tooling"),
)
# TECHNOLOGIES_END

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build the four Gold tables with Spark SQL.
# github_repo_id and technology_id stay stable across rebuilds.
# A repository found by more than one technology is counted in each technology.

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
for required in (
    REPOSITORY_SNAPSHOT_TABLE,
    DISCOVERY_MATCH_TABLE,
    TRACKED_REPOSITORY_TABLE,
):
    if not spark.catalog.tableExists(required):
        raise RuntimeError(required + " does not exist. Run nb_silver first.")

spark.createDataFrame(list(TECHNOLOGIES), ["technology_id", "technology_name"]).createOrReplaceTempView(
    "technology_seed"
)

spark.sql(
    """
    CREATE OR REPLACE TABLE gold.dim_technology AS
    SELECT technology_id, technology_name
    FROM technology_seed
    """
)

spark.sql(
    """
    CREATE OR REPLACE TABLE gold.dim_repository AS
    WITH current_snapshot AS (
        SELECT
            github_repo_id,
            full_name,
            owner_login,
            description,
            primary_language,
            topics_json,
            created_at,
            pushed_at,
            is_fork,
            is_archived
        FROM (
            SELECT
                github_repo_id,
                full_name,
                owner_login,
                description,
                primary_language,
                topics_json,
                created_at,
                pushed_at,
                is_fork,
                is_archived,
                ROW_NUMBER() OVER (
                    PARTITION BY github_repo_id
                    ORDER BY observed_at DESC, collection_run_id DESC
                ) AS recency
            FROM silver.repository_snapshot
            WHERE snapshot_status <> 'missing'
        ) AS ranked
        WHERE recency = 1
    ),
    previous_name AS (
        SELECT github_repo_id, previous_full_name
        FROM (
            SELECT
                names.github_repo_id,
                names.full_name AS previous_full_name,
                ROW_NUMBER() OVER (
                    PARTITION BY names.github_repo_id
                    ORDER BY names.observed_at DESC, names.collection_run_id DESC
                ) AS recency
            FROM silver.repository_snapshot AS names
            INNER JOIN current_snapshot AS current_repo
                ON names.github_repo_id = current_repo.github_repo_id
            WHERE names.snapshot_status <> 'missing'
                AND names.full_name IS NOT NULL
                AND current_repo.full_name IS NOT NULL
                AND names.full_name <> current_repo.full_name
        ) AS earlier
        WHERE recency = 1
    ),
    latest_discovery AS (
        SELECT
            github_repo_id,
            full_name,
            pushed_at
        FROM (
            SELECT
                github_repo_id,
                full_name,
                pushed_at,
                ROW_NUMBER() OVER (
                    PARTITION BY github_repo_id
                    ORDER BY discovered_at DESC, search_rank ASC
                ) AS recency
            FROM silver.discovery_match
        ) AS ranked
        WHERE recency = 1
    )
    SELECT
        tracked.github_repo_id,
        COALESCE(current_repo.full_name, discovered.full_name, tracked.full_name) AS full_name,
        previous_name.previous_full_name,
        COALESCE(
            current_repo.owner_login,
            element_at(
                split(
                    COALESCE(current_repo.full_name, discovered.full_name, tracked.full_name),
                    '/'
                ),
                1
            )
        ) AS owner_login,
        current_repo.description,
        current_repo.primary_language,
        current_repo.topics_json,
        current_repo.created_at,
        COALESCE(current_repo.pushed_at, discovered.pushed_at) AS pushed_at,
        current_repo.is_fork,
        current_repo.is_archived,
        tracked.tracking_status,
        tracked.first_discovered_at
    FROM silver.tracked_repository AS tracked
    LEFT JOIN current_snapshot AS current_repo
        ON tracked.github_repo_id = current_repo.github_repo_id
    LEFT JOIN previous_name
        ON tracked.github_repo_id = previous_name.github_repo_id
    LEFT JOIN latest_discovery AS discovered
        ON tracked.github_repo_id = discovered.github_repo_id
    """
)

spark.sql(
    """
    CREATE OR REPLACE TABLE gold.fact_repository_snapshot AS
    WITH usable AS (
        SELECT
            github_repo_id,
            collection_run_id,
            observed_at,
            stargazers_count,
            forks_count,
            LAG(stargazers_count) OVER (
                PARTITION BY github_repo_id
                ORDER BY observed_at, collection_run_id
            ) AS previous_stars,
            LAG(forks_count) OVER (
                PARTITION BY github_repo_id
                ORDER BY observed_at, collection_run_id
            ) AS previous_forks
        FROM silver.repository_snapshot
        WHERE snapshot_status <> 'missing'
    ),
    deltas AS (
        SELECT
            github_repo_id,
            collection_run_id,
            CASE
                WHEN previous_stars IS NULL OR stargazers_count IS NULL THEN NULL
                ELSE stargazers_count - previous_stars
            END AS star_delta,
            CASE
                WHEN previous_forks IS NULL OR forks_count IS NULL THEN NULL
                ELSE forks_count - previous_forks
            END AS fork_delta
        FROM usable
    )
    SELECT
        snapshot.github_repo_id,
        snapshot.collection_run_id,
        snapshot.observed_at,
        snapshot.stargazers_count,
        snapshot.forks_count,
        snapshot.subscribers_count,
        snapshot.open_issues_count,
        deltas.star_delta,
        deltas.fork_delta,
        snapshot.primary_language,
        snapshot.topics_json,
        snapshot.is_fork,
        snapshot.is_archived,
        CASE
            WHEN snapshot.created_at IS NULL OR snapshot.observed_at IS NULL THEN NULL
            ELSE datediff(to_date(snapshot.observed_at), to_date(snapshot.created_at))
        END AS days_since_created,
        CASE
            WHEN snapshot.pushed_at IS NULL OR snapshot.observed_at IS NULL THEN NULL
            ELSE datediff(to_date(snapshot.observed_at), to_date(snapshot.pushed_at))
        END AS days_since_push,
        snapshot.snapshot_status
    FROM silver.repository_snapshot AS snapshot
    LEFT JOIN deltas
        ON snapshot.github_repo_id = deltas.github_repo_id
        AND snapshot.collection_run_id = deltas.collection_run_id
    """
)

spark.sql(
    """
    CREATE OR REPLACE TABLE gold.fact_discovery AS
    SELECT
        github_repo_id,
        technology_id,
        query_id,
        collection_run_id,
        discovered_at,
        search_rank,
        result_cap_hit,
        incomplete_results
    FROM silver.discovery_match
    """
)

print("gold_status=replaced")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
