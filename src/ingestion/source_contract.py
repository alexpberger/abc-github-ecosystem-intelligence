"""Lakehouse names and ingestion limits for GitHub Ecosystem Intelligence.

Notebooks repeat the block between the markers. The parity test compares it.
"""

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
