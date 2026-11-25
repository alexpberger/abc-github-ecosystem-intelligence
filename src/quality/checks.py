"""Names of the data-quality checks the Fabric quality notebook must run.

A critical failure fails the notebook. A warning is written and does not stop the pipeline.
"""

CRITICAL_CHECKS = (
    "repository_responses_complete",
    "unique_snapshot_grain",
    "snapshot_repo_id_present",
    "fact_matches_silver",
    "no_orphan_snapshot_repository",
    "no_orphan_discovery_technology",
    "no_orphan_discovery_repository",
    "stars_and_forks_non_negative",
    "tracked_within_cap",
    "tracked_repos_snapshotted",
)

WARNING_CHECKS = (
    "search_result_cap_hit",
    "search_incomplete_results",
    "discovered_not_tracked",
)


def is_critical(check_name):
    return check_name in CRITICAL_CHECKS
