"""Names of the data-quality checks the Fabric quality notebook must run.

A critical failure fails the notebook. A warning is written and does not stop the pipeline.
"""

CRITICAL_CHECKS = (
    "bronze_batch_not_empty",
    "silver_exclusions_accounted",
    "fact_matches_silver",
    "no_orphan_date_keys",
    "no_orphan_product_keys",
    "no_orphan_customer_keys",
    "no_orphan_geography_keys",
    "unique_product_keys",
    "unique_customer_keys",
    "net_amount_matches_inputs",
    "unknown_customer_present",
)

WARNING_CHECKS = ("both_source_sheets_present",)


def is_critical(check_name: str) -> bool:
    return check_name in CRITICAL_CHECKS
