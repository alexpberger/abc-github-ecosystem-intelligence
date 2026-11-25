# Data model

Phase 1 publishes two dimensions and two facts. Column lists are in [data-dictionary.md](data-dictionary.md).

Bronze keeps the API responses. Silver and Gold are derived from every Bronze batch, not only the latest one.

## Grain

`silver.repository_snapshot` and `gold.fact_repository_snapshot` are one row per repository per `collection_run_id`.

`silver.discovery_match` and `gold.fact_discovery` are one row per repository, catalogue query, and `collection_run_id`.

`gold.dim_repository` is one row per repository ever discovered or snapshotted. `gold.dim_technology` is one row per focus area in the catalogue.

`github_repo_id` is GitHub's numeric repository id. It does not change when the repository is renamed. `technology_id` is a stable string from the catalogue, such as `agentic_ai`.

## Layers

### Bronze

Bronze is append-only. A run that is already present is not requested again.

`bronze.search_response` stores one GitHub search page. Pages from a window that was split again are kept with `keep_items = false` so the audit trail remains, without discovering the same repository twice. Leaf pages have `keep_items = true`. Each row records the lookback and the tracking cap used for that run.

`bronze.discovered_repository` stores one catalogue hit from a leaf page. The same repository can appear under more than one query.

`bronze.repository_response` stores one monitoring response. `response_json` is null when GitHub returns `304 Not Modified`. The ETag and the `full_name` used for the next call are columns, because a `304` has no body.

`collection_run_id` is the pipeline parameter. A blank discover parameter generates a UTC timestamp. Snapshot, when its parameter is blank, uses the latest discovery run. A scheduled pipeline should pass one shared id, normally the UTC date, so a retry on the same day does not create a second snapshot.

### Silver

`nb_silver` overwrites the Silver tables from all Bronze rows.

- `repository_snapshot` parses a `200` body. A `304` copies the latest earlier `200` for that repository and is marked `unchanged`. A `404` is marked `missing`, keeps the requested name, and clears the metrics. A `304` with no earlier body fails the notebook.
- `discovery_match` keeps the best search rank for each repository, query, and run.
- `tracked_repository` marks a repository `tracked` once any monitoring response exists. Everything else that was discovered is `discovered_only`.

Topics are a JSON array string. The SQL analytics endpoint does not expose a Delta array column, and a string stays readable there and in a later DuckDB job. License, default branch, and per-language byte counts stay in the raw JSON.

### Gold

`nb_gold_model` overwrites the four Gold tables with Spark SQL.

| Table | Grain | Key |
| --- | --- | --- |
| `gold.dim_technology` | One catalogue focus area | `technology_id` |
| `gold.dim_repository` | One repository | `github_repo_id` |
| `gold.fact_repository_snapshot` | One repository in one collection run | `github_repo_id`, `collection_run_id` |
| `gold.fact_discovery` | One repository, query, and run | `github_repo_id`, `query_id`, `collection_run_id` |

`dim_repository` uses the latest non-missing snapshot for the current description, language, topics, and flags. `previous_full_name` is the most recent different name, when the repository was renamed. A repository that was discovered but not snapshotted still has a row, with attributes taken from discovery.

`fact_repository_snapshot.star_delta` is the star count minus the previous non-missing snapshot. It is null on the first observation and on a missing row. A negative delta is a real change. `fork_delta` follows the same rule. `days_since_created` and `days_since_push` are calendar days between the observation and those timestamps.

There is no calendar, owner, language, or topic dimension. Group by `observed_at`, `primary_language`, or an `OPENJSON` of `topics_json`.

### Quality

`quality.check_result` is overwritten each run. A failed critical check fails the notebook, which fails the pipeline activity. Check names are listed in [AGENTS.md](../AGENTS.md).

## How the questions are answered

Phase 1 does not pre-aggregate these. [validation.sql](validation.sql) shows the T-SQL shape.

| Question | Query shape |
| --- | --- |
| Which repositories gained stars | `fact_repository_snapshot.star_delta` joined to `dim_repository` |
| Where new repositories appeared | `fact_discovery` joined to `dim_technology`, grouped by week |
| Growth inside a technology | Distinct `(github_repo_id, technology_id)` from `fact_discovery`, joined to the latest snapshot |
| Language mix | `GROUP BY primary_language` on `dim_repository` |
| Topic mix | `OPENJSON(topics_json)` on `fact_repository_snapshot` |
| Repositories left outside the cap | `dim_repository.tracking_status = 'discovered_only'` |

A repository found by two queries belongs to two technologies. Summing stars across technologies counts that repository more than once. The validation script says so next to the query.

## Keys and orphans

Every snapshot fact row must find a `dim_repository` row. Every discovery fact row must find both a repository and a technology. `dim_technology` is seeded from the catalogue, so the seven focus areas exist even when a run discovered nothing.

Missing language and missing topics stay null. They are not given an unknown member.

## What the history does not contain

GitHub does not provide the past star series. The first snapshot records the metrics at collection time, including for repositories created during the lookback. Older star counts are not reconstructed. Search can also omit a repository until GitHub has indexed it, or when a one-day window still has more than 1,000 hits. Those gaps are stored as `result_cap_hit` and `incomplete_results` rather than treated as a complete list.
