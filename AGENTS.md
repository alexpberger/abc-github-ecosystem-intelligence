# AGENTS.md

Engineering reference for humans and coding agents working on GitHub Ecosystem Intelligence.

This file describes the project as it is now. A feature listed under a later phase is planned, not implemented.

## Project overview

GitHub Ecosystem Intelligence is a public portfolio project for Alex Berger Consulting. It demonstrates an end-to-end data platform on Microsoft Fabric.

The platform collects public GitHub repositories in a focused set of technology areas and keeps a history of their metadata. It should let someone analyse:

- Where newly discovered repositories are appearing
- How stars and forks change between snapshots
- Language mix
- Topic mix
- Which discovered repositories are outside the tracking cap

Phase 1 is the Fabric lakehouse. A later portable operator, wider collection, and traction prediction are separate phases.

The developer knows Python, SQL, data engineering, and cloud application development, and is learning Fabric. Explain Fabric-specific behaviour in the docs. Keep PySpark readable. Do not add abstractions that hide the platform.

## Dataset

There is no static source file. The live source is the public GitHub REST API.

- Discovery uses `GET /search/repositories` for seven catalogue queries, one per focus area.
- Monitoring uses `GET /repos/{owner}/{repo}`, with `If-None-Match` when an ETag is known.
- Sample payloads used by unit tests live in `tests/fixtures/github/`. They are synthetic and small.
- Do not commit API tokens, response caches, or bulk API dumps.

GitHub search is not a complete census of new public repositories. The limits and the collection strategy are in [docs/decisions.md](docs/decisions.md) and [docs/data-model.md](docs/data-model.md).

## Architecture

One schema-enabled Fabric lakehouse, `lh_ecosystem`. No Fabric Warehouse.

| Schema | Tables | Role |
| --- | --- | --- |
| `bronze` | `search_response`, `discovered_repository`, `repository_response` | Append-only API responses and the rows extracted for the next step |
| `silver` | `repository_snapshot`, `discovery_match`, `tracked_repository` | Typed history, discovery matches, and the tracking registry |
| `gold` | `dim_repository`, `dim_technology`, `fact_repository_snapshot`, `fact_discovery` | The analytical model |
| `quality` | `check_result` | One row per data-quality check for the latest run |

Schemas must be enabled when the lakehouse is created. Fabric cannot turn them on later.

Processing:

1. `nb_discover` calls the Search API and appends a batch.
2. `nb_snapshot` refreshes the tracked repositories and appends their responses.
3. `nb_silver` rebuilds the Silver tables from all Bronze history with the DataFrame API.
4. `nb_gold_model` rebuilds the four Gold tables with Spark SQL.
5. `nb_quality_checks` writes `quality.check_result` and fails the notebook if a critical check fails.

A Data Factory pipeline, `pl_github_ecosystem`, runs those notebooks in that order. The repo contains a template at [docs/pipeline-template.json](docs/pipeline-template.json). It is not a Fabric item yet, because notebook activities require the workspace and notebook GUIDs created by the portal.

The diagram and the Git boundary are in [architecture/architecture.md](architecture/architecture.md).

## Technology choices

- Microsoft Fabric is the execution environment. Do not containerise it or reproduce it locally.
- OneLake stores Delta tables written by the notebooks.
- One lakehouse keeps Bronze, Silver, Gold, and quality checks in separate schemas and avoids a Warehouse in Phase 1.
- Data Factory pipelines orchestrate notebook runs.
- PySpark notebooks perform collection and transformation. Silver uses the DataFrame API. Gold uses Spark SQL.
- Delta Lake is the table format.
- The SQL analytics endpoint is how T-SQL is demonstrated. Queries live in [docs/validation.sql](docs/validation.sql).
- GitHub and Cursor are the local source-control and editing tools.
- Python under `src/` holds the client, catalogue, parsing, and tracking rules that can be unit-tested without Spark or the network. Ingestion notebooks repeat those functions, and tests check they still match.

## Repository structure

```text
abc-github-ecosystem-intelligence/
├── AGENTS.md
├── README.md
├── requirements-dev.txt
├── pytest.ini
├── architecture/
│ └── architecture.md
├── data/
│ └── README.md
├── docs/
├── fabric/
│ └── notebooks/
│     ├── nb_discover.Notebook/
│     ├── nb_snapshot.Notebook/
│     ├── nb_silver.Notebook/
│     ├── nb_gold_model.Notebook/
│     └── nb_quality_checks.Notebook/
├── src/
│   ├── ingestion/
│   ├── quality/
│   └── transformations/
└── tests/
    ├── fixtures/github/
    └── unit/
```

Fabric item folders are named `{displayName}.{ItemType}` and contain `.platform` plus the item definition. Connect the workspace Git integration to the `/fabric` directory, not the repository root, so docs and Python tests are not treated as Fabric items.

The lakehouse folder is created by Fabric after the portal item exists. Do not hand-author `lh_ecosystem.Lakehouse`. A hand-written folder would not reliably create a schema-enabled lakehouse, and Git does not store lakehouse tables or files.

Do not add folders for the Phase 2 operator, Streamlit, GitHub Actions, DuckDB, or a model-training project.

## Development principles

- Prefer a direct notebook over a framework. This is a learning project.
- Implement one phase at a time. Do not build Phase 2–4 features while finishing Phase 1.
- Keep secrets, tokens, and bulk API dumps out of Git.
- Do not invent Fabric item formats, GUIDs, or portal clicks. Use [docs/setup.md](docs/setup.md) and current Microsoft Learn docs.
- Do not claim a notebook, pipeline, or SQL query has been run in Fabric until it has.
- When Fabric writes lakehouse ids back into `notebook-content.py` after the lakehouse is attached, commit that generated metadata. Do not put placeholder GUIDs in first.

## Design system

Visual identity is taken from the separate project `abc-prediction-platform`. Do not modify that project. Tokens are in [docs/design-system.md](docs/design-system.md). Phase 1 does not add a theme file or a user interface.

## Business rules

The tested definitions live in:

- [src/ingestion/source_contract.py](src/ingestion/source_contract.py) — lakehouse name, table names, lookback, tracking cap
- [src/ingestion/queries.py](src/ingestion/queries.py) — catalogue, date windows, search URLs
- [src/ingestion/github_client.py](src/ingestion/github_client.py) — headers, retry, rate-limit pause
- [src/ingestion/parse.py](src/ingestion/parse.py) — payload extraction and search collection
- [src/transformations/rules.py](src/transformations/rules.py) — tracking selection, snapshot carry-forward, metric delta
- [src/quality/checks.py](src/quality/checks.py) — check names

`nb_discover` and `nb_snapshot` contain the same functions between marker comments. `tests/unit/test_notebook_parity.py` fails if those copies drift. Fabric Git sync deploys `/fabric` only, so the notebooks cannot import `src/`.

`github_repo_id` is the stable repository key. `full_name` can change after a rename. A repository can belong to more than one technology through `fact_discovery`.

Topics are stored as a JSON array string, `topics_json`, because the SQL analytics endpoint does not expose Delta array columns. `watchers_count` is ignored because GitHub returns it as a copy of the star count. `open_issues_count` includes pull requests and is stored as reported. `subscribers_count` is the watcher count and may be absent.

Net metrics are not calculated. `star_delta` and `fork_delta` are the change since the previous non-missing snapshot. The first observation and a missing repository have null deltas. A decrease is valid.

## Phase 1 incremental pattern

- Discover appends. If `collection_run_id` is already in `bronze.search_response`, the discover notebook calls GitHub again for nothing.
- If discovered rows exist for that id but the search batch does not, those discovered rows are deleted and the search is fetched again.
- Snapshot appends. If the same id is already in `bronze.repository_response`, it calls GitHub again for nothing.
- The tracking cap is the `max_tracked_repositories` value stored on that discovery batch. Repositories already present in an earlier response stay tracked. New ones fill any remaining room. Nothing already tracked is removed.
- Silver and Gold are rebuilt from all Bronze history. Rebuilding from only the latest batch would drop the time series.
- This is not a `MERGE` pipeline. A repeated run id does not create a second snapshot. A corrected load needs a new id.

## Data quality

`nb_quality_checks` writes every check to `quality.check_result` and raises if any critical check fails. Critical checks:

- `repository_responses_complete`
- `unique_snapshot_grain`
- `snapshot_repo_id_present`
- `fact_matches_silver`
- `no_orphan_snapshot_repository`
- `no_orphan_discovery_technology`
- `no_orphan_discovery_repository`
- `stars_and_forks_non_negative`
- `tracked_within_cap`
- `tracked_repos_snapshotted`

Warnings do not stop the pipeline:

- `search_result_cap_hit`
- `search_incomplete_results`
- `discovered_not_tracked`

Lowering the tracking cap below the number of repositories already tracked fails `tracked_within_cap`. Raise the cap again, or accept the failed check. The load does not drop repositories to make the check pass.

## Local verification

```bash
python -m pip install -r requirements-dev.txt
python -m pytest
```

Pytest covers the pure-Python rules and the notebook parity checks. It does not start Spark, call GitHub, or talk to Fabric.

## Roadmap

### Phase 1 — Fabric foundations (this repository state)

Source for the lakehouse pipeline is in the repo: notebooks, rules, quality checks, and setup instructions.

Not done, and not to be described as done until a developer completes [docs/setup.md](docs/setup.md):

- Fabric capacity, workspace, and schema-enabled lakehouse
- Git sync and lakehouse attachment
- A GitHub token in Key Vault, readable by the identity that runs the notebooks
- Pipeline item with real notebook and workspace ids
- A successful pipeline run and the T-SQL checks

### Phase 2 — Portable operator (not started)

A separate implementation intended to run without a continuously assigned Fabric capacity:

- Python for ingestion and transformations
- GitHub Actions for a daily schedule
- Parquet for stored tables
- DuckDB for analytical queries
- An optional Streamlit app

Do not add that implementation while finishing Phase 1. The seam is already the tested Python in `src/` and the column names in the Gold tables.

### Phase 3 — Wider collection (not started)

- More catalogue queries
- Incremental Silver and Gold
- Language byte breakdowns or a broader discovery source
- Scheduling and monitoring beyond the manual pipeline run

### Phase 4 — Learning extension (not started)

Early repository traction prediction is the candidate. Decide the design when the phase starts. Do not add a model directory before then.

## Out of scope until a later phase explicitly takes it on

- LLM classification, embeddings, or an agent
- A custom frontend
- GitHub Archive ingestion
- Reconstructing star history from stargazer pages
- The `/languages` endpoint
- Streaming
- A Fabric Warehouse
- Docker, Dev Containers, and Terraform

## Instructions for later sessions

1. Read this file, [docs/decisions.md](docs/decisions.md), and [docs/data-model.md](docs/data-model.md) before changing the model.
2. Inspect the repository before editing. Do not commit tokens or API dumps.
3. Keep `src/` and the marked notebook blocks in sync. Run `python -m pytest` after changing either.
4. Follow Fabric’s Git item format for anything under `fabric/`. Unique `.platform` `logicalId` values must not be copied from another item.
5. Leave pipeline JSON in `docs/` until real workspace and notebook ids exist.
6. Do not implement Phase 2–4 work as part of a Phase 1 fix.
7. In the session summary, separate files changed, portal steps still required, and what was actually executed.
