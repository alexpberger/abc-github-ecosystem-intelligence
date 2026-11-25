# Architectural decisions

Decisions recorded for Phase 1. Later phases can supersede them in this log rather than silently changing behaviour.

## D1. One schema-enabled lakehouse

**Decision.** Use a single lakehouse, `lh_ecosystem`, with schemas `bronze`, `silver`, `gold`, and `quality`. Do not add a Fabric Warehouse.

**Why.** The first release is one pipeline and one team. Schemas keep the Medallion layers visible. The lakehouse SQL analytics endpoint already provides T-SQL.

**Consequence.** Schemas must be enabled in the portal at creation time. The lakehouse item is not hand-written in Git.

## D2. Notebooks in Git, pipeline as a template

**Decision.** Commit five notebooks under `fabric/notebooks/` in Fabric's `notebook-content.py` format. Keep the pipeline JSON in `docs/pipeline-template.json` until it contains real workspace and notebook ids.

**Why.** A notebook activity requires `notebookId` and `workspaceId`. Those GUIDs do not exist before the workspace does.

**Consequence.** The first Fabric sync creates notebooks only. The developer attaches the lakehouse, fills the template, and syncs the pipeline as a second step. See [setup.md](setup.md).

## D3. Curated search, not a census

**Decision.** Discover repositories with seven Search API queries, one topic per focus area, plus `is:public fork:false`. Do not walk `GET /repositories`, the public events feed, or GH Archive in Phase 1.

**Why.** Search returns at most 1,000 results for a query, does not sort by creation time, and can stop early with `incomplete_results`. The events feed is only the latest few hundred events. Listing every public repository is the wrong tool for a topic-focused learning project.

**Consequence.** A window whose `total_count` exceeds 1,000, or whose result is incomplete, is split down to one day. A one-day window that still overflows is stored with `result_cap_hit` and is not treated as complete. More topics are added by extending `QUERIES`, not by adding tables.

## D4. History starts when we collect it

**Decision.** Snapshots record the metrics returned at collection time. Do not reconstruct older star counts from stargazer pages.

**Why.** The repository API does not provide a historical metric series. Stargazer pagination would spend the rate limit on a handful of popular repositories.

**Consequence.** The lookback finds repositories created in that window and takes their first snapshot now. Growth appears only after a later run. The default lookback is 14 inclusive days ending on the UTC date of the run.

## D5. Append raw responses, rebuild derived history

**Decision.** Bronze appends a new `collection_run_id` and skips an id that already has its commit table. Silver and Gold overwrite from all Bronze history.

**Why.** Skipping a known run is the incremental and retry-safe part. Rebuilding Silver from only the latest batch would delete earlier snapshots. The default cap keeps a full rebuild small.

**Consequence.** Discover treats `bronze.search_response` as the commit marker. Snapshot treats `bronze.repository_response` as its commit marker. A corrected load uses a new run id. `MERGE` is not part of Phase 1.

## D6. Stable repository id and four Gold tables

**Decision.** Use `github_repo_id` and `technology_id` as natural keys. Publish `dim_repository`, `dim_technology`, `fact_repository_snapshot`, and `fact_discovery` only.

**Why.** GitHub's numeric id survives a rename. A separate owner, language, topic, or date dimension is not required to group the facts. A bridge table is not required because discovery already records the technology.

**Consequence.** Language is a column. Topics are a JSON array string, `topics_json`, because the SQL analytics endpoint does not expose Delta array columns. A repository in two technologies is counted in both when those facts are summed. Validation SQL says so.

## D7. Tracking cap without eviction

**Decision.** Default `max_tracked_repositories` is 500. Repositories that already have a monitoring response stay tracked. New repositories fill any remaining room, ordered by stars and then by latest push. The rest stay `discovered_only`.

**Why.** Five hundred repository reads fit in the authenticated core limit of 5,000 requests per hour, with room left for retries. Dropping a tracked repository would open a hole in its series.

**Consequence.** Discovery can see more repositories than the snapshot step reads. Lowering the cap below the number already tracked fails `tracked_within_cap` instead of silently dropping rows.

## D8. Conditional reads and explicit missing rows

**Decision.** Send the stored ETag on the next repository read. Copy metrics forward on `304`. Write a `missing` snapshot on `404` and keep the history.

**Why.** An unchanged repository should not spend core quota. A deleted or privatised repository should remain visible as missing rather than disappear from the series.

**Consequence.** `watchers_count` is ignored because it duplicates stars. `subscribers_count` is the watcher field and may be null. `open_issues_count` includes pull requests and is stored as GitHub reports it. The `/languages` endpoint is not called.

## D9. Rules live in src and in the notebooks

**Decision.** Pure Python in `src/` is the tested definition of the client, catalogue, parsing, and tracking rules. The two ingestion notebooks contain the same functions. Pytest compares the text.

**Why.** Fabric Git sync deploys `/fabric` only. Copying the functions into the notebook is what will actually run.

**Consequence.** A change to either copy fails `tests/unit/test_notebook_parity.py` until the other copy matches. There is no plugin or client framework around those functions.

## D10. Token in Key Vault

**Decision.** The Fabric notebooks read the GitHub token with `notebookutils.credentials.getSecret(key_vault_url, secret_name)`. The token is never a notebook default, a pipeline parameter, or a committed file.

**Why.** Microsoft Learn documents that call and tells notebooks not to embed secrets. A fine-grained personal access token with public-repository read, or a classic token with no extra scopes, is enough for this public data.

**Consequence.** The identity that executes the notebook needs permission to get that secret. `key_vault_url` is the vault URL, for example `https://example.vault.azure.net/`. Local experiments can set `GITHUB_TOKEN` only when the vault URL parameter is empty. Pytest does not read either one.

## D11. Design identity comes from abc-prediction-platform

**Decision.** Document that project's colour and type tokens in [design-system.md](design-system.md). Do not invent a second palette. Do not add a theme file or a user interface in Phase 1.

## D12. No local Fabric stand-in

**Decision.** Do not add Docker, a Dev Container, Terraform, or a local Spark project in Phase 1.

**Why.** Fabric execution cannot be reproduced faithfully that way, and the learning goal is the managed platform. Local pytest covers the rules that do not need Spark or GitHub.
