# Data dictionary

Types below are the types the notebooks write. The SQL analytics endpoint may show Spark booleans as `bit` and timestamps without a time zone. Timestamp values are UTC.

## bronze.search_response

One search page. `keep_items` is false for a window that was split into smaller windows.

| Column | Type | Description |
| --- | --- | --- |
| `collection_run_id` | string | Pipeline run id |
| `query_id` | string | Catalogue query, such as `topic_rag` |
| `technology_id` | string | Focus area, such as `rag` |
| `window_start` | string | Inclusive `created` date, `yyyy-MM-dd` |
| `window_end` | string | Inclusive `created` date |
| `page_number` | int | Search page, starting at 1 |
| `request_url` | string | URL that was called |
| `http_status` | int | HTTP status |
| `rate_limit_remaining` | int | `x-ratelimit-remaining` after the call. Null when the header is absent |
| `total_count` | long | `total_count` from the search body |
| `incomplete_results` | boolean | GitHub's `incomplete_results` flag |
| `result_cap_hit` | boolean | True when a one-day window still could not be read completely |
| `keep_items` | boolean | True when repositories on this page are part of discovery |
| `lookback_days` | int | Lookback requested for the run |
| `max_tracked_repositories` | int | Tracking cap stored for the snapshot step |
| `response_json` | string | Raw response body |
| `collected_at` | timestamp | UTC timestamp captured once per discover batch |

## bronze.discovered_repository

One catalogue hit from a leaf search page.

| Column | Type | Description |
| --- | --- | --- |
| `collection_run_id` | string | Pipeline run id |
| `query_id` | string | Catalogue query |
| `technology_id` | string | Focus area for that query |
| `github_repo_id` | long | GitHub repository id |
| `full_name` | string | `owner/name` at discovery |
| `stargazers_count` | long | Stars reported by search. Used only to rank new repositories |
| `pushed_at` | string | Push timestamp as returned by search |
| `search_rank` | int | Position across pages of that window, starting at 1 |
| `result_cap_hit` | boolean | Cap flag copied from the leaf window |
| `incomplete_results` | boolean | Incomplete flag copied from the page |
| `collected_at` | timestamp | Same UTC timestamp as the search batch |

## bronze.repository_response

One monitoring response.

| Column | Type | Description |
| --- | --- | --- |
| `collection_run_id` | string | Pipeline run id |
| `github_repo_id` | long | GitHub repository id |
| `full_name` | string | Name from a `200` body when present, otherwise the name that was requested |
| `request_url` | string | URL that was called |
| `http_status` | int | `200`, `304`, or `404` |
| `etag` | string | ETag to send on the next conditional request |
| `rate_limit_remaining` | int | Core rate-limit header. Null when absent |
| `response_json` | string | Raw body. Null for `304` |
| `collected_at` | timestamp | UTC timestamp captured once per snapshot batch |

## silver.repository_snapshot

One observation of one repository.

| Column | Type | Description |
| --- | --- | --- |
| `github_repo_id` | long | Stable repository id |
| `collection_run_id` | string | Run that produced the observation |
| `observed_at` | timestamp | When the response was collected |
| `full_name` | string | `owner/name` |
| `owner_login` | string | Owner login. Null on a missing repository |
| `description` | string | Description. Blank becomes null |
| `primary_language` | string | GitHub's `language` field. Null when GitHub has none |
| `topics_json` | string | JSON array of topic strings, or null |
| `created_at` | timestamp | Repository creation time |
| `updated_at` | timestamp | GitHub `updated_at` |
| `pushed_at` | timestamp | Last push |
| `stargazers_count` | long | Stars. Null when `snapshot_status` is `missing` |
| `forks_count` | long | Forks. Null when missing |
| `subscribers_count` | long | Watchers. Null when GitHub omits the field or the row is missing |
| `open_issues_count` | long | Open issues plus pull requests, as reported by GitHub |
| `is_fork` | boolean | True when the repository is a fork |
| `is_archived` | boolean | True when the repository is archived |
| `snapshot_status` | string | `refreshed`, `unchanged`, or `missing` |
| `etag` | string | ETag stored with the observation |

## silver.discovery_match

One kept search hit.

| Column | Type | Description |
| --- | --- | --- |
| `github_repo_id` | long | Repository id |
| `query_id` | string | Catalogue query |
| `technology_id` | string | Focus area |
| `collection_run_id` | string | Discovery run |
| `full_name` | string | Name returned by search |
| `stargazers_count` | long | Stars returned by search |
| `pushed_at` | timestamp | Parsed search push time |
| `discovered_at` | timestamp | When the search batch was collected |
| `search_rank` | int | Best rank kept for this repository, query, and run |
| `result_cap_hit` | boolean | Leaf window exceeded the search cap |
| `incomplete_results` | boolean | Leaf page was marked incomplete |

## silver.tracked_repository

The registry used by the next snapshot and by Gold.

| Column | Type | Description |
| --- | --- | --- |
| `github_repo_id` | long | Repository id |
| `full_name` | string | Latest known name |
| `first_discovered_at` | timestamp | Earliest discovery time. Null only if a response exists without a discovery row |
| `tracking_status` | string | `tracked` or `discovered_only` |
| `etag` | string | Latest monitoring ETag. Null until the first response |

## gold.dim_technology

| Column | Type | Description |
| --- | --- | --- |
| `technology_id` | string | Stable id: `agentic_ai`, `llm_frameworks`, `rag`, `mcp`, `ai_infrastructure`, `data_engineering`, `developer_tooling` |
| `technology_name` | string | Display name |

The seven rows are seeded from the catalogue on every Gold build.

## gold.dim_repository

| Column | Type | Description |
| --- | --- | --- |
| `github_repo_id` | long | Natural key |
| `full_name` | string | Current name |
| `previous_full_name` | string | Most recent different name. Null when the name has not changed |
| `owner_login` | string | Owner login, or the owner segment of `full_name` |
| `description` | string | Latest non-missing description |
| `primary_language` | string | Latest non-missing primary language |
| `topics_json` | string | Latest non-missing topic JSON |
| `created_at` | timestamp | Creation time from the latest non-missing snapshot |
| `pushed_at` | timestamp | Latest known push time |
| `is_fork` | boolean | Latest non-missing fork flag. Null when the repository has never been snapshotted |
| `is_archived` | boolean | Latest non-missing archived flag |
| `tracking_status` | string | `tracked` or `discovered_only` |
| `first_discovered_at` | timestamp | First time a catalogue query returned the repository |

## gold.fact_repository_snapshot

| Column | Type | Description |
| --- | --- | --- |
| `github_repo_id` | long | Repository |
| `collection_run_id` | string | Observation |
| `observed_at` | timestamp | Collection time |
| `stargazers_count` | long | Stars |
| `forks_count` | long | Forks |
| `subscribers_count` | long | Watchers |
| `open_issues_count` | long | Open issues including pull requests |
| `star_delta` | long | Change since the previous non-missing snapshot. Null on the first observation |
| `fork_delta` | long | Change in forks, with the same null rule |
| `primary_language` | string | Language at this observation |
| `topics_json` | string | Topics at this observation |
| `is_fork` | boolean | Fork flag at this observation |
| `is_archived` | boolean | Archived flag at this observation |
| `days_since_created` | int | Calendar days from `created_at` to `observed_at` |
| `days_since_push` | int | Calendar days from `pushed_at` to `observed_at` |
| `snapshot_status` | string | `refreshed`, `unchanged`, or `missing` |

## gold.fact_discovery

| Column | Type | Description |
| --- | --- | --- |
| `github_repo_id` | long | Repository |
| `technology_id` | string | Focus area of the query |
| `query_id` | string | Catalogue query |
| `collection_run_id` | string | Discovery run |
| `discovered_at` | timestamp | When the search batch was collected |
| `search_rank` | int | Rank inside that query window |
| `result_cap_hit` | boolean | The leaf window was over the search cap |
| `incomplete_results` | boolean | The leaf page was incomplete |

## quality.check_result

| Column | Type | Description |
| --- | --- | --- |
| `check_name` | string | Stable check name |
| `severity` | string | `critical` or `warning` |
| `passed` | boolean | True when the expectation holds |
| `observed_value` | string | What the notebook measured |
| `expected_value` | string | What the check required |
| `checked_at` | timestamp | UTC time the check table was written |
