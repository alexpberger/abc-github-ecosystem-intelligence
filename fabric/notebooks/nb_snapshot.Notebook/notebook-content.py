# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Parameters. Fabric overwrites these values when a pipeline activity supplies them.
collection_run_id = ""
key_vault_url = ""
secret_name = "github-token"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "tags": [
# META     "parameters"
# META   ]
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

# QUERIES_START
import urllib.parse
from datetime import timedelta

SEARCH_RESULT_CAP = 1000
SEARCH_PER_PAGE = 100
GITHUB_API_BASE = "https://api.github.com"

# One query per focus area. Add technologies by extending this tuple.
QUERIES = (
    {
        "query_id": "topic_ai_agents",
        "technology_id": "agentic_ai",
        "technology_name": "Agentic AI",
        "search": "topic:ai-agents is:public fork:false",
    },
    {
        "query_id": "topic_langchain",
        "technology_id": "llm_frameworks",
        "technology_name": "LLM frameworks",
        "search": "topic:langchain is:public fork:false",
    },
    {
        "query_id": "topic_rag",
        "technology_id": "rag",
        "technology_name": "RAG",
        "search": "topic:rag is:public fork:false",
    },
    {
        "query_id": "topic_model_context_protocol",
        "technology_id": "mcp",
        "technology_name": "MCP",
        "search": "topic:model-context-protocol is:public fork:false",
    },
    {
        "query_id": "topic_vllm",
        "technology_id": "ai_infrastructure",
        "technology_name": "AI infrastructure",
        "search": "topic:vllm is:public fork:false",
    },
    {
        "query_id": "topic_data_engineering",
        "technology_id": "data_engineering",
        "technology_name": "Data engineering",
        "search": "topic:data-engineering is:public fork:false",
    },
    {
        "query_id": "topic_devtools",
        "technology_id": "developer_tooling",
        "technology_name": "Developer tooling",
        "search": "topic:devtools is:public fork:false",
    },
)


def technologies():
    """Stable focus areas in catalogue order."""
    found = []
    seen = set()
    for query in QUERIES:
        technology_id = query["technology_id"]
        if technology_id in seen:
            continue
        seen.add(technology_id)
        found.append(
            {
                "technology_id": technology_id,
                "technology_name": query["technology_name"],
            }
        )
    return tuple(found)


def lookback_window(as_of, lookback_days):
    """Inclusive created-date window ending on as_of."""
    days = int(lookback_days)
    if days < 1:
        raise ValueError("lookback_days must be at least 1")
    return as_of - timedelta(days=days - 1), as_of


def window_day_count(start, end):
    return (end - start).days + 1


def split_window(start, end):
    """Split a window into two contiguous, non-empty parts."""
    if window_day_count(start, end) <= 1:
        raise ValueError("Cannot split a one-day window")
    left_days = window_day_count(start, end) // 2
    left_end = start + timedelta(days=left_days - 1)
    right_start = left_end + timedelta(days=1)
    return (start, left_end), (right_start, end)


def needs_split(total_count, incomplete, start, end):
    """True when a multi-day window is too wide to treat as complete."""
    if window_day_count(start, end) <= 1:
        return False
    if incomplete or total_count is None:
        return True
    return int(total_count) > SEARCH_RESULT_CAP


def cap_hit(total_count, incomplete, start, end):
    """True when a one-day window still cannot be read completely."""
    if window_day_count(start, end) > 1:
        return False
    if incomplete or total_count is None:
        return True
    return int(total_count) > SEARCH_RESULT_CAP


def pages_to_fetch(total_count, per_page=SEARCH_PER_PAGE):
    if total_count is None or int(total_count) <= 0:
        return 1
    capped = min(int(total_count), SEARCH_RESULT_CAP)
    pages = (capped + per_page - 1) // per_page
    return max(1, min(10, pages))


def search_text(base_search, start, end):
    return base_search + " created:" + start.isoformat() + ".." + end.isoformat()


def search_repositories_url(q, page, per_page=SEARCH_PER_PAGE):
    query = urllib.parse.urlencode(
        {"q": q, "per_page": str(per_page), "page": str(page)}
    )
    return GITHUB_API_BASE + "/search/repositories?" + query


def repository_url(full_name):
    if not full_name or "/" not in full_name:
        raise ValueError("full_name must look like owner/repository")
    owner, name = full_name.split("/", 1)
    if owner == "" or name == "":
        raise ValueError("full_name must look like owner/repository")
    return (
        GITHUB_API_BASE
        + "/repos/"
        + urllib.parse.quote(owner)
        + "/"
        + urllib.parse.quote(name)
    )


def require_run_id(run_id):
    text = str(run_id).strip()
    allowed = set(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.:-"
    )
    if not text or any(character not in allowed for character in text):
        raise ValueError(
            "collection_run_id must contain only letters, digits, and ._:-"
        )
    return text


def new_run_id(now):
    return now.strftime("%Y%m%dT%H%M%SZ")
# QUERIES_END

# CLIENT_START
import json
import time
import urllib.error
import urllib.request

GITHUB_ACCEPT = "application/vnd.github+json"
GITHUB_API_VERSION = "2022-11-28"
USER_AGENT = "abc-github-ecosystem-intelligence"
MAX_ATTEMPTS = 4


def build_headers(token, etag=None):
    if token is None or str(token).strip() == "":
        raise ValueError("GitHub token is required")
    headers = {
        "Accept": GITHUB_ACCEPT,
        "Authorization": "Bearer " + str(token).strip(),
        "User-Agent": USER_AGENT,
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }
    if etag:
        headers["If-None-Match"] = etag
    return headers


def header_map(headers):
    if headers is None:
        return {}
    return {str(key).lower(): value for key, value in headers.items()}


def _as_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def rate_limit_fields(headers):
    mapped = header_map(headers)
    return {
        "remaining": _as_int(mapped.get("x-ratelimit-remaining")),
        "reset": _as_int(mapped.get("x-ratelimit-reset")),
        "retry_after": _as_int(mapped.get("retry-after")),
    }


def is_rate_limited(status, rate, body_text):
    if status == 429:
        return True
    if status != 403:
        return False
    if rate.get("retry_after") is not None:
        return True
    if rate.get("remaining") == 0:
        return True
    text = (body_text or "").lower()
    return "rate limit" in text or "secondary rate" in text


def should_retry(status, rate, body_text, attempt, max_attempts=MAX_ATTEMPTS):
    if attempt >= max_attempts - 1:
        return False
    if status in (500, 502, 503, 504):
        return True
    return is_rate_limited(status, rate, body_text)


def wait_seconds(status, rate, now_epoch, attempt):
    if rate.get("retry_after") is not None:
        return float(rate["retry_after"])
    limited = is_rate_limited(status, rate, "") or rate.get("remaining") == 0
    if limited and rate.get("reset") is not None:
        return max(0.0, float(rate["reset"]) - float(now_epoch)) + 1.0
    return float(min(60, 2**attempt))


def pause_if_exhausted(response, now_epoch):
    """Seconds to wait before the next request. Zero when budget remains."""
    remaining = response.get("rate_limit_remaining")
    reset = response.get("rate_limit_reset")
    if remaining == 0 and reset is not None:
        return max(0.0, float(reset) - float(now_epoch)) + 1.0
    return 0.0


def _decode(raw):
    if not raw:
        return ""
    if isinstance(raw, str):
        return raw
    return raw.decode("utf-8")


def github_get(url, token, etag=None, opener=None, sleep=None, clock=None):
    """GET one URL. Retries rate limits and 5xx. Does not call GitHub in tests."""
    opener = opener or urllib.request.urlopen
    sleep = sleep or time.sleep
    clock = clock or time.time
    last = None
    for attempt in range(MAX_ATTEMPTS):
        request = urllib.request.Request(
            url,
            headers=build_headers(token, etag),
            method="GET",
        )
        try:
            response = opener(request, timeout=60)
            status = response.status if hasattr(response, "status") else response.getcode()
            mapped = header_map(response.headers)
            raw = response.read()
        except urllib.error.HTTPError as exc:
            status = exc.code
            mapped = header_map(exc.headers)
            raw = exc.read()
        except urllib.error.URLError:
            if attempt >= MAX_ATTEMPTS - 1:
                raise
            sleep(wait_seconds(503, {}, clock(), attempt))
            continue
        text = _decode(raw)
        rate = rate_limit_fields(mapped)
        body = None
        if status != 304 and text:
            try:
                body = json.loads(text)
            except json.JSONDecodeError:
                body = None
        last = {
            "status": status,
            "headers": mapped,
            "text": None if status == 304 else text,
            "body": body,
            "etag": mapped.get("etag"),
            "rate_limit_remaining": rate["remaining"],
            "rate_limit_reset": rate["reset"],
            "url": url,
        }
        if should_retry(status, rate, text, attempt):
            sleep(wait_seconds(status, rate, clock(), attempt))
            continue
        return last
    return last


def require_success(response, allowed=(200,)):
    if response["status"] in allowed:
        return response
    snippet = (response.get("text") or "")[:300]
    raise RuntimeError(
        "GitHub request failed status="
        + str(response["status"])
        + " url="
        + str(response.get("url"))
        + " body="
        + snippet
    )
# CLIENT_END

# PARSE_START
import json


def parse_repository_body(body):
    """Fields promoted out of a repository payload. Anything else stays in raw JSON."""
    if body is None:
        body = {}
    owner = body.get("owner") or {}
    topics = body.get("topics")
    if topics is None:
        topics_json = None
    else:
        topics_json = json.dumps(list(topics))
    fork_value = body.get("fork")
    archived_value = body.get("archived")
    return {
        "github_repo_id": body.get("id"),
        "full_name": body.get("full_name"),
        "owner_login": owner.get("login"),
        "description": body.get("description"),
        "primary_language": body.get("language"),
        "topics_json": topics_json,
        "created_at": body.get("created_at"),
        "updated_at": body.get("updated_at"),
        "pushed_at": body.get("pushed_at"),
        "stargazers_count": body.get("stargazers_count"),
        "forks_count": body.get("forks_count"),
        "subscribers_count": body.get("subscribers_count"),
        "open_issues_count": body.get("open_issues_count"),
        "is_fork": None if fork_value is None else bool(fork_value),
        "is_archived": None if archived_value is None else bool(archived_value),
    }


def discovered_from_item(item):
    return {
        "github_repo_id": item.get("id"),
        "full_name": item.get("full_name"),
        "stargazers_count": item.get("stargazers_count"),
        "pushed_at": item.get("pushed_at"),
    }


def collect_search(query, start, end, fetch):
    """Fetch one catalogue query, splitting windows that exceed the search cap.

    fetch(url) returns a response dict from github_get. Parent windows that are
    split are stored with keep_items false so their rows are not discovered twice.
    """
    search_rows = []
    found = []

    def record(page_response, page, window_start, window_end, keep_items, cap_flag):
        body = page_response.get("body") or {}
        items = body.get("items") or []
        search_rows.append(
            {
                "query_id": query["query_id"],
                "technology_id": query["technology_id"],
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "page_number": page,
                "request_url": page_response.get("url"),
                "http_status": page_response.get("status"),
                "rate_limit_remaining": page_response.get("rate_limit_remaining"),
                "total_count": body.get("total_count"),
                "incomplete_results": bool(body.get("incomplete_results")),
                "result_cap_hit": cap_flag,
                "keep_items": keep_items,
                "response_json": page_response.get("text"),
            }
        )
        if not keep_items:
            return
        for index, item in enumerate(items):
            parsed = discovered_from_item(item)
            if parsed["github_repo_id"] is None or not parsed["full_name"]:
                continue
            parsed.update(
                {
                    "query_id": query["query_id"],
                    "technology_id": query["technology_id"],
                    "search_rank": (page - 1) * SEARCH_PER_PAGE + index + 1,
                    "result_cap_hit": cap_flag,
                    "incomplete_results": bool(body.get("incomplete_results")),
                }
            )
            found.append(parsed)

    def walk(window_start, window_end):
        query_text = search_text(query["search"], window_start, window_end)
        first = require_success(fetch(search_repositories_url(query_text, 1)))
        body = first.get("body") or {}
        total = body.get("total_count")
        incomplete = bool(body.get("incomplete_results"))
        if needs_split(total, incomplete, window_start, window_end):
            record(first, 1, window_start, window_end, False, False)
            left, right = split_window(window_start, window_end)
            walk(left[0], left[1])
            walk(right[0], right[1])
            return
        hit = cap_hit(total, incomplete, window_start, window_end)
        record(first, 1, window_start, window_end, True, hit)
        for page in range(2, pages_to_fetch(total) + 1):
            response = require_success(
                fetch(search_repositories_url(query_text, page))
            )
            record(response, page, window_start, window_end, True, hit)

    walk(start, end)
    return search_rows, found
# PARSE_END

# RULES_START
METRIC_FIELDS = (
    "full_name",
    "owner_login",
    "description",
    "primary_language",
    "topics_json",
    "created_at",
    "updated_at",
    "pushed_at",
    "stargazers_count",
    "forks_count",
    "subscribers_count",
    "open_issues_count",
    "is_fork",
    "is_archived",
)

TRACKED = "tracked"
DISCOVERED_ONLY = "discovered_only"
SNAPSHOT_REFRESHED = "refreshed"
SNAPSHOT_UNCHANGED = "unchanged"
SNAPSHOT_MISSING = "missing"


def _stars(candidate):
    value = candidate.get("stargazers_count")
    if value is None:
        return -1
    return int(value)


def _pushed_at(candidate):
    value = candidate.get("pushed_at")
    if value is None:
        return ""
    return str(value)


def select_tracked(candidates, already_tracked_ids, max_tracked):
    """Keep every repository already tracked. Fill remaining room by stars, then push time.

    Repositories that do not fit are discovered_only. Nothing already tracked is removed,
    even when that set is already larger than max_tracked.
    """
    limit = int(max_tracked)
    if limit < 0:
        raise ValueError("max_tracked must be zero or greater")
    best = {}
    for candidate in candidates:
        repo_id = candidate.get("github_repo_id")
        if repo_id is None:
            continue
        rank = (_stars(candidate), _pushed_at(candidate))
        current = best.get(repo_id)
        if current is None or rank > current[0]:
            best[repo_id] = (rank, candidate)
    already_ids = {repo_id for repo_id in already_tracked_ids if repo_id is not None}
    newcomers = [best[repo_id][1] for repo_id in best if repo_id not in already_ids]
    newcomers.sort(
        key=lambda item: (
            _stars(item),
            _pushed_at(item),
            -int(item["github_repo_id"]),
        ),
        reverse=True,
    )
    room = max(0, limit - len(already_ids))
    chosen = newcomers[:room]
    tracked_ids = already_ids | {item["github_repo_id"] for item in chosen}
    discovered_only = [item["github_repo_id"] for item in newcomers[room:]]
    return {
        "tracked": tuple(sorted(tracked_ids)),
        "discovered_only": tuple(sorted(discovered_only)),
    }


def _empty_metrics():
    return {field: None for field in METRIC_FIELDS}


def snapshot_from_response(response, previous):
    """Map one repository response to snapshot fields.

    304 copies the previous 200. 404 keeps the requested name and clears metrics.
    """
    status = response.get("status")
    if status == 200:
        parsed = parse_repository_body(response.get("body") or {})
        parsed["snapshot_status"] = SNAPSHOT_REFRESHED
        parsed["etag"] = response.get("etag")
        return parsed
    if status == 304:
        if not previous:
            raise ValueError(
                "Cannot carry forward a 304 response without a previous snapshot"
            )
        carried = {field: previous.get(field) for field in METRIC_FIELDS}
        carried["github_repo_id"] = previous.get("github_repo_id")
        carried["snapshot_status"] = SNAPSHOT_UNCHANGED
        carried["etag"] = response.get("etag") or previous.get("etag")
        return carried
    if status == 404:
        missing = _empty_metrics()
        missing["github_repo_id"] = response.get("github_repo_id")
        missing["full_name"] = response.get("full_name")
        missing["snapshot_status"] = SNAPSHOT_MISSING
        missing["etag"] = response.get("etag")
        return missing
    raise ValueError("Unexpected GitHub status " + str(status))


def resolve_snapshot_series(responses):
    """Apply snapshot_from_response in collection order, per repository."""
    last_good = {}
    resolved = []
    for response in responses:
        repo_id = response.get("github_repo_id")
        current = snapshot_from_response(response, last_good.get(repo_id))
        if current.get("github_repo_id") is None:
            current["github_repo_id"] = repo_id
        resolved.append(current)
        if response.get("status") == 200:
            last_good[repo_id] = current
    return resolved


def metric_delta(current, previous):
    """Change since the previous observation. None when either side is missing."""
    if current is None or previous is None:
        return None
    return int(current) - int(previous)
# RULES_END

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Refresh tracked repositories and append bronze.repository_response.
# The tracking cap is the value nb_discover stored for this collection_run_id.
# 304 responses are stored without a body. Silver copies the previous metrics forward.

import time
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window


def load_github_token(vault_url, secret):
    import os

    vault = str(vault_url).strip()
    if vault:
        token = notebookutils.credentials.getSecret(vault, str(secret).strip())
        if token is None or str(token).strip() == "":
            raise RuntimeError("Key Vault returned an empty GitHub token.")
        return str(token).strip()
    env_token = os.environ.get("GITHUB_TOKEN", "").strip()
    if env_token:
        return env_token
    raise RuntimeError(
        "Set key_vault_url to the Azure Key Vault URL, for example "
        "https://example.vault.azure.net/, or set GITHUB_TOKEN for a local experiment."
    )


def table_has_run(table_name, run_id):
    if not spark.catalog.tableExists(table_name):
        return False
    return (
        spark.table(table_name)
        .filter(F.col("collection_run_id") == F.lit(run_id))
        .limit(1)
        .count()
        > 0
    )


spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

if str(collection_run_id).strip():
    run_id = require_run_id(collection_run_id)
elif spark.catalog.tableExists(SEARCH_RESPONSE_TABLE):
    latest = (
        spark.table(SEARCH_RESPONSE_TABLE)
        .agg(F.max(F.struct("collected_at", "collection_run_id")).alias("latest"))
        .collect()[0]["latest"]
    )
    if latest is None:
        raise RuntimeError("bronze.search_response has no collection run. Run nb_discover first.")
    run_id = require_run_id(latest["collection_run_id"])
else:
    raise RuntimeError("Run nb_discover before nb_snapshot.")

if table_has_run(REPOSITORY_RESPONSE_TABLE, run_id):
    print("snapshot_status=skipped collection_run_id=" + run_id)
else:
    if not table_has_run(SEARCH_RESPONSE_TABLE, run_id):
        raise RuntimeError(
            "No discovery batch for collection_run_id="
            + run_id
            + ". Run nb_discover with the same id first."
        )
    tracked_cap = int(
        spark.table(SEARCH_RESPONSE_TABLE)
        .filter(F.col("collection_run_id") == F.lit(run_id))
        .select("max_tracked_repositories")
        .limit(1)
        .collect()[0]["max_tracked_repositories"]
    )
    candidates = []
    if spark.catalog.tableExists(DISCOVERED_REPOSITORY_TABLE):
        ranked = spark.table(DISCOVERED_REPOSITORY_TABLE).withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("github_repo_id").orderBy(
                    F.col("collected_at").desc(), F.col("search_rank").asc()
                )
            ),
        )
        candidates = [
            {
                "github_repo_id": int(row["github_repo_id"]),
                "full_name": row["full_name"],
                "stargazers_count": row["stargazers_count"],
                "pushed_at": row["pushed_at"],
            }
            for row in ranked.filter(F.col("rn") == 1)
            .select("github_repo_id", "full_name", "stargazers_count", "pushed_at")
            .collect()
        ]
    already = []
    names = {}
    etags = {}
    if spark.catalog.tableExists(REPOSITORY_RESPONSE_TABLE):
        responses = spark.table(REPOSITORY_RESPONSE_TABLE)
        already = [
            int(row["github_repo_id"])
            for row in responses.select("github_repo_id").distinct().collect()
        ]
        latest_response = responses.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("github_repo_id").orderBy(F.col("collected_at").desc())
            ),
        ).filter(F.col("rn") == 1)
        for row in latest_response.select("github_repo_id", "full_name", "etag").collect():
            repo_id = int(row["github_repo_id"])
            names[repo_id] = row["full_name"]
            etags[repo_id] = row["etag"]
    for candidate in candidates:
        names.setdefault(candidate["github_repo_id"], candidate["full_name"])

    selected = select_tracked(candidates, already, tracked_cap)
    print(
        "snapshot_plan collection_run_id="
        + run_id
        + " tracked="
        + str(len(selected["tracked"]))
        + " discovered_only="
        + str(len(selected["discovered_only"]))
    )
    if not selected["tracked"]:
        print("snapshot_status=empty collection_run_id=" + run_id)
    else:
        token = load_github_token(key_vault_url, secret_name)
        pause_state = {"seconds": 0.0}

        def fetch(url, etag):
            if pause_state["seconds"] > 0:
                time.sleep(pause_state["seconds"])
                pause_state["seconds"] = 0.0
            response = github_get(url, token, etag=etag)
            pause_state["seconds"] = pause_if_exhausted(response, time.time())
            return response

        payload = []
        for repo_id in selected["tracked"]:
            full_name = names.get(repo_id)
            if not full_name:
                raise RuntimeError(
                    "Tracked repository " + str(repo_id) + " has no full_name."
                )
            url = repository_url(full_name)
            response = require_success(
                fetch(url, etags.get(repo_id)),
                allowed=(200, 304, 404),
            )
            body_name = None
            if response["status"] == 200 and response.get("body"):
                body_name = response["body"].get("full_name")
            remaining = response.get("rate_limit_remaining")
            payload.append(
                (
                    run_id,
                    int(repo_id),
                    body_name or full_name,
                    url,
                    int(response["status"]),
                    response.get("etag"),
                    None if remaining is None else int(remaining),
                    None if response["status"] == 304 else response.get("text"),
                )
            )
        schema = StructType(
            [
                StructField("collection_run_id", StringType(), False),
                StructField("github_repo_id", LongType(), False),
                StructField("full_name", StringType(), True),
                StructField("request_url", StringType(), False),
                StructField("http_status", IntegerType(), False),
                StructField("etag", StringType(), True),
                StructField("rate_limit_remaining", IntegerType(), True),
                StructField("response_json", StringType(), True),
            ]
        )
        collected_text = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        frame = spark.createDataFrame(payload, schema).withColumn(
            "collected_at", F.to_timestamp(F.lit(collected_text))
        )
        (
            frame.write.format("delta")
            .mode("append")
            .saveAsTable(REPOSITORY_RESPONSE_TABLE)
        )
        print(
            "snapshot_status=appended collection_run_id="
            + run_id
            + " repositories="
            + str(len(payload))
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
