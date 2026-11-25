"""Tracking selection and snapshot carry-forward.

Notebooks repeat the marked block. Silver implements the same 200, 304, and 404
outcomes with the DataFrame API.
"""

from src.ingestion.parse import parse_repository_body

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
