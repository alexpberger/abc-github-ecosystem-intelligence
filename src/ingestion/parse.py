"""Turn GitHub search and repository payloads into row dictionaries."""

from src.ingestion.github_client import require_success
from src.ingestion.queries import (
    SEARCH_PER_PAGE,
    cap_hit,
    needs_split,
    pages_to_fetch,
    search_repositories_url,
    search_text,
    split_window,
)

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
