import json
import re
import urllib.parse
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from src.ingestion.github_client import USER_AGENT
from src.ingestion.parse import collect_search, discovered_from_item, parse_repository_body
from src.ingestion.queries import (
    QUERIES,
    SEARCH_RESULT_CAP,
    cap_hit,
    lookback_window,
    needs_split,
    new_run_id,
    pages_to_fetch,
    repository_url,
    require_run_id,
    search_repositories_url,
    search_text,
    split_window,
    technologies,
    window_day_count,
)
from src.ingestion.source_contract import (
    DEFAULT_LOOKBACK_DAYS,
    MAX_TRACKED_REPOSITORIES,
    USER_AGENT as CONTRACT_USER_AGENT,
)

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "github"
OPERATORS = re.compile(r"\b(AND|OR|NOT)\b")


def test_catalogue_has_one_query_per_focus_area():
    assert len(QUERIES) == 7
    assert len(technologies()) == 7
    assert len({query["query_id"] for query in QUERIES}) == 7
    assert [query["technology_id"] for query in QUERIES] == [
        item["technology_id"] for item in technologies()
    ]
    for query in QUERIES:
        assert query["search"].startswith("topic:")
        assert "is:public" in query["search"]
        assert "fork:false" in query["search"]
        assert OPERATORS.search(query["search"]) is None
        assert len(query["search"]) < 256


def test_contract_limits_match_the_client():
    assert DEFAULT_LOOKBACK_DAYS == 14
    assert MAX_TRACKED_REPOSITORIES == 500
    assert CONTRACT_USER_AGENT == USER_AGENT
    assert SEARCH_RESULT_CAP == 1000


def test_lookback_window_is_inclusive():
    start, end = lookback_window(date(2026, 11, 24), 14)
    assert end == date(2026, 11, 24)
    assert window_day_count(start, end) == 14
    assert start == date(2026, 11, 11)


def test_lookback_rejects_zero_days():
    with pytest.raises(ValueError):
        lookback_window(date(2026, 11, 24), 0)


def test_split_windows_cover_the_lookback_without_overlap():
    start, end = lookback_window(date(2026, 11, 24), 14)

    def leaves(window_start, window_end):
        if window_day_count(window_start, window_end) == 1:
            return [(window_start, window_end)]
        left, right = split_window(window_start, window_end)
        return leaves(left[0], left[1]) + leaves(right[0], right[1])

    parts = leaves(start, end)
    days = []
    for part_start, part_end in parts:
        current = part_start
        while current <= part_end:
            days.append(current)
            current += timedelta(days=1)
    assert days == [start + timedelta(days=offset) for offset in range(14)]
    with pytest.raises(ValueError):
        split_window(start, start)


def test_split_and_cap_rules():
    one_day = (date(2026, 11, 24), date(2026, 11, 24))
    two_days = (date(2026, 11, 23), date(2026, 11, 24))
    assert needs_split(1001, False, *two_days)
    assert not needs_split(1000, False, *two_days)
    assert needs_split(10, True, *two_days)
    assert needs_split(None, False, *two_days)
    assert not needs_split(5000, True, *one_day)
    assert cap_hit(1001, False, *one_day)
    assert cap_hit(10, True, *one_day)
    assert not cap_hit(1001, False, *two_days)
    assert pages_to_fetch(0) == 1
    assert pages_to_fetch(100) == 1
    assert pages_to_fetch(101) == 2
    assert pages_to_fetch(5000) == 10


def test_urls_quote_the_query_and_repository_name():
    url = search_repositories_url("topic:rag is:public fork:false", 2)
    parsed = urllib.parse.urlparse(url)
    params = urllib.parse.parse_qs(parsed.query)
    assert parsed.path == "/search/repositories"
    assert params["page"] == ["2"]
    assert params["per_page"] == ["100"]
    assert params["q"] == ["topic:rag is:public fork:false"]
    assert repository_url("example/agent-kit").endswith("/repos/example/agent-kit")
    assert "hello%20world" in repository_url("example/hello world")
    with pytest.raises(ValueError):
        repository_url("missing-slash")


def test_run_id_rejects_injection_characters():
    assert require_run_id("20261124T120000Z") == "20261124T120000Z"
    assert new_run_id(datetime(2026, 11, 24, 12, 0, 0)) == "20261124T120000Z"
    with pytest.raises(ValueError):
        require_run_id("run'; drop table")
    with pytest.raises(ValueError):
        require_run_id("")


def test_fixture_repository_keeps_missing_subscribers_and_topics():
    payload = json.loads((FIXTURES / "repository.json").read_text(encoding="utf-8"))
    parsed = parse_repository_body(payload)
    assert parsed["github_repo_id"] == 101
    assert parsed["owner_login"] == "example"
    assert parsed["primary_language"] == "Python"
    assert parsed["subscribers_count"] is None
    assert json.loads(parsed["topics_json"]) == ["ai-agents", "llm"]
    assert parsed["is_fork"] is False
    page = json.loads((FIXTURES / "search_page.json").read_text(encoding="utf-8"))
    assert discovered_from_item(page["items"][0])["full_name"] == "example/agent-kit"


def _response(url, total_count, items, incomplete=False):
    body = {
        "total_count": total_count,
        "incomplete_results": incomplete,
        "items": items,
    }
    return {
        "status": 200,
        "body": body,
        "text": json.dumps(body),
        "url": url,
        "rate_limit_remaining": 20,
        "rate_limit_reset": None,
        "etag": None,
    }


def test_collect_search_drops_parent_windows_and_keeps_days():
    query = QUERIES[0]
    calls = []

    def fetch(url):
        calls.append(url)
        created = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["q"][0]
        created = created.split("created:")[1]
        start_text, end_text = created.split("..")
        start = date.fromisoformat(start_text)
        end = date.fromisoformat(end_text)
        if window_day_count(start, end) > 1:
            return _response(
                url,
                2000,
                [{"id": 1, "full_name": "ignore/parent", "stargazers_count": 9}],
            )
        return _response(
            url,
            1,
            [
                {
                    "id": int(start.strftime("%Y%m%d")),
                    "full_name": "example/repo",
                    "stargazers_count": 3,
                    "pushed_at": "2026-11-02T00:00:00Z",
                }
            ],
        )

    rows, found = collect_search(query, date(2026, 11, 22), date(2026, 11, 24), fetch)
    assert {item["github_repo_id"] for item in found} == {20261122, 20261123, 20261124}
    assert all(item["technology_id"] == "agentic_ai" for item in found)
    assert any(row["keep_items"] is False for row in rows)
    assert all(row["result_cap_hit"] is False for row in rows if row["keep_items"])
    assert "ignore/parent" not in {item["full_name"] for item in found}
    assert calls


def test_collect_search_records_a_one_day_cap():
    query = {
        "query_id": "topic_rag",
        "technology_id": "rag",
        "technology_name": "RAG",
        "search": "topic:rag is:public fork:false",
    }

    def fetch(url):
        page = int(urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["page"][0])
        return _response(
            url,
            1500,
            [{"id": page, "full_name": "example/rag-" + str(page), "stargazers_count": 1}],
        )

    rows, found = collect_search(query, date(2026, 11, 24), date(2026, 11, 24), fetch)
    assert len(rows) == 10
    assert all(row["result_cap_hit"] is True and row["keep_items"] is True for row in rows)
    assert [item["search_rank"] for item in found] == list(range(1, 1001, 100))
    text = search_text(query["search"], date(2026, 11, 24), date(2026, 11, 24))
    assert text.endswith("created:2026-11-24..2026-11-24")
