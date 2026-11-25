import pytest

from src.quality.checks import CRITICAL_CHECKS, WARNING_CHECKS, is_critical
from src.transformations.rules import (
    metric_delta,
    resolve_snapshot_series,
    select_tracked,
    snapshot_from_response,
)


def test_select_tracked_keeps_existing_repos_and_ranks_newcomers():
    candidates = [
        {"github_repo_id": 3, "stargazers_count": 5, "pushed_at": "2026-11-01T00:00:00Z"},
        {"github_repo_id": 3, "stargazers_count": 9, "pushed_at": "2026-11-02T00:00:00Z"},
        {"github_repo_id": 2, "stargazers_count": 9, "pushed_at": "2026-11-03T00:00:00Z"},
        {"github_repo_id": 4, "stargazers_count": 1, "pushed_at": "2026-11-04T00:00:00Z"},
        {"github_repo_id": None, "stargazers_count": 100, "pushed_at": None},
    ]
    selected = select_tracked(candidates, already_tracked_ids=[8], max_tracked=2)
    assert selected["tracked"] == (2, 8)
    assert selected["discovered_only"] == (3, 4)


def test_select_tracked_does_not_evict_when_the_cap_is_already_full():
    selected = select_tracked(
        [{"github_repo_id": 5, "stargazers_count": 1000, "pushed_at": "2026-11-01T00:00:00Z"}],
        already_tracked_ids=[1, 2],
        max_tracked=1,
    )
    assert selected["tracked"] == (1, 2)
    assert selected["discovered_only"] == (5,)
    with pytest.raises(ValueError):
        select_tracked([], [], -1)


def test_equal_stars_prefer_the_later_push_then_the_smaller_id():
    candidates = [
        {"github_repo_id": 20, "stargazers_count": 4, "pushed_at": "2026-11-02T00:00:00Z"},
        {"github_repo_id": 10, "stargazers_count": 4, "pushed_at": "2026-11-02T00:00:00Z"},
        {"github_repo_id": 30, "stargazers_count": 4, "pushed_at": "2026-11-01T00:00:00Z"},
    ]
    selected = select_tracked(candidates, [], max_tracked=2)
    assert selected["tracked"] == (10, 20)
    assert selected["discovered_only"] == (30,)


def test_snapshot_series_carries_304_and_clears_404():
    first = {
        "status": 200,
        "github_repo_id": 101,
        "etag": '"one"',
        "body": {
            "id": 101,
            "full_name": "example/agent-kit",
            "stargazers_count": 12,
            "forks_count": 2,
            "owner": {"login": "example"},
            "topics": ["ai-agents"],
            "fork": False,
            "archived": False,
        },
    }
    unchanged = {
        "status": 304,
        "github_repo_id": 101,
        "etag": '"two"',
        "full_name": "example/agent-kit",
    }
    missing = {
        "status": 404,
        "github_repo_id": 101,
        "etag": None,
        "full_name": "example/agent-kit",
    }
    resolved = resolve_snapshot_series([first, unchanged, missing])
    assert resolved[0]["snapshot_status"] == "refreshed"
    assert resolved[0]["stargazers_count"] == 12
    assert resolved[1]["snapshot_status"] == "unchanged"
    assert resolved[1]["stargazers_count"] == 12
    assert resolved[1]["etag"] == '"two"'
    assert resolved[2]["snapshot_status"] == "missing"
    assert resolved[2]["stargazers_count"] is None
    assert resolved[2]["full_name"] == "example/agent-kit"
    with pytest.raises(ValueError):
        snapshot_from_response({"status": 304, "etag": '"x"'}, None)
    with pytest.raises(ValueError):
        snapshot_from_response({"status": 500}, None)


def test_metric_delta_allows_a_decrease_and_skips_nulls():
    assert metric_delta(8, 10) == -2
    assert metric_delta(10, 10) == 0
    assert metric_delta(None, 10) is None
    assert metric_delta(10, None) is None


def test_critical_check_names_are_stable():
    assert "fact_matches_silver" in CRITICAL_CHECKS
    assert "tracked_within_cap" in CRITICAL_CHECKS
    assert "search_result_cap_hit" in WARNING_CHECKS
    assert is_critical("unique_snapshot_grain")
    assert not is_critical("discovered_not_tracked")
