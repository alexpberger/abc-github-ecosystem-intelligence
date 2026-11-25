import io
import json
import urllib.error

import pytest

from src.ingestion.github_client import (
    GITHUB_API_VERSION,
    USER_AGENT,
    build_headers,
    github_get,
    pause_if_exhausted,
    require_success,
    should_retry,
)


class FakeResponse:
    def __init__(self, status, headers, body):
        self.status = status
        self.headers = headers
        self._body = body if isinstance(body, bytes) else body.encode("utf-8")

    def read(self):
        return self._body

    def getcode(self):
        return self.status


def test_headers_carry_the_token_version_and_etag():
    headers = build_headers(" secret ", etag='W/"abc"')
    assert headers["Authorization"] == "Bearer secret"
    assert headers["User-Agent"] == USER_AGENT
    assert headers["X-GitHub-Api-Version"] == GITHUB_API_VERSION
    assert headers["If-None-Match"] == 'W/"abc"'
    with pytest.raises(ValueError):
        build_headers("  ")


def test_github_get_retries_a_server_error_then_returns_the_body():
    calls = []
    sleeps = []

    def opener(request, timeout):
        calls.append((request.full_url, timeout, request.get_header("User-agent")))
        if len(calls) == 1:
            raise urllib.error.HTTPError(
                request.full_url,
                503,
                "unavailable",
                {"x-ratelimit-remaining": "20"},
                io.BytesIO(b'{"message":"unavailable"}'),
            )
        return FakeResponse(
            200,
            {"etag": '"repo"', "x-ratelimit-remaining": "19", "x-ratelimit-reset": "50"},
            b'{"id": 5, "full_name": "example/repo"}',
        )

    response = github_get(
        "https://api.github.com/repos/example/repo",
        "token",
        opener=opener,
        sleep=sleeps.append,
        clock=lambda: 10,
    )
    assert response["status"] == 200
    assert response["body"]["id"] == 5
    assert response["etag"] == '"repo"'
    assert sleeps == [1.0]
    assert calls[0][1] == 60
    assert "token" not in json.dumps({key: response[key] for key in ("status", "url", "body")})


def test_github_get_does_not_retry_a_normal_forbidden_response():
    def opener(request, timeout):
        raise urllib.error.HTTPError(
            request.full_url,
            403,
            "forbidden",
            {"x-ratelimit-remaining": "10"},
            io.BytesIO(b'{"message":"Resource not accessible"}'),
        )

    response = github_get(
        "https://api.github.com/repos/example/repo",
        "token",
        opener=opener,
        sleep=lambda seconds: (_ for _ in ()).throw(AssertionError(seconds)),
        clock=lambda: 0,
    )
    assert response["status"] == 403
    with pytest.raises(RuntimeError):
        require_success(response)


def test_rate_limit_403_retries_until_reset_delay():
    sleeps = []
    attempts = {"count": 0}

    def opener(request, timeout):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise urllib.error.HTTPError(
                request.full_url,
                403,
                "rate",
                {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "25"},
                io.BytesIO(b'{"message":"API rate limit exceeded"}'),
            )
        return FakeResponse(304, {"etag": '"same"', "x-ratelimit-remaining": "4999"}, b"")

    response = github_get(
        "https://api.github.com/repos/example/repo",
        "token",
        etag='"same"',
        opener=opener,
        sleep=sleeps.append,
        clock=lambda: 10,
    )
    assert response["status"] == 304
    assert response["text"] is None
    assert response["body"] is None
    assert sleeps == [16.0]
    assert should_retry(404, {"remaining": 10, "reset": None, "retry_after": None}, "", 0) is False


def test_pause_if_exhausted_is_zero_while_budget_remains():
    assert pause_if_exhausted({"rate_limit_remaining": 2, "rate_limit_reset": 100}, 90) == 0
    assert pause_if_exhausted({"rate_limit_remaining": 0, "rate_limit_reset": 100}, 90) == 11
