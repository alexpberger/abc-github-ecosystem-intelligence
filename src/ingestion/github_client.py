"""Small GitHub REST client with retry and rate-limit handling."""

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
