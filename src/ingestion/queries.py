"""Search catalogue and date-window planning for GitHub discovery."""

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
