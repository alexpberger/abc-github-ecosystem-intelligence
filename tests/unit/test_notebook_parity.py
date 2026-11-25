import ast
from pathlib import Path

from src.ingestion.queries import technologies
from src.ingestion.source_contract import (
    DIM_REPOSITORY_TABLE,
    DIM_TECHNOLOGY_TABLE,
    FACT_DISCOVERY_TABLE,
    FACT_REPOSITORY_SNAPSHOT_TABLE,
)
from src.quality.checks import CRITICAL_CHECKS, WARNING_CHECKS

ROOT = Path(__file__).resolve().parents[2]
NOTEBOOKS = ROOT / "fabric" / "notebooks"

REGIONS = (
    ("src/ingestion/source_contract.py", "# CONTRACT_START", "# CONTRACT_END"),
    ("src/ingestion/queries.py", "# QUERIES_START", "# QUERIES_END"),
    ("src/ingestion/github_client.py", "# CLIENT_START", "# CLIENT_END"),
    ("src/ingestion/parse.py", "# PARSE_START", "# PARSE_END"),
    ("src/transformations/rules.py", "# RULES_START", "# RULES_END"),
)
INGESTION = (
    NOTEBOOKS / "nb_discover.Notebook" / "notebook-content.py",
    NOTEBOOKS / "nb_snapshot.Notebook" / "notebook-content.py",
)
CONTRACT_NOTEBOOKS = INGESTION + (
    NOTEBOOKS / "nb_silver.Notebook" / "notebook-content.py",
    NOTEBOOKS / "nb_gold_model.Notebook" / "notebook-content.py",
    NOTEBOOKS / "nb_quality_checks.Notebook" / "notebook-content.py",
)


def extract(path, start, end):
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    start_index = lines.index(start)
    end_index = lines.index(end, start_index + 1)
    return "\n".join(lines[start_index + 1 : end_index])


def test_ingestion_notebooks_match_the_python_modules():
    for notebook in INGESTION:
        for relative, start, end in REGIONS:
            assert extract(notebook, start, end) == extract(ROOT / relative, start, end)


def test_contract_blocks_match_source_contract():
    expected = extract(ROOT / "src/ingestion/source_contract.py", "# CONTRACT_START", "# CONTRACT_END")
    for notebook in CONTRACT_NOTEBOOKS:
        assert extract(notebook, "# CONTRACT_START", "# CONTRACT_END") == expected


def test_shared_notebook_code_runs():
    source = "\n\n".join(
        extract(INGESTION[0], start, end) for _relative, start, end in REGIONS
    )
    namespace = {}
    exec(source, namespace)
    assert namespace["SEARCH_RESPONSE_TABLE"] == "bronze.search_response"
    assert namespace["QUERIES"][2]["technology_id"] == "rag"
    assert namespace["build_headers"]("token")["Authorization"] == "Bearer token"
    assert namespace["select_tracked"]([], [], 0) == {
        "tracked": (),
        "discovered_only": (),
    }


def test_gold_technologies_match_the_catalogue():
    namespace = {}
    exec(
        extract(
            NOTEBOOKS / "nb_gold_model.Notebook" / "notebook-content.py",
            "# TECHNOLOGIES_START",
            "# TECHNOLOGIES_END",
        ),
        namespace,
    )
    expected = tuple(
        (item["technology_id"], item["technology_name"]) for item in technologies()
    )
    assert namespace["TECHNOLOGIES"] == expected


def test_gold_notebook_replaces_the_four_tables():
    text = (NOTEBOOKS / "nb_gold_model.Notebook" / "notebook-content.py").read_text(
        encoding="utf-8"
    )
    for table_name in (
        DIM_REPOSITORY_TABLE,
        DIM_TECHNOLOGY_TABLE,
        FACT_REPOSITORY_SNAPSHOT_TABLE,
        FACT_DISCOVERY_TABLE,
    ):
        assert "CREATE OR REPLACE TABLE " + table_name in text


def test_quality_notebook_names_every_check():
    text = (NOTEBOOKS / "nb_quality_checks.Notebook" / "notebook-content.py").read_text(
        encoding="utf-8"
    )
    for name in CRITICAL_CHECKS + WARNING_CHECKS:
        assert '"' + name + '"' in text


def test_discover_parameters_use_the_default_limits():
    text = (NOTEBOOKS / "nb_discover.Notebook" / "notebook-content.py").read_text(
        encoding="utf-8"
    )
    assert 'lookback_days = "14"' in text
    assert 'max_tracked_repositories = "500"' in text


def test_notebooks_are_valid_python():
    paths = list(NOTEBOOKS.glob("*/notebook-content.py"))
    names = sorted(path.parent.name for path in paths)
    assert names == [
        "nb_discover.Notebook",
        "nb_gold_model.Notebook",
        "nb_quality_checks.Notebook",
        "nb_silver.Notebook",
        "nb_snapshot.Notebook",
    ]
    for path in paths:
        ast.parse(path.read_text(encoding="utf-8"))
