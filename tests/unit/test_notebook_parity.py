import ast
from pathlib import Path

from src.ingestion.source_contract import (
    BRONZE_TABLE,
    DEFAULT_BATCH_ID,
    SHEET_NAMES,
    SOURCE_COLUMNS,
    SOURCE_FILE_NAME,
    SOURCE_RELATIVE_PATH,
)
from src.quality.checks import CRITICAL_CHECKS, WARNING_CHECKS
from src.transformations.rules import (
    BUSINESS_COLUMNS,
    GIFT_PREFIXES,
    ITEM_TYPE_BY_CODE,
    PRODUCT_CODE_PATTERN,
)

ROOT = Path(__file__).resolve().parents[2]
BRONZE = ROOT / "fabric" / "notebooks" / "nb_bronze_ingest.Notebook" / "notebook-content.py"
SILVER = ROOT / "fabric" / "notebooks" / "nb_silver_clean.Notebook" / "notebook-content.py"
QUALITY = ROOT / "fabric" / "notebooks" / "nb_quality_checks.Notebook" / "notebook-content.py"
GOLD = ROOT / "fabric" / "notebooks" / "nb_gold_model.Notebook" / "notebook-content.py"


def extract(path: Path, start: str, end: str) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    start_index = lines.index(start)
    end_index = lines.index(end, start_index + 1)
    return "\n".join(lines[start_index + 1 : end_index])


def test_bronze_notebook_matches_source_contract():
    namespace: dict[str, object] = {}
    exec(extract(BRONZE, "# CONTRACT_START", "# CONTRACT_END"), namespace)
    assert namespace["SOURCE_FILE_NAME"] == SOURCE_FILE_NAME
    assert namespace["SOURCE_RELATIVE_PATH"] == SOURCE_RELATIVE_PATH
    assert namespace["SHEET_NAMES"] == SHEET_NAMES
    assert namespace["SOURCE_COLUMNS"] == SOURCE_COLUMNS
    assert namespace["DEFAULT_BATCH_ID"] == DEFAULT_BATCH_ID
    assert namespace["BRONZE_TABLE"] == BRONZE_TABLE


def test_silver_and_quality_notebooks_share_rule_constants():
    silver: dict[str, object] = {}
    quality: dict[str, object] = {}
    exec(extract(SILVER, "# RULES_START", "# RULES_END"), silver)
    exec(extract(QUALITY, "# RULES_START", "# RULES_END"), quality)
    assert silver["ITEM_TYPE_BY_CODE"] == ITEM_TYPE_BY_CODE
    assert silver["PRODUCT_CODE_PATTERN"] == PRODUCT_CODE_PATTERN
    assert silver["GIFT_PREFIXES"] == GIFT_PREFIXES
    assert quality["ITEM_TYPE_BY_CODE"] == ITEM_TYPE_BY_CODE
    assert quality["GIFT_PREFIXES"] == GIFT_PREFIXES
    assert quality["PRODUCT_CODE_PATTERN"] == PRODUCT_CODE_PATTERN


def test_silver_and_quality_notebooks_share_the_transform():
    silver = extract(SILVER, "# TRANSFORM_START", "# TRANSFORM_END")
    quality = extract(QUALITY, "# TRANSFORM_START", "# TRANSFORM_END")
    assert silver == quality
    assert "def prepare_sales_lines(" in silver
    for column in BUSINESS_COLUMNS:
        assert f'"{column}"' in silver


def test_notebooks_are_valid_python():
    for path in (BRONZE, SILVER, GOLD, QUALITY):
        ast.parse(path.read_text(encoding="utf-8"))


def test_quality_notebook_names_every_check():
    text = QUALITY.read_text(encoding="utf-8")
    for name in CRITICAL_CHECKS + WARNING_CHECKS:
        assert f'"{name}"' in text
