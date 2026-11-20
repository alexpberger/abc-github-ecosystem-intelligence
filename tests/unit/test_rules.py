from decimal import Decimal

from src.quality.checks import CRITICAL_CHECKS, is_critical
from src.transformations.rules import (
    classify_item_type,
    is_cancellation,
    line_kind,
    net_amount,
    normalize_customer_id,
)


def test_cancellation_prefix_is_case_insensitive():
    assert is_cancellation("C536548")
    assert is_cancellation("c536548")
    assert is_cancellation("  C1")
    assert not is_cancellation("536548")
    assert not is_cancellation("A563185")
    assert not is_cancellation(None)
    assert not is_cancellation("")
    assert not is_cancellation("   ")


def test_line_kind_distinguishes_cancellations_and_adjustments():
    assert line_kind("C536548") == "cancellation"
    assert line_kind("a563185") == "adjustment"
    assert line_kind("489434") == "sale"
    assert line_kind(None) == "sale"
    assert line_kind("") == "sale"


def test_customer_id_normalizes_missing_and_decimal_zero():
    assert normalize_customer_id("13085.0") == "13085"
    assert normalize_customer_id("13085.00") == "13085"
    assert normalize_customer_id(13085.0) == "13085"
    assert normalize_customer_id(13085) == "13085"
    assert normalize_customer_id("17850") == "17850"
    assert normalize_customer_id(None) is None
    assert normalize_customer_id("") is None
    assert normalize_customer_id("  ") is None
    assert normalize_customer_id("nan") is None
    assert normalize_customer_id("None") is None
    assert normalize_customer_id(float("nan")) is None


def test_item_type_classifies_products_and_known_charges():
    assert classify_item_type("85123A") == "product"
    assert classify_item_type("85123a") == "product"
    assert classify_item_type("POST") == "postage"
    assert classify_item_type("post") == "postage"
    assert classify_item_type("DOT") == "postage"
    assert classify_item_type("C2") == "carriage"
    assert classify_item_type("m") == "manual"
    assert classify_item_type("D") == "discount"
    assert classify_item_type("BANK CHARGES") == "fee"
    assert classify_item_type("AMAZONFEE") == "fee"
    assert classify_item_type("CRUK") == "fee"
    assert classify_item_type("S") == "sample"
    assert classify_item_type("gift_0001_10") == "gift_voucher"
    assert classify_item_type("GIFT") == "gift_voucher"
    assert classify_item_type("B") == "adjustment"
    assert classify_item_type("ADJUST") == "adjustment"
    assert classify_item_type("ADJUST2") == "adjustment"
    assert classify_item_type("1234") == "non_standard"
    assert classify_item_type("TEST001") == "non_standard"
    assert classify_item_type(None) == "non_standard"
    assert classify_item_type("") == "non_standard"


def test_net_amount_keeps_the_sign_of_the_quantity():
    assert net_amount(-1, Decimal("2.50")) == Decimal("-2.5000")
    assert net_amount(2, Decimal("1.5")) == Decimal("3.0000")
    assert net_amount(3, 2) == Decimal("6.0000")
    assert net_amount(None, Decimal("1")) is None
    assert net_amount(1, None) is None


def test_critical_check_names_are_stable():
    assert "fact_matches_silver" in CRITICAL_CHECKS
    assert "unknown_customer_present" in CRITICAL_CHECKS
    assert is_critical("bronze_batch_not_empty")
    assert not is_critical("both_source_sheets_present")
