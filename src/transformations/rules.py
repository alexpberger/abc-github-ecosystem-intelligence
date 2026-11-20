"""Business rules for Online Retail II lines.

Notebooks repeat the code maps in this module. The parity test compares values,
not formatting, so either side can be edited as long as both are updated.
"""

from __future__ import annotations

import re
from decimal import Decimal, ROUND_HALF_UP

PRODUCT_CODE_PATTERN = r"^\d{5}[A-Za-z]?$"
PRODUCT_CODE_RE = re.compile(PRODUCT_CODE_PATTERN)

# Known non-merchandise codes. Matched after the stock code is trimmed and uppercased.
# Anything else that is not a standard product code becomes non_standard.
ITEM_TYPE_BY_CODE = {
    "POST": "postage",
    "DOT": "postage",
    "C2": "carriage",
    "C3": "carriage",
    "M": "manual",
    "D": "discount",
    "BANK CHARGES": "fee",
    "AMAZONFEE": "fee",
    "CRUK": "fee",
    "S": "sample",
    "B": "adjustment",
    "ADJUST": "adjustment",
    "ADJUST2": "adjustment",
}

GIFT_PREFIXES = ("GIFT",)

BUSINESS_COLUMNS = (
    "invoice",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country",
)

UNKNOWN_CUSTOMER_KEY = -1
UNKNOWN_CUSTOMER_ID = "UNKNOWN"
UNKNOWN_GEOGRAPHY_KEY = -1
UNKNOWN_COUNTRY_NAME = "UNKNOWN"

_MISSING_TEXT = re.compile(r"(?i)^(nan|none|null)$")
_DECIMAL_CUSTOMER = re.compile(r"^(\d+)\.0+$")


def normalize_stock_code(stock_code: object) -> str | None:
    if stock_code is None:
        return None
    text = str(stock_code).strip().upper()
    if text == "" or _MISSING_TEXT.match(text):
        return None
    return text


def classify_item_type(stock_code: object) -> str:
    code = normalize_stock_code(stock_code)
    if code is None:
        return "non_standard"
    named = ITEM_TYPE_BY_CODE.get(code)
    if named is not None:
        return named
    if any(code.startswith(prefix) for prefix in GIFT_PREFIXES):
        return "gift_voucher"
    if PRODUCT_CODE_RE.match(code):
        return "product"
    return "non_standard"


def is_cancellation(invoice: object) -> bool:
    if invoice is None:
        return False
    text = str(invoice).strip().upper()
    if text == "":
        return False
    return text.startswith("C")


def line_kind(invoice: object) -> str:
    if invoice is None:
        return "sale"
    text = str(invoice).strip().upper()
    if text.startswith("C"):
        return "cancellation"
    if text.startswith("A"):
        return "adjustment"
    return "sale"


def normalize_customer_id(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float):
        if value != value:  # NaN
            return None
        if value == int(value):
            return str(int(value))
    text = str(value).strip()
    if text == "" or _MISSING_TEXT.match(text):
        return None
    decimal_customer = _DECIMAL_CUSTOMER.match(text)
    if decimal_customer:
        return decimal_customer.group(1)
    return text


def net_amount(quantity: object, unit_price: object) -> Decimal | None:
    if quantity is None or unit_price is None:
        return None
    amount = Decimal(str(quantity)) * Decimal(str(unit_price))
    return amount.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
