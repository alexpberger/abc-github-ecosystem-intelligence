"""Source file and column contract for Online Retail II.

The Bronze notebook repeats these constants. tests/unit/test_notebook_parity.py
checks that the two copies still match.
"""

SOURCE_FILE_NAME = "online_retail_II.xlsx"
SOURCE_RELATIVE_PATH = "Files/raw/online_retail_II.xlsx"
SHEET_NAMES = ("Year 2009-2010", "Year 2010-2011")
SOURCE_COLUMNS = (
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country",
)
DEFAULT_BATCH_ID = "online_retail_ii_full"
BRONZE_TABLE = "bronze.online_retail"
SILVER_TABLE = "silver.sales_line"
FACT_TABLE = "gold.fact_sales"
DIM_DATE_TABLE = "gold.dim_date"
DIM_PRODUCT_TABLE = "gold.dim_product"
DIM_CUSTOMER_TABLE = "gold.dim_customer"
DIM_GEOGRAPHY_TABLE = "gold.dim_geography"
QUALITY_TABLE = "quality.check_result"
LAKEHOUSE_NAME = "lh_retail"
SCHEMAS = ("bronze", "silver", "gold", "quality")
