# Data dictionary

Types below are the types the notebooks write. The SQL analytics endpoint may show Spark booleans as `bit`.

## bronze.online_retail

Landing table. Source columns stay under their workbook names and are strings.

| Column | Type | Description |
| --- | --- | --- |
| `Invoice` | string | Invoice number as read. A leading `C` marks a cancellation. A leading `A` marks an adjustment. |
| `StockCode` | string | Product or charge code as read. |
| `Description` | string | Item description as read. May be blank. |
| `Quantity` | string | Quantity as read, including negatives. |
| `InvoiceDate` | string | Invoice timestamp as read. May be a datetime string or an Excel serial. |
| `Price` | string | Unit price in GBP as read. |
| `Customer ID` | string | Customer number as read. Often blank. Floats may arrive as `13085.0`. |
| `Country` | string | Customer country as read. |
| `source_file` | string | `online_retail_II.xlsx` |
| `source_sheet` | string | `Year 2009-2010` or `Year 2010-2011` |
| `ingestion_timestamp` | timestamp | UTC timestamp captured once per batch |
| `ingestion_batch_id` | string | Parameter identifying the load. Default `online_retail_ii_full` |

## silver.sales_line

One surviving business line from the latest Bronze batch.

| Column | Type | Description |
| --- | --- | --- |
| `invoice` | string | Trimmed invoice number |
| `stock_code` | string | Trimmed, uppercased stock code |
| `description` | string | Trimmed description, null when blank |
| `quantity` | long | Whole-number quantity, signed |
| `invoice_date` | timestamp | Parsed invoice timestamp |
| `unit_price` | decimal(18,4) | Unit price in GBP, signed if the source price is signed |
| `customer_id` | string | Digits only when the source had a trailing `.0`. Null when missing. |
| `country` | string | Trimmed country. Null when blank. |
| `line_kind` | string | `sale`, `cancellation`, or `adjustment` |
| `is_cancellation` | boolean | True when `line_kind` is `cancellation` |
| `item_type` | string | `product`, `postage`, `carriage`, `manual`, `discount`, `fee`, `sample`, `gift_voucher`, `adjustment`, or `non_standard` |
| `net_amount` | decimal(18,4) | `quantity * unit_price`, signed |

## gold.dim_date

| Column | Type | Description |
| --- | --- | --- |
| `date_key` | int | `yyyyMMdd` |
| `calendar_date` | date | Calendar day |
| `calendar_year` | int | Year |
| `calendar_quarter` | int | Quarter, 1–4 |
| `month_of_year` | int | Month, 1–12 |
| `month_name` | string | English month name |
| `day_of_month` | int | Day of month |
| `day_name` | string | English weekday name |
| `year_month` | string | `yyyy-MM` |

The spine includes every day from the first invoice date to the last, including days with no sales.

## gold.dim_product

| Column | Type | Description |
| --- | --- | --- |
| `product_key` | int | Surrogate, unique per build |
| `stock_code` | string | Natural key |
| `description` | string | Latest non-null description |
| `item_type` | string | Same vocabulary as Silver |
| `is_merchandise` | boolean | True when `item_type` is `product` |

## gold.dim_customer

| Column | Type | Description |
| --- | --- | --- |
| `customer_key` | int | Surrogate. `-1` is the unknown member. |
| `customer_id` | string | Natural key. `UNKNOWN` for key `-1`. |
| `first_invoice_date` | timestamp | Earliest invoice for this customer. Null for the unknown member. |
| `last_invoice_date` | timestamp | Latest invoice for this customer. Null for the unknown member. |
| `latest_country` | string | Country on the latest invoice. Not the geography used by the fact. |

## gold.dim_geography

| Column | Type | Description |
| --- | --- | --- |
| `geography_key` | int | Surrogate. `-1` is the unknown member. |
| `country_name` | string | Country on the sales line. `UNKNOWN` for key `-1`. |

No ISO code is stored. The source does not include one.

## gold.fact_sales

| Column | Type | Description |
| --- | --- | --- |
| `sales_line_key` | string | SHA-256 of the Silver business columns |
| `invoice_number` | string | Invoice |
| `date_key` | int | Join to `dim_date` |
| `product_key` | int | Join to `dim_product` |
| `customer_key` | int | Join to `dim_customer`. `-1` when the line has no customer. |
| `geography_key` | int | Join to `dim_geography`. `-1` when the line has no country. |
| `quantity` | long | Signed quantity |
| `unit_price` | decimal(18,4) | Unit price |
| `net_amount` | decimal(18,4) | Signed line amount |
| `is_cancellation` | boolean | Cancellation flag from Silver |
| `line_kind` | string | `sale`, `cancellation`, or `adjustment` |
| `item_type` | string | Item classification from Silver |

`line_kind` is on the fact so adjustment lines can be excluded from sales and return measures without another join back to Silver.

## quality.check_result

| Column | Type | Description |
| --- | --- | --- |
| `check_name` | string | Stable name from `src/quality/checks.py` |
| `severity` | string | `critical` or `warning` |
| `passed` | boolean | True when the check succeeded |
| `observed_value` | string | Value seen in this run |
| `expected_value` | string | Value the check required |
| `checked_at` | timestamp | Time the quality notebook wrote the row |
