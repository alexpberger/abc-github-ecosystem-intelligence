# Data model

Phase 1 publishes one fact at sales-line grain and four dimensions. Column lists are in [data-dictionary.md](data-dictionary.md).

The source workbook is not modified. Bronze keeps its columns. Silver and Gold are derived.

## Grain

`silver.sales_line` and `gold.fact_sales` are one row per distinct business line in the latest Bronze batch, after incomplete lines are removed.

A business line is:

`invoice`, `stock_code`, `description`, `quantity`, `invoice_date`, `unit_price`, `customer_id`, `country`

The two yearly sheets overlap on 1–9 December 2010. Exact matches on those columns are kept once.

## Layers

### Bronze

`bronze.online_retail` is append-only. Each load stamps:

- `source_file`
- `source_sheet` (`Year 2009-2010` or `Year 2010-2011`)
- `ingestion_timestamp` (one value for the batch)
- `ingestion_batch_id` (notebook parameter, default `online_retail_ii_full`)

Source values are stored as strings so the landing step does not recast numbers or dates. Reloading the same batch id is a no-op. A new id appends another snapshot. Silver reads only the latest snapshot.

### Silver

`nb_silver_clean` overwrites `silver.sales_line` from that latest batch.

- Names are snake_case.
- Stock codes are trimmed and uppercased.
- Customer ids lose a trailing `.0` and blank values become null.
- Quantity becomes a whole number. Unit price becomes `decimal(18,4)`. Invoice date becomes a timestamp. Excel serials in the range 35000–60000 are accepted, then common datetime strings. A quantity or price that is not a plain number is treated as missing, and that line is excluded. The original text stays in Bronze.
- `line_kind` is `cancellation` when the invoice starts with `C`, `adjustment` when it starts with `A`, otherwise `sale`.
- `is_cancellation` is true only for the `C` prefix.
- `item_type` follows [decisions.md](decisions.md) D6.
- `net_amount` is signed quantity times unit price.

Lines missing invoice, stock code, invoice date, quantity, or unit price are excluded. They are not written to an error table in Phase 1. The quality notebook recomputes the exclusion and fails if Silver's count is not explained by duplicates plus those invalid lines.

### Gold

`nb_gold_model` overwrites the Gold tables with Spark SQL.

| Table | Grain | Key |
| --- | --- | --- |
| `gold.dim_date` | One calendar day from the earliest invoice date through the latest | `date_key` as `yyyyMMdd` |
| `gold.dim_product` | One stock code | `product_key` |
| `gold.dim_customer` | One customer id, plus the unknown member | `customer_key` |
| `gold.dim_geography` | One country, plus the unknown member | `geography_key` |
| `gold.fact_sales` | One Silver line | `sales_line_key` (SHA-256 of the business columns) |

Surrogate keys other than the unknown members come from `row_number()` at build time. They are stable only for a given overwrite, not across historical Gold versions. Phase 1 does not keep Gold history.

`dim_product.description` prefers the latest non-null description for that stock code. `is_merchandise` is true when `item_type` is `product`.

`dim_customer` stores the first and last invoice timestamps and the country on the latest invoice. The fact does not use that country. Geography on the fact comes from the line.

There is no category attribute. See D5 in [decisions.md](decisions.md).

### Quality

`quality.check_result` is overwritten each run. A failed critical check fails the notebook, which fails the pipeline activity. Check names are listed in [AGENTS.md](../AGENTS.md).

## How the business questions are answered

Phase 1 does not pre-aggregate these. [validation.sql](validation.sql) shows the T-SQL shape. Phase 2 can turn them into DAX measures.

| Question | Filter or grouping |
| --- | --- |
| Revenue development | `SUM(net_amount)` by `dim_date` |
| Product performance | `fact_sales` joined to `dim_product`, usually `is_merchandise = 1` |
| Returns and cancellations | `is_cancellation` or `line_kind` |
| Geography | `dim_geography.country_name` |
| Repeat purchase | Known customers (`customer_key <> -1`) with more than one distinct `invoice_number` on non-cancellation lines |
| Customers with no id | `customer_key = -1` |

Adjustments (`line_kind = 'adjustment'`) are neither sales nor cancellations. Leave them out of gross-sales and return measures.

## Keys and orphans

Every fact row must find a date, product, customer, and geography key. Missing customer and country resolve to key `-1`. Missing product or date is a failed load, because Silver already requires stock code and invoice date, and the calendar covers every date in Silver.
