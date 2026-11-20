# AGENTS.md

Engineering reference for humans and coding agents working on ABC Retail Intelligence.

This file describes the project as it is now. A feature listed under a later phase is planned, not implemented.

## Project overview

ABC Retail Intelligence is a public portfolio project for Alex Berger Consulting. It demonstrates an end-to-end retail analytics platform on Microsoft Fabric.

The business case is Retail Performance & Customer Intelligence. The platform should let a retailer analyse:

- Sales and revenue development
- Product performance
- Customer purchasing behaviour
- Repeat purchases
- Returns and cancellations
- Geographic sales distribution

Phase 1 prepares a Medallion pipeline from the UCI Online Retail II spreadsheet to a dimensional Gold model. Power BI, production hardening, and an AI assistant are later phases.

The developer knows Python, SQL, data science, and cloud application development, and is learning Fabric. Explain Fabric-specific behaviour in the docs. Keep PySpark readable. Do not add abstractions that hide the platform.

## Dataset

UCI Online Retail II, already stored locally and described in [data/README.md](data/README.md).

- File: `data/raw/online_retail_II.xlsx` (not committed)
- Sheets: `Year 2009-2010` (525,461 data rows) and `Year 2010-2011` (541,910 data rows)
- Columns: `Invoice`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `Price`, `Customer ID`, `Country`
- Source: https://archive.ics.uci.edu/dataset/502/online+retail+ii

Do not modify the spreadsheet. Do not commit raw dataset files.

Important dataset traits, handled in Silver and documented in [docs/data-model.md](docs/data-model.md):

- Invoices starting with `C` are cancellations and usually have a negative quantity. They stay in the model.
- Invoices starting with `A` are adjustments. They stay in the model and are not treated as sales.
- `Customer ID` is missing on a large minority of lines. Those lines remain in the fact and use the unknown customer member.
- The source has no product category. Phase 1 does not invent one.
- Some stock codes are charges or adjustments (postage, carriage, manual, discount, fees, samples, gift vouchers), not merchandise.
- Both sheets contain 1–9 December 2010. Exact duplicate business rows are removed in Silver.

## Architecture

One schema-enabled Fabric lakehouse, `lh_retail`. No Fabric Warehouse.

| Schema | Table | Role |
| --- | --- | --- |
| `bronze` | `online_retail` | Append-only landed source rows, plus ingestion metadata |
| `silver` | `sales_line` | Typed, deduplicated sales lines |
| `gold` | `fact_sales`, `dim_date`, `dim_product`, `dim_customer`, `dim_geography` | Dimensional model at sales-line grain |
| `quality` | `check_result` | One row per data-quality check for the latest run |

Schemas must be enabled when the lakehouse is created. Fabric cannot turn them on later.

OneLake layout inside the lakehouse:

- `Files/raw/online_retail_II.xlsx` is the uploaded source. Files are not Delta tables and are not copied into Git.
- `Tables/` holds the Delta tables. The SQL analytics endpoint exposes those tables as read-only T-SQL. It is not a second copy of the data.

Processing:

1. `nb_bronze_ingest` reads both sheets and appends a batch.
2. `nb_silver_clean` rebuilds `silver.sales_line` from the latest Bronze batch with the DataFrame API.
3. `nb_gold_model` rebuilds the Gold tables with Spark SQL.
4. `nb_quality_checks` writes `quality.check_result` and fails the notebook if a critical check fails.

A Data Factory pipeline, `pl_retail_medallion`, runs those notebooks in that order. The repo contains a template at [docs/pipeline-template.json](docs/pipeline-template.json). It is not a Fabric item yet, because notebook activities require the workspace and notebook GUIDs created by the portal.

The full diagram and the Git boundary are in [architecture/architecture.md](architecture/architecture.md).

## Technology choices

- Microsoft Fabric is the execution environment. Do not containerise it or reproduce it locally.
- OneLake stores files and Delta tables.
- One lakehouse keeps Bronze, Silver, Gold, and quality checks in separate schemas and avoids a Warehouse in Phase 1.
- Data Factory pipelines orchestrate notebook runs.
- PySpark notebooks perform ingestion and transformation. Silver uses the DataFrame API. Gold uses Spark SQL, so both are visible in the project.
- Delta Lake is the table format.
- The SQL analytics endpoint is how T-SQL is demonstrated. Queries live in [docs/validation.sql](docs/validation.sql).
- Power BI is the future business interface (Phase 2). There is no custom React frontend.
- GitHub and Cursor are the local source-control and editing tools.
- Python under `src/` holds the business rules that can be unit-tested without Spark. Notebooks repeat those constants, and tests check they still match.

## Repository structure

```text
abc-ms-fabric-retail-demo/
├── AGENTS.md
├── README.md
├── requirements-dev.txt
├── pytest.ini
├── architecture/
│   └── architecture.md
├── data/
│   ├── README.md
│   └── raw/                  # spreadsheet is local and gitignored
├── docs/
│   ├── data-dictionary.md
│   ├── data-model.md
│   ├── decisions.md
│   ├── design-system.md
│   ├── initial-planning-prompt.md
│   ├── pipeline-template.json
│   ├── setup.md
│   └── validation.sql
├── fabric/
│   └── notebooks/
│       ├── nb_bronze_ingest.Notebook/
│       ├── nb_silver_clean.Notebook/
│       ├── nb_gold_model.Notebook/
│       └── nb_quality_checks.Notebook/
├── src/
│   ├── ingestion/
│   ├── quality/
│   └── transformations/
└── tests/
    └── unit/
```

Fabric item folders are named `{displayName}.{ItemType}` and contain `.platform` plus the item definition. Connect the workspace Git integration to the `/fabric` directory, not the repository root, so docs and Python tests are not treated as Fabric items.

The lakehouse folder is created by Fabric after the portal item exists. Do not hand-author `lh_retail.Lakehouse`. A hand-written folder would not reliably create a schema-enabled lakehouse, and Git does not store lakehouse tables or files.

Do not add empty directories for Phase 2 reports, Terraform, Docker, or an AI application.

## Development principles

- Prefer a direct notebook over a framework. This is an 80/20 learning project.
- Implement one phase at a time. Do not build Phase 2–4 features while finishing Phase 1.
- Keep secrets, tokens, connection strings, and the raw spreadsheet out of Git.
- Do not invent Fabric item formats, GUIDs, or portal clicks. Use [docs/setup.md](docs/setup.md) and current Microsoft Learn docs.
- Do not claim a notebook, pipeline, or SQL query has been run in Fabric until it has.
- When Fabric writes lakehouse ids back into `notebook-content.py` after the lakehouse is attached, commit that generated metadata. Do not put placeholder GUIDs in first.

## Design system

Visual identity is taken from the separate project `abc-prediction-platform`. Do not modify that project. Tokens, type, and the Phase 2 Power BI note are in [docs/design-system.md](docs/design-system.md).

Phase 1 does not build a frontend and does not add a Power BI theme file.

## Business rules

The tested definitions live in:

- [src/ingestion/source_contract.py](src/ingestion/source_contract.py) — file name, sheet names, source columns, table names
- [src/transformations/rules.py](src/transformations/rules.py) — cancellation, line kind, customer id, item type, net amount
- [src/quality/checks.py](src/quality/checks.py) — critical check names

Silver and the quality notebook each contain the same transform function. `tests/unit/test_notebook_parity.py` fails if those copies drift from each other or from `src/`.

Item type is `product` only for a stock code of five digits plus an optional trailing letter, after the known non-product codes are classified. Everything else is `non_standard` or a named non-product type. Rows are not dropped because of item type.

Net amount is signed `quantity * unit_price`. Cancellations reduce net sales.

## Phase 1 incremental pattern

- Bronze appends. If `ingestion_batch_id` is already present, the ingest notebook loads nothing.
- Silver and Gold overwrite from the latest Bronze batch, chosen by `ingestion_timestamp`.
- Re-running the same batch id does not refresh Bronze. A corrected load needs a new batch id. Downstream tables then follow that latest batch. Older batches remain in Bronze for inspection.
- This is not a `MERGE` or watermark pipeline. Those belong to Phase 3.

## Data quality

`nb_quality_checks` writes every check to `quality.check_result` and raises if any critical check fails. Critical checks:

- `bronze_batch_not_empty`
- `silver_exclusions_accounted` — Silver count equals a fresh count of deduplicated valid rows
- `fact_matches_silver`
- `no_orphan_date_keys`
- `no_orphan_product_keys`
- `no_orphan_customer_keys`
- `no_orphan_geography_keys`
- `unique_product_keys`
- `unique_customer_keys`
- `net_amount_matches_inputs`
- `unknown_customer_present`

Rows missing invoice, stock code, invoice date, quantity, or unit price are excluded in Silver and must be part of that accounting identity. Exact duplicate business rows are removed and must be part of it too.

## Local verification

```bash
python -m pip install -r requirements-dev.txt
python -m pytest
```

Pytest covers the pure-Python rules and the notebook parity checks. It does not start Spark and it does not talk to Fabric.

## Roadmap

### Phase 1 — Fabric foundations (this repository state)

Source for the lakehouse pipeline is in the repo: notebooks, rules, quality checks, and setup instructions.

Not done, and not to be described as done until a developer completes [docs/setup.md](docs/setup.md):

- Fabric capacity, workspace, and schema-enabled lakehouse
- Git sync and lakehouse attachment
- Upload of the spreadsheet into OneLake
- Pipeline item with real notebook and workspace ids
- A successful pipeline run and the T-SQL checks

### Phase 2 — Business intelligence (not started)

- Refine the Gold model if the report work shows a real gap
- Power BI semantic model on the lakehouse, using Direct Lake where it fits
- DAX measures
- A three-page report: Executive Overview, Product & Sales Analysis, Customer Analysis
- A Power BI theme JSON translated from [docs/design-system.md](docs/design-system.md)

### Phase 3 — Production practices (not started)

- Incremental processing with `MERGE`
- Schema evolution
- Stronger idempotency
- Broader quality checks
- Scheduling and monitoring
- Lineage and access control
- A Git-based promotion flow or deployment pipelines
- Infrastructure automation only where it earns its place

### Phase 4 — AI extension (not started)

A separate Retail Intelligence Agent may answer questions about the Gold model, explain metric changes, and summarise performance. Likely shape: Python, FastAPI, an LLM, and read-only SQL against curated data. It stays outside the Fabric platform. Decide the design when the phase starts. Do not add an agent directory before then.

## Out of scope until a later phase explicitly takes it on

- AI agents or LLM calls
- Streaming and Real-Time Intelligence
- Spark performance tuning beyond clear, ordinary code
- A Fabric Warehouse
- Forecasting models
- Enterprise governance programmes
- A three-environment deployment estate
- A custom React dashboard
- Docker, Dev Containers, and Container Apps
- Terraform or other infrastructure-as-code

## Instructions for later sessions

1. Read this file, [docs/decisions.md](docs/decisions.md), and [docs/data-model.md](docs/data-model.md) before changing the model.
2. Inspect the repository before editing. Do not replace the raw spreadsheet.
3. Keep `src/` and the notebook rule blocks in sync. Run `python -m pytest` after changing either.
4. Follow Fabric’s Git item format for anything under `fabric/`. Unique `.platform` `logicalId` values must not be copied from another item.
5. Leave pipeline JSON in `docs/` until real workspace and notebook ids exist.
6. Do not implement Phase 2–4 work as part of a Phase 1 fix.
7. In the session summary, separate files changed, portal steps still required, and what was actually executed.
