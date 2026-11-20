# Architectural decisions

Decisions recorded for Phase 1. Later phases can supersede them in this log rather than silently changing behaviour.

## D1. One schema-enabled lakehouse

**Decision.** Use a single lakehouse, `lh_retail`, with schemas `bronze`, `silver`, `gold`, and `quality`. Do not add a Fabric Warehouse.

**Why.** The dataset is one workbook and one team. Schemas keep the Medallion layers visible. The lakehouse SQL analytics endpoint already provides T-SQL, which is the Phase 1 requirement.

**Consequence.** Schemas must be enabled in the portal at creation time. The lakehouse item is not hand-written in Git, because that path would not reliably create a schema-enabled lakehouse.

## D2. Notebooks in Git, pipeline as a template

**Decision.** Commit four notebooks under `fabric/notebooks/` in Fabric's `notebook-content.py` format. Keep the pipeline JSON in `docs/pipeline-template.json` until it contains real workspace and notebook ids.

**Why.** A `TridentNotebook` activity requires `notebookId` and `workspaceId`. Those GUIDs do not exist before the workspace does. Syncing a pipeline full of placeholders could make Git update fail.

**Consequence.** The first Fabric sync creates notebooks only. The developer attaches the lakehouse, fills the template, and syncs the pipeline as a second step. See [setup.md](setup.md).

## D3. Cancellations stay in the model

**Decision.** Keep cancellation lines (`Invoice` starting with `C`) and adjustment lines (`Invoice` starting with `A`). Net amount is signed quantity times unit price.

**Why.** Returns and cancellations are part of the business case. Dropping them would make revenue look like gross demand and would hide the behaviour Phase 2 needs to show.

**Consequence.** Gross sales, returns, and net sales are filters on `line_kind` and `is_cancellation`, not separate pipelines.

## D4. Unknown customer and unknown country members

**Decision.** Lines with no customer id stay in Silver and point at `gold.dim_customer` key `-1` (`customer_id = 'UNKNOWN'`). Lines with no country point at `gold.dim_geography` key `-1` (`country_name = 'UNKNOWN'`).

**Why.** About a quarter of published Online Retail II lines have no customer id. Dropping them would understate revenue and product sales. The same rule for country keeps the fact at the same grain as Silver, so the quality check can require equal counts.

**Consequence.** Customer-level measures in Phase 2 must exclude customer key `-1`. Geography reports should show `UNKNOWN` rather than omit those lines.

## D5. No invented product category

**Decision.** `dim_product` has stock code, description, item type, and an `is_merchandise` flag. It has no category.

**Why.** The workbook has no category column. A text-mined or handmade category list would look like source data and would be wrong often enough to mislead a demo.

**Consequence.** "Product performance" in Phase 1 and Phase 2 means stock code and item type. A category dimension waits for a real mapping.

## D6. Non-product stock codes are classified, not removed

**Decision.** Known charge and adjustment codes are labelled (`postage`, `carriage`, `manual`, `discount`, `fee`, `sample`, `gift_voucher`, `adjustment`). A five-digit code with an optional trailing letter is `product`. Any other code is `non_standard`.

**Why.** Postage and fees are real money movements, but they are not merchandise. Keeping them in the fact, with a flag, lets a report exclude them without losing the rows.

**Codes.** `POST` and `DOT` postage; `C2` and `C3` carriage; `M` manual; `D` discount; `BANK CHARGES`, `AMAZONFEE`, and `CRUK` fees; `S` sample; `B`, `ADJUST`, and `ADJUST2` adjustments. Codes starting with `GIFT` (including `gift_0001_…`) are gift vouchers. `CRUK` and `ADJUST2` come from the published profile of this dataset and are included so those charges are not left as `non_standard`.

## D7. December 2010 sheet overlap

**Decision.** Silver drops exact duplicate business rows after trim and type parsing.

**Why.** `Year 2009-2010` and `Year 2010-2011` both contain 1–9 December 2010. Unioning the sheets without a dedupe would double those lines.

**Consequence.** Duplicate removal is exact on the typed business columns. It is not a fuzzy match. The quality check recomputes the same function and requires the Silver count to match.

## D8. Latest-batch full refresh

**Decision.** Bronze appends a new `ingestion_batch_id` and skips an id that already exists. Silver and Gold overwrite from the batch with the greatest `ingestion_timestamp`.

**Why.** The source is one historical file. A watermark merge would be ceremony. The batch id still shows the incremental shape Phase 3 will harden.

**Consequence.** Replacing a bad batch means loading a new id, not reusing the old one. Older Bronze batches are not deleted.

## D9. Rules live twice, tests keep them aligned

**Decision.** Pure Python in `src/` is the tested definition of codes, classifications, and check names. Notebooks contain the same constants and a shared Spark transform. Pytest compares them.

**Why.** Fabric notebooks do not import this repository as a package. Copying the rules into the notebook is what will actually run. The parity test is the guard against drift.

## D10. Excel reader chosen at runtime

**Decision.** The Bronze notebook tries Spark's `excel` format first. If that read fails, it uses `pandas.read_excel` on `/lakehouse/default/Files/...` and creates a Spark frame. The notebook prints which path it used.

**Why.** Fabric runtimes have not always exposed the same Excel source. The file is about 45 MB, so a driver-side pandas read is acceptable for this demo if Spark cannot read it.

**Consequence.** Do not document a specific reader as the one that ran until a Fabric execution prints it.

## D11. Design identity comes from abc-prediction-platform

**Decision.** Document that project's colour and type tokens in [design-system.md](design-system.md). Do not invent a second palette. Translate the tokens to a Power BI theme in Phase 2, not now.

## D12. No local Fabric stand-in

**Decision.** Do not add Docker, a Dev Container, Terraform, or a local Spark project in Phase 1.

**Why.** Fabric execution cannot be reproduced faithfully that way, and the learning goal is the managed platform. Local pytest covers the rules that do not need Spark.
