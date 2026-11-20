# Fabric setup

Phase 1 source is in Git. The Fabric workspace, lakehouse, file upload, and pipeline are not. Complete the steps below in order. Do not mark a step done in the docs until you have done it.

Nothing in this file has been executed against a Fabric tenant from this repository.

## 1. Capacity and workspace

1. Sign in to the [Fabric portal](https://app.fabric.microsoft.com/).
2. Start a Fabric trial capacity if the tenant does not already have one you can use.
3. Create a development workspace, for example `abc-retail-intelligence-dev`, and attach it to that capacity.

## 2. Create the lakehouse with schemas

1. In the workspace, create a lakehouse named `lh_retail`.
2. Enable **Lakehouse schemas** in the creation dialog. This cannot be turned on after the lakehouse exists. If the control is missing, stop and check current Microsoft Learn documentation for "lakehouse schemas" before creating a schema-less lakehouse.
3. Open the lakehouse and create four schemas: `bronze`, `silver`, `gold`, and `quality`. Spark can also run `CREATE SCHEMA IF NOT EXISTS`, and the notebooks do that, but the lakehouse itself must already be schema-enabled.

Do not add a Warehouse.

## 3. Connect Git

1. In workspace settings, open Git integration.
2. Connect this GitHub repository.
3. Use branch `main`.
4. Set the Git folder to `/fabric`. Connecting the repository root would mix notebooks with docs and tests.
5. Sync from Git into the workspace.

The sync should create four notebooks:

- `nb_bronze_ingest`
- `nb_silver_clean`
- `nb_gold_model`
- `nb_quality_checks`

It should not create a pipeline. The pipeline template is outside `/fabric` until step 6.

## 4. Attach the lakehouse

For each of the four notebooks:

1. Open the notebook.
2. Set `lh_retail` as the default lakehouse.
3. Confirm the notebook can see the lakehouse explorer.

Commit the notebook metadata Fabric writes back into `notebook-content.py`. Those files will then contain the real lakehouse and workspace ids. That commit is expected. Do not copy ids from another item's `.platform` file.

## 5. Upload the spreadsheet

The workbook stays out of Git. From the lakehouse explorer:

1. Open **Files**.
2. Create a folder named `raw` if it is not there.
3. Upload the local file `data/raw/online_retail_II.xlsx`.
4. Leave the file name unchanged.

The Bronze notebook reads `Files/raw/online_retail_II.xlsx`.

## 6. Create the pipeline

Notebook activities need GUIDs. Take them from the browser address bar after the notebooks exist:

- Workspace id: the GUID in the `/groups/{workspace-id}/` segment.
- Notebook id: the GUID that identifies that notebook in the same URL.

Do not invent either value.

Then:

1. Copy [pipeline-template.json](pipeline-template.json) to `fabric/pipelines/pl_retail_medallion.DataPipeline/pipeline-content.json`.
2. Replace every `REPLACE_WITH_WORKSPACE_ID` with the workspace GUID.
3. Replace each notebook placeholder with that notebook's GUID.
4. Add `fabric/pipelines/pl_retail_medallion.DataPipeline/.platform` using a new logical id (a freshly generated UUID). Do not reuse a notebook logical id.

```json
{
  "version": "2.0",
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/platform/platformProperties.json",
  "config": {
    "logicalId": "GENERATE_A_NEW_UUID"
  },
  "metadata": {
    "type": "DataPipeline",
    "displayName": "pl_retail_medallion",
    "description": "Runs Bronze, Silver, Gold, and quality notebooks in order."
  }
}
```

5. Commit and sync from Git again.
6. Open the pipeline in the portal and confirm each notebook activity resolves. If the parameter payload is rejected by the current pipeline schema, set notebook parameter `ingestion_batch_id` to `online_retail_ii_full` in the activity UI and commit the JSON Fabric writes.

Activity order:

1. `nb_bronze_ingest`
2. `nb_silver_clean`
3. `nb_gold_model`
4. `nb_quality_checks`

Each activity depends on the previous one succeeding.

## 7. Run and validate

1. Run `pl_retail_medallion` once. The default batch id is `online_retail_ii_full`.
2. If a critical quality check fails, the quality notebook raises and the pipeline stops. Read `quality.check_result` before changing rules.
3. Open the SQL analytics endpoint for `lh_retail`. If new tables are missing, refresh endpoint metadata from the portal, then retry.
4. Run [validation.sql](validation.sql). These statements are T-SQL, not Spark SQL.

A second pipeline run with the same batch id should skip the Bronze append and rebuild Silver and Gold from that batch.

To load the file again, pass a new `ingestion_batch_id`. Do not reuse an id if you need a new Bronze snapshot.

## What this repository cannot confirm

- That the trial capacity and workspace exist
- That schemas were enabled
- That Git sync succeeded
- Which Excel reader the Bronze notebook used
- That the pipeline JSON was accepted without a portal edit
- That the validation queries returned rows

Record those results outside this file after a real run. Update [decisions.md](decisions.md) if the portal forces a different choice.
