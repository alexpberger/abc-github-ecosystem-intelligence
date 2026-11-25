# Fabric setup

Phase 1 source is in Git. The Fabric workspace, lakehouse, Key Vault secret, and pipeline are not. Complete the steps below in order. Do not mark a step done in the docs until you have done it.

Nothing in this file has been executed against a Fabric tenant from this repository.

## 1. Capacity and workspace

1. Sign in to the [Fabric portal](https://app.fabric.microsoft.com/).
2. Start a Fabric trial capacity if the tenant does not already have one you can use.
3. Create a development workspace, for example `abc-github-ecosystem-dev`, and attach it to that capacity.

## 2. Create the lakehouse with schemas

1. In the workspace, create a lakehouse named `lh_ecosystem`.
2. Enable **Lakehouse schemas** in the creation dialog. This cannot be turned on after the lakehouse exists. If the control is missing, stop and check current Microsoft Learn documentation for "lakehouse schemas" before creating a schema-less lakehouse.
3. Open the lakehouse and create four schemas: `bronze`, `silver`, `gold`, and `quality`. Spark can also run `CREATE SCHEMA IF NOT EXISTS`, and the notebooks do that, but the lakehouse itself must already be schema-enabled.

Do not add a Warehouse.

## 3. Connect Git

1. In workspace settings, open Git integration.
2. Connect this GitHub repository.
3. Use branch `main`.
4. Set the Git folder to `/fabric`. Connecting the repository root would mix notebooks with docs and tests.
5. Sync from Git into the workspace.

The sync should create five notebooks:

- `nb_discover`
- `nb_snapshot`
- `nb_silver`
- `nb_gold_model`
- `nb_quality_checks`

It should not create a pipeline. The pipeline template is outside `/fabric` until step 6.

## 4. Attach the lakehouse

For each of the five notebooks:

1. Open the notebook.
2. Set `lh_ecosystem` as the default lakehouse.
3. Confirm the notebook can see the lakehouse explorer.

Commit the notebook metadata Fabric writes back into `notebook-content.py`. Those files will then contain the real lakehouse and workspace ids. That commit is expected. Do not copy ids from another item's `.platform` file.

## 5. Store the GitHub token

Create a GitHub personal access token that can read public repositories. A fine-grained token with public-repository read, or a classic token with no extra scopes, is enough. Do not grant write scopes.

Store the token as a secret in Azure Key Vault. Do not put it in a notebook cell, a pipeline parameter, or Git.

The notebooks call `notebookutils.credentials.getSecret(key_vault_url, secret_name)`, which is the method documented by Microsoft Learn:

- [NotebookUtils credentials utilities](https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-credentials)

`key_vault_url` is the vault URL, including the scheme and trailing slash, for example `https://example.vault.azure.net/`. `secret_name` defaults to `github-token`. The identity that executes the notebook needs permission to get that secret. Learn describes this call as using the current user's credentials. Confirm the current portal behaviour before relying on a workspace identity alone.

A local experiment can leave `key_vault_url` empty and set `GITHUB_TOKEN` in the session. That path is not the Fabric setup.

## 6. Create the pipeline

Notebook activities need GUIDs. Take them from the browser address bar after the notebooks exist:

- Workspace id: the GUID in the `/groups/{workspace-id}/` segment.
- Notebook id: the GUID that identifies that notebook in the same URL.

Do not invent either value.

Then:

1. Copy [pipeline-template.json](pipeline-template.json) to `fabric/pipelines/pl_github_ecosystem.DataPipeline/pipeline-content.json`.
2. Replace every `REPLACE_WITH_WORKSPACE_ID` with the workspace GUID.
3. Replace each notebook placeholder with that notebook's GUID.
4. Replace both `REPLACE_WITH_COLLECTION_RUN_ID` values with the same pipeline expression, `@pipeline().parameters.collection_run_id`. Add a pipeline parameter `collection_run_id`. For one snapshot per UTC day, set the parameter default to `@utcNow('yyyyMMdd')`. Do not call `utcNow()` separately inside each activity, or the two notebooks can disagree by a second.
5. Replace both `REPLACE_WITH_KEY_VAULT_URL` values with the vault URL. The secret value stays in Key Vault.
6. Add `fabric/pipelines/pl_github_ecosystem.DataPipeline/.platform` using a new logical id (a freshly generated UUID). Do not reuse a notebook logical id.

```json
{
  "version": "2.0",
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/platform/platformProperties.json",
  "config": {
    "logicalId": "GENERATE_A_NEW_UUID"
  },
  "metadata": {
    "type": "DataPipeline",
    "displayName": "pl_github_ecosystem",
    "description": "Discovers GitHub repositories, snapshots the tracked set, then builds Silver, Gold, and quality checks."
  }
}
```

7. Commit and sync from Git again.
8. Open the pipeline in the portal and confirm each notebook activity resolves. If the parameter payload is rejected by the current pipeline schema, set the same values in the activity UI and commit the JSON Fabric writes.

Activity order:

1. `nb_discover`
2. `nb_snapshot`
3. `nb_silver`
4. `nb_gold_model`
5. `nb_quality_checks`

Each activity depends on the previous one succeeding.

Discover receives `lookback_days` (`14`) and `max_tracked_repositories` (`500`). Snapshot reads the cap from the discovery batch. Both receive the same `collection_run_id`, vault URL, and secret name.

## 7. Run and validate

1. Run `pl_github_ecosystem` once.
2. If a critical quality check fails, the quality notebook raises and the pipeline stops. Read `quality.check_result` before changing rules. A warning for a search cap or for repositories outside the tracking cap does not stop the pipeline.
3. Open the SQL analytics endpoint for `lh_ecosystem`. If new tables are missing, refresh endpoint metadata from the portal, then retry.
4. Run [validation.sql](validation.sql). These statements are T-SQL, not Spark SQL.

A second pipeline run with the same `collection_run_id` skips the GitHub calls and rebuilds Silver and Gold from the stored batches.

To collect again on the same day, pass a new `collection_run_id`. Do not reuse an id if you need another snapshot.

## What this repository cannot confirm

- That the trial capacity and workspace exist
- That schemas were enabled
- That Git sync succeeded
- That the executing identity can get the Key Vault secret
- That the pipeline JSON was accepted without a portal edit
- Which catalogue queries hit the 1,000-result cap
- That the validation queries returned rows

Record those results outside this file after a real run. Update [decisions.md](decisions.md) if the portal forces a different choice.
