# Setup and runbook

## Prerequisites

| Requirement | Status in this workspace |
|---|---|
| Unity Catalog metastore | present (`metastore_azure_centralindia`) |
| Catalog | `azurelearn` (the metastore has no storage root, so a new catalog cannot be created from the CLI) |
| Schemas | `bronze`, `silver`, `gold`, `quarantine`, `audit` |
| External location | `project` → `abfss://destination@learnazure11.dfs.core.windows.net/` |
| Storage credential | `financial-databricks` (Azure managed identity) |
| Compute | **serverless only** — the Jobs API rejects `new_cluster` here |
| CLI | bundled with the VS Code extension at `~/.vscode/extensions/databricks.databricks-*/bin/databricks.exe` |

No account key, SAS token or password is used anywhere. Storage access goes
through the managed-identity credential behind the external location.

## First run

```bash
# from the repo root
databricks bundle validate -t dev -p azure-databricks
databricks bundle deploy   -t dev -p azure-databricks
databricks bundle run financial_universe_pipeline -t dev -p azure-databricks
```

Or in the UI: **Jobs & Pipelines → `[dev] Financial Universe Control Tower` → Run now**.

## Job parameters

| Parameter | Default | Purpose |
|---|---|---|
| `catalog` | `azurelearn` | Catalog holding the medallion schemas |
| `snapshot_date` | `{{job.start_time.iso_date}}` | The snapshot being processed |
| `source_root` | `abfss://…/bronze` | Landing area scanned for CSVs |
| `silver_root` | `abfss://…/silver` | External location for silver tables |
| `gold_root` | `abfss://…/gold` | External location for gold tables |
| `reprocess` | `false` | Reserved for forced reprocessing |

Reprocess an earlier snapshot without touching anything else:

```bash
databricks bundle run financial_universe_pipeline -t dev \
  --notebook-params snapshot_date=2026-08-01
```

Re-running a snapshot is safe. Every stage rewrites only that snapshot's
partition (`replaceWhere`) or MERGEs on a deterministic key, so a repeat run
produces the same result rather than duplicate rows.

## Where things land

```
azurelearn
├── bronze        immutable dated snapshots, one table per asset class
├── silver        instrument_staging, dim_instrument, dim_entity, fact_listing,
│                 dim_identifier, dim_classification, instrument_quality,
│                 fact_instrument_changes, ref_exchange/country/currency
├── gold          gold_security_master, gold_instrument_changes,
│                 gold_data_quality, gold_universe_summary, dim_*/fact_listing
├── quarantine    rejected_records
└── audit         pipeline_run_log, source_profile, dq_rule_catalogue,
                  dq_scoring_profile, ingestion_control
```

Silver and gold are **external** Delta tables: the files live under the project's
own ADLS container, and the tables are still queryable as
`azurelearn.silver.<table>`.

## Observability

Every stage writes a row to `azurelearn.audit.pipeline_run_log`:

```sql
SELECT stage, status, duration_seconds,
       records_read, records_written, records_rejected, quality_score
FROM   azurelearn.audit.pipeline_run_log
WHERE  pipeline_run_id = '<run id>'
ORDER  BY start_time;
```

A failed stage is logged with its error before the exception is re-raised, so a
failure is still observable rather than vanishing.

## Tuning the quality model

Weights are data, not code:

```sql
UPDATE azurelearn.audit.dq_scoring_profile
SET    weight = 30
WHERE  asset_class = 'EQUITY' AND component = 'identifier_completeness';
```

Each asset class must still total 100 — the pipeline raises a clear error on the
next run if it does not. Delta history records who changed what.

## Local validation

The transformation logic can be exercised without a cluster:

```bash
python tests/local_pipeline_test.py
```

It runs asset-class detection, canonical projection, key derivation, entity
resolution, the quality rules and a self-comparison change-detection check
against the CSVs in `bronze/data`, and exits non-zero on any failed invariant.

## Gotchas already handled

- **`input_file_name()` is blocked under Unity Catalog.** File lineage uses
  `_metadata.file_path`. Because `_metadata` is a hidden, file-backed-only
  column, it is promoted to a real column *per file, before any union* — it does
  not survive one.
- **Folder names do not indicate asset class.** Ingestion classifies each file by
  its column signature and reports folders containing mixed classes.
- **`GBp` is pence, not a mis-cased `GBP`.** It is folded to `GBX` so the unit is
  not silently changed.
- **`MCE` and `KEW` appear in the currency column but are not currencies.** They
  are named explicitly so the failure reason is precise.
