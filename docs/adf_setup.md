# ADF Setup — Financial Universe Control Tower

## What was built

All ADF artifacts live under `adf/` and follow the standard ADF Git folder convention so they can be imported directly via **ADF Studio → Manage → Git configuration → Import**.

```
adf/
├── factory/
│   └── FinancialUniverseControlTower.json   ← global parameters + repo config
├── linkedService/
│   ├── LS_HTTP_GitHub.json                  ← anonymous HTTP to raw.githubusercontent.com
│   ├── LS_ADLS_Destination.json             ← managed identity to learnazure11 ADLS Gen2
│   └── LS_Databricks.json                   ← MSI auth to Databricks workspace
├── dataset/
│   ├── DS_HTTP_GitHub_CSV.json              ← parameterized GitHub CSV source
│   └── DS_ADLS_Bronze_CSV.json             ← parameterized ADLS Gen2 sink
├── pipeline/
│   ├── PL_Ingest_GitHub_to_Bronze.json      ← downloads all CSVs → bronze folders
│   └── PL_Orchestrate_Control_Tower.json    ← master: ingest → trigger Databricks → poll
└── trigger/
    └── TR_Daily_Control_Tower.json          ← daily 06:00 UTC schedule (paused by default)
```

---

## Source → Destination mapping

Data comes from the public [FinanceDatabase](https://github.com/JerBouma/FinanceDatabase) repository via anonymous HTTP (`raw.githubusercontent.com`).

| GitHub folder | ADLS path (inside `destination` container) | Exchange files included |
|---|---|---|
| `database/etfs/` | `bronze/etfs/` | BER, FRA, GER, JPX, LSE, NYQ, VIE, PCX, NMS, TOR, MUN, EBS, MIL, PAR, HAM |
| `database/equities/` | `bronze/equities_data/` | BER, BSE, FRA, GER, JPX, LSE, NSE, NYQ, SHZ, NMS, TOR, ASX, HKG, KOE, MCE, MUN, PAR, STO, VIE |
| `database/funds/` | `bronze/funds_data/` | BER, BSE, FRA, JPX, LSE, SHZ, VIE, MCE, NAS, TOR, MEX, STU |

To add more exchanges, append entries to the `file_list` default parameter in `PL_Ingest_GitHub_to_Bronze.json` — no pipeline logic changes needed.

---

## One-time setup steps before first run

### 1. Fill in the Databricks linked service placeholders

Open `adf/linkedService/LS_Databricks.json` and replace:

| Placeholder | Value |
|---|---|
| `<workspace-id>` | Numeric ID from your Databricks workspace URL, e.g. `1234567890` |
| `<subscription-id>` | Your Azure subscription GUID |
| `<resource-group>` | Resource group containing the Databricks workspace |
| `<workspace-name>` | Name of the Databricks workspace resource |

### 2. Assign the ADF Managed Identity access to ADLS

The `LS_ADLS_Destination` linked service uses ADF's system-assigned Managed Identity (no keys stored). Grant it **Storage Blob Data Contributor** on the `learnazure11` storage account.

### 3. Set the Databricks Job ID and PAT in the orchestration pipeline

In `PL_Orchestrate_Control_Tower.json` (or override at trigger level):

- `databricks_job_id` — the numeric ID of the deployed `[dev] Financial Universe Control Tower` job. Find it in **Databricks → Workflows → your job → Job ID**.
- `databricks_pat` — a Databricks Personal Access Token. **Do not hardcode this.** Store it in Azure Key Vault and reference it via an AKV-backed parameter or a secret in the ADF linked service.

### 4. Connect ADF to your Git repo (optional but recommended)

In `adf/factory/FinancialUniverseControlTower.json`, fill in `repoConfiguration`:

```json
"accountName": "your-github-username-or-org",
"repositoryName": "data1234",
"collaborationBranch": "main",
"rootFolder": "/adf"
```

Then in ADF Studio → Manage → Git configuration, point to the same repo. ADF will import all JSON artifacts from `/adf`.

### 5. Enable the trigger for prod

The trigger `TR_Daily_Control_Tower` is set to `"runtimeState": "Stopped"` by default (safe for dev). Enable it in ADF Studio → Manage → Triggers, or change `runtimeState` to `"Started"` and publish.

---

## End-to-end execution flow

```
TR_Daily_Control_Tower (06:00 UTC)
  └─► PL_Orchestrate_Control_Tower
        │
        ├─ [1] Resolve_Snapshot_Date  → sets today's yyyy-MM-dd
        │
        ├─ [2] Stage1_Ingest_GitHub_to_Bronze
        │         └─► PL_Ingest_GitHub_to_Bronze
        │               └─ ForEach (10 concurrent copies)
        │                     └─ Copy: DS_HTTP_GitHub_CSV → DS_ADLS_Bronze_CSV
        │                        (destination/bronze/etfs/*.csv)
        │                        (destination/bronze/equities_data/*.csv)
        │                        (destination/bronze/funds_data/*.csv)
        │
        ├─ [3] Stage2_Trigger_Databricks_Job
        │         POST /api/2.1/jobs/run-now  →  returns run_id
        │
        ├─ [4] Capture_Databricks_Run_Id
        │
        ├─ [5] Stage3_Poll_Until_Terminal  (Until loop, polls every 60s)
        │         GET /api/2.1/jobs/runs/get?run_id=...
        │
        └─ [6] Assert_Databricks_Success
                  → Fail activity if result_state ≠ SUCCESS
```

The Databricks job then runs its own 8-stage pipeline:
`00_setup → 01_source_profiling → 02_bronze_ingestion → 03_schema_standardization → 04_master_data → 05_data_quality → 06_change_detection → 07_gold_processing → 08_sql_views`

---

## Re-running a specific snapshot

Trigger the master pipeline manually with:

```
snapshot_date = 2026-08-19
reprocess     = true
```

This re-downloads the CSVs (overwriting existing bronze files — idempotent) and tells Databricks to rewrite that snapshot's partition.

---

## Adding more exchanges

Edit the `file_list` default value in `PL_Ingest_GitHub_to_Bronze.json`:

```json
{ "asset_folder": "equities", "bronze_folder": "bronze/equities_data", "exchange_code": "SAO" }
```

The exchange code must match a CSV filename that exists in the GitHub repo (e.g. `SAO.csv` under `database/equities/`).
