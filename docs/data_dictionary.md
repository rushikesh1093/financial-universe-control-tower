# Complete Data Dictionary - Financial Universe Control Tower

This document provides a comprehensive column-level dictionary for all datasets, tables, and views across the Medallion schemas in Unity Catalog (`azurelearn`).

---

## 1. Bronze Layer Schema (`azurelearn.bronze`)

The Bronze layer contains raw, unmodified source snapshots partitioned by `snapshot_date`.

### `bronze.equities` / `bronze.etfs` / `bronze.funds` / `bronze.indices` / `bronze.currencies` / `bronze.cryptocurrencies` / `bronze.money_markets`

| Column Name | Data Type | Nullable | Description |
|---|---|---|---|
| `symbol` | STRING | No | Source ticker / instrument symbol |
| `name` | STRING | Yes | Instrument or company name |
| `exchange` | STRING | Yes | Exchange venue identifier |
| `country` | STRING | Yes | Country name or ISO code |
| `currency` | STRING | Yes | Trading currency code |
| `sector` | STRING | Yes | Top-level sector classification (Equities/Funds) |
| `industry_group` | STRING | Yes | Industry group classification |
| `industry` | STRING | Yes | Specific industry classification |
| `market_cap` | STRING / DOUBLE | Yes | Market capitalization |
| `isin` | STRING | Yes | International Securities Identification Number |
| `cusip` | STRING | Yes | Committee on Uniform Securities Identification Procedures code |
| `figi` | STRING | Yes | Financial Instrument Global Identifier |
| `composite_figi` | STRING | Yes | Composite FIGI code |
| `shareclass_figi` | STRING | Yes | Share Class FIGI code |
| `source_file` | STRING | No | Original source file path |
| `source_url` | STRING | No | Repository URL source |
| `source_version` | STRING | No | Ingestion code version |
| `ingestion_timestamp` | TIMESTAMP | No | Ingestion execution timestamp (UTC) |
| `pipeline_run_id` | STRING | No | Data Factory / Databricks execution UUID |
| `snapshot_date` | DATE | No | Partition key (YYYY-MM-DD) |

---

## 2. Silver Layer Schema (`azurelearn.silver`)

### 2.1 `silver.dim_instrument`
Canonical security dimension.

| Column Name | Data Type | Nullable | Key | Description |
|---|---|---|---|---|
| `instrument_id` | STRING | No | PK | SHA-256 hash of ISIN or fallback listing ID |
| `instrument_type` | STRING | No | | Standardized asset class (`EQUITY`, `ETF`, `FUND`, `INDEX`, `CURRENCY`, `CRYPTO`, `MONEY_MARKET`) |
| `symbol` | STRING | No | | Primary symbol representation |
| `instrument_name` | STRING | Yes | | Canonical instrument name |
| `currency` | STRING | Yes | | Primary trading currency |
| `country` | STRING | Yes | | Domicile country |
| `status` | STRING | No | | Active status (`ACTIVE`, `DELISTED`, `SUSPENDED`) |
| `created_at` | TIMESTAMP | No | | Record creation timestamp |
| `updated_at` | TIMESTAMP | No | | Record update timestamp |

### 2.2 `silver.dim_entity`
Canonical issuer / company dimension resolved via graph connected components.

| Column Name | Data Type | Nullable | Key | Description |
|---|---|---|---|---|
| `entity_id` | STRING | No | PK | Hash label of graph connected component |
| `entity_name` | STRING | No | | Resolved canonical company name |
| `country` | STRING | Yes | | Primary country of incorporation |
| `entity_type` | STRING | No | | Entity classification (`CORPORATION`, `FUND_ISSUER`, `GOVERNMENT`, `ISSUER`) |
| `created_at` | TIMESTAMP | No | | Entity creation timestamp |
| `updated_at` | TIMESTAMP | No | | Entity last update timestamp |

### 2.3 `silver.fact_listing`
Exchange listing fact table linking instruments to venues and entities.

| Column Name | Data Type | Nullable | Key | Description |
|---|---|---|---|---|
| `listing_id` | STRING | No | PK | SHA-256 hash of `(asset_class, symbol, exchange)` |
| `entity_id` | STRING | Yes | FK | Foreign key to `dim_entity` (NULL for index/FX/crypto) |
| `instrument_id` | STRING | No | FK | Foreign key to `dim_instrument` |
| `symbol` | STRING | No | | Venue ticker symbol |
| `exchange` | STRING | No | | Exchange MIC or code |
| `market` | STRING | Yes | | Market segment |
| `currency` | STRING | Yes | | Listing trading currency |
| `is_primary` | BOOLEAN | No | | Flag indicating primary venue listing |
| `effective_from` | DATE | No | | SCD Type 2 start date |
| `effective_to` | DATE | No | | SCD Type 2 end date (`9999-12-31` if active) |
| `is_current` | BOOLEAN | No | | SCD Type 2 current record indicator |

### 2.4 `silver.dim_identifier`
Identifier mapping table supporting multiple financial identifiers per security.

| Column Name | Data Type | Nullable | Key | Description |
|---|---|---|---|---|
| `identifier_id` | STRING | No | PK | SHA-256 hash of `(instrument_id, identifier_type, identifier_value)` |
| `instrument_id` | STRING | No | FK | Foreign key to `dim_instrument` |
| `identifier_type` | STRING | No | | Code type (`ISIN`, `CUSIP`, `FIGI`, `COMPOSITE_FIGI`, `SHARECLASS_FIGI`) |
| `identifier_value` | STRING | No | | Raw identifier code value |
| `is_primary` | BOOLEAN | No | | Flag indicating primary identifier for type |
| `effective_from` | DATE | No | | SCD Type 2 start date |
| `effective_to` | DATE | No | | SCD Type 2 end date |
| `is_current` | BOOLEAN | No | | SCD Type 2 active indicator |

### 2.5 `silver.dim_classification`
SCD Type 2 taxonomy history.

| Column Name | Data Type | Nullable | Key | Description |
|---|---|---|---|---|
| `classification_id` | STRING | No | PK | SHA-256 hash of classification snapshot |
| `instrument_id` | STRING | No | FK | Foreign key to `dim_instrument` |
| `sector` | STRING | Yes | | High-level sector classification |
| `industry_group` | STRING | Yes | | Industry group |
| `industry` | STRING | Yes | | Industry specialization |
| `effective_from` | DATE | No | | Effective start date |
| `effective_to` | DATE | No | | Effective end date |
| `is_current` | BOOLEAN | No | | Active flag |

---

## 3. Gold Layer Schema (`azurelearn.gold`)

### 3.1 `gold.gold_security_master`
Curated trusted master data table containing current valid instruments.

| Column Name | Data Type | Nullable | Description |
|---|---|---|---|
| `instrument_id` | STRING | No | Canonical security identifier |
| `entity_id` | STRING | Yes | Canonical issuer identifier |
| `listing_id` | STRING | No | Primary listing identifier |
| `symbol` | STRING | No | Ticker symbol |
| `instrument_name` | STRING | Yes | Full instrument name |
| `asset_class` | STRING | No | Asset class code |
| `exchange` | STRING | Yes | Primary exchange |
| `country` | STRING | Yes | Domicile country |
| `currency` | STRING | Yes | Trading currency |
| `sector` | STRING | Yes | Current sector |
| `industry` | STRING | Yes | Current industry |
| `isin` | STRING | Yes | Validated ISIN code |
| `cusip` | STRING | Yes | Validated CUSIP code |
| `figi` | STRING | Yes | Validated FIGI code |
| `quality_score` | DOUBLE | No | Quality score (0–100) |
| `quality_band` | STRING | No | Quality band (`TRUSTED`, `REVIEW`) |
| `snapshot_date` | DATE | No | Snapshot partition date |

### 3.2 `gold.gold_instrument_changes`
Granular column-level audit table of changes between source snapshots.

| Column Name | Data Type | Nullable | Description |
|---|---|---|---|
| `change_id` | STRING | No | Unique change event ID |
| `instrument_id` | STRING | No | Affected instrument ID |
| `listing_id` | STRING | No | Affected listing ID |
| `symbol` | STRING | No | Ticker symbol |
| `change_date` | DATE | No | Snapshot date change was detected |
| `change_type` | STRING | No | Categorized change type (`NEW`, `REMOVED`, `MODIFIED`, `RECLASSIFIED`, `RELISTED`, `IDENTIFIER_CHANGED`, `DATA_QUALITY_ISSUE`) |
| `column_changed` | STRING | Yes | Name of specific modified attribute |
| `old_value` | STRING | Yes | Attribute value prior to snapshot |
| `new_value` | STRING | Yes | Attribute value in new snapshot |
| `source_snapshot` | DATE | No | Source snapshot date |
| `pipeline_run_id` | STRING | No | Execution run UUID |

### 3.3 `gold.gold_universe_summary`
Aggregated overview metrics for executive dashboards.

| Column Name | Data Type | Nullable | Description |
|---|---|---|---|
| `snapshot_date` | DATE | No | Snapshot date |
| `asset_class` | STRING | No | Asset class group |
| `country` | STRING | Yes | Country |
| `exchange` | STRING | Yes | Exchange venue |
| `sector` | STRING | Yes | Sector |
| `currency` | STRING | Yes | Trading currency |
| `total_instruments` | LONG | No | Total instrument count |
| `active_instruments` | LONG | No | Total active count |
| `trusted_count` | LONG | No | Count in TRUSTED band (≥90) |
| `review_count` | LONG | No | Count in REVIEW band (75–89) |
| `quarantine_count` | LONG | No | Count in QUARANTINE band (<75) |
| `avg_quality_score` | DOUBLE | No | Average data quality score |

---

## 4. Quarantine Schema (`azurelearn.quarantine`)

### `quarantine.rejected_records`
Isolated record repository for records failing validation rules.

| Column Name | Data Type | Nullable | Description |
|---|---|---|---|
| `record_id` | STRING | No | SHA-256 hash of rejected record payload |
| `source_dataset` | STRING | No | Asset class / source file path |
| `source_snapshot` | DATE | No | Source snapshot date |
| `failure_reason` | STRING | No | Standardized error code (`INVALID_IDENTIFIER`, `DUPLICATE_IDENTIFIER`, `UNKNOWN_EXCHANGE`, `MISSING_SYMBOL`, etc.) |
| `failed_column` | STRING | Yes | Target column causing validation rejection |
| `original_record` | STRING | No | Full raw CSV row serialized as JSON |
| `pipeline_run_id` | STRING | No | Execution run UUID |
| `quarantine_timestamp` | TIMESTAMP | No | Timestamp of quarantine isolation |

---

## 5. Audit Schema (`azurelearn.audit`)

### 5.1 `audit.pipeline_run_log`
Execution run audit log for all pipeline stages.

| Column Name | Data Type | Nullable | Description |
|---|---|---|---|
| `pipeline_run_id` | STRING | No | Pipeline execution UUID |
| `stage` | STRING | No | Notebook stage name (`00_setup` to `08_sql_views`) |
| `start_time` | TIMESTAMP | No | Stage execution start time |
| `end_time` | TIMESTAMP | No | Stage execution completion time |
| `duration_seconds` | DOUBLE | No | Execution duration in seconds |
| `records_read` | LONG | Yes | Input record count |
| `records_written` | LONG | Yes | Output record count written to target |
| `records_rejected` | LONG | Yes | Count of records routed to quarantine |
| `quality_score` | DOUBLE | Yes | Overall quality score for processed batch |
| `status` | STRING | No | Stage completion status (`SUCCESS`, `FAILED`, `WARNING`) |
| `error_message` | STRING | Yes | Detailed stack trace / error message if failed |

### 5.2 `audit.dq_scoring_profile`
Configurable scoring weight matrix.

| Column Name | Data Type | Nullable | Description |
|---|---|---|---|
| `asset_class` | STRING | No | Target asset class (`EQUITY`, `ETF`, `FUND`, etc.) |
| `component` | STRING | No | Rule component name |
| `weight` | DOUBLE | No | Weight percentage (must sum to 100 per asset class) |
| `created_at` | TIMESTAMP | No | Profile creation timestamp |
| `updated_at` | TIMESTAMP | No | Profile update timestamp |
