# Power BI - Financial Universe Control Tower Specification

## Executive Summary

The **Financial Universe Control Tower** includes a 5-page interactive Power BI reporting solution connected directly to the Gold and Serving layers in Azure Databricks Unity Catalog (`azurelearn`) and Azure SQL. This document defines the functional design, visual layouts, KPI metrics, and drill-down paths for each page of the dashboard.

---

## Dashboard Architecture & Data Source Integration

```mermaid
flowchart TD
    subgraph Unity Catalog Gold Layer (azurelearn)
        GSM[(gold_security_master)]
        GIC[(gold_instrument_changes)]
        GDQ[(gold_data_quality)]
        GUS[(gold_universe_summary)]
    end

    subgraph Power BI Storage Mode
        PBI_DQ[DirectQuery Mode / Dual Mode]
    end

    subgraph Power BI 5-Page Control Tower
        P1[Page 1: Universe Overview]
        P2[Page 2: Instrument Change Monitor]
        P3[Page 3: Classification Intelligence]
        P4[Page 4: Data Quality Control Tower]
        P5[Page 5: Security Master Explorer]
    end

    GSM --> PBI_DQ
    GIC --> PBI_DQ
    GDQ --> PBI_DQ
    GUS --> PBI_DQ
    PBI_DQ --> P1
    PBI_DQ --> P2
    PBI_DQ --> P3
    PBI_DQ --> P4
    PBI_DQ --> P5
```

---

## Page 1: Universe Overview

### Objective:
Provides C-level executives and portfolio managers with a high-level summary of the global financial instrument universe.

### Top Card KPI Metrics:
- **Total Instruments**: Count of active instruments in current snapshot
- **Active Instruments**: Count of non-delisted instruments
- **New Instruments**: Count of instruments added in current snapshot
- **Removed Instruments**: Count of delisted / removed instruments
- **Modified Instruments**: Count of instruments with metadata updates
- **Average Data Quality Score**: Weighted quality score across universe (0–100%)

### Visual Layout:
1. **Instrument Count by Asset Class**: Horizontal Stacked Bar Chart (`EQUITY`, `ETF`, `FUND`, `INDEX`, `CURRENCY`, `CRYPTO`, `MONEY_MARKET`).
2. **Instrument Count by Country**: Filled Map / Treemap showing global geographical distribution.
3. **Top 10 Exchange Venues**: Donut Chart ranking exchanges by listing volume.
4. **Currency Breakdown**: Column Chart displaying top trading currencies (`USD`, `EUR`, `JPY`, `GBP`, `GBX`).

---

## Page 2: Instrument Change Monitor

### Objective:
Enables data governance teams and portfolio risk managers to monitor daily instrument mutations across snapshots.

### Top Card KPI Metrics:
- **New Additions** (`NEW`)
- **Removals** (`REMOVED`)
- **Metadata Modifications** (`MODIFIED`)
- **Sector Reclassifications** (`RECLASSIFIED`)
- **Exchange Relistings** (`RELISTED`)
- **Identifier Mutations** (`IDENTIFIER_CHANGED`)

### Visual Layout & Interactivity:
1. **Daily Change Trend Line Chart**: Time series displaying change volume over time by `change_type`.
2. **Change Type Distribution Pie Chart**: Breakdown of total changes across categories.
3. **Top Affected Exchanges Bar Chart**: Ranks exchanges with the highest frequency of changes.
4. **Top Affected Industries Matrix**: Table displaying reclassification shifts from `old_value` to `new_value`.

---

## Page 3: Classification Intelligence

### Objective:
Enables quantitative analysts to analyze portfolio sector exposures and drill into industry classification hierarchies.

### Hierarchical Drill-Down Structure:
$$\text{Sector} \longrightarrow \text{Industry Group} \longrightarrow \text{Industry} \longrightarrow \text{Country} \longrightarrow \text{Exchange}$$

### Visual Layout:
1. **Interactive Sunburst / Decomposition Tree**: Displays instrument distribution down the classification hierarchy.
2. **Sector Concentration Heatmap**: Color-coded matrix showing instrument density per country and sector.
3. **Classification Historical Timeline**: Visualizes historical SCD Type 2 sector shifts over time.

---

## Page 4: Data Quality Control Tower

### Objective:
Provides data stewards with operational visibility into data health, quality scores, and quarantined records.

### Top Card KPI Metrics:
- **Overall Quality Score** (e.g. `94.2%`)
- **Trusted Instruments Count** (`Score >= 90`)
- **Review Required Count** (`75 <= Score <= 89`)
- **Quarantined Records Count** (`Score < 75`)
- **Identifier Conflicts Count**
- **Classification Conflicts Count**

### Visual Layout:
1. **Quality Band Gauge Chart**: Displays proportion of instruments in `TRUSTED`, `REVIEW`, and `QUARANTINE`.
2. **Rule Failure Breakdown Bar Chart**: Ranks failure reasons (`MISSING_SYMBOL`, `INVALID_IDENTIFIER`, `UNKNOWN_EXCHANGE`, etc.).
3. **Quarantine Detail Table**: Interactive grid displaying `record_id`, `source_dataset`, `failure_reason`, `failed_column`, and raw JSON payload with a "Re-process" action trigger.

---

## Page 5: Security Master Explorer

### Objective:
Serves as an operational search engine for analysts to look up canonical financial instruments and their linked venue listings and identifiers.

### Search & Filter Controls:
- **Search Boxes**: Ticker Symbol, Instrument Name, ISIN, CUSIP, FIGI
- **Dropdown Slicers**: Country, Exchange Venue, Asset Class, Sector, Industry

### Result Grid Layout:
A master-detail view displaying:
- **Canonical Security Details**: `instrument_id`, `symbol`, `instrument_name`, `asset_class`, `country`, `currency`.
- **Linked Identifiers Card**: All associated ISIN, CUSIP, FIGI, Composite FIGI, Share Class FIGI codes.
- **Linked Venue Listings Table**: List of all global exchanges where the security trades, indicating `is_primary` venue.
- **Data Quality Scorecard**: Breakdown of component scores for the selected security.
