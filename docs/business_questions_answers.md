# Advanced Business Questions & SQL Query Reference

## Executive Summary

Section 23 of the **Financial Universe Control Tower** specification PDF defines 18 advanced business questions required to demonstrate platform analytical capabilities. This document provides complete technical answers, business justification, and production-ready ANSI SQL queries for all 18 questions against Unity Catalog (`azurelearn`).

---

## Category 1: Financial Universe Analytics

### Question 1: How many unique instruments currently exist?
- **Business Value**: Provides the exact count of distinct global financial securities, avoiding venue-listing inflation.
```sql
SELECT COUNT(DISTINCT instrument_id) AS total_unique_instruments
FROM azurelearn.gold.gold_security_master
WHERE snapshot_date = CURRENT_DATE();
```

### Question 2: How many unique entities exist?
- **Business Value**: Identifies the true number of corporate issuers / parent entities in the master data universe.
```sql
SELECT COUNT(DISTINCT entity_id) AS total_unique_entities
FROM azurelearn.silver.dim_entity;
```

### Question 3: How many exchange listings exist?
- **Business Value**: Measures total venue trading listings across all global exchanges.
```sql
SELECT COUNT(DISTINCT listing_id) AS total_exchange_listings
FROM azurelearn.silver.fact_listing
WHERE is_current = TRUE;
```

### Question 4: Which exchanges have the largest instrument universe?
- **Business Value**: Ranks exchange venues by liquidity and security coverage for market connectivity planning.
```sql
SELECT 
    exchange,
    COUNT(DISTINCT instrument_id) AS instrument_count,
    DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT instrument_id) DESC) AS exchange_rank
FROM azurelearn.gold.gold_security_master
WHERE snapshot_date = CURRENT_DATE() AND exchange IS NOT NULL
GROUP BY exchange
ORDER BY instrument_count DESC
LIMIT 10;
```

### Question 5: Which countries have the largest number of financial instruments?
- **Business Value**: Evaluates geographical distribution of financial instruments for country risk exposure analysis.
```sql
SELECT 
    country,
    COUNT(DISTINCT instrument_id) AS instrument_count,
    ROUND(100.0 * COUNT(DISTINCT instrument_id) / SUM(COUNT(DISTINCT instrument_id)) OVER(), 2) AS pct_share
FROM azurelearn.gold.gold_security_master
WHERE snapshot_date = CURRENT_DATE() AND country IS NOT NULL
GROUP BY country
ORDER BY instrument_count DESC
LIMIT 10;
```

---

## Category 2: Change Intelligence Analytics

### Question 6: How many new instruments appeared this period?
- **Business Value**: Tracks new IPOs, listings, and additions to the universe between consecutive snapshots.
```sql
SELECT COUNT(DISTINCT listing_id) AS new_instruments_count
FROM azurelearn.gold.gold_instrument_changes
WHERE change_date = CURRENT_DATE() AND change_type = 'NEW';
```

### Question 7: Which instruments were removed?
- **Business Value**: Identifies delisted, expired, or merged financial securities.
```sql
SELECT 
    symbol,
    instrument_id,
    listing_id,
    old_value AS removed_instrument_details
FROM azurelearn.gold.gold_instrument_changes
WHERE change_date = CURRENT_DATE() AND change_type = 'REMOVED';
```

### Question 8: Which instruments changed classification?
- **Business Value**: Monitors industry sector reclassifications (e.g. GICS changes) impacting sector-allocated portfolios.
```sql
SELECT 
    symbol,
    instrument_id,
    column_changed,
    old_value AS previous_classification,
    new_value AS updated_classification
FROM azurelearn.gold.gold_instrument_changes
WHERE change_date = CURRENT_DATE() AND change_type = 'RECLASSIFIED';
```

### Question 9: Which exchanges experienced the largest changes?
- **Business Value**: Detects market volatility or venue restructuring by measuring daily change volume per exchange.
```sql
SELECT 
    fl.exchange,
    COUNT(ic.change_id) AS total_changes,
    COUNT(CASE WHEN ic.change_type = 'NEW' THEN 1 END) AS additions,
    COUNT(CASE WHEN ic.change_type = 'REMOVED' THEN 1 END) AS removals,
    COUNT(CASE WHEN ic.change_type = 'RECLASSIFIED' THEN 1 END) AS reclassifications
FROM azurelearn.gold.gold_instrument_changes ic
JOIN azurelearn.silver.fact_listing fl ON ic.listing_id = fl.listing_id
WHERE ic.change_date = CURRENT_DATE()
GROUP BY fl.exchange
ORDER BY total_changes DESC;
```

### Question 10: Which identifiers changed between snapshots?
- **Business Value**: Isolates ISIN/CUSIP updates preventing silent breaks in downstream data joins and trade matching.
```sql
SELECT 
    symbol,
    instrument_id,
    column_changed AS identifier_type,
    old_value AS previous_identifier,
    new_value AS updated_identifier
FROM azurelearn.gold.gold_instrument_changes
WHERE change_date = CURRENT_DATE() AND change_type = 'IDENTIFIER_CHANGED';
```

---

## Category 3: Data Quality Analytics

### Question 11: Which asset class has the highest data-quality failure rate?
- **Business Value**: Directs data steward remediation efforts toward asset classes with the lowest data quality.
```sql
SELECT 
    asset_class,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN quality_band = 'QUARANTINE' THEN 1 END) AS quarantine_count,
    ROUND(100.0 * COUNT(CASE WHEN quality_band = 'QUARANTINE' THEN 1 END) / COUNT(*), 2) AS failure_rate_pct
FROM azurelearn.gold.gold_universe_summary
WHERE snapshot_date = CURRENT_DATE()
GROUP BY asset_class
ORDER BY failure_rate_pct DESC;
```

### Question 12: Which exchanges generate the most duplicate records?
- **Business Value**: Pinpoints data vendors or exchanges with noisy, multi-listed duplicate ticker issues.
```sql
SELECT 
    source_dataset AS exchange_or_dataset,
    COUNT(*) AS duplicate_record_count
FROM azurelearn.quarantine.rejected_records
WHERE failure_reason IN ('DUPLICATE_VENUE_LISTING', 'DUPLICATE_IDENTIFIER')
GROUP BY source_dataset
ORDER BY duplicate_record_count DESC;
```

### Question 13: Which identifiers have the highest conflict rate?
- **Business Value**: Identifies conflicting external identifiers (e.g. same FIGI assigned to multiple instruments).
```sql
SELECT 
    failure_reason,
    failed_column,
    COUNT(*) AS conflict_count
FROM azurelearn.quarantine.rejected_records
WHERE failure_reason LIKE '%CONFLICT%' OR failure_reason LIKE '%INVALID%'
GROUP BY failure_reason, failed_column
ORDER BY conflict_count DESC;
```

### Question 14: How many instruments require manual review?
- **Business Value**: Measures the operational workload for data stewards in the `REVIEW` score band (75–89).
```sql
SELECT 
    COUNT(DISTINCT instrument_id) AS instruments_needing_review,
    AVG(quality_score) AS average_review_score
FROM azurelearn.gold.gold_security_master
WHERE snapshot_date = CURRENT_DATE() AND quality_band = 'REVIEW';
```

---

## Category 4: Master Data Analytics

### Question 15: How many companies have multiple exchange listings?
- **Business Value**: Quantifies cross-border and multi-venue company listings.
```sql
SELECT COUNT(*) AS multi_listed_companies_count
FROM (
    SELECT entity_id, COUNT(DISTINCT exchange) AS exchange_count
    FROM azurelearn.silver.fact_listing
    WHERE entity_id IS NOT NULL AND is_current = TRUE
    GROUP BY entity_id
    HAVING COUNT(DISTINCT exchange) > 1
);
```

### Question 16: Which instruments have multiple identifiers?
- **Business Value**: Validates mapping coverage across ISIN, CUSIP, FIGI, and Composite FIGI codes.
```sql
SELECT 
    instrument_id,
    COUNT(DISTINCT identifier_type) AS identifier_type_count,
    COLLECT_SET(identifier_type) AS available_types
FROM azurelearn.silver.dim_identifier
WHERE is_current = TRUE
GROUP BY instrument_id
HAVING COUNT(DISTINCT identifier_type) > 1
ORDER BY identifier_type_count DESC;
```

### Question 17: Which instruments cannot be mapped to a canonical entity?
- **Business Value**: Reports unmapped instruments (e.g. Indices, FX, Crypto, or unlinked OTC equities) requiring entity graph investigation.
```sql
SELECT 
    fl.symbol,
    fl.instrument_id,
    fl.exchange,
    sm.asset_class
FROM azurelearn.silver.fact_listing fl
JOIN azurelearn.gold.gold_security_master sm ON fl.instrument_id = sm.instrument_id
WHERE fl.entity_id IS NULL AND fl.is_current = TRUE;
```

### Question 18: What was an instrument's classification at a historical point in time?
- **Business Value**: Executes point-in-time time-travel queries for auditability and backtesting.
```sql
-- Query classification of Instrument 'inst_037833100' on historical date '2025-06-15'
SELECT 
    instrument_id,
    sector,
    industry_group,
    industry,
    effective_from,
    effective_to
FROM azurelearn.silver.dim_classification
WHERE instrument_id = 'inst_037833100'
  AND '2025-06-15' BETWEEN effective_from AND effective_to;
```
