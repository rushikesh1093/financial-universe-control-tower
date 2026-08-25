-- =====================================================================
-- Financial Universe Control Tower - the 18 business questions (PDF §23)
--
-- Run these in the Databricks SQL Editor against the `fuct-sql` warehouse.
-- Everything below reads the gold Delta tables and views in Unity Catalog;
-- there is no separate serving database.
--
--   Catalog: azurelearn      Schemas: gold, silver, quarantine, audit
-- =====================================================================


-- ---------------------------------------------------------------------
-- FINANCIAL UNIVERSE
-- ---------------------------------------------------------------------

-- Q1. How many unique instruments currently exist?
-- Counts the security, not the ticker: cross-listings collapsed by ISIN.
SELECT COUNT(DISTINCT instrument_id) AS unique_instruments
FROM   azurelearn.gold.gold_security_master;


-- Q2. How many unique entities (issuers) exist?
-- NULL entity_id is deliberate for indices, FX and crypto - they have no issuer.
SELECT COUNT(DISTINCT entity_id) AS unique_entities
FROM   azurelearn.gold.gold_security_master
WHERE  entity_id IS NOT NULL;


-- Q3. How many exchange listings exist?
SELECT COUNT(*) AS listings
FROM   azurelearn.gold.fact_listing
WHERE  is_current;


-- Q4. Which exchanges have the largest instrument universe?
SELECT l.exchange_code,
       e.exchange_name,
       e.country,
       e.venue_type,
       COUNT(DISTINCT l.instrument_id) AS instruments,
       COUNT(*)                        AS listings
FROM   azurelearn.gold.fact_listing l
LEFT   JOIN azurelearn.gold.dim_exchange e ON e.exchange_code = l.exchange_code
WHERE  l.is_current
GROUP  BY l.exchange_code, e.exchange_name, e.country, e.venue_type
ORDER  BY instruments DESC
LIMIT  25;


-- Q5. Which countries have the largest number of financial instruments?
SELECT COALESCE(country, '(not supplied)') AS country,
       COUNT(*)                            AS instruments,
       COUNT(DISTINCT entity_id)           AS entities,
       ROUND(AVG(quality_score), 2)        AS avg_quality_score
FROM   azurelearn.gold.gold_security_master
GROUP  BY country
ORDER  BY instruments DESC
LIMIT  25;


-- ---------------------------------------------------------------------
-- CHANGE INTELLIGENCE
-- Needs two snapshots. On the first run every row is NEW by definition.
-- ---------------------------------------------------------------------

-- Q6. How many new instruments appeared this period?
SELECT change_date, COUNT(*) AS new_instruments
FROM   azurelearn.gold.gold_instrument_changes
WHERE  change_type = 'NEW'
GROUP  BY change_date
ORDER  BY change_date DESC;


-- Q7. Which instruments were removed?
SELECT change_date, symbol, exchange, asset_class, old_value AS last_known_name
FROM   azurelearn.gold.gold_instrument_changes
WHERE  change_type = 'REMOVED'
ORDER  BY change_date DESC
LIMIT  100;


-- Q8. Which instruments changed classification?
SELECT change_date, symbol, exchange, instrument_name,
       column_changed, old_value, new_value
FROM   azurelearn.gold.gold_instrument_changes
WHERE  change_type = 'RECLASSIFIED'
ORDER  BY change_date DESC
LIMIT  100;


-- Q9. Which exchanges experienced the largest changes?
SELECT exchange,
       COUNT(*)                                                  AS total_changes,
       SUM(CASE WHEN change_type = 'NEW'                THEN 1 ELSE 0 END) AS new_,
       SUM(CASE WHEN change_type = 'REMOVED'            THEN 1 ELSE 0 END) AS removed,
       SUM(CASE WHEN change_type = 'MODIFIED'           THEN 1 ELSE 0 END) AS modified,
       SUM(CASE WHEN change_type = 'RECLASSIFIED'       THEN 1 ELSE 0 END) AS reclassified,
       SUM(CASE WHEN change_type = 'IDENTIFIER_CHANGED' THEN 1 ELSE 0 END) AS identifier_changed
FROM   azurelearn.gold.gold_instrument_changes
GROUP  BY exchange
ORDER  BY total_changes DESC
LIMIT  25;


-- Q10. Which identifiers changed between snapshots?
SELECT change_date, symbol, exchange, instrument_name,
       column_changed AS identifier_type, old_value, new_value
FROM   azurelearn.gold.gold_instrument_changes
WHERE  change_type = 'IDENTIFIER_CHANGED'
ORDER  BY change_date DESC
LIMIT  100;


-- ---------------------------------------------------------------------
-- DATA QUALITY
-- ---------------------------------------------------------------------

-- Q11. Which asset class has the highest data-quality failure rate?
SELECT asset_class,
       COUNT(*)                                                       AS records,
       SUM(CASE WHEN is_quarantined THEN 1 ELSE 0 END)                AS quarantined,
       ROUND(100.0 * SUM(CASE WHEN is_quarantined THEN 1 ELSE 0 END)
             / COUNT(*), 2)                                           AS quarantine_rate_pct,
       ROUND(AVG(quality_score), 2)                                   AS avg_quality_score,
       ROUND(AVG(dq_failure_count), 2)                                AS avg_failures_per_record
FROM   azurelearn.gold.gold_data_quality
GROUP  BY asset_class
ORDER  BY quarantine_rate_pct DESC;


-- Q12. Which exchanges generate the most duplicate records?
SELECT exchange,
       COUNT(*) AS duplicate_records
FROM   azurelearn.gold.gold_data_quality
WHERE  ARRAY_CONTAINS(dq_failures, 'DUPLICATE_INSTRUMENT')
GROUP  BY exchange
ORDER  BY duplicate_records DESC
LIMIT  25;


-- Q13. Which identifiers have the highest conflict rate?
-- A single identifier resolving to more than one canonical instrument. These
-- are flagged for review, never silently merged.
SELECT 'ISIN' AS identifier_type,
       COUNT(*) AS conflicting_records
FROM   azurelearn.gold.gold_data_quality
WHERE  isin_instrument_count > 1
UNION ALL
SELECT 'FIGI', COUNT(*)
FROM   azurelearn.gold.gold_data_quality
WHERE  figi_instrument_count > 1;


-- Q14. How many instruments require manual review?
SELECT quality_band,
       COUNT(*)                     AS instruments,
       ROUND(AVG(quality_score), 2) AS avg_score
FROM   azurelearn.gold.gold_security_master
GROUP  BY quality_band
ORDER  BY avg_score DESC;


-- ---------------------------------------------------------------------
-- MASTER DATA
-- ---------------------------------------------------------------------

-- Q15. How many companies have multiple exchange listings?
-- This is the question that justifies separating entity from listing.
SELECT COUNT(*) AS entities_with_multiple_venues
FROM   azurelearn.gold.vw_entity_listings
WHERE  venue_count > 1;

-- ...and the companies themselves:
SELECT entity_name, entity_country, venue_count, listing_count, venues
FROM   azurelearn.gold.vw_entity_listings
WHERE  venue_count > 1
ORDER  BY venue_count DESC, entity_name
LIMIT  50;


-- Q16. Which instruments have multiple identifiers?
-- The base table calls this column `instrument_type`. Only the
-- vw_security_master view exposes it as `asset_class`.
SELECT symbol, instrument_name, instrument_type AS asset_class,
       identifier_type_count,
       isin, cusip, figi, composite_figi, shareclass_figi
FROM   azurelearn.gold.gold_security_master
WHERE  identifier_type_count > 1
ORDER  BY identifier_type_count DESC, symbol
LIMIT  50;


-- Q17. Which instruments cannot be mapped to a canonical entity?
-- Expected to be dominated by indices, FX pairs and crypto pairs: they have no
-- issuer, so a NULL entity is correct rather than a failure.
SELECT instrument_type AS asset_class,
       COUNT(*)        AS unmapped_instruments
FROM   azurelearn.gold.gold_security_master
WHERE  entity_id IS NULL
GROUP  BY instrument_type
ORDER  BY unmapped_instruments DESC;


-- Q18. What was an instrument's classification at a historical point in time?
-- SCD Type 2 point-in-time lookup. Change the date to travel through history.
SELECT symbol,
       instrument_name,
       sector,
       industry_group,
       industry,
       effective_from,
       effective_to,
       is_current
FROM   azurelearn.gold.vw_classification_history
WHERE  DATE('2026-08-22') BETWEEN effective_from AND effective_to
  AND  symbol = 'AA'
ORDER  BY effective_from;


-- ---------------------------------------------------------------------
-- BONUS: pipeline observability (PDF §24)
-- ---------------------------------------------------------------------

-- Every stage of every run: timings, volumes, rejects, status.
SELECT pipeline_run_id, stage, status, duration_seconds,
       records_read, records_written, records_rejected, quality_score
FROM   azurelearn.gold.vw_pipeline_runs
ORDER  BY start_time DESC
LIMIT  50;


-- Delta time travel: the same table as it stood at an earlier version.
-- DESCRIBE HISTORY azurelearn.gold.gold_security_master;
-- SELECT COUNT(*) FROM azurelearn.gold.gold_security_master VERSION AS OF 0;
