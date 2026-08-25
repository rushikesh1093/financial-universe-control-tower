# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - SQL serving views
# MAGIC
# MAGIC Publishes business-facing SQL views over the gold layer so analysts query
# MAGIC `azurelearn.gold.vw_*` instead of joining fact and dimension tables by hand.
# MAGIC
# MAGIC These are Delta-backed views in Unity Catalog, so they work from the
# MAGIC Databricks SQL Editor, a SQL warehouse, Power BI, or any ODBC/JDBC client -
# MAGIC no separate serving database is involved.
# MAGIC
# MAGIC The views map onto the reporting surfaces the brief asks for: universe
# MAGIC overview, change monitor, classification drill-down, data-quality control
# MAGIC tower, and the security-master explorer.

# COMMAND ----------

import os
import sys


def _add_utilities_to_path() -> str:
    candidates = []
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        nb_dir = os.path.dirname(ctx.notebookPath().get())
        candidates.append("/Workspace" + os.path.dirname(nb_dir) + "/utilities")
    except Exception:
        pass
    candidates.append(os.path.abspath(os.path.join(os.getcwd(), "..", "utilities")))
    for candidate in candidates:
        if os.path.isdir(candidate):
            if candidate not in sys.path:
                sys.path.insert(0, candidate)
            return candidate
    raise RuntimeError("Could not locate the 'fuct' utilities package: " + repr(candidates))


_add_utilities_to_path()

from fuct import config as cfg  # noqa: E402

settings = cfg.resolve(dbutils)
CAT = settings.catalog
print("publishing views into {0}.gold".format(CAT))

# COMMAND ----------

VIEWS = {}

# --- Page 1: Universe Overview -------------------------------------------
VIEWS["vw_universe_overview"] = """
SELECT
    instrument_type              AS asset_class,
    country,
    primary_exchange             AS exchange,
    currency,
    quality_band,
    status,
    COUNT(*)                     AS instrument_count,
    COUNT(DISTINCT entity_id)    AS entity_count,
    SUM(listing_count)           AS listing_count,
    ROUND(AVG(quality_score), 2) AS avg_quality_score
FROM {cat}.gold.gold_security_master
GROUP BY instrument_type, country, primary_exchange, currency, quality_band, status
"""

# --- Page 5: Security Master Explorer ------------------------------------
VIEWS["vw_security_master"] = """
SELECT
    instrument_id,
    symbol,
    instrument_name,
    instrument_type      AS asset_class,
    entity_id,
    entity_name,
    entity_type,
    primary_exchange,
    country,
    currency,
    status,
    isin, cusip, figi, composite_figi, shareclass_figi,
    identifier_type_count,
    sector, industry_group, industry,
    category_group, category, family,
    listing_count,
    venue_count,
    venues,
    market_cap,
    quality_score,
    quality_band,
    dq_failures,
    entity_resolution_method,
    instrument_key_source,
    snapshot_date
FROM {cat}.gold.gold_security_master
"""

# --- Entity / listing relationships --------------------------------------
# The question the canonical model exists to answer: which companies trade in
# more than one place, and where.
VIEWS["vw_entity_listings"] = """
SELECT
    e.entity_id,
    e.entity_name,
    e.country                       AS entity_country,
    e.entity_type,
    COUNT(DISTINCT l.listing_id)    AS listing_count,
    COUNT(DISTINCT l.exchange_code) AS venue_count,
    SORT_ARRAY(COLLECT_SET(l.exchange_code)) AS venues,
    SORT_ARRAY(COLLECT_SET(l.country_name))  AS venue_countries,
    MAX(CASE WHEN l.is_primary THEN l.exchange_code END) AS primary_exchange
FROM {cat}.gold.dim_entity e
JOIN {cat}.gold.fact_listing l
  ON l.entity_id = e.entity_id
WHERE l.is_current
GROUP BY e.entity_id, e.entity_name, e.country, e.entity_type
"""

# --- Page 2: Change Monitor ----------------------------------------------
VIEWS["vw_instrument_changes"] = """
SELECT
    c.change_id,
    c.change_date,
    c.change_type,
    c.column_changed,
    c.old_value,
    c.new_value,
    c.asset_class,
    c.symbol,
    c.exchange,
    c.instrument_id,
    c.instrument_name,
    c.entity_name,
    c.source_snapshot,
    c.previous_snapshot,
    c.pipeline_run_id
FROM {cat}.gold.gold_instrument_changes c
"""

VIEWS["vw_change_summary"] = """
SELECT
    change_date,
    change_type,
    asset_class,
    exchange,
    COUNT(*)                        AS change_count,
    COUNT(DISTINCT instrument_id)   AS instruments_affected
FROM {cat}.gold.gold_instrument_changes
GROUP BY change_date, change_type, asset_class, exchange
"""

# --- Page 3: Classification Intelligence ---------------------------------
VIEWS["vw_classification_hierarchy"] = """
SELECT
    COALESCE(sector, '(unclassified)')         AS sector,
    COALESCE(industry_group, '(unclassified)') AS industry_group,
    COALESCE(industry, '(unclassified)')       AS industry,
    COALESCE(country, '(not supplied)')        AS country,
    COALESCE(primary_exchange, '(none)')       AS exchange,
    instrument_type                            AS asset_class,
    COUNT(*)                                   AS instrument_count,
    COUNT(DISTINCT entity_id)                  AS entity_count,
    ROUND(AVG(quality_score), 2)               AS avg_quality_score
FROM {cat}.gold.gold_security_master
GROUP BY sector, industry_group, industry, country, primary_exchange, instrument_type
"""

# --- Page 4: Data Quality Control Tower ----------------------------------
VIEWS["vw_data_quality"] = """
SELECT
    q.listing_id,
    q.instrument_id,
    q.asset_class,
    q.symbol,
    q.exchange,
    q.instrument_name,
    q.quality_score,
    q.quality_band,
    q.score_identifier_completeness,
    q.score_classification_completeness,
    q.score_exchange_validity,
    q.score_country_validity,
    q.score_currency_validity,
    q.score_duplicate_risk,
    q.dq_failures,
    q.dq_failure_count,
    q.is_quarantined,
    q.snapshot_date
FROM {cat}.gold.gold_data_quality q
"""

# One row per (record, failed rule), joined to the rule catalogue so the
# dashboard can explain *why* something failed rather than showing a code.
# The explode happens in a subquery rather than as a LATERAL VIEW: Spark SQL
# does not accept a JOIN after a LATERAL VIEW clause.
VIEWS["vw_quality_failures"] = """
SELECT
    x.snapshot_date,
    x.asset_class,
    x.exchange,
    x.symbol,
    x.instrument_name,
    x.quality_score,
    x.quality_band,
    x.rule_code,
    r.dimension,
    r.severity,
    r.target_column,
    r.description
FROM (
    SELECT
        q.snapshot_date,
        q.asset_class,
        q.exchange,
        q.symbol,
        q.instrument_name,
        q.quality_score,
        q.quality_band,
        EXPLODE(q.dq_failures) AS rule_code
    FROM {cat}.gold.gold_data_quality q
) x
LEFT JOIN {cat}.audit.dq_rule_catalogue r
  ON r.rule_code = x.rule_code
"""

VIEWS["vw_quarantine"] = """
SELECT
    record_id,
    source_dataset,
    source_file,
    source_snapshot,
    asset_class,
    failure_reason,
    failed_column,
    original_record,
    pipeline_run_id,
    quarantine_timestamp
FROM {cat}.quarantine.rejected_records
"""

# --- Identifier mapping ---------------------------------------------------
VIEWS["vw_identifier_map"] = """
SELECT
    i.instrument_id,
    m.symbol,
    m.instrument_name,
    i.identifier_type,
    i.identifier_value,
    i.is_primary,
    i.is_valid_format,
    i.effective_from,
    i.effective_to,
    i.is_current
FROM {cat}.gold.dim_identifier i
LEFT JOIN {cat}.gold.gold_security_master m
  ON m.instrument_id = i.instrument_id
"""

# --- SCD2: classification as at any historical date ----------------------
# The point-in-time question: "what was this instrument's classification on
# date X?" Callers filter on `AS OF` between effective_from and effective_to.
VIEWS["vw_classification_history"] = """
SELECT
    c.instrument_id,
    m.symbol,
    m.instrument_name,
    c.sector,
    c.industry_group,
    c.industry,
    c.category_group,
    c.category,
    c.family,
    c.effective_from,
    c.effective_to,
    c.is_current
FROM {cat}.silver.dim_classification c
LEFT JOIN {cat}.gold.gold_security_master m
  ON m.instrument_id = c.instrument_id
"""

# --- Pipeline observability ----------------------------------------------
VIEWS["vw_pipeline_runs"] = """
SELECT
    pipeline_run_id,
    stage,
    snapshot_date,
    status,
    start_time,
    end_time,
    duration_seconds,
    records_read,
    records_written,
    records_rejected,
    records_modified,
    quality_score,
    target_table,
    error_message
FROM {cat}.audit.pipeline_run_log
"""

# COMMAND ----------

created = []
for name, body in VIEWS.items():
    fq = "{0}.gold.{1}".format(CAT, name)
    spark.sql(
        "CREATE OR REPLACE VIEW {0} AS {1}".format(fq, body.format(cat=CAT))
    )
    n = spark.sql("SELECT COUNT(*) AS n FROM {0}".format(fq)).first()["n"]
    created.append((name, n))
    print("  {0:<32} {1:>10,} rows".format(name, n))

# COMMAND ----------

display(spark.createDataFrame(created, "view_name string, rows long"))
print("\n{0} views published into {1}.gold".format(len(created), CAT))
