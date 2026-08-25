# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Schema standardisation
# MAGIC
# MAGIC Projects seven source layouts onto one canonical staging schema and derives
# MAGIC the keys the master-data layer depends on.
# MAGIC
# MAGIC ## Grain
# MAGIC
# MAGIC | Key | Grain | Derivation |
# MAGIC |---|---|---|
# MAGIC | `listing_id` | one per source row - a security *on a venue* | `sha2(asset_class, symbol, exchange)` |
# MAGIC | `instrument_id` | the security itself | `sha2('ISIN:' + isin)` when a valid ISIN exists, else `listing_id` |
# MAGIC | `entity_id` | the issuer | resolved in notebook 04 |
# MAGIC
# MAGIC ISIN is what collapses cross-listings into one instrument. In this snapshot
# MAGIC 4,006 of 7,025 distinct ISINs appear on more than one venue - for example
# MAGIC `IE00B5KQNG97` trades on BER, GER, FRA and LSE. Keying instruments on
# MAGIC symbol alone would report those as five separate securities.
# MAGIC
# MAGIC Keys are content hashes, not sequences, so re-running a snapshot reproduces
# MAGIC them exactly and the downstream MERGE updates instead of duplicating.

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

from pyspark.sql import functions as F  # noqa: E402

from fuct import audit, config as cfg, reference as ref, schemas, transforms as tf, writer  # noqa: E402

# COMMAND ----------

settings = cfg.resolve(dbutils)
print(settings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union the snapshot onto the canonical schema

# COMMAND ----------

control = (
    spark.table(settings.audit("ingestion_control").replace("`", ""))
    .where(F.col("is_active"))
    .collect()
)

canonical = None
for entry in control:
    dataset = entry["dataset"]
    table = settings.bronze(dataset).replace("`", "")
    if not spark.catalog.tableExists(table):
        print("  skipping {0} - not yet ingested".format(dataset))
        continue

    snapshot = spark.table(table).where(
        F.col("snapshot_date") == F.to_date(F.lit(settings.snapshot_date))
    )
    asset_class = snapshot.select("asset_class").first()
    if asset_class is None:
        print("  skipping {0} - no rows for {1}".format(dataset, settings.snapshot_date))
        continue

    projected = schemas.to_canonical(snapshot, asset_class["asset_class"])
    canonical = projected if canonical is None else canonical.unionByName(projected)

if canonical is None:
    raise RuntimeError(
        "No bronze data for snapshot {0}. Run 02_bronze_ingestion first.".format(
            settings.snapshot_date
        )
    )

print("canonical columns:", len(canonical.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalise values
# MAGIC
# MAGIC Currency aliases are folded first (`GBp` is pence sterling, not a
# MAGIC mis-cased `GBP`, so upper-casing alone would silently change the unit).
# MAGIC Identifiers are stripped of punctuation and blanked when too short to be
# MAGIC real, which keeps a junk value from becoming a join key.

# COMMAND ----------

currency_alias = F.col("currency")
for source_code, target_code in ref.CURRENCY_ALIASES.items():
    currency_alias = F.when(
        F.col("currency") == F.lit(source_code), F.lit(target_code)
    ).otherwise(currency_alias)

# An FX pair has no single trading currency - `currencies.csv` has no `currency`
# column at all, only base/quote. Its price *is* expressed in the quote currency
# (USD/AED is quoted in AED), so that is the honest value to carry. Without this
# every FX row scores zero on currency validity, which is 45% of the CURRENCY
# profile, and the entire asset class lands in quarantine for an attribute the
# source never claimed to provide.
canonical = canonical.withColumn(
    "currency",
    F.when(
        (F.col("asset_class") == F.lit(cfg.CURRENCY)) & F.col("currency").isNull(),
        F.col("quote_currency"),
    ).otherwise(F.col("currency")),
)

normalised = (
    canonical.withColumn("symbol", F.upper(F.trim(F.col("symbol"))))
    .withColumn("exchange", F.upper(F.trim(F.col("exchange"))))
    .withColumn("mic", F.upper(F.trim(F.col("mic"))))
    .withColumn("currency_raw", F.col("currency"))
    .withColumn(
        "currency",
        F.when(F.col("currency").isNull(), F.lit(None)).otherwise(
            F.upper(currency_alias)
        ),
    )
    .withColumn("normalised_name", tf.normalise_name(F.col("instrument_name")))
    .withColumn("symbol_root", tf.symbol_root(F.col("symbol")))
    .withColumn("isin", tf.clean_identifier(F.col("isin")))
    .withColumn("cusip", tf.clean_identifier(F.col("cusip")))
    .withColumn("figi", tf.clean_identifier(F.col("figi")))
    .withColumn("composite_figi", tf.clean_identifier(F.col("composite_figi")))
    .withColumn("shareclass_figi", tf.clean_identifier(F.col("shareclass_figi")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive keys
# MAGIC
# MAGIC An ISIN is only trusted as an instrument key when it is *well formed*. A
# MAGIC malformed ISIN becomes its own instrument rather than silently merging
# MAGIC unrelated securities under a corrupt key.

# COMMAND ----------

keyed = (
    normalised.withColumn(
        "listing_id",
        tf.surrogate_key(F.col("asset_class"), F.col("symbol"), F.col("exchange")),
    )
    .withColumn(
        "isin_is_valid",
        F.col("isin").isNotNull() & tf.is_valid_isin(F.col("isin")),
    )
    .withColumn(
        "instrument_id",
        F.when(
            F.col("isin_is_valid"),
            tf.surrogate_key(F.lit("ISIN"), F.col("isin")),
        ).otherwise(F.col("listing_id")),
    )
    .withColumn(
        "instrument_key_source",
        F.when(F.col("isin_is_valid"), F.lit("ISIN")).otherwise(
            F.lit("SYMBOL_EXCHANGE")
        ),
    )
    .withColumn("status", F.when(F.col("delisted") == True, F.lit("DELISTED")).otherwise(F.lit("ACTIVE")))  # noqa: E712
    .withColumn("snapshot_date", F.to_date(F.lit(settings.snapshot_date)))
    .withColumn("pipeline_run_id", F.lit(settings.pipeline_run_id))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist the standardised snapshot

# COMMAND ----------

target = settings.silver("instrument_staging")
location = settings.silver_location("instrument_staging")

with audit.StageRun(spark, settings, "03_schema_standardization", target) as run:
    row_count = keyed.count()
    run.records_read = row_count

    writer.save(
        spark,
        keyed,
        target,
        location=location,
        partition_by=["snapshot_date"],
        replace_where="snapshot_date = '{0}'".format(settings.snapshot_date),
    )
    run.records_written = row_count

print("standardised {0:,} rows -> {1}".format(row_count, target))
print("stored at   :", writer.describe_location(spark, target))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the grain collapse

# COMMAND ----------

grain = keyed.agg(
    F.count(F.lit(1)).alias("source_rows"),
    F.countDistinct("listing_id").alias("distinct_listings"),
    F.countDistinct("instrument_id").alias("distinct_instruments"),
    F.countDistinct("isin").alias("distinct_isins"),
)
display(grain)

# COMMAND ----------

display(
    keyed.groupBy("asset_class")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("listing_id").alias("listings"),
        F.countDistinct("instrument_id").alias("instruments"),
        F.countDistinct("exchange").alias("venues"),
        F.round(
            100.0 * F.sum(F.col("isin").isNotNull().cast("int")) / F.count(F.lit(1)), 1
        ).alias("pct_with_isin"),
    )
    .orderBy(F.desc("rows"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-listed instruments
# MAGIC
# MAGIC The securities that appear on more than one venue - the case the canonical
# MAGIC model exists to handle.

# COMMAND ----------

cross_listed = (
    keyed.groupBy("instrument_id")
    .agg(
        F.countDistinct("exchange").alias("venue_count"),
        F.collect_set("exchange").alias("venues"),
        F.first("instrument_name").alias("example_name"),
        F.first("isin").alias("isin"),
    )
    .where(F.col("venue_count") > 1)
)
print("instruments listed on more than one venue:", cross_listed.count())
display(cross_listed.orderBy(F.desc("venue_count")).limit(25))

# COMMAND ----------

