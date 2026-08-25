# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Gold layer and star schema
# MAGIC
# MAGIC Builds the business-ready datasets and the conformed star schema that
# MAGIC downstream SQL and BI consume. These are Delta tables in Unity Catalog,
# MAGIC so they are queryable directly with SQL - no separate serving layer.
# MAGIC
# MAGIC | Gold table | Contents |
# MAGIC |---|---|
# MAGIC | `gold_security_master` | current trusted version of every instrument |
# MAGIC | `gold_instrument_changes` | detected changes between snapshots |
# MAGIC | `gold_data_quality` | scores, bands and failure detail |
# MAGIC | `gold_universe_summary` | counts by asset class, country, exchange, sector, industry, currency |
# MAGIC
# MAGIC Star schema: `DimDate`, `DimCountry`, `DimExchange`, `DimCurrency`,
# MAGIC `DimAssetType`, `DimSector`, `DimIndustry`, `DimInstrument`, `DimEntity`,
# MAGIC `DimIdentifier` around `FactListing`.
# MAGIC
# MAGIC Primary keys are the deterministic content hashes produced upstream, so the
# MAGIC same snapshot always yields the same keys and gold can be rebuilt without
# MAGIC renumbering anything.

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

from fuct import audit, config as cfg, transforms as tf, writer  # noqa: E402

# COMMAND ----------

settings = cfg.resolve(dbutils)
snapshot = F.to_date(F.lit(settings.snapshot_date))
print(settings)


def s(name):
    return settings.silver(name).replace("`", "")


def g(name):
    return settings.gold(name).replace("`", "")


def overwrite(df, table, partition_by=None):
    """Publish a gold table, honouring `gold_root` when one is configured."""
    # `table` arrives already unquoted from g(); recover the bare name so the
    # external location is one folder per table.
    name = table.split(".")[-1]
    return writer.save(
        spark,
        df,
        table,
        location=settings.gold_location(name),
        partition_by=[partition_by] if isinstance(partition_by, str) else partition_by,
    )


# COMMAND ----------

instruments = spark.table(s("dim_instrument"))
entities = spark.table(s("dim_entity"))
listings = spark.table(s("fact_listing")).where(F.col("is_current"))
identifiers = spark.table(s("dim_identifier")).where(F.col("is_current"))
classifications = spark.table(s("dim_classification")).where(F.col("is_current"))
quality = spark.table(s("instrument_quality")).where(F.col("snapshot_date") == snapshot)
exchange_ref = spark.table(s("ref_exchange"))
country_ref = spark.table(s("ref_country"))
currency_ref = spark.table(s("ref_currency"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_security_master
# MAGIC
# MAGIC The current trusted view of each instrument, with its resolved entity, its
# MAGIC classification, its identifiers collapsed into one row, and its listing
# MAGIC footprint. Quality is carried at instrument grain by taking the best score
# MAGIC across that instrument's listings - one bad cross-listing should not
# MAGIC condemn a security that is clean on its home market.

# COMMAND ----------

instrument_quality = quality.groupBy("instrument_id").agg(
    F.round(F.max("quality_score"), 2).alias("quality_score"),
    F.round(F.avg("quality_score"), 2).alias("avg_listing_quality_score"),
    F.min(F.col("is_quarantined").cast("int")).alias("_min_quarantined"),
    F.flatten(F.collect_set("dq_failures")).alias("dq_failures"),
).withColumn("is_quarantined", F.col("_min_quarantined") == 1).drop("_min_quarantined")

instrument_quality = instrument_quality.withColumn(
    "quality_band",
    F.when(F.col("quality_score") >= 90, F.lit("TRUSTED"))
    .when(F.col("quality_score") >= 75, F.lit("REVIEW"))
    .otherwise(F.lit("QUARANTINE")),
).withColumn("dq_failures", F.array_distinct(F.col("dq_failures")))

listing_rollup = listings.groupBy("instrument_id").agg(
    F.countDistinct("listing_id").alias("listing_count"),
    F.countDistinct("exchange").alias("venue_count"),
    F.sort_array(F.collect_set("exchange")).alias("venues"),
    # Prefixed because dim_instrument also carries `primary_exchange`; an
    # unqualified reference after the join would be ambiguous.
    F.max(F.when(F.col("is_primary"), F.col("exchange"))).alias("listing_primary_exchange"),
    F.max(F.when(F.col("is_primary"), F.col("symbol"))).alias("listing_primary_symbol"),
    F.max(F.when(F.col("is_primary"), F.col("venue_country"))).alias("primary_venue_country"),
)

# Prefixed: dim_instrument carries its own denormalised isin/cusip/figi, so
# unprefixed names would be ambiguous after the join. The SCD2-backed
# dim_identifier rollup is the authoritative one and wins in the select below.
identifier_rollup = identifiers.groupBy("instrument_id").agg(
    F.max(F.when(F.col("identifier_type") == "ISIN", F.col("identifier_value"))).alias("idr_isin"),
    F.max(F.when(F.col("identifier_type") == "CUSIP", F.col("identifier_value"))).alias("idr_cusip"),
    F.max(F.when(F.col("identifier_type") == "FIGI", F.col("identifier_value"))).alias("idr_figi"),
    F.max(
        F.when(F.col("identifier_type") == "COMPOSITE_FIGI", F.col("identifier_value"))
    ).alias("idr_composite_figi"),
    F.max(
        F.when(F.col("identifier_type") == "SHARECLASS_FIGI", F.col("identifier_value"))
    ).alias("idr_shareclass_figi"),
    F.countDistinct("identifier_type").alias("idr_identifier_type_count"),
)

security_master = (
    instruments.alias("i")
    .join(entities.select("entity_id", "entity_name", F.col("country").alias("entity_country"),
                          "entity_type").alias("e"), "entity_id", "left")
    .join(
        classifications.select(
            "instrument_id", "sector", "industry_group", "industry",
            "category_group", "category", "family",
            F.col("effective_from").alias("classification_effective_from"),
        ).alias("c"),
        "instrument_id",
        "left",
    )
    .join(identifier_rollup, "instrument_id", "left")
    .join(listing_rollup, "instrument_id", "left")
    .join(instrument_quality, "instrument_id", "left")
    .select(
        F.col("i.instrument_id"),
        F.col("i.instrument_type"),
        F.coalesce(F.col("listing_primary_symbol"), F.col("i.symbol")).alias("symbol"),
        F.col("i.instrument_name"),
        F.col("i.currency"),
        F.coalesce(F.col("i.country"), F.col("entity_country")).alias("country"),
        F.col("i.status"),
        F.col("i.entity_id"),
        F.col("entity_name"),
        F.col("entity_type"),
        F.col("i.entity_resolution_method"),
        F.col("i.instrument_key_source"),
        F.coalesce(
            F.col("listing_primary_exchange"), F.col("i.primary_exchange")
        ).alias("primary_exchange"),
        F.col("primary_venue_country"),
        F.coalesce(F.col("listing_count"), F.lit(0)).alias("listing_count"),
        F.coalesce(F.col("venue_count"), F.lit(0)).alias("venue_count"),
        F.col("venues"),
        # From dim_classification (`c`), which is the SCD2 history table and the
        # authority on classification; dim_instrument carries a denormalised
        # copy of the same column names.
        F.col("c.sector").alias("sector"),
        F.col("c.industry_group").alias("industry_group"),
        F.col("c.industry").alias("industry"),
        F.col("c.category_group").alias("category_group"),
        F.col("c.category").alias("category"),
        F.col("c.family").alias("family"),
        F.col("classification_effective_from"),
        F.col("idr_isin").alias("isin"),
        F.col("idr_cusip").alias("cusip"),
        F.col("idr_figi").alias("figi"),
        F.col("idr_composite_figi").alias("composite_figi"),
        F.col("idr_shareclass_figi").alias("shareclass_figi"),
        F.coalesce(F.col("idr_identifier_type_count"), F.lit(0)).alias("identifier_type_count"),
        F.col("i.market_cap"), F.col("i.website"),
        F.coalesce(F.col("quality_score"), F.lit(0.0)).alias("quality_score"),
        F.coalesce(F.col("quality_band"), F.lit("QUARANTINE")).alias("quality_band"),
        F.coalesce(F.col("is_quarantined"), F.lit(False)).alias("is_quarantined"),
        F.col("dq_failures"),
        F.lit(settings.snapshot_date).alias("snapshot_date"),
        F.lit(settings.pipeline_run_id).alias("pipeline_run_id"),
    )
)

with audit.StageRun(spark, settings, "07_gold.security_master", g("gold_security_master")) as run:
    run.records_read = instruments.count()
    written = overwrite(security_master, g("gold_security_master"))
    run.records_written = written

print("gold_security_master:", written)
display(spark.table(g("gold_security_master")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_instrument_changes

# COMMAND ----------

changes_source = s("fact_instrument_changes")
if spark.catalog.tableExists(changes_source):
    changes = (
        spark.table(changes_source)
        .join(
            instruments.select("instrument_id", "instrument_name", "entity_id"),
            "instrument_id",
            "left",
        )
        .join(entities.select("entity_id", "entity_name"), "entity_id", "left")
    )
    n = overwrite(changes, g("gold_instrument_changes"), partition_by="change_date")
    print("gold_instrument_changes:", n)
    display(
        spark.table(g("gold_instrument_changes"))
        .groupBy("change_date", "change_type")
        .agg(F.count(F.lit(1)).alias("changes"))
        .orderBy(F.desc("change_date"), F.desc("changes"))
    )
else:
    print("No change table yet - run 06_change_detection first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_data_quality

# COMMAND ----------

data_quality = quality.join(
    instruments.select("instrument_id", "entity_id", F.col("country").alias("instrument_country")),
    "instrument_id",
    "left",
).withColumn("pipeline_run_id", F.lit(settings.pipeline_run_id))

n = overwrite(data_quality, g("gold_data_quality"), partition_by="snapshot_date")
print("gold_data_quality:", n)

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_universe_summary
# MAGIC
# MAGIC One long-format table rather than six wide ones, so Power BI can drive every
# MAGIC breakdown from a single measure and a slicer on `dimension`.

# COMMAND ----------

master = spark.table(g("gold_security_master"))

summary_dimensions = [
    ("ASSET_CLASS", F.col("instrument_type")),
    ("COUNTRY", F.col("country")),
    ("EXCHANGE", F.col("primary_exchange")),
    ("CURRENCY", F.col("currency")),
    ("SECTOR", F.col("sector")),
    ("INDUSTRY", F.col("industry")),
    ("CATEGORY", F.col("category")),
    ("QUALITY_BAND", F.col("quality_band")),
    ("STATUS", F.col("status")),
]

universe_summary = None
for name, column in summary_dimensions:
    part = (
        master.withColumn("dimension_value", F.coalesce(column, F.lit("(not supplied)")))
        .groupBy("dimension_value")
        .agg(
            F.count(F.lit(1)).alias("instrument_count"),
            F.countDistinct("entity_id").alias("entity_count"),
            F.sum("listing_count").alias("listing_count"),
            F.round(F.avg("quality_score"), 2).alias("avg_quality_score"),
            F.sum(F.when(F.col("quality_band") == "TRUSTED", 1).otherwise(0)).alias("trusted"),
            F.sum(F.when(F.col("quality_band") == "REVIEW", 1).otherwise(0)).alias("review"),
            F.sum(F.when(F.col("quality_band") == "QUARANTINE", 1).otherwise(0)).alias("quarantined"),
        )
        .withColumn("dimension", F.lit(name))
    )
    universe_summary = part if universe_summary is None else universe_summary.unionByName(part)

universe_summary = universe_summary.select(
    "dimension", "dimension_value", "instrument_count", "entity_count",
    "listing_count", "avg_quality_score", "trusted", "review", "quarantined",
).withColumn("snapshot_date", F.lit(settings.snapshot_date))

n = overwrite(universe_summary, g("gold_universe_summary"))
print("gold_universe_summary:", n)
display(
    spark.table(g("gold_universe_summary"))
    .where(F.col("dimension") == "ASSET_CLASS")
    .orderBy(F.desc("instrument_count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Star schema dimensions

# COMMAND ----------

# --- DimDate ---------------------------------------------------------------
# Spans every snapshot the platform holds, so point-in-time queries against the
# SCD2 tables always find a matching date row.
snapshot_dates = spark.table(s("instrument_staging")).select("snapshot_date").distinct()
bounds = snapshot_dates.agg(
    F.min("snapshot_date").alias("lo"), F.max("snapshot_date").alias("hi")
).first()

dim_date = (
    spark.sql(
        "SELECT explode(sequence(to_date('{0}'), to_date('{1}'), interval 1 day)) AS date".format(
            bounds["lo"], bounds["hi"]
        )
    )
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
    .select("date_key", "date", "year", "quarter", "month", "month_name",
            "day", "day_of_week", "day_name", "is_weekend")
)
print("DimDate:", overwrite(dim_date, g("dim_date")))

# COMMAND ----------

# --- DimCountry / DimExchange / DimCurrency / DimAssetType -----------------
# Reference-driven, but extended with any value the source actually used so the
# fact table's foreign keys always resolve. Values not in the reference are
# marked so the quality dashboard can surface them.

observed_countries = master.select(F.col("country").alias("country_name")).where(
    F.col("country").isNotNull()
).distinct()

dim_country = (
    country_ref.join(observed_countries, "country_name", "full_outer")
    .withColumn("is_in_reference", F.col("country_iso2").isNotNull())
    .withColumn("region", F.coalesce(F.col("region"), F.lit("Unclassified")))
    .select("country_name", "country_iso2", "region", "is_in_reference")
    .where(F.col("country_name").isNotNull())
)
print("DimCountry:", overwrite(dim_country, g("dim_country")))

observed_exchanges = (
    listings.select(F.col("exchange").alias("exchange_code"))
    .where(F.col("exchange").isNotNull())
    .distinct()
)
dim_exchange = (
    exchange_ref.join(observed_exchanges, "exchange_code", "full_outer")
    .withColumn("is_in_reference", F.col("exchange_name").isNotNull())
    .withColumn(
        "exchange_name", F.coalesce(F.col("exchange_name"), F.lit("(unknown venue)"))
    )
    .withColumn("venue_type", F.coalesce(F.col("venue_type"), F.lit("UNKNOWN")))
    .select("exchange_code", "exchange_name", "mic", "country", "currency",
            "venue_type", "primary_rank", "is_verified", "is_in_reference")
    .where(F.col("exchange_code").isNotNull())
)
print("DimExchange:", overwrite(dim_exchange, g("dim_exchange")))

observed_currencies = master.select(F.col("currency").alias("currency_code")).where(
    F.col("currency").isNotNull()
).distinct()
dim_currency = (
    currency_ref.join(observed_currencies, "currency_code", "full_outer")
    .withColumn("is_in_reference", F.col("currency_name").isNotNull())
    .withColumn(
        "currency_name", F.coalesce(F.col("currency_name"), F.lit("(unrecognised)"))
    )
    .withColumn("currency_kind", F.coalesce(F.col("currency_kind"), F.lit("UNKNOWN")))
    .select("currency_code", "currency_name", "currency_kind", "is_in_reference")
    .where(F.col("currency_code").isNotNull())
)
print("DimCurrency:", overwrite(dim_currency, g("dim_currency")))

dim_asset_type = spark.createDataFrame(
    [
        (
            asset_class,
            asset_class.replace("_", " ").title(),
            asset_class in cfg.ENTITY_BEARING,
            asset_class in cfg.IDENTIFIER_BEARING,
            asset_class in cfg.CLASSIFICATION_BEARING,
        )
        for asset_class in cfg.ASSET_CLASSES
    ],
    "asset_class string, asset_class_name string, is_entity_bearing boolean, "
    "is_identifier_bearing boolean, is_classification_bearing boolean",
)
print("DimAssetType:", overwrite(dim_asset_type, g("dim_asset_type")))

# COMMAND ----------

# --- DimSector / DimIndustry ----------------------------------------------
dim_sector = (
    master.select("sector")
    .where(F.col("sector").isNotNull())
    .distinct()
    .withColumn("sector_key", tf.surrogate_key(F.col("sector")))
    .select("sector_key", F.col("sector").alias("sector_name"))
)
print("DimSector:", overwrite(dim_sector, g("dim_sector")))

dim_industry = (
    master.select("sector", "industry_group", "industry")
    .where(F.col("industry").isNotNull())
    .distinct()
    .withColumn(
        "industry_key",
        tf.surrogate_key(F.col("sector"), F.col("industry_group"), F.col("industry")),
    )
    .withColumn("sector_key", tf.surrogate_key(F.col("sector")))
    .select("industry_key", "sector_key", F.col("sector").alias("sector_name"),
            F.col("industry_group").alias("industry_group_name"),
            F.col("industry").alias("industry_name"))
)
print("DimIndustry:", overwrite(dim_industry, g("dim_industry")))

# COMMAND ----------

# --- DimInstrument / DimEntity / DimIdentifier -----------------------------
dim_instrument = master.select(
    "instrument_id", "instrument_type", "symbol", "instrument_name", "currency",
    "country", "status", "entity_id", "sector", "industry_group", "industry",
    "category_group", "category", "family", "isin", "cusip", "figi",
    "composite_figi", "shareclass_figi", "market_cap", "website",
    "quality_score", "quality_band", "listing_count", "venue_count",
    "primary_exchange",
).withColumn(
    "sector_key", F.when(F.col("sector").isNotNull(), tf.surrogate_key(F.col("sector")))
).withColumn(
    "industry_key",
    F.when(
        F.col("industry").isNotNull(),
        tf.surrogate_key(F.col("sector"), F.col("industry_group"), F.col("industry")),
    ),
)
print("DimInstrument:", overwrite(dim_instrument, g("dim_instrument")))

dim_entity = entities.select(
    "entity_id", "entity_name", "entity_name_normalised", "country", "entity_type"
).join(
    master.groupBy("entity_id").agg(
        F.countDistinct("instrument_id").alias("instrument_count"),
        F.sum("listing_count").alias("listing_count"),
        F.countDistinct("primary_exchange").alias("venue_count"),
    ),
    "entity_id",
    "left",
)
print("DimEntity:", overwrite(dim_entity, g("dim_entity")))

dim_identifier = identifiers.select(
    "identifier_id", "instrument_id", "identifier_type", "identifier_value",
    "is_valid_format", "is_primary", "effective_from", "effective_to", "is_current",
)
print("DimIdentifier:", overwrite(dim_identifier, g("dim_identifier")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## FactListing
# MAGIC
# MAGIC The grain of the star: one row per security per venue. Every dimension
# MAGIC reference is emitted as a resolvable key so the Azure SQL foreign keys hold.

# COMMAND ----------

fact_listing = (
    listings.alias("l")
    .join(master.select("instrument_id", "sector", "industry_group", "industry",
                        "quality_score", "quality_band").alias("m"), "instrument_id", "left")
    .withColumn(
        "sector_key", F.when(F.col("sector").isNotNull(), tf.surrogate_key(F.col("sector")))
    )
    .withColumn(
        "industry_key",
        F.when(
            F.col("industry").isNotNull(),
            tf.surrogate_key(F.col("sector"), F.col("industry_group"), F.col("industry")),
        ),
    )
    .withColumn(
        "effective_from_key", F.date_format(F.col("effective_from"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "effective_to_key", F.date_format(F.col("effective_to"), "yyyyMMdd").cast("int")
    )
    .select(
        "listing_id", "instrument_id", "entity_id", "symbol",
        F.col("exchange").alias("exchange_code"), "mic", "market",
        F.col("currency").alias("currency_code"),
        F.col("venue_country").alias("country_name"),
        "venue_type", "is_primary", "status",
        "sector_key", "industry_key", "quality_score", "quality_band",
        "effective_from", "effective_to", "effective_from_key", "effective_to_key",
        "is_current",
    )
)
print("FactListing:", overwrite(fact_listing, g("fact_listing")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referential integrity check
# MAGIC
# MAGIC Every foreign key in `FactListing` must resolve before the serving layer is
# MAGIC loaded, otherwise the Azure SQL constraints will reject the batch.

# COMMAND ----------

fact = spark.table(g("fact_listing"))
checks = [
    ("exchange_code -> DimExchange", fact.join(
        spark.table(g("dim_exchange")), "exchange_code", "left_anti"
    ).where(F.col("exchange_code").isNotNull()).count()),
    ("currency_code -> DimCurrency", fact.join(
        spark.table(g("dim_currency")), "currency_code", "left_anti"
    ).where(F.col("currency_code").isNotNull()).count()),
    ("country_name -> DimCountry", fact.join(
        spark.table(g("dim_country")), "country_name", "left_anti"
    ).where(F.col("country_name").isNotNull()).count()),
    ("instrument_id -> DimInstrument", fact.join(
        spark.table(g("dim_instrument")), "instrument_id", "left_anti"
    ).count()),
    ("sector_key -> DimSector", fact.join(
        spark.table(g("dim_sector")), "sector_key", "left_anti"
    ).where(F.col("sector_key").isNotNull()).count()),
    ("industry_key -> DimIndustry", fact.join(
        spark.table(g("dim_industry")), "industry_key", "left_anti"
    ).where(F.col("industry_key").isNotNull()).count()),
]

failed = [(name, count) for name, count in checks if count]
for name, count in checks:
    print("  {0:<34} {1}".format(name, "OK" if not count else "{0} orphans".format(count)))

if failed:
    raise RuntimeError(
        "Referential integrity failed before serving: "
        + "; ".join("{0} has {1} orphans".format(n, c) for n, c in failed)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control tower headline numbers

# COMMAND ----------

headline = master.agg(
    F.countDistinct("instrument_id").alias("instruments"),
    F.countDistinct("entity_id").alias("entities"),
    F.sum("listing_count").alias("listings"),
    F.countDistinct("primary_exchange").alias("exchanges"),
    F.countDistinct("country").alias("countries"),
    F.round(F.avg("quality_score"), 2).alias("avg_quality_score"),
    F.sum(F.when(F.col("quality_band") == "TRUSTED", 1).otherwise(0)).alias("trusted"),
    F.sum(F.when(F.col("quality_band") == "REVIEW", 1).otherwise(0)).alias("review"),
    F.sum(F.when(F.col("quality_band") == "QUARANTINE", 1).otherwise(0)).alias("quarantined"),
)
display(headline)

# COMMAND ----------

print("gold layer complete for snapshot", settings.snapshot_date)
