# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Master data: entity, instrument, listing, identifier, classification
# MAGIC
# MAGIC ## Why entity, instrument and listing are three tables
# MAGIC
# MAGIC ```
# MAGIC ENTITY       Alcoa Corporation                     the issuer
# MAGIC   +-- INSTRUMENT  ordinary shares, US0138721065    the security
# MAGIC        +-- LISTING  NYQ:AA                          where it trades
# MAGIC        +-- LISTING  BER:ALU
# MAGIC        +-- LISTING  FRA:ALU
# MAGIC ```
# MAGIC
# MAGIC Flattening these into one row per ticker would report a company listed on
# MAGIC four venues as four companies, inflate every count in the control tower,
# MAGIC and make "which companies have multiple listings" unanswerable. They are
# MAGIC also governed differently: a listing can be added or withdrawn without the
# MAGIC issuer changing, and an issuer can be renamed without any listing moving.
# MAGIC
# MAGIC ## Entity resolution
# MAGIC
# MAGIC Connected components over an evidence graph. Two rows are linked when they
# MAGIC share an ISIN, a composite FIGI, or a normalised name within one country.
# MAGIC Transitive closure then merges chains, so a NYQ<->BER link by ISIN and a
# MAGIC BER<->FRA link by name resolve to a single entity. Blocking keys with
# MAGIC implausibly many members are dropped - a "shared" key across hundreds of
# MAGIC rows is a data defect, not one gigantic company.
# MAGIC
# MAGIC Instruments with no issuer (indices, FX pairs, crypto pairs) keep a NULL
# MAGIC `entity_id` rather than an invented one, and are reported in gold as
# MAGIC "cannot be mapped to a canonical entity".

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

from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import Window, functions as F  # noqa: E402

from fuct import audit, config as cfg, transforms as tf, writer  # noqa: E402

# COMMAND ----------

settings = cfg.resolve(dbutils)
snapshot = F.to_date(F.lit(settings.snapshot_date))
print(settings)

# COMMAND ----------

staging = spark.table(settings.silver("instrument_staging").replace("`", "")).where(
    F.col("snapshot_date") == snapshot
)
exchange_ref = spark.table(settings.silver("ref_exchange").replace("`", ""))

staged = (
    staging.join(
        F.broadcast(
            exchange_ref.select(
                F.col("exchange_code"),
                F.col("primary_rank"),
                F.col("country").alias("exchange_country"),
                F.col("venue_type"),
            )
        ),
        staging["exchange"] == F.col("exchange_code"),
        "left",
    )
    .withColumn(
        "primary_rank",
        F.coalesce(F.col("primary_rank"), F.lit(999)),
    )
    .drop("exchange_code")
)

print("staged rows:", staged.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve entities

# COMMAND ----------

with audit.StageRun(spark, settings, "04_master_data.entity_resolution") as run:
    run.records_read = staged.count()
    resolved = tf.resolve_entities(spark, staged)
    resolved_count = resolved.count()
    run.records_written = resolved_count

display(
    resolved.groupBy("entity_resolution_method")
    .agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("entity_id").alias("entities"))
    .orderBy(F.desc("rows"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_entity

# COMMAND ----------

entities = tf.choose_entity_attributes(resolved).dropDuplicates(["entity_id"])
entity_table = settings.silver("dim_entity").replace("`", "")

entity_rows = (
    entities.withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("source_snapshot", snapshot)
)

entity_location = settings.silver_location("dim_entity")
if entity_location:
    writer.adopt_orphaned_location(spark, entity_table, entity_location)
if not spark.catalog.tableExists(entity_table):
    _w = entity_rows.write.format("delta")
    if entity_location:
        _w = _w.option("path", entity_location)
    _w.saveAsTable(entity_table)
else:
    (
        DeltaTable.forName(spark, entity_table)
        .alias("t")
        .merge(entity_rows.alias("s"), "t.entity_id = s.entity_id")
        .whenMatchedUpdate(
            condition=(
                "t.entity_name <> s.entity_name OR "
                "t.country IS DISTINCT FROM s.country OR "
                "t.entity_type <> s.entity_type"
            ),
            set={
                "entity_name": "s.entity_name",
                "entity_name_normalised": "s.entity_name_normalised",
                "country": "s.country",
                "entity_type": "s.entity_type",
                "updated_at": F.current_timestamp(),
                "source_snapshot": "s.source_snapshot",
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

print("entities:", spark.table(entity_table).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_instrument
# MAGIC
# MAGIC One row per security. Where an instrument has several listings the
# MAGIC canonical attributes are taken from its highest-ranked venue, so a US
# MAGIC company's name and currency come from NYSE rather than from a thinly
# MAGIC traded Berlin cross-listing.

# COMMAND ----------

best_listing = Window.partitionBy("instrument_id").orderBy(
    F.col("primary_rank").asc_nulls_last(),
    F.col("status").asc(),                      # ACTIVE before DELISTED
    F.length(F.col("instrument_name")).desc_nulls_last(),
    F.col("listing_id").asc(),                  # deterministic tie-break
)

instruments = (
    resolved.withColumn("_rn", F.row_number().over(best_listing))
    .where(F.col("_rn") == 1)
    .select(
        "instrument_id",
        F.col("asset_class").alias("instrument_type"),
        "symbol",
        "symbol_root",
        "instrument_name",
        "normalised_name",
        "currency",
        "country",
        "status",
        "entity_id",
        "entity_resolution_method",
        "instrument_key_source",
        "isin",
        "cusip",
        "figi",
        "composite_figi",
        "shareclass_figi",
        "sector",
        "industry_group",
        "industry",
        "category_group",
        "category",
        "family",
        "market_cap",
        "website",
        F.col("exchange").alias("primary_exchange"),
    )
    .withColumn("source_snapshot", snapshot)
)

instrument_table = settings.silver("dim_instrument").replace("`", "")
instrument_rows = instruments.withColumn("created_at", F.current_timestamp()).withColumn(
    "updated_at", F.current_timestamp()
)

with audit.StageRun(spark, settings, "04_master_data.dim_instrument", instrument_table) as run:
    run.records_read = resolved_count
    _loc = settings.silver_location("dim_instrument")
    if _loc:
        writer.adopt_orphaned_location(spark, instrument_table, _loc)
    if not spark.catalog.tableExists(instrument_table):
        _w = instrument_rows.write.format("delta")
        if _loc:
            _w = _w.option("path", _loc)
        _w.saveAsTable(instrument_table)
        run.records_written = spark.table(instrument_table).count()
    else:
        target = DeltaTable.forName(spark, instrument_table)
        before = target.toDF().count()
        update_cols = {
            c: "s." + c
            for c in instrument_rows.columns
            if c not in ("instrument_id", "created_at")
        }
        update_cols["updated_at"] = F.current_timestamp()
        (
            target.alias("t")
            .merge(instrument_rows.alias("s"), "t.instrument_id = s.instrument_id")
            .whenMatchedUpdate(
                condition="t.instrument_name IS DISTINCT FROM s.instrument_name "
                "OR t.status IS DISTINCT FROM s.status "
                "OR t.currency IS DISTINCT FROM s.currency "
                "OR t.country IS DISTINCT FROM s.country "
                "OR t.entity_id IS DISTINCT FROM s.entity_id",
                set=update_cols,
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        after = target.toDF().count()
        run.records_written = after
        run.records_modified = after - before

print("instruments:", spark.table(instrument_table).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_listing (SCD Type 2)
# MAGIC
# MAGIC A listing is primary when its venue country matches the issuer's country of
# MAGIC domicile; failing that, the highest-ranked venue wins. Index calculators and
# MAGIC price aggregators are excluded from the contest - nothing actually trades
# MAGIC there.

# COMMAND ----------

listing_rank = Window.partitionBy("instrument_id").orderBy(
    F.col("_home_market").desc(),
    F.col("primary_rank").asc_nulls_last(),
    F.col("listing_id").asc(),
)

listings = (
    resolved.withColumn(
        "_home_market",
        F.when(
            F.col("exchange_country").isNotNull()
            & F.col("country").isNotNull()
            & (F.col("exchange_country") == F.col("country")),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    .withColumn(
        "_tradable",
        F.coalesce(F.col("venue_type") == F.lit("EXCHANGE"), F.lit(False)).cast("int"),
    )
    .withColumn("_rn", F.row_number().over(listing_rank))
    .select(
        "listing_id",
        "instrument_id",
        "entity_id",
        "symbol",
        "exchange",
        "mic",
        "market",
        "currency",
        F.col("exchange_country").alias("venue_country"),
        F.col("venue_type"),
        (F.col("_rn") == 1).alias("is_primary"),
        "status",
    )
)

listing_result = tf.scd2_merge(
    spark,
    settings.silver("fact_listing"),
    listings,
    business_key=["listing_id"],
    tracked_columns=[
        "instrument_id", "entity_id", "symbol", "exchange", "mic", "market",
        "currency", "venue_country", "is_primary", "status",
    ],
    effective_date=settings.snapshot_date,
    close_absent=True,
    location=settings.silver_location("fact_listing"),
)
print("fact_listing:", listing_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_identifier (SCD Type 2)
# MAGIC
# MAGIC Unpivoted so an instrument can carry any number of identifiers. Each row is
# MAGIC validated against its format; a malformed value is retained and marked
# MAGIC rather than dropped, because "the source gave us a bad ISIN" is itself
# MAGIC information the stewards need.

# COMMAND ----------

identifier_types = [
    ("ISIN", "isin", tf.is_valid_isin),
    ("CUSIP", "cusip", tf.is_valid_cusip),
    ("FIGI", "figi", tf.is_valid_figi),
    ("COMPOSITE_FIGI", "composite_figi", tf.is_valid_figi),
    ("SHARECLASS_FIGI", "shareclass_figi", tf.is_valid_figi),
]

identifiers = None
for type_name, column, validator in identifier_types:
    part = (
        resolved.where(F.col(column).isNotNull())
        .select(
            "instrument_id",
            F.lit(type_name).alias("identifier_type"),
            F.col(column).alias("identifier_value"),
            validator(F.col(column)).alias("is_valid_format"),
        )
        .distinct()
    )
    identifiers = part if identifiers is None else identifiers.unionByName(part)

# ISIN is the primary identifier when present; otherwise the first by a fixed
# type precedence, so the choice is stable across runs.
type_precedence = F.when(F.col("identifier_type") == "ISIN", 1).when(
    F.col("identifier_type") == "CUSIP", 2
).when(F.col("identifier_type") == "FIGI", 3).when(
    F.col("identifier_type") == "COMPOSITE_FIGI", 4
).otherwise(5)

identifiers = (
    identifiers.withColumn("_prec", type_precedence)
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("instrument_id").orderBy(
                F.col("is_valid_format").desc(), F.col("_prec").asc(),
                F.col("identifier_value").asc(),
            )
        ),
    )
    .withColumn("is_primary", F.col("_rn") == 1)
    .withColumn(
        "identifier_id",
        tf.surrogate_key(
            F.col("instrument_id"), F.col("identifier_type"), F.col("identifier_value")
        ),
    )
    .drop("_prec", "_rn")
)

identifier_result = tf.scd2_merge(
    spark,
    settings.silver("dim_identifier"),
    identifiers,
    business_key=["identifier_id"],
    tracked_columns=["instrument_id", "identifier_type", "identifier_value",
                     "is_valid_format", "is_primary"],
    effective_date=settings.snapshot_date,
    close_absent=True,
    location=settings.silver_location("dim_identifier"),
)
print("dim_identifier:", identifier_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_classification (SCD Type 2)
# MAGIC
# MAGIC This is the table that answers *"what was this instrument's classification
# MAGIC on a given date?"*. A reclassification closes the current row
# MAGIC (`effective_to = snapshot - 1 day`, `is_current = false`) and opens a new
# MAGIC one, so history is never overwritten.

# COMMAND ----------

classifications = (
    resolved.select(
        "instrument_id",
        "asset_class",
        "sector",
        "industry_group",
        "industry",
        "category_group",
        "category",
        "family",
    )
    .dropDuplicates(["instrument_id"])
    .withColumn(
        "classification_id",
        tf.surrogate_key(
            F.col("instrument_id"),
            F.col("sector"), F.col("industry_group"), F.col("industry"),
            F.col("category_group"), F.col("category"),
        ),
    )
)

classification_result = tf.scd2_merge(
    spark,
    settings.silver("dim_classification"),
    classifications,
    business_key=["instrument_id"],
    tracked_columns=["sector", "industry_group", "industry",
                     "category_group", "category", "family"],
    effective_date=settings.snapshot_date,
    close_absent=False,   # a delisted instrument keeps its last known classification
    location=settings.silver_location("dim_classification"),
)
print("dim_classification:", classification_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Master-data summary

# COMMAND ----------

summary = spark.createDataFrame(
    [
        ("dim_entity", spark.table(settings.silver("dim_entity").replace("`", "")).count()),
        ("dim_instrument", spark.table(instrument_table).count()),
        ("fact_listing", spark.table(settings.silver("fact_listing").replace("`", "")).count()),
        ("dim_identifier", spark.table(settings.silver("dim_identifier").replace("`", "")).count()),
        (
            "dim_classification",
            spark.table(settings.silver("dim_classification").replace("`", "")).count(),
        ),
    ],
    "table string, rows long",
)
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Entities with more than one exchange listing

# COMMAND ----------

multi_listed = (
    spark.table(settings.silver("fact_listing").replace("`", ""))
    .where(F.col("is_current"))
    .where(F.col("entity_id").isNotNull())
    .groupBy("entity_id")
    .agg(
        F.countDistinct("exchange").alias("venue_count"),
        F.collect_set("exchange").alias("venues"),
        F.countDistinct("instrument_id").alias("instruments"),
    )
    .where(F.col("venue_count") > 1)
    .join(
        spark.table(settings.silver("dim_entity").replace("`", "")).select(
            "entity_id", "entity_name", "country"
        ),
        "entity_id",
        "left",
    )
)
print("entities listed on more than one venue:", multi_listed.count())
display(multi_listed.orderBy(F.desc("venue_count")).limit(25))

# COMMAND ----------

