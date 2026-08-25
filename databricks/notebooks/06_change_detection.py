# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Financial universe change detection
# MAGIC
# MAGIC Compares two source snapshots and reports **exactly what changed, column by
# MAGIC column**.
# MAGIC
# MAGIC The scenario this exists for: yesterday 160,000 instruments, today 160,000
# MAGIC instruments. A row-count comparison concludes "no change". It is wrong. The
# MAGIC same total can hide instruments added and removed in equal number, hundreds
# MAGIC of reclassifications, and identifier changes that silently break downstream
# MAGIC joins.
# MAGIC
# MAGIC ## Method
# MAGIC
# MAGIC A full outer join on `listing_id` - the stable natural key
# MAGIC `(asset_class, symbol, exchange)` - classifies every key as NEW, REMOVED or
# MAGIC present in both. For keys present in both, the tracked columns are unpivoted
# MAGIC with `stack()` so one output row is emitted per *changed column*, not per
# MAGIC changed record. The change type is driven by which column moved:
# MAGIC
# MAGIC | Columns | Change type |
# MAGIC |---|---|
# MAGIC | `sector`, `industry_group`, `industry`, `category*` | `RECLASSIFIED` |
# MAGIC | `exchange`, `mic`, `market` | `RELISTED` |
# MAGIC | `isin`, `cusip`, `figi`, `composite_figi`, `shareclass_figi` | `IDENTIFIER_CHANGED` |
# MAGIC | `instrument_name`, `currency`, `country`, `status` | `MODIFIED` |
# MAGIC
# MAGIC A record whose quality band degrades also raises `DATA_QUALITY_ISSUE`, so a
# MAGIC source that starts shipping worse data is visible even when no business
# MAGIC value changed.

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

from fuct import audit, config as cfg, schemas, transforms as tf, writer  # noqa: E402

# COMMAND ----------

settings = cfg.resolve(dbutils)
print(settings)

staging_table = settings.silver("instrument_staging").replace("`", "")
staging = spark.table(staging_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select the comparison pair

# COMMAND ----------

available = sorted(
    r["snapshot_date"].isoformat()
    for r in staging.select("snapshot_date").distinct().collect()
)
print("snapshots available:", available)

current_date = settings.snapshot_date
earlier = [d for d in available if d < current_date]
previous_date = earlier[-1] if earlier else None

if previous_date is None:
    print(
        "Only one snapshot ({0}) exists, so there is no prior state to compare "
        "against. Recording it as the baseline: every instrument is NEW by "
        "definition, and the first real comparison happens on the next "
        "ingestion.".format(current_date)
    )
else:
    print("comparing {0} (current) against {1} (previous)".format(current_date, previous_date))

# COMMAND ----------

COMPARE_COLUMNS = list(schemas.TRACKED_COLUMNS.keys())

# `exchange` is both an identity column and a tracked column (a listing moving
# venue is a RELISTED change), so the two lists overlap. Selecting the union
# naively would project it twice and make `cur_exchange` ambiguous.
IDENTITY_COLUMNS = ["listing_id", "instrument_id", "asset_class", "symbol", "exchange"]
SELECT_COLUMNS = IDENTITY_COLUMNS + [
    c for c in COMPARE_COLUMNS if c not in IDENTITY_COLUMNS
]

current = staging.where(
    F.col("snapshot_date") == F.to_date(F.lit(current_date))
).select(*SELECT_COLUMNS)

if previous_date:
    previous = staging.where(
        F.col("snapshot_date") == F.to_date(F.lit(previous_date))
    ).select(*SELECT_COLUMNS)
else:
    previous = current.limit(0)

# The join below is on `listing_id`, so that key must be unique on both sides.
# It is not always: the source genuinely ships the same symbol twice on the
# same exchange (ECC on NYQ). Left unhandled, a duplicated key on both sides
# produces a 2x2 cartesian and the detector invents changes - reporting the
# same field moving in both directions at once.
#
# Collapse to one row per listing, ordered by a hash of the tracked values so
# the survivor is the same on both sides and across runs. The duplication
# itself is not hidden: DUPLICATE_INSTRUMENT still reports it and the row still
# goes to quarantine.
from pyspark.sql import Window  # noqa: E402


def dedupe_by_listing(df, label):
    ranked = df.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("listing_id").orderBy(
                tf.row_hash(COMPARE_COLUMNS).asc()
            )
        ),
    )
    kept = ranked.where(F.col("_rn") == 1).drop("_rn")
    dropped = df.count() - kept.count()
    if dropped:
        print("  {0}: collapsed {1} duplicate listing_id row(s)".format(label, dropped))
    return kept


current = dedupe_by_listing(current, "current snapshot")
previous = dedupe_by_listing(previous, "previous snapshot")

cur = current.select(
    [F.col(c).alias("cur_" + c) for c in current.columns]
)
prev = previous.select([F.col(c).alias("prev_" + c) for c in previous.columns])

joined = cur.join(
    prev, F.col("cur_listing_id") == F.col("prev_listing_id"), "full_outer"
)

print("joined keys:", joined.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## NEW and REMOVED

# COMMAND ----------

appeared = joined.where(F.col("prev_listing_id").isNull()).select(
    F.col("cur_listing_id").alias("listing_id"),
    F.col("cur_instrument_id").alias("instrument_id"),
    F.col("cur_asset_class").alias("asset_class"),
    F.col("cur_symbol").alias("symbol"),
    F.col("cur_exchange").alias("exchange"),
    F.lit("NEW").alias("change_type"),
    F.lit(None).cast("string").alias("column_changed"),
    F.lit(None).cast("string").alias("old_value"),
    F.col("cur_instrument_name").alias("new_value"),
)

removed = joined.where(F.col("cur_listing_id").isNull()).select(
    F.col("prev_listing_id").alias("listing_id"),
    F.col("prev_instrument_id").alias("instrument_id"),
    F.col("prev_asset_class").alias("asset_class"),
    F.col("prev_symbol").alias("symbol"),
    F.col("prev_exchange").alias("exchange"),
    F.lit("REMOVED").alias("change_type"),
    F.lit(None).cast("string").alias("column_changed"),
    F.col("prev_instrument_name").alias("old_value"),
    F.lit(None).cast("string").alias("new_value"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column-level changes
# MAGIC
# MAGIC `stack()` turns one wide row into one row per tracked column. Comparison is
# MAGIC NULL-safe (`<=>` negated), so "value appeared" and "value disappeared" both
# MAGIC register as changes instead of vanishing into NULL propagation.

# COMMAND ----------

both = joined.where(
    F.col("cur_listing_id").isNotNull() & F.col("prev_listing_id").isNotNull()
)

stack_args = ", ".join(
    "'{0}', prev_{0}, cur_{0}".format(column) for column in COMPARE_COLUMNS
)
stack_expr = "stack({0}, {1}) as (column_changed, old_value, new_value)".format(
    len(COMPARE_COLUMNS), stack_args
)

change_type_expr = F.lit("MODIFIED")
for column, change_type in schemas.TRACKED_COLUMNS.items():
    change_type_expr = F.when(
        F.col("column_changed") == F.lit(column), F.lit(change_type)
    ).otherwise(change_type_expr)

modified = (
    both.select(
        F.col("cur_listing_id").alias("listing_id"),
        F.col("cur_instrument_id").alias("instrument_id"),
        F.col("cur_asset_class").alias("asset_class"),
        F.col("cur_symbol").alias("symbol"),
        F.col("cur_exchange").alias("exchange"),
        F.expr(stack_expr),
    )
    .where(~F.col("old_value").eqNullSafe(F.col("new_value")))
    .withColumn("change_type", change_type_expr)
    .select(
        "listing_id", "instrument_id", "asset_class", "symbol", "exchange",
        "change_type", "column_changed", "old_value", "new_value",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality degradation

# COMMAND ----------

quality_table = settings.silver("instrument_quality").replace("`", "")
quality_changes = None

if previous_date and spark.catalog.tableExists(quality_table):
    quality = spark.table(quality_table)
    cur_q = quality.where(F.col("snapshot_date") == F.to_date(F.lit(current_date))).select(
        F.col("listing_id"),
        F.col("instrument_id"),
        F.col("asset_class"),
        F.col("symbol"),
        F.col("exchange"),
        F.col("quality_band").alias("cur_band"),
        F.col("quality_score").alias("cur_score"),
    )
    prev_q = quality.where(
        F.col("snapshot_date") == F.to_date(F.lit(previous_date))
    ).select(
        F.col("listing_id").alias("p_listing_id"),
        F.col("quality_band").alias("prev_band"),
        F.col("quality_score").alias("prev_score"),
    )
    band_order = (
        F.when(F.col("cur_band") == "TRUSTED", 3)
        .when(F.col("cur_band") == "REVIEW", 2)
        .otherwise(1)
    )
    prev_band_order = (
        F.when(F.col("prev_band") == "TRUSTED", 3)
        .when(F.col("prev_band") == "REVIEW", 2)
        .otherwise(1)
    )
    quality_changes = (
        cur_q.join(prev_q, cur_q["listing_id"] == prev_q["p_listing_id"], "inner")
        .where(band_order < prev_band_order)
        .select(
            "listing_id", "instrument_id", "asset_class", "symbol", "exchange",
            F.lit("DATA_QUALITY_ISSUE").alias("change_type"),
            F.lit("quality_band").alias("column_changed"),
            F.col("prev_band").alias("old_value"),
            F.col("cur_band").alias("new_value"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist `fact_instrument_changes`

# COMMAND ----------

changes = appeared.unionByName(removed).unionByName(modified)
if quality_changes is not None:
    changes = changes.unionByName(quality_changes)

changes = (
    changes.withColumn("change_date", F.to_date(F.lit(current_date)))
    .withColumn("source_snapshot", F.lit(current_date))
    .withColumn("previous_snapshot", F.lit(previous_date))
    .withColumn("pipeline_run_id", F.lit(settings.pipeline_run_id))
    .withColumn(
        "change_id",
        tf.surrogate_key(
            F.col("listing_id"),
            F.col("change_type"),
            F.coalesce(F.col("column_changed"), F.lit("")),
            F.lit(current_date),
        ),
    )
    .select(
        "change_id", "listing_id", "instrument_id", "asset_class", "symbol",
        "exchange", "change_date", "change_type", "column_changed",
        "old_value", "new_value", "source_snapshot", "previous_snapshot",
        "pipeline_run_id",
    )
)

changes_table = settings.silver("fact_instrument_changes")

with audit.StageRun(spark, settings, "06_change_detection", changes_table) as run:
    change_count = changes.count()
    run.records_read = joined.count()
    run.records_written = change_count
    run.records_modified = change_count

    # Idempotent: re-running this snapshot replaces only its own partition, so
    # previously detected changes for other dates survive.
    writer.save(
        spark,
        changes,
        changes_table,
        location=settings.silver_location("fact_instrument_changes"),
        partition_by=["change_date"],
        replace_where="change_date = '{0}'".format(current_date),
    )

print("changes recorded:", change_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change summary
# MAGIC
# MAGIC This is the answer to "did the universe change?" that a row count cannot give.

# COMMAND ----------

display(
    changes.groupBy("change_type")
    .agg(
        F.count(F.lit(1)).alias("changes"),
        F.countDistinct("listing_id").alias("distinct_listings"),
    )
    .orderBy(F.desc("changes"))
)

# COMMAND ----------

current_count = current.count()
previous_count = previous.count()
print("previous snapshot rows :", previous_count)
print("current  snapshot rows :", current_count)
print("net row delta          :", current_count - previous_count)
print("actual changes detected:", change_count)
if previous_count == current_count and change_count:
    print(
        "\nRow counts are identical but {0} changes were detected - exactly the "
        "case a count-based check would have missed.".format(change_count)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Most affected columns and venues

# COMMAND ----------

display(
    changes.where(F.col("column_changed").isNotNull())
    .groupBy("column_changed", "change_type")
    .agg(F.count(F.lit(1)).alias("changes"))
    .orderBy(F.desc("changes"))
)

# COMMAND ----------

display(
    changes.groupBy("exchange")
    .agg(
        F.count(F.lit(1)).alias("changes"),
        F.countDistinct("listing_id").alias("listings_affected"),
    )
    .orderBy(F.desc("changes"))
    .limit(25)
)

# COMMAND ----------

