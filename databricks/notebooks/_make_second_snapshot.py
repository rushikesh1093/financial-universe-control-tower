# Databricks notebook source
# MAGIC %md
# MAGIC # Build a second source snapshot (with known, injected changes)
# MAGIC
# MAGIC Change detection is the project's headline requirement, and with a single
# MAGIC snapshot it cannot be demonstrated at all - every row is `NEW` by
# MAGIC definition. This produces a second snapshot so the comparison is real.
# MAGIC
# MAGIC Crucially the changes are **injected deliberately and recorded**, so the
# MAGIC detector can be graded against ground truth instead of being taken on
# MAGIC trust: if it reports 500 reclassifications, we can check that they are the
# MAGIC same 500 that were planted.
# MAGIC
# MAGIC The scenario from the brief is reproduced on purpose - roughly equal
# MAGIC numbers of additions and removals, so the **total row count barely moves
# MAGIC while thousands of things underneath it change**. A row-count check would
# MAGIC report "no change" and be wrong.
# MAGIC
# MAGIC ## Safety
# MAGIC
# MAGIC Reads the existing bronze Delta tables and writes CSVs to a **separate**
# MAGIC landing folder. The original `bronze/` in ADLS is never written to.
# MAGIC
# MAGIC Selection is by hash of the business key, not by random sampling, so
# MAGIC re-running produces byte-identical output.

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

from fuct import config as cfg, schemas  # noqa: E402

settings = cfg.resolve(dbutils)

for name, default in [
    ("target_root", "abfss://destination@learnazure11.dfs.core.windows.net/bronze_v2"),
    ("base_snapshot", "2026-08-22"),
]:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass

TARGET = dbutils.widgets.get("target_root").strip()
BASE = dbutils.widgets.get("base_snapshot").strip()

if TARGET.rstrip("/") == settings.source_root.rstrip("/"):
    raise ValueError(
        "target_root must differ from source_root - refusing to overwrite the "
        "original landing area."
    )

print("base snapshot :", BASE)
print("writing to    :", TARGET)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mutation plan
# MAGIC
# MAGIC `bucket` is a stable 0-9999 value derived from the symbol, so which rows
# MAGIC change is deterministic and reproducible.

# COMMAND ----------

# Each entry: (change kind, bucket range). Ranges are disjoint so no row
# receives two conflicting mutations.
PLAN = {
    "REMOVED": (0, 12),             # ~0.12% of rows deleted
    "MODIFIED": (20, 140),          # name changes
    "RECLASSIFIED": (200, 240),     # sector / industry changes (equities only)
    "IDENTIFIER_CHANGED": (300, 303),  # ISIN changes
    "RELISTED": (400, 407),         # mic / market changes
}
for kind, (lo, hi) in PLAN.items():
    print("  {0:<20} buckets {1}-{2}".format(kind, lo, hi))

# COMMAND ----------

AUDIT_COLS = set(schemas.BRONZE_AUDIT_COLUMNS) | {"_corrupt_record"}

manifest = None
written = []

for asset_class, dataset in sorted(cfg.DATASET_FOR_ASSET_CLASS.items()):
    table = settings.bronze(dataset).replace("`", "")
    if not spark.catalog.tableExists(table):
        continue

    src = spark.table(table).where(F.col("snapshot_date") == F.to_date(F.lit(BASE)))
    if src.limit(1).count() == 0:
        continue

    source_cols = [c for c in src.columns if c not in AUDIT_COLS]
    df = src.select(*source_cols)

    # Deterministic bucket from the symbol.
    df = df.withColumn("_bucket", F.abs(F.hash(F.col("symbol")) % F.lit(10000)))

    def in_bucket(kind):
        lo, hi = PLAN[kind]
        return (F.col("_bucket") >= lo) & (F.col("_bucket") < hi)

    # --- REMOVED: drop the rows entirely -------------------------------
    removed = df.where(in_bucket("REMOVED")).select(
        F.col("symbol"), F.col("exchange"),
        F.lit(asset_class).alias("asset_class"),
        F.lit("REMOVED").alias("expected_change"),
        F.lit(None).cast("string").alias("column_changed"),
    )
    df = df.where(~in_bucket("REMOVED"))

    # --- MODIFIED: change the instrument name --------------------------
    # `coalesce` first: concat() on a NULL yields NULL, so without it the ~16k
    # rows that have no name would be recorded as mutated while their value
    # never actually changed - inflating the manifest and making the detector
    # look like it had missed them.
    df = df.withColumn(
        "name",
        F.when(
            in_bucket("MODIFIED"),
            F.concat(F.coalesce(F.col("name"), F.lit("")), F.lit(" (Renamed)")),
        ).otherwise(F.col("name")),
    ) if "name" in df.columns else df

    modified = df.where(in_bucket("MODIFIED")).select(
        "symbol", "exchange", F.lit(asset_class).alias("asset_class"),
        F.lit("MODIFIED").alias("expected_change"),
        F.lit("instrument_name").alias("column_changed"),
    )

    # --- RECLASSIFIED: change industry (equities carry it) -------------
    reclassified = None
    if "industry" in df.columns:
        df = df.withColumn(
            "industry",
            F.when(
                in_bucket("RECLASSIFIED") & F.col("industry").isNotNull(),
                F.lit("Reclassified Industry"),
            ).otherwise(F.col("industry")),
        )
        reclassified = df.where(
            in_bucket("RECLASSIFIED") & (F.col("industry") == "Reclassified Industry")
        ).select(
            "symbol", "exchange", F.lit(asset_class).alias("asset_class"),
            F.lit("RECLASSIFIED").alias("expected_change"),
            F.lit("industry").alias("column_changed"),
        )
    elif "category" in df.columns:
        df = df.withColumn(
            "category",
            F.when(
                in_bucket("RECLASSIFIED") & F.col("category").isNotNull(),
                F.lit("Reclassified Category"),
            ).otherwise(F.col("category")),
        )
        reclassified = df.where(
            in_bucket("RECLASSIFIED") & (F.col("category") == "Reclassified Category")
        ).select(
            "symbol", "exchange", F.lit(asset_class).alias("asset_class"),
            F.lit("RECLASSIFIED").alias("expected_change"),
            F.lit("category").alias("column_changed"),
        )

    # --- IDENTIFIER_CHANGED: rewrite the ISIN --------------------------
    identifier = None
    if "isin" in df.columns:
        df = df.withColumn(
            "isin",
            F.when(
                in_bucket("IDENTIFIER_CHANGED") & F.col("isin").isNotNull(),
                F.concat(F.substring(F.col("isin"), 1, 2), F.lit("ZZ9999999Z")),
            ).otherwise(F.col("isin")),
        )
        identifier = df.where(
            in_bucket("IDENTIFIER_CHANGED") & F.col("isin").endswith("ZZ9999999Z")
        ).select(
            "symbol", "exchange", F.lit(asset_class).alias("asset_class"),
            F.lit("IDENTIFIER_CHANGED").alias("expected_change"),
            F.lit("isin").alias("column_changed"),
        )

    # --- RELISTED: change the MIC, keeping symbol+exchange stable ------
    # Deliberately not changing `exchange`: that is part of the listing's
    # natural key, so altering it would (correctly) read as a REMOVE plus an
    # ADD rather than as a relisting of the same listing.
    relisted = None
    if "mic" in df.columns:
        df = df.withColumn(
            "mic",
            F.when(
                in_bucket("RELISTED") & F.col("mic").isNotNull(), F.lit("XXXX")
            ).otherwise(F.col("mic")),
        )
        relisted = df.where(
            in_bucket("RELISTED") & (F.col("mic") == "XXXX")
        ).select(
            "symbol", "exchange", F.lit(asset_class).alias("asset_class"),
            F.lit("RELISTED").alias("expected_change"),
            F.lit("mic").alias("column_changed"),
        )

    # --- NEW: synthesise additions by cloning rows under new symbols ----
    additions = (
        df.where((F.col("_bucket") >= 500) & (F.col("_bucket") < 512))
        .withColumn("symbol", F.concat(F.col("symbol"), F.lit(".NEW")))
    )
    added = additions.select(
        "symbol", "exchange", F.lit(asset_class).alias("asset_class"),
        F.lit("NEW").alias("expected_change"),
        F.lit(None).cast("string").alias("column_changed"),
    )

    out = df.unionByName(additions).drop("_bucket")

    path = TARGET.rstrip("/") + "/" + dataset
    out.coalesce(1).write.mode("overwrite").option("header", "true").option(
        "escape", '"'
    ).csv(path)
    written.append((dataset, asset_class, out.count()))
    print("  {0:<18} {1:<14} {2:>8,} rows -> {3}".format(
        dataset, asset_class, out.count(), path))

    for part in (removed, modified, reclassified, identifier, relisted, added):
        if part is not None:
            manifest = part if manifest is None else manifest.unionByName(part)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record the ground truth

# COMMAND ----------

manifest_table = settings.audit("injected_changes").replace("`", "")
manifest.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(manifest_table)

display(
    spark.table(manifest_table)
    .groupBy("expected_change")
    .agg(F.count(F.lit(1)).alias("injected"))
    .orderBy(F.desc("injected"))
)

# COMMAND ----------

base_rows = sum(
    spark.table(settings.bronze(d).replace("`", ""))
    .where(F.col("snapshot_date") == F.to_date(F.lit(BASE)))
    .count()
    for _, d in sorted(cfg.DATASET_FOR_ASSET_CLASS.items())
    if spark.catalog.tableExists(settings.bronze(d).replace("`", ""))
)
new_rows = sum(r[2] for r in written)

print("base snapshot rows   : {0:,}".format(base_rows))
print("second snapshot rows : {0:,}".format(new_rows))
print("net difference       : {0:+,}".format(new_rows - base_rows))
print(
    "\nThe totals are close by design. A row-count check would call this "
    "'no change' - the point of the exercise."
)
