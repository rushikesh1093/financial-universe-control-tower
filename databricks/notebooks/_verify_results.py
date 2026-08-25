# Databricks notebook source
# MAGIC %md
# MAGIC # Verification - is the output actually right?
# MAGIC
# MAGIC A green job only proves nothing threw. This checks the numbers mean
# MAGIC something: that grains collapse as intended, that entity resolution found
# MAGIC real cross-listings, that quality scores are in range, and that the
# MAGIC known data defects were caught rather than swallowed.

# COMMAND ----------

import json
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

from fuct import config as cfg  # noqa: E402

settings = cfg.resolve(dbutils)
out = {"catalog": settings.catalog, "snapshot": settings.snapshot_date}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table inventory

# COMMAND ----------

counts = {}
for schema in ("bronze", "silver", "gold", "quarantine", "audit"):
    for row in spark.sql("SHOW TABLES IN `{0}`.`{1}`".format(settings.catalog, schema)).collect():
        fq = "{0}.{1}.{2}".format(settings.catalog, schema, row["tableName"])
        try:
            counts[fq] = spark.table(fq).count()
        except Exception as exc:  # noqa: BLE001
            counts[fq] = "ERROR " + str(exc)[:80]

for k in sorted(counts):
    print("{0:<58} {1:>10}".format(k, counts[k]))
out["counts"] = counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Headline numbers

# COMMAND ----------

master = spark.table("{0}.gold.gold_security_master".format(settings.catalog))
head = master.agg(
    F.countDistinct("instrument_id").alias("instruments"),
    F.countDistinct("entity_id").alias("entities"),
    F.sum("listing_count").alias("listings"),
    F.countDistinct("primary_exchange").alias("exchanges"),
    F.countDistinct("country").alias("countries"),
    F.round(F.avg("quality_score"), 2).alias("avg_quality"),
).first().asDict()
print(json.dumps(head, indent=1, default=str))
out["headline"] = {k: str(v) for k, v in head.items()}

# COMMAND ----------

print("--- by asset class ---")
for r in (
    master.groupBy("instrument_type")
    .agg(
        F.count(F.lit(1)).alias("instruments"),
        F.sum("listing_count").alias("listings"),
        F.round(F.avg("quality_score"), 1).alias("avg_score"),
    )
    .orderBy(F.desc("instruments"))
    .collect()
):
    print("  {0:<14} instruments={1:>7,} listings={2:>7,} avg_score={3}".format(
        r["instrument_type"], r["instruments"], r["listings"] or 0, r["avg_score"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-listing: did entity/instrument/listing actually collapse?

# COMMAND ----------

multi_venue = master.where(F.col("venue_count") > 1)
print("instruments on >1 venue:", multi_venue.count())
for r in multi_venue.orderBy(F.desc("venue_count")).limit(8).collect():
    print("  {0:<44} {1} venues  {2}".format(
        (r["instrument_name"] or "")[:42], r["venue_count"], r["venues"]))

print()
print("--- entity resolution method ---")
for r in (
    master.groupBy("entity_resolution_method")
    .agg(F.count(F.lit(1)).alias("n"))
    .orderBy(F.desc("n")).collect()
):
    print("  {0:<18} {1:>8,}".format(r["entity_resolution_method"], r["n"]))

print()
print("--- instrument key source ---")
for r in (
    master.groupBy("instrument_key_source")
    .agg(F.count(F.lit(1)).alias("n")).orderBy(F.desc("n")).collect()
):
    print("  {0:<18} {1:>8,}".format(r["instrument_key_source"], r["n"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality

# COMMAND ----------

print("--- quality bands ---")
for r in (
    master.groupBy("quality_band").agg(F.count(F.lit(1)).alias("n"))
    .orderBy(F.desc("n")).collect()
):
    print("  {0:<14} {1:>8,}".format(r["quality_band"], r["n"]))

bounds = master.agg(F.min("quality_score").alias("lo"), F.max("quality_score").alias("hi")).first()
print("score range:", bounds["lo"], "-", bounds["hi"])
assert 0 <= (bounds["lo"] or 0) and (bounds["hi"] or 0) <= 100, "score out of 0-100 range"

# COMMAND ----------

qtable = "{0}.quarantine.rejected_records".format(settings.catalog)
if spark.catalog.tableExists(qtable):
    print("--- quarantine reasons ---")
    for r in (
        spark.table(qtable).groupBy("failure_reason")
        .agg(F.count(F.lit(1)).alias("n")).orderBy(F.desc("n")).collect()
    ):
        print("  {0:<28} {1:>8,}".format(r["failure_reason"], r["n"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change detection (first snapshot = baseline, everything NEW)

# COMMAND ----------

ctable = "{0}.gold.gold_instrument_changes".format(settings.catalog)
if spark.catalog.tableExists(ctable):
    for r in (
        spark.table(ctable).groupBy("change_type")
        .agg(F.count(F.lit(1)).alias("n")).orderBy(F.desc("n")).collect()
    ):
        print("  {0:<22} {1:>8,}".format(r["change_type"], r["n"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where the data physically lives

# COMMAND ----------

for t in ("silver.instrument_staging", "silver.dim_instrument", "gold.gold_security_master"):
    fq = "{0}.{1}".format(settings.catalog, t)
    try:
        d = spark.sql("DESCRIBE DETAIL {0}".format(fq)).first()
        print("  {0:<44} {1}".format(t, d["location"]))
    except Exception as exc:  # noqa: BLE001
        print("  {0}: {1}".format(t, str(exc)[:120]))

# COMMAND ----------

print("\nSample of gold_security_master:")
master.select(
    "symbol", "instrument_name", "instrument_type", "primary_exchange",
    "country", "currency", "isin", "venue_count", "quality_score", "quality_band",
).where(F.col("venue_count") > 1).limit(10).show(truncate=40)

dbutils.notebook.exit(json.dumps(out)[:60000])
