# Databricks notebook source
# MAGIC %md
# MAGIC # Diagnostic - why is the quarantine rate so high?
# MAGIC
# MAGIC A 61% quarantine rate is not a plausible property of the source; it is
# MAGIC either a rule defect or a real duplication in the landing area. This
# MAGIC separates the two.

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
cat = settings.catalog
lines = []


def emit(text):
    print(text)
    lines.append(str(text))


# COMMAND ----------

emit("=== quarantine reasons (row = record x reason) ===")
q = spark.table("{0}.quarantine.rejected_records".format(cat))
for r in q.groupBy("failure_reason").agg(
    F.count(F.lit(1)).alias("rows"),
    F.countDistinct("record_id").alias("records"),
).orderBy(F.desc("rows")).collect():
    emit("  {0:<26} rows={1:>8,}".format(r["failure_reason"], r["rows"]))

emit("")
emit("=== distinct records quarantined, by asset class ===")
for r in q.groupBy("asset_class").agg(
    F.countDistinct("record_id").alias("records")
).orderBy(F.desc("records")).collect():
    emit("  {0:<16} {1:>8,}".format(r["asset_class"], r["records"]))

# COMMAND ----------

emit("")
emit("=== duplicate symbol+exchange: which source files collide? ===")
staging = spark.table("{0}.silver.instrument_staging".format(cat))

dupes = (
    staging.groupBy("asset_class", "symbol", "exchange")
    .agg(
        F.count(F.lit(1)).alias("n"),
        F.collect_set("source_file").alias("files"),
    )
    .where(F.col("n") > 1)
)
emit("  duplicated (asset_class,symbol,exchange) groups: {0:,}".format(dupes.count()))

emit("")
emit("  file pairs that collide most:")
pair_counts = (
    dupes.withColumn("file_pair", F.concat_ws("  ||  ", F.sort_array(F.col("files"))))
    .groupBy("file_pair")
    .agg(F.count(F.lit(1)).alias("colliding_groups"))
    .orderBy(F.desc("colliding_groups"))
)
for r in pair_counts.limit(10).collect():
    pair = r["file_pair"]
    short = "  ||  ".join(p.split("/")[-2] + "/" + p.split("/")[-1] for p in pair.split("  ||  "))
    emit("    {0:>7,}  {1}".format(r["colliding_groups"], short))

# COMMAND ----------

emit("")
emit("=== row counts per source file ===")
for r in (
    staging.groupBy("source_file", "asset_class")
    .agg(F.count(F.lit(1)).alias("rows"))
    .orderBy(F.desc("rows"))
    .collect()
):
    emit("  {0:<34} {1:<14} {2:>8,}".format(
        "/".join(r["source_file"].split("/")[-2:]), r["asset_class"], r["rows"]))

# COMMAND ----------

emit("")
emit("=== quality band by asset class (all records) ===")
dq = spark.table("{0}.silver.instrument_quality".format(cat))
for r in dq.groupBy("asset_class", "quality_band").agg(
    F.count(F.lit(1)).alias("n")
).orderBy("asset_class", "quality_band").collect():
    emit("  {0:<16} {1:<12} {2:>8,}".format(r["asset_class"], r["quality_band"], r["n"]))

dbutils.notebook.exit("\n".join(lines)[:60000])
