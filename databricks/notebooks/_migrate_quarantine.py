# Databricks notebook source
# MAGIC %md
# MAGIC # Maintenance - move quarantine into the lake container
# MAGIC
# MAGIC `quarantine.rejected_records` was created as a **managed** table, so its
# MAGIC files live in Databricks-managed storage and never appear in the project's
# MAGIC ADLS container. The brief lists `quarantine/` as a lake folder beside
# MAGIC bronze, silver and gold, and a data steward looking at the container would
# MAGIC reasonably conclude nothing was being rejected at all.
# MAGIC
# MAGIC This reads the existing rows, recreates the table as **external** under
# MAGIC `quarantine_root`, and writes them back - so no snapshot has to be
# MAGIC reprocessed to relocate the data.

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

from fuct import config as cfg  # noqa: E402

settings = cfg.resolve(dbutils)
table = settings.quarantine("rejected_records").replace("`", "")
location = settings.quarantine_location("rejected_records")

if not location:
    raise ValueError("Set the 'quarantine_root' parameter before running this.")

print("table   :", table)
print("target  :", location)

# COMMAND ----------

if not spark.catalog.tableExists(table):
    print("Nothing to migrate - the table does not exist yet. The next pipeline "
          "run will create it directly at the external location.")
    dbutils.notebook.exit("nothing-to-migrate")

current_location = ""
try:
    current_location = spark.sql("DESCRIBE DETAIL {0}".format(table)).first()["location"] or ""
except Exception:  # noqa: BLE001
    pass
print("current :", current_location or "(managed)")

if current_location.rstrip("/") == location.rstrip("/"):
    print("Already external at the target location - nothing to do.")
    dbutils.notebook.exit("already-external")

# COMMAND ----------

# Materialise the existing rows before dropping the table. `count()` forces the
# read so the DataFrame is not lazily re-evaluated against a table that no
# longer exists.
existing = spark.table(table)
before = existing.count()
by_snapshot = (
    existing.groupBy("source_snapshot").agg(F.count(F.lit(1)).alias("rows")).collect()
)
print("rows to migrate:", before)
for r in by_snapshot:
    print("   {0}: {1:,}".format(r["source_snapshot"], r["rows"]))

staged_path = location.rstrip("/") + "_migrating"
existing.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).partitionBy("snapshot_date").save(staged_path)
print("staged to:", staged_path)

# COMMAND ----------

staged = spark.read.format("delta").load(staged_path)
assert staged.count() == before, "staged copy is incomplete - aborting before drop"

spark.sql("DROP TABLE {0}".format(table))
print("dropped managed table")

(
    staged.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", location)
    .partitionBy("snapshot_date")
    .saveAsTable(table)
)
print("recreated as external at", location)

# COMMAND ----------

after = spark.table(table).count()
detail = spark.sql("DESCRIBE DETAIL {0}".format(table)).first()
print("rows before :", before)
print("rows after  :", after)
print("location    :", detail["location"])
assert after == before, "row count changed during migration"

dbutils.fs.rm(staged_path, recurse=True)
print("cleaned up staging path")

display(
    spark.table(table)
    .groupBy("source_snapshot", "failure_reason")
    .agg(F.count(F.lit(1)).alias("rows"))
    .orderBy(F.desc("source_snapshot"), F.desc("rows"))
)
