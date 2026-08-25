# Databricks notebook source
# MAGIC %md
# MAGIC # Maintenance - reset the silver and gold layers
# MAGIC
# MAGIC **Destructive.** Deletes everything under `silver_root` and `gold_root` and
# MAGIC drops the corresponding Unity Catalog tables, so the pipeline can rebuild
# MAGIC from bronze.
# MAGIC
# MAGIC What it does **not** touch:
# MAGIC
# MAGIC * `bronze_root` - the source CSVs are never deleted
# MAGIC * the `bronze`, `quarantine` and `audit` schemas - run history is preserved
# MAGIC
# MAGIC Guarded by a `confirm` parameter that must be the literal string `DELETE`,
# MAGIC so it cannot run by accident or as part of the scheduled pipeline.

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

from fuct import config as cfg  # noqa: E402

settings = cfg.resolve(dbutils)

try:
    dbutils.widgets.text("confirm", "")
except Exception:
    pass
confirm = (dbutils.widgets.get("confirm") or "").strip()

if confirm != "DELETE":
    raise RuntimeError(
        "Refusing to run: set the 'confirm' parameter to the literal string "
        "DELETE to acknowledge that silver and gold will be erased."
    )

report = {"dropped_tables": [], "deleted_paths": [], "errors": [], "kept": []}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Drop the Unity Catalog tables in silver and gold

# COMMAND ----------

for schema in (cfg.SCHEMA_SILVER, cfg.SCHEMA_GOLD):
    try:
        rows = spark.sql(
            "SHOW TABLES IN `{0}`.`{1}`".format(settings.catalog, schema)
        ).collect()
    except Exception as exc:  # noqa: BLE001
        report["errors"].append("SHOW TABLES {0}: {1}".format(schema, str(exc)[:200]))
        continue

    for row in rows:
        fq = "`{0}`.`{1}`.`{2}`".format(settings.catalog, schema, row["tableName"])
        try:
            spark.sql("DROP TABLE IF EXISTS {0}".format(fq))
            report["dropped_tables"].append(fq.replace("`", ""))
            print("dropped", fq.replace("`", ""))
        except Exception as exc:  # noqa: BLE001
            report["errors"].append("DROP {0}: {1}".format(fq, str(exc)[:200]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delete the storage under silver and gold
# MAGIC
# MAGIC Children are removed one by one rather than deleting the root itself, so
# MAGIC the container layout survives and the delete is reported item by item.

# COMMAND ----------

for label, root in (("silver", settings.silver_root), ("gold", settings.gold_root)):
    if not root:
        report["kept"].append("{0}: no root configured".format(label))
        continue
    try:
        entries = dbutils.fs.ls(root)
    except Exception as exc:  # noqa: BLE001
        report["kept"].append("{0}: nothing to delete ({1})".format(label, str(exc)[:120]))
        continue

    for entry in entries:
        try:
            dbutils.fs.rm(entry.path, recurse=True)
            report["deleted_paths"].append(entry.path)
            print("deleted", entry.path)
        except Exception as exc:  # noqa: BLE001
            report["errors"].append("rm {0}: {1}".format(entry.path, str(exc)[:200]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Confirm bronze is intact

# COMMAND ----------

try:
    bronze_entries = dbutils.fs.ls(settings.source_root)
    report["bronze_intact"] = sorted(e.name.rstrip("/") for e in bronze_entries)
    print("bronze still contains:", report["bronze_intact"])
except Exception as exc:  # noqa: BLE001
    report["errors"].append("bronze check: " + str(exc)[:200])

# COMMAND ----------

print(json.dumps(report, indent=1)[:8000])
dbutils.notebook.exit(json.dumps(report)[:60000])
