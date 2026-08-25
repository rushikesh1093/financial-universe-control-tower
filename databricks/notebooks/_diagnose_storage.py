# Databricks notebook source
# MAGIC %md
# MAGIC # Diagnostic - inspect the ADLS medallion paths
# MAGIC
# MAGIC **Read only.** Lists what is under the silver/gold roots and reports which
# MAGIC folders are valid Delta tables, which are registered in Unity Catalog, and
# MAGIC which are neither (leftovers from a failed run).
# MAGIC
# MAGIC Exists because `DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION` only says a
# MAGIC location is "not empty and not a Delta table" - it does not say what is in
# MAGIC it, and nothing should be deleted from someone's storage account without
# MAGIC looking first.

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

# COMMAND ----------

report = {"silver_root": settings.silver_root, "gold_root": settings.gold_root, "entries": []}


def inspect(root: str, label: str):
    if not root:
        return
    try:
        entries = dbutils.fs.ls(root)
    except Exception as exc:  # noqa: BLE001
        report["entries"].append({"layer": label, "path": root, "error": str(exc)[:200]})
        return

    for entry in entries:
        item = {"layer": label, "name": entry.name.rstrip("/"), "path": entry.path,
                "is_dir": entry.isDir(), "size": entry.size}
        if entry.isDir():
            try:
                children = dbutils.fs.ls(entry.path)
                names = [c.name.rstrip("/") for c in children]
                item["child_count"] = len(children)
                item["children_sample"] = names[:8]
                item["is_delta"] = "_delta_log" in names
            except Exception as exc:  # noqa: BLE001
                item["error"] = str(exc)[:200]
        report["entries"].append(item)


inspect(settings.silver_root, "silver")
inspect(settings.gold_root, "gold")

# COMMAND ----------

# Cross-reference against what Unity Catalog actually knows about.
registered = {}
for schema in ("silver", "gold"):
    try:
        rows = spark.sql(
            "SHOW TABLES IN `{0}`.`{1}`".format(settings.catalog, schema)
        ).collect()
        registered[schema] = sorted(r["tableName"] for r in rows)
    except Exception as exc:  # noqa: BLE001
        registered[schema] = ["ERROR: " + str(exc)[:150]]

report["registered_tables"] = registered

# COMMAND ----------

for entry in report["entries"]:
    print(json.dumps(entry))
print("REGISTERED:", json.dumps(registered))

dbutils.notebook.exit(json.dumps(report)[:60000])
