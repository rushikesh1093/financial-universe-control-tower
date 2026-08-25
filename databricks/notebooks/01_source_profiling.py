# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Source profiling
# MAGIC
# MAGIC Profiles every file in the landing area **before** any ingestion logic runs,
# MAGIC and records the result in `audit.source_profile`.
# MAGIC
# MAGIC The profiler exists because the folder names in this dataset do not describe
# MAGIC their contents. Profiling `bronze/data` showed:
# MAGIC
# MAGIC | Folder | Columns found | Actual asset class |
# MAGIC |---|---|---|
# MAGIC | `funds/` | `sector, industry, isin, cusip, figi, market_cap` | **EQUITY** |
# MAGIC | `equities/` | `category_group, category, family` | **FUND** |
# MAGIC | `eft/` | `category_group, category, family, isin` | **ETF** |
# MAGIC
# MAGIC So asset class is derived from the **column signature**, and the folder's
# MAGIC declared class is kept only to raise a mismatch warning.

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

from fuct import audit, config as cfg  # noqa: E402

# COMMAND ----------

settings = cfg.resolve(dbutils)
print(settings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enumerate the landing area

# COMMAND ----------


def list_source_files(root: str):
    """Recursively list CSV files under the landing root."""
    found = []
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            entries = dbutils.fs.ls(current)
        except Exception as exc:  # noqa: BLE001
            print("cannot list", current, "->", exc)
            continue
        for entry in entries:
            if entry.isDir():
                stack.append(entry.path)
            elif entry.name.lower().endswith(".csv"):
                found.append((entry.path, entry.name, entry.size))
    return sorted(found)


source_files = list_source_files(settings.source_root)
print("found", len(source_files), "CSV files under", settings.source_root)
for path, name, size in source_files[:10]:
    print("  {0:>10,} bytes  {1}".format(size, path))

if not source_files:
    raise RuntimeError(
        "No CSV files under {0}. Upload bronze/data there first "
        "(see scripts/upload_bronze.ps1).".format(settings.source_root)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profile each file
# MAGIC
# MAGIC The header is read with `inferSchema=false` so profiling never depends on
# MAGIC Spark's type guesses - at bronze everything is a string, exactly as it
# MAGIC arrived.

# COMMAND ----------

declared_by_glob = {s.source_glob: s for s in cfg.SOURCE_REGISTRY}


def declared_for(path: str):
    """Match a physical file back to its control-table entry."""
    normalised = path.replace("\\", "/").rstrip("/")
    for source in cfg.SOURCE_REGISTRY:
        prefix = source.source_glob.split("*")[0].rstrip("/")
        if prefix.endswith(".csv"):
            if normalised.endswith(prefix):
                return source
        elif "/{0}/".format(prefix) in normalised:
            return source
    return None


profile_rows = []
with audit.StageRun(spark, settings, "01_source_profiling") as run:
    total_rows = 0
    for path, name, size in source_files:
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .option("multiLine", "true")
            .option("escape", '"')
            .csv(path)
        )
        header = df.columns
        detected = cfg.detect_asset_class(header)
        source = declared_for(path)
        declared = source.declared_asset_class if source else None
        row_count = df.count()
        total_rows += row_count

        null_counts = df.select(
            [
                F.sum(
                    F.when(
                        F.col("`{0}`".format(c)).isNull()
                        | (F.trim(F.col("`{0}`".format(c))) == ""),
                        1,
                    ).otherwise(0)
                ).alias(c)
                for c in header
            ]
        ).collect()[0].asDict()

        profile_rows.append(
            (
                path,
                name,
                source.dataset if source else None,
                declared,
                detected,
                bool(declared and detected and declared != detected),
                int(size),
                int(row_count),
                len(header),
                header,
                [
                    "{0}={1}".format(c, null_counts[c])
                    for c in header
                    if null_counts[c]
                ],
            )
        )
        flag = ""
        if declared and detected and declared != detected:
            flag = "  <-- declared {0}".format(declared)
        print(
            "{0:<28} rows={1:>7,}  cols={2:>2}  class={3}{4}".format(
                name, row_count, len(header), detected, flag
            )
        )

    run.records_read = total_rows
    run.records_written = len(profile_rows)

# COMMAND ----------

profile_df = spark.createDataFrame(
    profile_rows,
    "source_path string, file_name string, dataset string, declared_asset_class string, "
    "detected_asset_class string, class_mismatch boolean, size_bytes long, row_count long, "
    "column_count int, columns array<string>, null_counts array<string>",
).withColumn("snapshot_date", F.lit(settings.snapshot_date)).withColumn(
    "pipeline_run_id", F.lit(settings.pipeline_run_id)
)

profile_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    settings.audit("source_profile").replace("`", "")
)

display(
    profile_df.select(
        "dataset", "file_name", "declared_asset_class", "detected_asset_class",
        "class_mismatch", "row_count", "column_count",
    ).orderBy("dataset", "file_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema variation across the universe

# COMMAND ----------

display(
    profile_df.groupBy("detected_asset_class")
    .agg(
        F.countDistinct("source_path").alias("files"),
        F.sum("row_count").alias("rows"),
        F.countDistinct(F.concat_ws(",", F.col("columns"))).alias("distinct_layouts"),
        F.first("columns").alias("example_columns"),
    )
    .orderBy(F.desc("rows"))
)

# COMMAND ----------

unprofiled = [r for r in profile_rows if r[4] is None]
if unprofiled:
    raise RuntimeError(
        "No asset-class signature matched these files, so ingestion would not "
        "know how to standardise them: "
        + ", ".join(r[1] for r in unprofiled)
        + ". Add a signature to fuct.config.ASSET_CLASS_SIGNATURES."
    )

mismatches = [r for r in profile_rows if r[5]]
print(
    "profiled {0} files; {1} carry a class differing from their folder name "
    "(handled - detection wins).".format(len(profile_rows), len(mismatches))
)
