# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze ingestion
# MAGIC
# MAGIC Lands each source file as an immutable, dated snapshot with full lineage.
# MAGIC
# MAGIC ## Ingestion is discovery-driven, not folder-driven
# MAGIC
# MAGIC Every CSV under the landing root is found recursively, its header is read,
# MAGIC and its asset class is derived from the **column signature of that file**.
# MAGIC Files are then grouped by detected class and written to the matching bronze
# MAGIC table.
# MAGIC
# MAGIC This is deliberate. In this project the landing folders have been renamed
# MAGIC twice (`eft` -> `efts`, `equities` -> `funds_data`, `funds` ->
# MAGIC `equities_data`), their contents were swapped relative to their names, and
# MAGIC at least one file does not match the class its folder implies. Any mapping
# MAGIC that trusts a directory name is a latent bug; the header cannot lie.
# MAGIC
# MAGIC ## Other rules this stage enforces
# MAGIC
# MAGIC * Everything is read as a string. No inferred types, no coercion - bronze
# MAGIC   is what arrived.
# MAGIC * Nothing is filtered or de-duplicated. Rejection happens at silver, where
# MAGIC   there is a quarantine layer to receive it.
# MAGIC * Previous snapshots are never overwritten. Re-running *the same*
# MAGIC   `snapshot_date` replaces only that date's partition, so the stage is
# MAGIC   idempotent while remaining append-only across dates.

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

from fuct import audit, config as cfg, schemas, writer  # noqa: E402

# COMMAND ----------

settings = cfg.resolve(dbutils)
print(settings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover the landing area

# COMMAND ----------


def list_csv_files(root: str):
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
                found.append(entry.path)
    return sorted(found)


source_files = list_csv_files(settings.source_root)
print("found {0} CSV files under {1}".format(len(source_files), settings.source_root))
if not source_files:
    raise RuntimeError("No CSV files under " + settings.source_root)


def read_csv(path: str):
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")   # summaries contain embedded newlines
        .option("escape", '"')         # and embedded quotes
        .option("mode", "PERMISSIVE")
        .csv(path)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Classify every file by its header

# COMMAND ----------

by_asset_class = {}
classification = []

for path in source_files:
    header = [c for c in read_csv(path).columns if c != "_corrupt_record"]
    detected = cfg.detect_asset_class(header)
    if detected is None:
        raise RuntimeError(
            "No asset-class signature matched {0} (columns: {1}). "
            "Add one to fuct.config.ASSET_CLASS_SIGNATURES.".format(path, header)
        )
    by_asset_class.setdefault(detected, []).append(path)
    folder = path.rstrip("/").split("/")[-2] if "/" in path.rstrip("/") else ""
    classification.append((path, path.split("/")[-1], folder, detected, len(header)))
    print("  {0:<16} {1:<14} {2}".format(folder, detected, path.split("/")[-1]))

# COMMAND ----------

# Where a folder holds files of more than one class, the folder name is
# provably not a reliable label. Surfacing it explicitly beats letting it pass
# silently, because it usually means a file was uploaded to the wrong place.
folder_classes = {}
for _, name, folder, detected, _ in classification:
    folder_classes.setdefault(folder, set()).add(detected)

for folder, classes in sorted(folder_classes.items()):
    if len(classes) > 1:
        print(
            "  NOTE: folder '{0}' contains mixed asset classes {1} - "
            "each file is routed by its own header.".format(folder, sorted(classes))
        )

display(
    spark.createDataFrame(
        classification,
        "source_path string, file_name string, folder string, "
        "detected_asset_class string, column_count int",
    ).orderBy("folder", "file_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land each asset class

# COMMAND ----------

results = []

with audit.StageRun(spark, settings, "02_bronze_ingestion") as run:
    total = 0

    for asset_class, paths in sorted(by_asset_class.items()):
        dataset = cfg.DATASET_FOR_ASSET_CLASS[asset_class]

        # Read per file and union by name: files of one class share a signature
        # but need not share column order, and a stray extra column should widen
        # the table rather than fail the load.
        frame = None
        for path in paths:
            # `_metadata` is a hidden, file-backed-only column: it must be
            # promoted to a real column here, before the union, or the lineage
            # is unresolvable afterwards.
            part = read_csv(path).withColumn("source_file", F.col("_metadata.file_path"))
            frame = part if frame is None else frame.unionByName(part, allowMissingColumns=True)

        landed = schemas.add_bronze_audit_columns(frame, dataset, asset_class, settings)
        row_count = landed.count()
        total += row_count

        # Bronze stays a managed table: the raw CSVs already live in the
        # project's container, so mirroring them to a second external path
        # would duplicate the landing area rather than add anything.
        target = settings.bronze(dataset)
        writer.save(
            spark,
            landed,
            target,
            partition_by=["snapshot_date"],
            replace_where="snapshot_date = '{0}'".format(settings.snapshot_date),
        )

        results.append((dataset, asset_class, len(paths), row_count))
        print(
            "  {0:<18} {1:<14} {2} files  {3:>7,} rows".format(
                dataset, asset_class, len(paths), row_count
            )
        )

    run.records_read = total
    run.records_written = total
    run.records_rejected = 0

# COMMAND ----------

summary = spark.createDataFrame(
    results, "dataset string, asset_class string, files int, rows long"
)
display(summary.orderBy(F.desc("rows")))
print("total rows landed:", summary.agg(F.sum("rows")).first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snapshot history
# MAGIC
# MAGIC Proof that earlier snapshots are retained - this is what makes the universe
# MAGIC reproducible at a point in time.

# COMMAND ----------

history = None
for dataset, _, _, _ in results:
    part = (
        spark.table(settings.bronze(dataset).replace("`", ""))
        .groupBy("snapshot_date")
        .agg(F.count(F.lit(1)).alias("rows"))
        .withColumn("dataset", F.lit(dataset))
    )
    history = part if history is None else history.unionByName(part)

display(history.orderBy(F.desc("snapshot_date"), "dataset"))
