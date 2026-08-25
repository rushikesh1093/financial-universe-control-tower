# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Environment setup
# MAGIC
# MAGIC Creates the catalog, the medallion schemas, the landing volume and the
# MAGIC configuration tables the rest of the pipeline reads.
# MAGIC
# MAGIC Safe to re-run: every statement is `IF NOT EXISTS` or an idempotent
# MAGIC overwrite of reference data.

# COMMAND ----------

import os
import sys


def _add_utilities_to_path() -> str:
    """Put the `fuct` package on sys.path regardless of how this was deployed."""
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
    raise RuntimeError(
        "Could not locate the 'fuct' utilities package. Looked in: " + repr(candidates)
    )


print("utilities:", _add_utilities_to_path())

from fuct import audit, config as cfg, dq, reference as ref, writer  # noqa: E402

# COMMAND ----------

settings = cfg.resolve(dbutils)
print(settings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog, schemas and landing volume

# COMMAND ----------

# Creating a catalog needs CREATE_CATALOG on the metastore *and* a metastore
# storage root. Neither is guaranteed, and in this workspace the metastore has
# no storage root, so the catalog is expected to already exist. Only a genuinely
# missing catalog is fatal.
try:
    spark.sql("CREATE CATALOG IF NOT EXISTS `{0}`".format(settings.catalog))
except Exception as exc:  # noqa: BLE001
    print("Could not create catalog ({0}) - checking whether it exists.".format(exc))

if not spark.sql("SHOW CATALOGS").where(
    "catalog = '{0}'".format(settings.catalog)
).count():
    raise RuntimeError(
        "Catalog '{0}' does not exist and could not be created. Create it in the "
        "Databricks UI (Catalog > Create catalog, using Default Storage), then "
        "re-run with the `catalog` widget set to its name.".format(settings.catalog)
    )
print("catalog ready:", settings.catalog)

for schema in cfg.ALL_SCHEMAS:
    spark.sql(
        "CREATE SCHEMA IF NOT EXISTS `{0}`.`{1}`".format(settings.catalog, schema)
    )
    print("schema ready:", settings.catalog + "." + schema)

# COMMAND ----------

# The landing volume holds the raw CSV snapshots.  A Unity Catalog volume is
# used rather than a mount so no storage key is ever needed; if this workspace
# is not Unity Catalog enabled, set the `source_root` widget to a DBFS or
# abfss:// path instead and skip this cell.
try:
    spark.sql(
        "CREATE VOLUME IF NOT EXISTS `{0}`.`{1}`.`{2}`".format(
            settings.catalog, cfg.SCHEMA_BRONZE, settings.volume
        )
    )
    print("volume ready:", settings.volume_root)
except Exception as exc:  # noqa: BLE001
    print(
        "Could not create a Unity Catalog volume ({0}).\n"
        "Point the 'source_root' widget at a DBFS or abfss:// location "
        "and re-run the ingestion notebook.".format(exc)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference dimensions
# MAGIC
# MAGIC Exchange, country and currency reference data is derived from the values
# MAGIC actually present in the source, not from a global ISO catalogue - see the
# MAGIC scope note in `fuct/reference.py`. These are overwritten on every setup
# MAGIC run because they are code-managed, not accumulated.

# COMMAND ----------

for name, frame in (
    ("ref_exchange", ref.exchange_df(spark)),
    ("ref_country", ref.country_df(spark)),
    ("ref_currency", ref.currency_df(spark)),
):
    n = writer.save(
        spark, frame, settings.silver(name), location=settings.silver_location(name)
    )
    print("{0:<14} {1:>5} rows  {2}".format(
        name, n, writer.describe_location(spark, settings.silver(name))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration tables
# MAGIC
# MAGIC The scoring profile is seeded once and then owned by the business. Editing
# MAGIC it with an `UPDATE` retunes the quality model without a code change, and
# MAGIC Delta history records who changed what.

# COMMAND ----------

profiles = dq.load_scoring_profiles(spark, settings)
for asset_class, profile in sorted(profiles.items()):
    print("{0:<14} total={1:>3}  {2}".format(asset_class, sum(profile.values()), profile))

# COMMAND ----------

rules = dq.build_rules()
dq.rules_df(spark, rules).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(settings.audit("dq_rule_catalogue").replace("`", ""))
print("published", len(rules), "data-quality rules")

# COMMAND ----------

# The ingestion control table - the metadata-driven equivalent of an ADF
# lookup/ForEach control table. The pipeline processes whatever is ACTIVE here.
control_rows = [
    (
        s.dataset,
        s.source_glob,
        s.fmt,
        "bronze/" + s.dataset,
        bool(s.active),
        s.declared_asset_class,
        s.notes,
    )
    for s in cfg.SOURCE_REGISTRY
]
spark.createDataFrame(
    control_rows,
    "dataset string, source_glob string, format string, target_path string, "
    "is_active boolean, declared_asset_class string, notes string",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    settings.audit("ingestion_control").replace("`", "")
)

display(spark.table(settings.audit("ingestion_control").replace("`", "")))

# COMMAND ----------

audit.ensure_run_log(spark, settings)
print("setup complete for catalog:", settings.catalog)
