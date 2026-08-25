# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Data quality, scoring and quarantine
# MAGIC
# MAGIC Runs the rule catalogue over the standardised snapshot, scores every record,
# MAGIC and routes failures to quarantine **with their reason and full original
# MAGIC record attached**. Nothing is deleted.
# MAGIC
# MAGIC ## Two deliberate choices
# MAGIC
# MAGIC **Severity is not the same as score.** A rule marked `QUARANTINE` blocks the
# MAGIC record from the trusted master (no symbol, no venue, duplicate on the same
# MAGIC exchange). A rule marked `REVIEW` only lowers the score - a cross-listed
# MAGIC German security whose venue country differs from its domicile is *correct*,
# MAGIC not broken, and rejecting it would delete real instruments.
# MAGIC
# MAGIC **Weights are per asset class.** Scoring an FX pair with the equity profile
# MAGIC would fail it for having no ISIN, no sector and no country - none of which
# MAGIC an FX pair can ever have. Each asset class is scored only on the components
# MAGIC that apply to it, and every profile sums to 100. Weights live in
# MAGIC `audit.dq_scoring_profile` and are read at run time, so retuning the model
# MAGIC is an `UPDATE`, not a deployment.

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

from fuct import audit, config as cfg, dq, transforms as tf, writer  # noqa: E402

# COMMAND ----------

settings = cfg.resolve(dbutils)
snapshot = F.to_date(F.lit(settings.snapshot_date))
print(settings)

# COMMAND ----------

staging = spark.table(settings.silver("instrument_staging").replace("`", "")).where(
    F.col("snapshot_date") == snapshot
)

# The asset-type consistency rule compares the detected class against the class
# the control table declares for that dataset.
control = spark.table(settings.audit("ingestion_control").replace("`", "")).select(
    F.col("dataset").alias("source_dataset"),
    F.col("declared_asset_class"),
)

base = staging.join(F.broadcast(control), "source_dataset", "left")
print("records to assess:", base.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate rules

# COMMAND ----------

profiles = dq.load_scoring_profiles(spark, settings)
rules = dq.build_rules()
print("loaded {0} rules and {1} scoring profiles".format(len(rules), len(profiles)))

contextual = dq.with_quality_context(base, spark)
assessed = dq.evaluate(contextual, rules)
scored = dq.score(assessed, profiles)

total = scored.count()
print("assessed:", total)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality distribution

# COMMAND ----------

display(
    scored.groupBy("asset_class", "quality_band")
    .agg(
        F.count(F.lit(1)).alias("records"),
        F.round(F.avg("quality_score"), 2).alias("avg_score"),
        F.min("quality_score").alias("min_score"),
        F.max("quality_score").alias("max_score"),
    )
    .orderBy("asset_class", "quality_band")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule failure counts

# COMMAND ----------

failure_counts = (
    scored.select(F.explode_outer("dq_failures").alias("rule_code"))
    .where(F.col("rule_code").isNotNull())
    .groupBy("rule_code")
    .agg(F.count(F.lit(1)).alias("failures"))
    .join(
        spark.table(settings.audit("dq_rule_catalogue").replace("`", "")),
        "rule_code",
        "left",
    )
    .select("rule_code", "dimension", "severity", "failures", "description")
    .orderBy(F.desc("failures"))
)
display(failure_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine
# MAGIC
# MAGIC One row per (record, failure reason) so a steward can filter by reason. The
# MAGIC entire source record travels with it as JSON, which is what makes a
# MAGIC quarantined record reprocessable once the underlying issue is fixed.

# COMMAND ----------

source_columns = [c for c in staging.columns if not c.startswith("dq_")]

quarantined = (
    scored.where(F.col("is_quarantined"))
    .withColumn("failure_reason", F.explode("dq_quarantine_failures"))
    .withColumn(
        "original_record",
        F.to_json(F.struct(*[F.col("`{0}`".format(c)) for c in source_columns])),
    )
)

rule_columns = {r.code: r.column for r in rules}
failed_column = F.lit(None).cast("string")
for code, column in rule_columns.items():
    if column:
        failed_column = F.when(
            F.col("failure_reason") == F.lit(code), F.lit(column)
        ).otherwise(failed_column)

quarantine_records = quarantined.select(
    tf.surrogate_key(
        F.col("listing_id"), F.col("failure_reason"), F.lit(settings.snapshot_date)
    ).alias("record_id"),
    F.col("source_dataset"),
    F.col("source_file"),
    F.col("snapshot_date").cast("string").alias("source_snapshot"),
    F.col("asset_class"),
    F.col("failure_reason"),
    failed_column.alias("failed_column"),
    F.col("quality_score").cast("string").alias("failure_detail"),
    F.col("original_record"),
    F.lit(settings.pipeline_run_id).alias("pipeline_run_id"),
    F.current_timestamp().alias("quarantine_timestamp"),
    F.col("snapshot_date"),
)

with audit.StageRun(spark, settings, "05_data_quality") as run:
    run.records_read = total
    rejected = audit.write_quarantine(spark, settings, quarantine_records)
    run.records_rejected = rejected
    run.quality_score = float(
        scored.agg(F.round(F.avg("quality_score"), 2)).first()[0] or 0.0
    )

    quality_table = settings.silver("instrument_quality")
    quality = scored.select(
        "listing_id",
        "instrument_id",
        "asset_class",
        "symbol",
        "exchange",
        "instrument_name",
        "quality_score",
        "quality_band",
        *["score_" + c for c in dq.SCORE_COMPONENTS],
        "dq_failures",
        "dq_failure_count",
        "dq_quarantine_failures",
        "is_quarantined",
        "exchange_is_known",
        "country_is_known",
        "currency_is_known",
        "symbol_exchange_occurrences",
        "isin_instrument_count",
        "figi_instrument_count",
        "snapshot_date",
    ).withColumn("pipeline_run_id", F.lit(settings.pipeline_run_id))

    writer.save(
        spark,
        quality,
        quality_table,
        location=settings.silver_location("instrument_quality"),
        partition_by=["snapshot_date"],
        replace_where="snapshot_date = '{0}'".format(settings.snapshot_date),
    )
    run.records_written = total - rejected

print("quarantined rows (record x reason):", rejected)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine breakdown

# COMMAND ----------

display(
    spark.table(settings.quarantine("rejected_records").replace("`", ""))
    .where(F.col("source_snapshot") == F.lit(settings.snapshot_date))
    .groupBy("failure_reason", "asset_class")
    .agg(F.count(F.lit(1)).alias("records"))
    .orderBy(F.desc("records"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identifier conflicts
# MAGIC
# MAGIC The case the brief calls out explicitly: one identifier resolving to two
# MAGIC different canonical instruments. Flagged for review, never silently merged.

# COMMAND ----------

conflicts = (
    scored.where(
        (F.col("isin_instrument_count") > 1) | (F.col("figi_instrument_count") > 1)
    )
    .select(
        "symbol", "exchange", "instrument_name", "asset_class",
        "isin", "isin_instrument_count", "figi", "figi_instrument_count",
    )
    .orderBy(F.desc("isin_instrument_count"))
)
print("records with a conflicting identifier:", conflicts.count())
display(conflicts.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which asset class has the highest failure rate?

# COMMAND ----------

display(
    scored.groupBy("asset_class")
    .agg(
        F.count(F.lit(1)).alias("records"),
        F.sum(F.col("is_quarantined").cast("int")).alias("quarantined"),
        F.round(
            100.0 * F.sum(F.col("is_quarantined").cast("int")) / F.count(F.lit(1)), 2
        ).alias("quarantine_rate_pct"),
        F.round(F.avg("quality_score"), 2).alias("avg_quality_score"),
        F.round(F.avg("dq_failure_count"), 2).alias("avg_failures_per_record"),
    )
    .orderBy(F.desc("quarantine_rate_pct"))
)

# COMMAND ----------

