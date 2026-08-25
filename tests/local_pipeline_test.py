"""Run the transformation logic locally against the real bronze/data.

This exercises everything that does not need a Databricks runtime: canonical
projection, key derivation, entity resolution and the data-quality framework.
It runs on a local Spark session so the logic can be validated before anything
is deployed to a cluster.

    python tests/local_pipeline_test.py

Exits non-zero if any invariant fails.
"""

from __future__ import annotations

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "databricks", "utilities"))

from pyspark.sql import SparkSession, functions as F  # noqa: E402

from fuct import config as cfg, dq, schemas, transforms as tf  # noqa: E402

BRONZE = os.path.join(REPO_ROOT, "bronze", "data")

FAILURES = []


def check(label: str, condition: bool, detail: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    print("  [{0}] {1}{2}".format(status, label, ("  -> " + detail) if detail else ""))
    if not condition:
        FAILURES.append(label + (" -> " + detail if detail else ""))


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("fuct-local-test")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def read_source(spark, path, dataset):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(path)
    )
    asset_class = cfg.detect_asset_class(df.columns)
    if asset_class is None:
        raise AssertionError("no signature matched {0}: {1}".format(path, df.columns))
    return (
        df.withColumn("source_dataset", F.lit(dataset))
        .withColumn("source_file", F.lit(os.path.basename(path)))
        .withColumn("asset_class", F.lit(asset_class)),
        asset_class,
    )


def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    settings = cfg.Settings(snapshot_date="2026-08-22", pipeline_run_id="local-test")

    print("\n=== 1. Asset-class detection ===")
    expected = {
        "funds": cfg.EQUITY,        # folder mislabelled in the source
        "equities": cfg.FUND,       # folder mislabelled in the source
        "eft": cfg.ETF,
        "indices.csv": cfg.INDEX,
        "currencies.csv": cfg.CURRENCY,
        "cryptos.csv": cfg.CRYPTO,
        "moneymarkets.csv": cfg.MONEY_MARKET,
    }

    frames = []
    for source in cfg.SOURCE_REGISTRY:
        pattern = os.path.join(BRONZE, source.source_glob.replace("/", os.sep))
        df, asset_class = read_source(spark, pattern, source.dataset)
        folder = source.source_glob.split("/")[0]
        check(
            "{0:<18} -> {1}".format(folder, asset_class),
            asset_class == expected[folder],
            "expected " + expected[folder],
        )
        frames.append(schemas.to_canonical(df, asset_class))

    canonical = frames[0]
    for frame in frames[1:]:
        canonical = canonical.unionByName(frame)

    print("\n=== 2. Canonical projection ===")
    shapes = {tuple(f.columns) for f in frames}
    check("all asset classes share one schema", len(shapes) == 1,
          "{0} distinct shapes".format(len(shapes)))

    currency_alias = F.col("currency")
    for src, dst in {"GBp": "GBX"}.items():
        currency_alias = F.when(F.col("currency") == F.lit(src), F.lit(dst)).otherwise(
            currency_alias
        )

    normalised = (
        canonical.withColumn("symbol", F.upper(F.trim(F.col("symbol"))))
        .withColumn("exchange", F.upper(F.trim(F.col("exchange"))))
        .withColumn(
            "currency",
            F.when(F.col("currency").isNull(), F.lit(None)).otherwise(F.upper(currency_alias)),
        )
        .withColumn("normalised_name", tf.normalise_name(F.col("instrument_name")))
        .withColumn("symbol_root", tf.symbol_root(F.col("symbol")))
        .withColumn("isin", tf.clean_identifier(F.col("isin")))
        .withColumn("cusip", tf.clean_identifier(F.col("cusip")))
        .withColumn("figi", tf.clean_identifier(F.col("figi")))
        .withColumn("composite_figi", tf.clean_identifier(F.col("composite_figi")))
        .withColumn("shareclass_figi", tf.clean_identifier(F.col("shareclass_figi")))
    )

    keyed = (
        normalised.withColumn(
            "listing_id",
            tf.surrogate_key(F.col("asset_class"), F.col("symbol"), F.col("exchange")),
        )
        .withColumn("isin_is_valid", F.col("isin").isNotNull() & tf.is_valid_isin(F.col("isin")))
        .withColumn(
            "instrument_id",
            F.when(F.col("isin_is_valid"), tf.surrogate_key(F.lit("ISIN"), F.col("isin")))
            .otherwise(F.col("listing_id")),
        )
        .withColumn(
            "instrument_key_source",
            F.when(F.col("isin_is_valid"), F.lit("ISIN")).otherwise(F.lit("SYMBOL_EXCHANGE")),
        )
        .withColumn(
            "status",
            F.when(F.col("delisted") == F.lit(True), F.lit("DELISTED")).otherwise(F.lit("ACTIVE")),
        )
        .withColumn("snapshot_date", F.to_date(F.lit(settings.snapshot_date)))
    ).cache()

    total = keyed.count()
    print("\n=== 3. Grain ===")
    grain = keyed.agg(
        F.countDistinct("listing_id").alias("listings"),
        F.countDistinct("instrument_id").alias("instruments"),
        F.countDistinct("isin").alias("isins"),
    ).first()
    print("  source rows        : {0:,}".format(total))
    print("  distinct listings  : {0:,}".format(grain["listings"]))
    print("  distinct instruments: {0:,}".format(grain["instruments"]))
    print("  distinct ISINs     : {0:,}".format(grain["isins"]))
    check("listing_id is near-unique", grain["listings"] >= total - 5,
          "{0} listings for {1} rows".format(grain["listings"], total))
    check("ISIN collapses cross-listings", grain["instruments"] < grain["listings"],
          "{0} instruments < {1} listings".format(grain["instruments"], grain["listings"]))

    print("\n=== 4. Name normalisation ===")
    samples = (
        keyed.where(F.col("instrument_name").isNotNull())
        .select("instrument_name", "normalised_name")
        .limit(5)
        .collect()
    )
    for row in samples:
        print("    {0!r:<52} -> {1!r}".format(row["instrument_name"][:48], row["normalised_name"]))
    check("normalisation produces values", all(r["normalised_name"] for r in samples))

    print("\n=== 5. Entity resolution ===")
    exchange_ranks = [
        (code, meta["primary_rank"], meta["country"], meta["venue_type"])
        for code, meta in cfg_exchange_items()
    ]
    ranks = spark.createDataFrame(
        exchange_ranks,
        "exchange_code string, primary_rank int, exchange_country string, venue_type string",
    )
    staged = (
        keyed.join(F.broadcast(ranks), keyed["exchange"] == F.col("exchange_code"), "left")
        .withColumn("primary_rank", F.coalesce(F.col("primary_rank"), F.lit(999)))
        .drop("exchange_code")
    )

    resolved = tf.resolve_entities(spark, staged).cache()
    entity_stats = (
        resolved.where(F.col("entity_id").isNotNull())
        .agg(
            F.countDistinct("entity_id").alias("entities"),
            F.countDistinct("instrument_id").alias("instruments"),
        )
        .first()
    )
    print("  entities resolved  : {0:,}".format(entity_stats["entities"]))
    print("  from instruments   : {0:,}".format(entity_stats["instruments"]))
    check("entities collapse instruments",
          entity_stats["entities"] < entity_stats["instruments"],
          "{0} entities from {1} instruments".format(
              entity_stats["entities"], entity_stats["instruments"]))

    method_counts = (
        resolved.groupBy("entity_resolution_method")
        .agg(F.count(F.lit(1)).alias("rows"))
        .orderBy(F.desc("rows"))
        .collect()
    )
    for row in method_counts:
        print("    {0:<18} {1:>8,}".format(row["entity_resolution_method"], row["rows"]))

    multi = (
        resolved.where(F.col("entity_id").isNotNull())
        .groupBy("entity_id")
        .agg(
            F.countDistinct("exchange").alias("venues"),
            F.sort_array(F.collect_set("exchange")).alias("venue_list"),
            F.first("instrument_name").alias("name"),
        )
        .where(F.col("venues") > 1)
    )
    multi_count = multi.count()
    print("  entities on >1 venue: {0:,}".format(multi_count))
    check("cross-listings detected", multi_count > 0)
    for row in multi.orderBy(F.desc("venues")).limit(5).collect():
        print("    {0:<44} {1} venues {2}".format(
            (row["name"] or "")[:42], row["venues"], row["venue_list"]))

    check("non-entity classes stay unmapped",
          resolved.where(
              F.col("asset_class").isin("INDEX", "CURRENCY", "CRYPTO")
              & F.col("entity_id").isNotNull()
          ).count() == 0)

    print("\n=== 6. Data quality ===")
    profiles = dq.DEFAULT_SCORING_PROFILES
    for asset_class, profile in sorted(profiles.items()):
        check("{0:<14} weights sum to 100".format(asset_class), sum(profile.values()) == 100,
              "got {0}".format(sum(profile.values())))

    base = resolved.withColumn("declared_asset_class", F.col("asset_class"))
    contextual = dq.with_quality_context(base, spark)
    assessed = dq.evaluate(contextual, dq.build_rules())
    scored = dq.score(assessed, profiles).cache()

    bands = (
        scored.groupBy("asset_class", "quality_band")
        .agg(F.count(F.lit(1)).alias("n"), F.round(F.avg("quality_score"), 1).alias("avg"))
        .orderBy("asset_class", "quality_band")
        .collect()
    )
    print("  {0:<14} {1:<12} {2:>8} {3:>7}".format("asset_class", "band", "records", "avg"))
    for row in bands:
        print("  {0:<14} {1:<12} {2:>8,} {3:>7}".format(
            row["asset_class"], row["quality_band"], row["n"], row["avg"]))

    bounds = scored.agg(
        F.min("quality_score").alias("lo"), F.max("quality_score").alias("hi")
    ).first()
    check("scores within 0-100", 0 <= bounds["lo"] and bounds["hi"] <= 100,
          "range {0}-{1}".format(bounds["lo"], bounds["hi"]))

    print("\n  rule failures:")
    failures = (
        scored.select(F.explode("dq_failures").alias("rule"))
        .groupBy("rule")
        .agg(F.count(F.lit(1)).alias("n"))
        .orderBy(F.desc("n"))
        .collect()
    )
    for row in failures:
        print("    {0:<32} {1:>8,}".format(row["rule"], row["n"]))

    quarantined = scored.where(F.col("is_quarantined")).count()
    print("\n  quarantined records : {0:,} of {1:,}".format(quarantined, total))
    check("quarantine is a minority", quarantined < total * 0.10,
          "{0:.2%} quarantined".format(quarantined / total))
    check("duplicate ECC caught",
          scored.where(F.col("symbol_exchange_occurrences") > 1).count() > 0)

    print("\n=== 7. Change detection (self-comparison must be empty) ===")
    compare_cols = list(schemas.TRACKED_COLUMNS.keys())
    snap = keyed.select("listing_id", *compare_cols)
    cur = snap.select([F.col(c).alias("cur_" + c) for c in snap.columns])
    prev = snap.select([F.col(c).alias("prev_" + c) for c in snap.columns])
    joined = cur.join(prev, F.col("cur_listing_id") == F.col("prev_listing_id"), "full_outer")
    stack_args = ", ".join("'{0}', prev_{0}, cur_{0}".format(c) for c in compare_cols)
    changes = (
        joined.select(
            F.col("cur_listing_id").alias("listing_id"),
            F.expr("stack({0}, {1}) as (column_changed, old_value, new_value)".format(
                len(compare_cols), stack_args)),
        )
        .where(~F.col("old_value").eqNullSafe(F.col("new_value")))
        .count()
    )
    check("identical snapshots produce zero changes", changes == 0,
          "{0} spurious changes".format(changes))

    print("\n" + "=" * 62)
    if FAILURES:
        print("FAILED ({0}):".format(len(FAILURES)))
        for failure in FAILURES:
            print("  - " + failure)
        return 1
    print("All checks passed.")
    return 0


def cfg_exchange_items():
    from fuct import reference as ref

    return sorted(ref.EXCHANGE_REFERENCE.items())


if __name__ == "__main__":
    sys.exit(main())
