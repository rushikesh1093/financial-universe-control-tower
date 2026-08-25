"""Canonical schema definitions and source-to-canonical column mapping.

The seven source datasets have five different column layouts.  Rather than
union them (which would produce a very wide, mostly-null table whose meaning
depends on which file a row came from), every source is projected onto one
canonical staging schema.  Columns that an asset class does not carry are
explicitly ``NULL`` and the data-quality layer knows not to penalise them.
"""

from __future__ import annotations

from typing import Dict, List

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from . import config as cfg

# --------------------------------------------------------------------------
# Bronze
# --------------------------------------------------------------------------

#: Lineage columns stamped onto every bronze row.  These satisfy the
#: traceability requirement: a curated record can always be walked back to the
#: exact file, snapshot and pipeline run that produced it.
BRONZE_AUDIT_COLUMNS: List[str] = [
    "source_dataset",
    "source_file",
    "source_url",
    "source_version",
    "asset_class",
    "ingestion_timestamp",
    "pipeline_run_id",
    "snapshot_date",
]


def add_bronze_audit_columns(
    df: DataFrame,
    dataset: str,
    asset_class: str,
    settings: "cfg.Settings",
) -> DataFrame:
    """Stamp source lineage onto a raw source DataFrame."""
    # File lineage comes from `_metadata.file_path`, not `input_file_name()`,
    # which Unity Catalog blocks with UC_COMMAND_NOT_SUPPORTED. `_metadata` is a
    # *hidden* column that only exists on a DataFrame still backed directly by
    # files - it does not survive a union - so the caller is expected to have
    # materialised it per file already. Fall back to reading it here only when
    # the DataFrame is still file-backed.
    source_file = (
        F.col("source_file")
        if "source_file" in df.columns
        else F.col("_metadata.file_path")
    )
    return (
        df.withColumn("source_dataset", F.lit(dataset))
        .withColumn("source_file", source_file)
        .withColumn("source_url", F.lit(cfg.SOURCE_URL))
        .withColumn("source_version", F.lit(settings.source_version))
        .withColumn("asset_class", F.lit(asset_class))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(settings.pipeline_run_id))
        .withColumn("snapshot_date", F.to_date(F.lit(settings.snapshot_date)))
    )


# --------------------------------------------------------------------------
# Canonical staging schema (the "one shape" every asset class lands in)
# --------------------------------------------------------------------------

#: Canonical column -> the source column it is read from, when present.
#: A canonical column absent from a given source is filled with NULL.
CANONICAL_COLUMNS: List[str] = [
    # identity
    "symbol",
    "instrument_name",
    "summary",
    # venue
    "exchange",
    "mic",
    "market",
    "country",
    # money
    "currency",
    # GICS-style classification (equities)
    "sector",
    "industry_group",
    "industry",
    # category taxonomy (ETFs / funds / indices)
    "category_group",
    "category",
    "family",
    # asset-class specific
    "base_currency",
    "quote_currency",
    "crypto_asset",
    # identifiers
    "isin",
    "cusip",
    "figi",
    "composite_figi",
    "shareclass_figi",
    # descriptive
    "market_cap",
    "website",
    "city",
    "state",
    "zipcode",
    "delisted",
]

#: Source column renames.  Anything not listed keeps its source name.
SOURCE_RENAMES: Dict[str, str] = {
    "name": "instrument_name",
    "cryptocurrency": "crypto_asset",
}


def to_canonical(df: DataFrame, asset_class: str) -> DataFrame:
    """Project a raw source DataFrame onto the canonical staging schema.

    Missing columns are added as typed NULLs so that every asset class produces
    an identical shape and the downstream union is safe.
    """
    renamed = df
    for src, dst in SOURCE_RENAMES.items():
        if src in renamed.columns and dst not in renamed.columns:
            renamed = renamed.withColumnRenamed(src, dst)

    present = set(renamed.columns)
    projection = []
    for col in CANONICAL_COLUMNS:
        if col in present:
            if col == "delisted":
                projection.append(_parse_bool(F.col(col)).alias(col))
            else:
                projection.append(_clean_string(F.col(col)).alias(col))
        else:
            cast = "boolean" if col == "delisted" else "string"
            projection.append(F.lit(None).cast(cast).alias(col))

    keep_audit = [c for c in BRONZE_AUDIT_COLUMNS if c in present]
    return renamed.select(*projection, *keep_audit).withColumn(
        "asset_class", F.lit(asset_class)
    )


def _clean_string(col):
    """Trim, and fold the many spellings of "no value" to a real NULL.

    The source uses empty strings, whitespace, and the literal strings
    ``nan``/``None``/``null`` interchangeably.  Normalising them here means the
    completeness checks measure real missingness rather than formatting.
    """
    trimmed = F.trim(col.cast(StringType()))
    return F.when(
        (trimmed == "")
        | (F.lower(trimmed).isin("nan", "none", "null", "n/a", "na", "-")),
        F.lit(None).cast(StringType()),
    ).otherwise(trimmed)


def _parse_bool(col):
    trimmed = F.lower(F.trim(col.cast(StringType())))
    return (
        F.when(trimmed.isin("true", "t", "yes", "y", "1"), F.lit(True))
        .when(trimmed.isin("false", "f", "no", "n", "0"), F.lit(False))
        .otherwise(F.lit(None).cast(BooleanType()))
    )


# --------------------------------------------------------------------------
# Silver / gold table schemas
# --------------------------------------------------------------------------

#: Columns whose change between two snapshots is tracked at column level by the
#: change-detection layer, mapped to the change_type they raise.
TRACKED_COLUMNS: Dict[str, str] = {
    "instrument_name": "MODIFIED",
    "currency": "MODIFIED",
    "country": "MODIFIED",
    "status": "MODIFIED",
    "sector": "RECLASSIFIED",
    "industry_group": "RECLASSIFIED",
    "industry": "RECLASSIFIED",
    "category_group": "RECLASSIFIED",
    "category": "RECLASSIFIED",
    "exchange": "RELISTED",
    "mic": "RELISTED",
    "market": "RELISTED",
    "isin": "IDENTIFIER_CHANGED",
    "cusip": "IDENTIFIER_CHANGED",
    "figi": "IDENTIFIER_CHANGED",
    "composite_figi": "IDENTIFIER_CHANGED",
    "shareclass_figi": "IDENTIFIER_CHANGED",
}

QUARANTINE_SCHEMA = StructType(
    [
        StructField("record_id", StringType(), False),
        StructField("source_dataset", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("source_snapshot", StringType(), True),
        StructField("asset_class", StringType(), True),
        StructField("failure_reason", StringType(), False),
        StructField("failed_column", StringType(), True),
        StructField("failure_detail", StringType(), True),
        StructField("original_record", StringType(), True),
        StructField("pipeline_run_id", StringType(), True),
        StructField("quarantine_timestamp", TimestampType(), True),
    ]
)
