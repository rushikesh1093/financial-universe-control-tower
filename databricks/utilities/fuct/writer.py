"""Table writing helpers that honour an external storage location.

Every silver/gold table goes through :func:`save`. When the run supplies a
``silver_root`` / ``gold_root``, the table is registered in Unity Catalog as an
**external** Delta table whose files live under that ADLS path; otherwise it is
a managed table. Nothing else in the pipeline needs to know which it is.

Registering as an external table (rather than just writing files to the path)
means the data is both stored in the project's own container *and* queryable as
``catalog.schema.table``, so downstream SQL does not have to reference URLs.
"""

from __future__ import annotations

from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession


def _plain(table: str) -> str:
    return table.replace("`", "")


def adopt_orphaned_location(spark: SparkSession, table: str, location: str) -> bool:
    """Re-attach a table whose data survived but whose registration did not.

    When a task dies between writing Delta files and registering the table, the
    catalog has no table but the path holds a complete Delta log. The next run
    then fails with DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION, and every retry
    fails the same way - the pipeline cannot heal itself.

    If the location is a valid Delta table, register it and let the normal
    overwrite proceed. If it holds something that is *not* Delta, do nothing:
    that is unowned data and silently overwriting it would be destructive.

    Returns True if a table was adopted.
    """
    name = _plain(table)
    if spark.catalog.tableExists(name):
        return False
    try:
        from delta.tables import DeltaTable

        if not DeltaTable.isDeltaTable(spark, location):
            return False
    except Exception:  # noqa: BLE001 - path unreadable or absent: nothing to adopt
        return False

    spark.sql(
        "CREATE TABLE IF NOT EXISTS {0} USING DELTA LOCATION '{1}'".format(name, location)
    )
    print("adopted orphaned Delta location for {0}: {1}".format(name, location))
    return True


def save(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    location: Optional[str] = None,
    partition_by: Optional[Sequence[str]] = None,
    replace_where: Optional[str] = None,
) -> int:
    """Write ``df`` to ``table``, replacing either the whole table or one partition.

    ``replace_where`` makes a re-run idempotent: only the matching partition is
    rewritten and every other snapshot is left untouched. It is only applied
    when the table already exists, because ``replaceWhere`` on a first write
    has nothing to replace.
    """
    name = _plain(table)
    if location:
        adopt_orphaned_location(spark, table, location)
    exists = spark.catalog.tableExists(name)

    writer = df.write.format("delta").mode("overwrite")

    if exists and replace_where:
        writer = writer.option("replaceWhere", replace_where).option("mergeSchema", "true")
    else:
        writer = writer.option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        # `path` on a create is what makes the table EXTERNAL. It must not be
        # passed on a replaceWhere write, and must not change once the table
        # exists, so it is only set on the creating write.
        if location and not exists:
            writer = writer.option("path", location)

    writer.saveAsTable(name)
    return spark.table(name).count()


def describe_location(spark: SparkSession, table: str) -> str:
    """Return the physical location of a table, for the run log."""
    try:
        rows = spark.sql("DESCRIBE DETAIL {0}".format(table)).select("location").collect()
        return rows[0]["location"] if rows else ""
    except Exception:  # noqa: BLE001
        return ""
