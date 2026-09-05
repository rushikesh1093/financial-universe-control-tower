"""Pipeline observability: run log, stage metrics and lineage.

Every notebook stage opens a :class:`StageRun`, which writes one row per stage
into ``audit.pipeline_run_log`` with the counts the brief asks for (records
read / written / rejected / modified, quality score, status, timings).  A
failure is recorded with its traceback before being re-raised, so a failed run
is still observable rather than vanishing.
"""

from __future__ import annotations

import time
import traceback
from typing import Optional

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from . import config as cfg

RUN_LOG_SCHEMA = StructType(
    [
        StructField("pipeline_run_id", StringType(), False),
        StructField("stage", StringType(), False),
        StructField("snapshot_date", StringType(), True),
        StructField("status", StringType(), False),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("records_read", LongType(), True),
        StructField("records_written", LongType(), True),
        StructField("records_rejected", LongType(), True),
        StructField("records_modified", LongType(), True),
        StructField("quality_score", DoubleType(), True),
        StructField("target_table", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("logged_at", TimestampType(), False),
    ]
)


def ensure_run_log(spark: SparkSession, settings: "cfg.Settings") -> str:
    table = settings.audit("pipeline_run_log").replace("`", "")
    if not spark.catalog.tableExists(table):
        spark.createDataFrame([], RUN_LOG_SCHEMA).write.format("delta").saveAsTable(table)
    return table


class StageRun:
    """Context manager that times a stage and records its outcome.

    Usage::

        with audit.StageRun(spark, settings, "02_bronze_ingestion") as run:
            run.records_read = df.count()
            ...
            run.records_written = written
    """

    def __init__(
        self,
        spark: SparkSession,
        settings: "cfg.Settings",
        stage: str,
        target_table: Optional[str] = None,
    ):
        self.spark = spark
        self.settings = settings
        self.stage = stage
        self.target_table = target_table
        self.records_read: Optional[int] = None
        self.records_written: Optional[int] = None
        self.records_rejected: Optional[int] = None
        self.records_modified: Optional[int] = None
        self.quality_score: Optional[float] = None
        self._start_wall = None
        self._start_perf = None

    def __enter__(self) -> "StageRun":
        ensure_run_log(self.spark, self.settings)
        self._start_wall = time.time()
        self._start_perf = time.perf_counter()
        print("[{0}] started (run_id={1})".format(self.stage, self.settings.pipeline_run_id))
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        duration = time.perf_counter() - self._start_perf
        status = "SUCCEEDED" if exc_type is None else "FAILED"
        error = None
        if exc_type is not None:
            error = "".join(traceback.format_exception_only(exc_type, exc)).strip()

        row = [
            (
                self.settings.pipeline_run_id,
                self.stage,
                self.settings.snapshot_date,
                status,
                _ts(self._start_wall),
                _ts(time.time()),
                float(round(duration, 3)),
                _long(self.records_read),
                _long(self.records_written),
                _long(self.records_rejected),
                _long(self.records_modified),
                float(self.quality_score) if self.quality_score is not None else None,
                self.target_table,
                error,
                _ts(time.time()),
            )
        ]
        (
            self.spark.createDataFrame(row, RUN_LOG_SCHEMA)
            .write.format("delta")
            .mode("append")
            .saveAsTable(self.settings.audit("pipeline_run_log").replace("`", ""))
        )
        print(
            "[{0}] {1} in {2:.1f}s  read={3} written={4} rejected={5}".format(
                self.stage,
                status,
                duration,
                self.records_read,
                self.records_written,
                self.records_rejected,
            )
        )
        # Never swallow the exception - the job must still fail loudly.
        return False


def _ts(epoch: Optional[float]):
    import datetime as _dt

    return _dt.datetime.fromtimestamp(epoch) if epoch else None


def _long(value: Optional[int]):
    return int(value) if value is not None else None


def write_quarantine(
    spark: SparkSession,
    settings: "cfg.Settings",
    df,
    reason_col: str = "failure_reason",
) -> int:
    """Append rejected records to the quarantine layer.

    Invalid records are isolated with their reason and the full original
    record, never dropped, so a data steward can review and reprocess them.
    """
    table = settings.quarantine("rejected_records").replace("`", "")
    count = df.count()
    if not count:
        # No quarantine records for this snapshot. Still ensure the table exists
        # so downstream queries (e.g. Power BI, audit joins) never hit
        # AnalysisException on an empty first run. Create a zero-row shell table.
        if not spark.catalog.tableExists(table):
            from pyspark.sql.types import (
                DoubleType, StringType, StructField, StructType, TimestampType,
            )
            empty_schema = StructType([
                StructField("record_id", StringType(), True),
                StructField("source_dataset", StringType(), True),
                StructField("source_file", StringType(), True),
                StructField("source_snapshot", StringType(), True),
                StructField("asset_class", StringType(), True),
                StructField("failure_reason", StringType(), True),
                StructField("failed_column", StringType(), True),
                StructField("failure_detail", StringType(), True),
                StructField("original_record", StringType(), True),
                StructField("pipeline_run_id", StringType(), True),
                StructField("quarantine_timestamp", TimestampType(), True),
                StructField("snapshot_date", StringType(), True),
            ])
            create = (
                spark.createDataFrame([], empty_schema)
                .write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("snapshot_date")
            )
            location = settings.quarantine_location("rejected_records")
            if location:
                create = create.option("path", location)
            create.saveAsTable(table)
        return 0

    # Replace this snapshot's partition rather than appending. An append makes
    # the stage non-idempotent: re-running the same snapshot stacks another full
    # copy of its rejects, which is how 15,534 duplicate records became 77,680
    # across five runs. Earlier snapshots are untouched.
    #
    # Ensure snapshot_date in the replaceWhere predicate is always a plain
    # yyyy-MM-dd string. If the Silver table stored snapshot_date as a
    # TimestampType, the column arrives here as "2026-09-04 00:00:00" after
    # cast("string"), which would never match the date-only predicate.
    safe_snapshot_date = settings.snapshot_date[:10]  # "yyyy-MM-dd"

    if spark.catalog.tableExists(table):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option(
                "replaceWhere",
                "snapshot_date = '{0}'".format(safe_snapshot_date),
            )
            .option("mergeSchema", "true")
            .saveAsTable(table)
        )
    else:
        create = (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("snapshot_date")
        )
        location = settings.quarantine_location("rejected_records")
        if location:
            # `path` on the creating write registers an EXTERNAL table, so the
            # rejects land in the project's own lake container.
            create = create.option("path", location)
        create.saveAsTable(table)
    return count
