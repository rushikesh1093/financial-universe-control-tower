"""Core transformations: keys, normalisation, entity resolution and SCD Type 2.

The two pieces worth reading carefully are :func:`resolve_entities` (how a
company is distinguished from an instrument and from an exchange listing) and
:func:`scd2_merge` (how classification history is preserved).
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from . import config as cfg
from . import writer as _writer

# --------------------------------------------------------------------------
# Deterministic surrogate keys
# --------------------------------------------------------------------------
#
# Keys are content-derived hashes rather than monotonically-increasing ids.
# That is what makes the pipeline idempotent and reproducible: re-running the
# same snapshot regenerates byte-identical keys, so a MERGE updates rather than
# duplicating, and a key can be recomputed from source at any time.

_KEY_NULL = "NULL"


def surrogate_key(*cols) -> "F.Column":
    """A stable 64-hex-character key over the given columns.

    NULL is folded to a sentinel rather than the empty string so that
    ``("A", None)`` and ``("A", "")`` do not collide.
    """
    parts = [F.coalesce(c.cast("string"), F.lit(_KEY_NULL)) for c in cols]
    return F.sha2(F.concat_ws("", *parts), 256)


def row_hash(cols: Sequence[str]) -> "F.Column":
    """Hash of the given column values, used for change detection."""
    return surrogate_key(*[F.col(c) for c in cols])


# --------------------------------------------------------------------------
# Normalisation
# --------------------------------------------------------------------------

#: Legal-form suffixes stripped before comparing company names.  The source
#: renders the same issuer as "Alcoa Corporation" on NYQ and "ALCOA CORP" on
#: BER, so suffix removal materially improves entity matching.
_LEGAL_SUFFIXES = [
    "INCORPORATED", "INC", "CORPORATION", "CORP", "COMPANY", "LIMITED", "LTD",
    "PUBLIC LIMITED COMPANY", "PLC", "LLC", "LLP", "LP",
    "AKTIENGESELLSCHAFT", "AG", "KGAA", "GMBH", "SE",
    "NAAMLOZE VENNOOTSCHAP", "NV", "BV", "SA", "SAS", "SARL", "SPA", "SRL",
    "AB", "ASA", "AS", "OYJ", "OY", "APS",
    "PT", "TBK", "BHD", "PCL", "JSC", "PJSC", "OAO", "PAO",
    "TRUST", "GROUP", "HOLDINGS", "HOLDING",
    "ADR", "GDR", "SPONSORED ADR", "SPON ADR",
]

#: Share-class / listing decoration stripped from names.
_CLASS_TOKENS = [
    "CLASS A", "CLASS B", "CLASS C", "CL A", "CL B", "CL C",
    "ORDINARY SHARES", "ORD SHS", "ORD", "REGISTERED SHARES", "REG SHS",
    "COMMON STOCK", "COM STK", "SHS", "NPV", "DL", "EO", "SF", "YC",
]


def normalise_name(col) -> "F.Column":
    """Fold a company / instrument name to a comparable form.

    Upper-cases, removes punctuation and legal-form suffixes, and collapses
    whitespace.  Deliberately conservative: it does not attempt fuzzy matching,
    because a false merge of two issuers is far more damaging downstream than a
    missed one (a missed one simply stays a separate entity).
    """
    out = F.upper(F.trim(col))
    # Drop bracketed and trailing qualifiers such as "(Class A)" or "- DL -,001".
    out = F.regexp_replace(out, r"\([^)]*\)", " ")
    out = F.regexp_replace(out, r"[^A-Z0-9 ]", " ")
    for token in _CLASS_TOKENS:
        out = F.regexp_replace(out, r"(?<![A-Z0-9])" + token + r"(?![A-Z0-9])", " ")
    for suffix in _LEGAL_SUFFIXES:
        out = F.regexp_replace(out, r"(?<![A-Z0-9])" + suffix + r"(?![A-Z0-9])", " ")
    out = F.trim(F.regexp_replace(out, r"\s+", " "))
    return F.when(out == "", F.lit(None)).otherwise(out)


def symbol_root(col) -> "F.Column":
    """Strip the venue suffix from a ticker: ``07G.DE`` -> ``07G``.

    Only a short alphabetic suffix is removed, so ``BRK.B`` (a share class) and
    ``AAVE-CAD`` (a crypto pair) are left intact.
    """
    upper = F.upper(F.trim(col))
    return F.when(
        upper.rlike(r"^[^.]+\.[A-Z]{1,3}$"), F.regexp_extract(upper, r"^([^.]+)\.[A-Z]{1,3}$", 1)
    ).otherwise(upper)


def clean_identifier(col) -> "F.Column":
    """Upper-case an identifier and drop obviously non-conforming values."""
    out = F.upper(F.regexp_replace(F.trim(col), r"[^A-Za-z0-9]", ""))
    return F.when((out == "") | (F.length(out) < 6), F.lit(None)).otherwise(out)


# --- identifier format validation -----------------------------------------

def is_valid_isin(col) -> "F.Column":
    """ISIN: 2 country letters + 9 alphanumerics + 1 check digit."""
    return col.rlike(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def is_valid_cusip(col) -> "F.Column":
    return col.rlike(r"^[A-Z0-9]{9}$")


def is_valid_figi(col) -> "F.Column":
    """FIGI: 'BBG' + 8 consonant-only alphanumerics + check digit."""
    return col.rlike(r"^BBG[A-Z0-9]{8}[0-9]$")


# --------------------------------------------------------------------------
# Entity resolution
# --------------------------------------------------------------------------
#
# Three distinct concepts, deliberately kept apart:
#
#   ENTITY      the issuing company            (Alcoa Corporation)
#   INSTRUMENT  the security it issued         (Alcoa ordinary shares, ISIN US0138721065)
#   LISTING     where that security trades     (NYQ:AA, BER:ALU, FRA:ALU)
#
# Collapsing them would mean a company listed on four venues counts as four
# companies, which is exactly the failure mode the brief calls out.
#
# Resolution is a connected-components pass over an evidence graph.  Two rows
# are linked when they share a strong identifier (ISIN, composite FIGI) or a
# normalised name within the same country of domicile.  Transitive closure then
# merges chains: NYQ<->BER via ISIN and BER<->FRA via name become one entity.
#
# The components are computed with union-find on the driver rather than with an
# iterative Spark label-propagation.  Propagation needs to cache each round to
# avoid recomputing its lineage, and caching is unavailable on serverless
# compute; union-find also terminates exactly instead of after N rounds.


def blocking_keys(df: DataFrame, node_col: str = "instrument_id") -> DataFrame:
    """Emit ``(node, bkey, evidence)`` for every piece of same-issuer evidence.

    Blocking keys with an implausible number of members are dropped: a key
    "shared" by hundreds of rows is a data defect (a generic name, a placeholder
    identifier), not evidence of one gigantic company.

    Note this returns *memberships*, not pairwise edges. A block of 64 members
    is 64 rows here but would be 4,032 edges - the membership form is what keeps
    the graph small enough to resolve on the driver.
    """
    max_block = 64

    def _keys_for(key_col: "F.Column", evidence: str) -> DataFrame:
        blocked = df.select(
            F.col(node_col).alias("node"), key_col.alias("bkey")
        ).where(F.col("bkey").isNotNull())
        return (
            blocked.withColumn(
                "block_size", F.count("*").over(Window.partitionBy("bkey"))
            )
            .where((F.col("block_size") > 1) & (F.col("block_size") <= max_block))
            .select("node", "bkey", F.lit(evidence).alias("evidence"))
        )

    isin = _keys_for(F.col("isin"), "ISIN")
    figi = _keys_for(F.col("composite_figi"), "COMPOSITE_FIGI")
    name = _keys_for(
        F.when(
            F.col("normalised_name").isNotNull() & F.col("country").isNotNull(),
            F.concat_ws("|", F.col("normalised_name"), F.col("country")),
        ),
        "NAME_COUNTRY",
    )
    return isin.unionByName(figi).unionByName(name).distinct()


def _union_find(memberships) -> dict:
    """Group nodes sharing any blocking key into components.

    Classic union-find with path compression and union by rank. Chosen over an
    iterative Spark label-propagation because that needs to cache each round to
    avoid recomputing its lineage, and caching is unavailable on serverless
    compute. The membership set here is a few hundred thousand short strings at
    most, so resolving it on the driver is both cheaper and exact - no iteration
    limit to converge within.
    """
    parent: dict = {}
    rank: dict = {}

    def find(x):
        parent.setdefault(x, x)
        rank.setdefault(x, 0)
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:          # path compression
            parent[x], x = root, parent[x]
        return root

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if rank[ra] < rank[rb]:
            ra, rb = rb, ra
        parent[rb] = ra
        if rank[ra] == rank[rb]:
            rank[ra] += 1

    by_key: dict = {}
    for node, bkey, _evidence in memberships:
        by_key.setdefault(bkey, []).append(node)

    for members in by_key.values():
        first = members[0]
        for other in members[1:]:
            union(first, other)

    # The union-find root is whichever node happened to win the union-by-rank
    # contest, which depends on the order rows arrived in - and Spark does not
    # guarantee collect() order. Using it directly would make entity_id vary
    # between runs of the *same* snapshot, so SCD2 would see a change every run
    # and endlessly close and reopen rows.
    #
    # Re-label each component by the smallest node id it contains. That is a
    # property of the set, not of the traversal, so it is stable across runs.
    roots = {node: find(node) for node in parent}
    canonical: dict = {}
    for node, root in roots.items():
        if root not in canonical or node < canonical[root]:
            canonical[root] = node
    return {node: canonical[root] for node, root in roots.items()}


def resolve_entities(spark: SparkSession, instruments: DataFrame) -> DataFrame:
    """Attach ``entity_id`` and ``entity_resolution_method`` to instruments.

    Only entity-bearing asset classes participate. Indices, FX pairs and crypto
    pairs have no issuer, so they keep a NULL entity rather than an invented one
    - and the gold layer reports them under "instruments that cannot be mapped
    to a canonical entity".
    """
    entity_bearing = instruments.where(
        F.col("asset_class").isin(*sorted(cfg.ENTITY_BEARING))
    )
    other = instruments.where(~F.col("asset_class").isin(*sorted(cfg.ENTITY_BEARING)))

    memberships = [
        (r["node"], r["bkey"], r["evidence"])
        for r in blocking_keys(entity_bearing).collect()
    ]
    component_of = _union_find(memberships)

    # Strongest evidence per node, for reporting how each entity was resolved.
    precedence = {"ISIN": 0, "COMPOSITE_FIGI": 1, "NAME_COUNTRY": 2}
    best: dict = {}
    for node, _bkey, evidence in memberships:
        if node not in best or precedence[evidence] < precedence[best[node]]:
            best[node] = evidence

    if component_of:
        mapping = spark.createDataFrame(
            [(n, c, best.get(n, "SINGLETON")) for n, c in component_of.items()],
            "instrument_id string, component_id string, evidence string",
        )
    else:
        mapping = spark.createDataFrame(
            [], "instrument_id string, component_id string, evidence string"
        )

    resolved = (
        entity_bearing.join(F.broadcast(mapping), "instrument_id", "left")
        # A node in no block is its own entity - a company with a single listing
        # is still a company.
        .withColumn(
            "component_id", F.coalesce(F.col("component_id"), F.col("instrument_id"))
        )
        .withColumn(
            "entity_resolution_method",
            F.coalesce(F.col("evidence"), F.lit("SINGLETON")),
        )
        .withColumn("entity_id", surrogate_key(F.col("component_id")))
        .drop("component_id", "evidence")
    )

    unresolved = other.withColumn(
        "entity_id", F.lit(None).cast("string")
    ).withColumn("entity_resolution_method", F.lit("NOT_APPLICABLE"))

    return resolved.unionByName(unresolved)


def choose_entity_attributes(resolved: DataFrame) -> DataFrame:
    """Pick one canonical name / country / type per resolved entity.

    The winning name is the one appearing on the highest-ranked venue, breaking
    ties on the longest name (the German venues abbreviate aggressively, so the
    longer rendering is almost always the fuller legal name) and finally on the
    name itself so the result is deterministic.
    """
    ranked = Window.partitionBy("entity_id").orderBy(
        F.col("primary_rank").asc_nulls_last(),
        F.length(F.col("instrument_name")).desc_nulls_last(),
        F.col("instrument_name").asc_nulls_last(),
    )
    return (
        resolved.where(F.col("entity_id").isNotNull())
        .withColumn("_rn", F.row_number().over(ranked))
        .where(F.col("_rn") == 1)
        .select(
            "entity_id",
            F.col("instrument_name").alias("entity_name"),
            F.col("normalised_name").alias("entity_name_normalised"),
            "country",
            F.when(F.col("asset_class") == cfg.EQUITY, F.lit("CORPORATE"))
            .when(F.col("asset_class").isin(cfg.FUND, cfg.ETF), F.lit("FUND_ISSUER"))
            .when(F.col("asset_class") == cfg.MONEY_MARKET, F.lit("FUND_ISSUER"))
            .otherwise(F.lit("UNKNOWN"))
            .alias("entity_type"),
        )
    )


# --------------------------------------------------------------------------
# Slowly Changing Dimension Type 2
# --------------------------------------------------------------------------


def scd2_merge(
    spark: SparkSession,
    target_table: str,
    updates: DataFrame,
    business_key: Sequence[str],
    tracked_columns: Sequence[str],
    effective_date: str,
    close_absent: bool = True,
    location: Optional[str] = None,
) -> dict:
    """Apply an SCD Type 2 upsert of ``updates`` into ``target_table``.

    A row version is closed and superseded only when one of ``tracked_columns``
    actually changes, so re-running the same snapshot is a no-op - which is
    what makes the pipeline idempotent.

    ``close_absent`` also expires versions whose business key has disappeared
    from the source, so a delisted instrument stops being ``is_current``
    instead of lingering forever.

    Returns a dict of row counts for the audit log.
    """
    from delta.tables import DeltaTable

    end_of_time = cfg.SCD_END_OF_TIME
    key_cols = list(business_key)
    tracked = list(tracked_columns)

    staged = (
        updates.withColumn("row_hash", row_hash(tracked))
        .withColumn("effective_from", F.to_date(F.lit(effective_date)))
        .withColumn("effective_to", F.to_date(F.lit(end_of_time)))
        .withColumn("is_current", F.lit(True))
    )

    # A MERGE aborts with DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
    # if two source rows match the same target row, so the business key must be
    # unique in the source. It is not always: this source genuinely contains the
    # same symbol twice on the same exchange (ECC on NYQ), which makes
    # listing_id non-unique.
    #
    # Collapsing to one row per key here is a presentation decision, not a way
    # of hiding the problem - the duplicate is independently detected by the
    # DUPLICATE_INSTRUMENT rule and lands in quarantine with its full original
    # record. `row_hash` orders the tie so the survivor is the same on every
    # run, which keeps the pipeline reproducible.
    dedup = Window.partitionBy(*key_cols).orderBy(F.col("row_hash").asc())
    deduped = (
        staged.withColumn("_dedup_rn", F.row_number().over(dedup))
        .where(F.col("_dedup_rn") == 1)
        .drop("_dedup_rn")
    )
    duplicates_collapsed = staged.count() - deduped.count()
    if duplicates_collapsed:
        print(
            "  {0}: collapsed {1} duplicate row(s) on {2} before merging".format(
                target_table.replace("`", ""), duplicates_collapsed, key_cols
            )
        )
    staged = deduped

    if location:
        # Heal a location left behind by a task that died between writing the
        # Delta files and registering the table.
        _writer.adopt_orphaned_location(spark, target_table, location)

    if not spark.catalog.tableExists(target_table.replace("`", "")):
        create = staged.write.format("delta")
        if location:
            # `path` on the creating write is what registers this as an
            # EXTERNAL table rooted in the project's own storage container.
            create = create.option("path", location)
        create.saveAsTable(target_table.replace("`", ""))
        return {
            "inserted": staged.count(),
            "closed": 0,
            "expired_absent": 0,
            "unchanged": 0,
            "duplicates_collapsed": duplicates_collapsed,
        }

    delta_target = DeltaTable.forName(spark, target_table.replace("`", ""))
    current = delta_target.toDF().where(F.col("is_current"))

    # Plain qualified names rather than backticked ones: the business keys here
    # are simple identifiers, and `col("s.`k`")` relies on the parser handling a
    # quoted identifier inside a qualified reference.
    join_cond = [F.col("s." + k) == F.col("t." + k) for k in key_cols]
    comparison = (
        staged.alias("s")
        .join(current.alias("t"), join_cond, "left")
        .select(
            *[F.col("s." + k).alias(k) for k in key_cols],
            F.col("s.row_hash").alias("new_hash"),
            F.col("t.row_hash").alias("old_hash"),
        )
    )
    changed_keys = comparison.where(
        F.col("old_hash").isNull() | (F.col("old_hash") != F.col("new_hash"))
    ).select(*key_cols)
    unchanged_count = comparison.where(
        F.col("old_hash").isNotNull() & (F.col("old_hash") == F.col("new_hash"))
    ).count()

    # `to_write` is derived from the target table and is used again *after* the
    # MERGE below has modified it. That is safe, but only for a reason worth
    # stating, because caching it to freeze the value is not an option on
    # serverless compute:
    #
    #   - a changed key has its current row closed by the MERGE, so on
    #     re-evaluation it finds no current row, old_hash is NULL, and it is
    #     still classified as changed;
    #   - an unchanged key keeps its current row with an equal hash, so it is
    #     still classified as unchanged.
    #
    # The set is therefore a fixed point of the MERGE. Do not add a filter here
    # that is not also a fixed point of it.
    to_write = staged.join(F.broadcast(changed_keys), key_cols, "left_semi")
    insert_count = to_write.count()

    # Step 1 - close the superseded current rows.
    merge_condition = " AND ".join(
        "t.`{0}` = s.`{0}`".format(k) for k in key_cols
    )
    (
        delta_target.alias("t")
        .merge(to_write.alias("s"), merge_condition + " AND t.is_current = true")
        .whenMatchedUpdate(
            set={
                "is_current": F.lit(False),
                "effective_to": F.date_sub(F.to_date(F.lit(effective_date)), 1),
            }
        )
        .execute()
    )

    # Step 2 - append the new versions.
    to_write.write.format("delta").mode("append").saveAsTable(
        target_table.replace("`", "")
    )

    expired_absent = 0
    if close_absent:
        absent = (
            delta_target.toDF()
            .where(F.col("is_current"))
            .join(staged.select(*key_cols).distinct(), key_cols, "left_anti")
            .select(*key_cols)
        )
        expired_absent = absent.count()
        if expired_absent:
            (
                delta_target.alias("t")
                .merge(absent.alias("s"), merge_condition + " AND t.is_current = true")
                .whenMatchedUpdate(
                    set={
                        "is_current": F.lit(False),
                        "effective_to": F.date_sub(
                            F.to_date(F.lit(effective_date)), 1
                        ),
                    }
                )
                .execute()
            )

    return {
        "inserted": insert_count,
        "closed": insert_count,
        "expired_absent": expired_absent,
        "unchanged": unchanged_count,
        "duplicates_collapsed": duplicates_collapsed,
    }
