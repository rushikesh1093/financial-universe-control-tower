"""Configurable data-quality rules, scoring model and quarantine routing.

Two design points from the brief drive this module:

1. **Scoring is configuration, not code.**  Weights live in a Delta config
   table (seeded from :data:`DEFAULT_SCORING_PROFILES`) and are read at run
   time, so tuning the model does not mean editing a transformation.

2. **Weights are asset-class aware.**  Applying the equity weighting to every
   asset class would be misleading: an FX pair has no ISIN and an index has no
   country of domicile, so a single fixed profile would score the entire
   non-equity universe as "quarantine" for attributes that can never exist.
   Each asset class gets a profile over the components that genuinely apply to
   it, and every profile sums to 100.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from . import config as cfg
from . import reference as ref
from . import transforms as tf

# --------------------------------------------------------------------------
# Quality dimensions and failure reasons
# --------------------------------------------------------------------------

COMPLETENESS = "COMPLETENESS"
UNIQUENESS = "UNIQUENESS"
CONSISTENCY = "CONSISTENCY"
VALIDITY = "VALIDITY"
REFERENTIAL = "REFERENTIAL_INTEGRITY"

SEVERITY_QUARANTINE = "QUARANTINE"  # record cannot be trusted downstream
SEVERITY_REVIEW = "REVIEW"          # record is usable but flagged

# Failure reasons emitted into the quarantine layer.
MISSING_SYMBOL = "MISSING_SYMBOL"
MISSING_NAME = "MISSING_NAME"
MISSING_EXCHANGE = "MISSING_EXCHANGE"
MISSING_CURRENCY = "MISSING_CURRENCY"
MISSING_COUNTRY = "MISSING_COUNTRY"
MISSING_IDENTIFIER = "MISSING_IDENTIFIER"
INVALID_IDENTIFIER = "INVALID_IDENTIFIER"
DUPLICATE_IDENTIFIER = "DUPLICATE_IDENTIFIER"
DUPLICATE_INSTRUMENT = "DUPLICATE_INSTRUMENT"
UNKNOWN_EXCHANGE = "UNKNOWN_EXCHANGE"
UNKNOWN_COUNTRY = "UNKNOWN_COUNTRY"
UNKNOWN_CURRENCY = "UNKNOWN_CURRENCY"
EXCHANGE_COUNTRY_CONFLICT = "EXCHANGE_COUNTRY_CONFLICT"
CLASSIFICATION_CONFLICT = "CLASSIFICATION_CONFLICT"
IDENTIFIER_ENTITY_CONFLICT = "IDENTIFIER_ENTITY_CONFLICT"
ASSET_TYPE_CONFLICT = "ASSET_TYPE_CONFLICT"


@dataclass(frozen=True)
class Rule:
    """One data-quality rule.

    ``predicate`` returns a boolean Column that is **true when the rule is
    violated**.  ``applies_to`` restricts the rule to the asset classes where
    the attribute is meaningful.
    """

    code: str
    dimension: str
    severity: str
    column: Optional[str]
    description: str
    predicate: Callable[[], "F.Column"]
    applies_to: frozenset = field(default_factory=lambda: frozenset(cfg.ASSET_CLASSES))


def _missing(col: str):
    return lambda: F.col(col).isNull()


def build_rules() -> List[Rule]:
    """The active rule set.

    Ordered by dimension so the quality report reads coherently.
    """
    all_classes = frozenset(cfg.ASSET_CLASSES)

    rules: List[Rule] = [
        # --- Completeness ------------------------------------------------
        Rule(MISSING_SYMBOL, COMPLETENESS, SEVERITY_QUARANTINE, "symbol",
             "Instrument has no symbol and cannot be addressed.",
             _missing("symbol"), all_classes),
        Rule(MISSING_NAME, COMPLETENESS, SEVERITY_REVIEW, "instrument_name",
             "Instrument has no name.", _missing("instrument_name"), all_classes),
        Rule(MISSING_EXCHANGE, COMPLETENESS, SEVERITY_QUARANTINE, "exchange",
             "A tradable instrument has no exchange, so it cannot be located "
             "on any venue.",
             _missing("exchange"), cfg.TRADABLE),
        Rule("MISSING_VENUE", COMPLETENESS, SEVERITY_REVIEW, "exchange",
             "No publishing venue recorded. Expected for some index and rate "
             "series, so this is a review signal rather than a rejection - "
             "an index is published, not traded.",
             _missing("exchange"),
             frozenset(cfg.ASSET_CLASSES) - cfg.TRADABLE),
        Rule(MISSING_CURRENCY, COMPLETENESS, SEVERITY_REVIEW, "currency",
             "Instrument has no trading currency.", _missing("currency"), all_classes),
        Rule(MISSING_COUNTRY, COMPLETENESS, SEVERITY_REVIEW, "country",
             "Instrument has no country of domicile.",
             _missing("country"), frozenset({cfg.EQUITY})),
        Rule(MISSING_IDENTIFIER, COMPLETENESS, SEVERITY_REVIEW, "isin",
             "Instrument carries no ISIN, CUSIP or FIGI.",
             lambda: (
                 F.col("isin").isNull()
                 & F.col("cusip").isNull()
                 & F.col("figi").isNull()
                 & F.col("composite_figi").isNull()
                 & F.col("shareclass_figi").isNull()
             ),
             cfg.IDENTIFIER_BEARING),

        # --- Validity ----------------------------------------------------
        Rule(INVALID_IDENTIFIER, VALIDITY, SEVERITY_REVIEW, "isin",
             "ISIN present but does not match the ISO 6166 format.",
             lambda: F.col("isin").isNotNull() & ~tf.is_valid_isin(F.col("isin")),
             cfg.IDENTIFIER_BEARING),
        Rule("INVALID_CUSIP", VALIDITY, SEVERITY_REVIEW, "cusip",
             "CUSIP present but is not 9 alphanumeric characters.",
             lambda: F.col("cusip").isNotNull() & ~tf.is_valid_cusip(F.col("cusip")),
             cfg.IDENTIFIER_BEARING),
        Rule("INVALID_FIGI", VALIDITY, SEVERITY_REVIEW, "figi",
             "FIGI present but does not match the OpenFIGI format.",
             lambda: F.col("figi").isNotNull() & ~tf.is_valid_figi(F.col("figi")),
             cfg.IDENTIFIER_BEARING),
        Rule(UNKNOWN_EXCHANGE, VALIDITY, SEVERITY_REVIEW, "exchange",
             "Exchange code is not present in the venue reference.",
             lambda: F.col("exchange").isNotNull() & (F.col("exchange_is_known") == F.lit(False)),
             all_classes),
        Rule(UNKNOWN_COUNTRY, VALIDITY, SEVERITY_REVIEW, "country",
             "Country is not present in the country reference.",
             lambda: F.col("country").isNotNull() & (F.col("country_is_known") == F.lit(False)),
             all_classes),
        Rule(UNKNOWN_CURRENCY, VALIDITY, SEVERITY_REVIEW, "currency",
             "Currency code is not a recognised currency.",
             lambda: F.col("currency").isNotNull() & (F.col("currency_is_known") == F.lit(False)),
             all_classes),

        # --- Uniqueness --------------------------------------------------
        Rule(DUPLICATE_INSTRUMENT, UNIQUENESS, SEVERITY_QUARANTINE, "symbol",
             "The same symbol appears more than once on the same exchange "
             "within one snapshot.",
             lambda: F.col("symbol_exchange_occurrences") > 1, all_classes),
        Rule(DUPLICATE_IDENTIFIER, UNIQUENESS, SEVERITY_REVIEW, "isin",
             "The same ISIN maps to more than one canonical instrument.",
             lambda: F.col("isin_instrument_count") > 1, cfg.IDENTIFIER_BEARING),
        Rule(IDENTIFIER_ENTITY_CONFLICT, UNIQUENESS, SEVERITY_REVIEW, "figi",
             "The same FIGI resolves to more than one canonical instrument; "
             "flagged for review rather than silently merged.",
             lambda: F.col("figi_instrument_count") > 1, cfg.IDENTIFIER_BEARING),

        # --- Consistency -------------------------------------------------
        Rule(EXCHANGE_COUNTRY_CONFLICT, CONSISTENCY, SEVERITY_REVIEW, "country",
             "Trading venue country and the instrument's stated country of "
             "domicile disagree. Expected for genuine cross-listings, so this "
             "is a review signal rather than a rejection.",
             lambda: (
                 F.col("exchange_country").isNotNull()
                 & F.col("country").isNotNull()
                 & (F.col("exchange_country") != F.col("country"))
                 & ~F.col("is_non_trading_venue")
             ),
             frozenset({cfg.EQUITY})),
        Rule("CURRENCY_VENUE_CONFLICT", CONSISTENCY, SEVERITY_REVIEW, "currency",
             "Trading currency differs from the venue's settlement currency.",
             lambda: (
                 F.col("exchange_currency").isNotNull()
                 & F.col("currency").isNotNull()
                 & (F.col("exchange_currency") != F.col("currency"))
                 & ~F.col("is_non_trading_venue")
             ),
             frozenset({cfg.EQUITY, cfg.ETF, cfg.FUND})),
        Rule(CLASSIFICATION_CONFLICT, CONSISTENCY, SEVERITY_REVIEW, "industry",
             "The same instrument carries more than one distinct "
             "sector/industry combination within one snapshot.",
             lambda: F.col("classification_variants") > 1,
             cfg.CLASSIFICATION_BEARING),
        Rule(ASSET_TYPE_CONFLICT, CONSISTENCY, SEVERITY_REVIEW, "asset_class",
             "Detected asset class disagrees with the class declared for the "
             "source dataset.",
             lambda: (
                 F.col("declared_asset_class").isNotNull()
                 & (F.col("declared_asset_class") != F.col("asset_class"))
             ),
             all_classes),
    ]
    return rules


# --------------------------------------------------------------------------
# Scoring model
# --------------------------------------------------------------------------

SCORE_COMPONENTS = (
    "identifier_completeness",
    "classification_completeness",
    "exchange_validity",
    "country_validity",
    "currency_validity",
    "duplicate_risk",
)

#: asset class -> component -> weight.  Every profile sums to 100.  The EQUITY
#: profile is the one specified in the brief; the others redistribute the
#: weight of components that cannot apply to that asset class.
DEFAULT_SCORING_PROFILES: Dict[str, Dict[str, int]] = {
    cfg.EQUITY: {
        "identifier_completeness": 25,
        "classification_completeness": 20,
        "exchange_validity": 15,
        "country_validity": 15,
        "currency_validity": 10,
        "duplicate_risk": 15,
    },
    # country_validity is 0: the ETF source layout has no `country` column at
    # all, so any weight on it is an unreachable penalty. Its 10 points are
    # redistributed across the attributes an ETF does carry.
    cfg.ETF: {
        "identifier_completeness": 20,
        "classification_completeness": 20,
        "exchange_validity": 20,
        "country_validity": 0,
        "currency_validity": 20,
        "duplicate_risk": 20,
    },
    cfg.FUND: {
        "identifier_completeness": 0,
        "classification_completeness": 30,
        "exchange_validity": 20,
        "country_validity": 5,
        "currency_validity": 20,
        "duplicate_risk": 25,
    },
    cfg.INDEX: {
        "identifier_completeness": 0,
        "classification_completeness": 25,
        "exchange_validity": 25,
        "country_validity": 5,
        "currency_validity": 20,
        "duplicate_risk": 25,
    },
    cfg.CURRENCY: {
        "identifier_completeness": 0,
        "classification_completeness": 0,
        "exchange_validity": 10,
        "country_validity": 0,
        "currency_validity": 45,
        "duplicate_risk": 45,
    },
    cfg.CRYPTO: {
        "identifier_completeness": 0,
        "classification_completeness": 0,
        "exchange_validity": 10,
        "country_validity": 0,
        "currency_validity": 45,
        "duplicate_risk": 45,
    },
    cfg.MONEY_MARKET: {
        "identifier_completeness": 0,
        "classification_completeness": 15,
        "exchange_validity": 25,
        "country_validity": 0,
        "currency_validity": 30,
        "duplicate_risk": 30,
    },
}

#: score -> band.  Boundaries from the brief.
TRUSTED_MIN = 90
REVIEW_MIN = 75

BAND_TRUSTED = "TRUSTED"
BAND_REVIEW = "REVIEW"
BAND_QUARANTINE = "QUARANTINE"


#: Bump when DEFAULT_SCORING_PROFILES changes meaning. The config table stores
#: the version it was seeded from; a mismatch reseeds it. Without this the table
#: silently pins the model to whatever the first run wrote, so a corrected
#: weighting would never reach production.
SCORING_PROFILE_VERSION = 2


def scoring_profiles_df(spark: SparkSession) -> DataFrame:
    rows = [
        (asset_class, component, int(weight), int(SCORING_PROFILE_VERSION))
        for asset_class, profile in sorted(DEFAULT_SCORING_PROFILES.items())
        for component, weight in sorted(profile.items())
    ]
    return spark.createDataFrame(
        rows, "asset_class string, component string, weight int, version int"
    )


def load_scoring_profiles(
    spark: SparkSession, settings: "cfg.Settings"
) -> Dict[str, Dict[str, int]]:
    """Read weights from the config table, seeding it on first run.

    Reading from a table rather than from the constant above is what makes the
    methodology configurable: an analyst can retune the model with an UPDATE
    and the next run picks it up, with the change captured in Delta history.
    """
    table = settings.audit("dq_scoring_profile")
    plain = table.replace("`", "")

    stored_version = None
    if spark.catalog.tableExists(plain):
        try:
            row = spark.table(plain).agg({"version": "max"}).first()
            stored_version = row[0] if row else None
        except Exception:  # noqa: BLE001 - pre-versioning table has no column
            stored_version = None

    if stored_version != SCORING_PROFILE_VERSION:
        # Reseed: the shipped model has changed. Any hand-tuned weights from the
        # previous version are superseded, which is why the version bump is
        # deliberate and Delta history keeps the old values.
        print(
            "reseeding {0}: stored version {1} -> {2}".format(
                plain, stored_version, SCORING_PROFILE_VERSION
            )
        )
        scoring_profiles_df(spark).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(plain)

    profiles: Dict[str, Dict[str, int]] = {}
    for row in spark.table(plain).collect():
        profiles.setdefault(row["asset_class"], {})[row["component"]] = int(row["weight"])

    for asset_class, profile in profiles.items():
        total = sum(profile.values())
        if total != 100:
            raise ValueError(
                "Scoring profile for {0} sums to {1}, expected 100. "
                "Fix {2} before running.".format(asset_class, total, plain)
            )
    return profiles


def _profile_column(profiles: Dict[str, Dict[str, int]], component: str):
    """Map asset_class -> weight for one component as a Spark expression."""
    expr = F.lit(0)
    for asset_class, profile in sorted(profiles.items()):
        expr = F.when(
            F.col("asset_class") == F.lit(asset_class),
            F.lit(int(profile.get(component, 0))),
        ).otherwise(expr)
    return expr


def score(df: DataFrame, profiles: Dict[str, Dict[str, int]]) -> DataFrame:
    """Attach per-component sub-scores, a 0-100 total and a trust band.

    Each component yields a 0.0-1.0 pass fraction which is multiplied by its
    weight.  Components carrying zero weight for an asset class contribute
    nothing, so an FX pair is never penalised for lacking an ISIN.
    """
    # Scored against the identifiers the asset class's *source schema actually
    # carries*, not against a fixed set of three. The ETF layout has only an
    # `isin` column - no cusip, no figi - so grading it out of three guaranteed
    # a maximum of 1/3 and put a ceiling of ~77 on every ETF, meaning none could
    # ever reach TRUSTED. Equities do carry all three.
    equity_identifiers = (
        F.coalesce(F.col("isin").isNotNull().cast("double"), F.lit(0.0))
        + F.coalesce(F.col("cusip").isNotNull().cast("double"), F.lit(0.0))
        + F.coalesce(
            (
                F.col("figi").isNotNull()
                | F.col("composite_figi").isNotNull()
                | F.col("shareclass_figi").isNotNull()
            ).cast("double"),
            F.lit(0.0),
        )
    ) / F.lit(3.0)

    etf_identifiers = F.coalesce(F.col("isin").isNotNull().cast("double"), F.lit(0.0))

    identifier_score = (
        F.when(F.col("asset_class") == F.lit(cfg.EQUITY), equity_identifiers)
        .when(F.col("asset_class") == F.lit(cfg.ETF), etf_identifiers)
        .otherwise(F.lit(1.0))
    )

    gics_score = (
        F.coalesce(F.col("sector").isNotNull().cast("double"), F.lit(0.0))
        + F.coalesce(F.col("industry_group").isNotNull().cast("double"), F.lit(0.0))
        + F.coalesce(F.col("industry").isNotNull().cast("double"), F.lit(0.0))
    ) / F.lit(3.0)

    category_score = (
        F.coalesce(F.col("category_group").isNotNull().cast("double"), F.lit(0.0))
        + F.coalesce(F.col("category").isNotNull().cast("double"), F.lit(0.0))
    ) / F.lit(2.0)

    classification_score = (
        F.when(F.col("asset_class").isin(*sorted(cfg.CLASSIFICATION_BEARING)), gics_score)
        .when(F.col("asset_class").isin(*sorted(cfg.CATEGORY_BEARING)), category_score)
        .otherwise(F.lit(1.0))
    )

    exchange_score = (
        F.when(F.col("exchange").isNull(), F.lit(0.0))
        .when(F.col("exchange_is_known"), F.lit(1.0))
        .otherwise(F.lit(0.0))
    )
    country_score = (
        F.when(F.col("country").isNull(), F.lit(0.0))
        .when(F.col("country_is_known"), F.lit(1.0))
        .otherwise(F.lit(0.0))
    )
    currency_score = (
        F.when(F.col("currency").isNull(), F.lit(0.0))
        .when(F.col("currency_is_known"), F.lit(1.0))
        .otherwise(F.lit(0.0))
    )
    duplicate_score = (
        F.when(F.col("symbol_exchange_occurrences") > 1, F.lit(0.0))
        .when(
            F.coalesce(F.col("isin_instrument_count"), F.lit(1)) > 1, F.lit(0.25)
        )
        .when(
            F.coalesce(F.col("figi_instrument_count"), F.lit(1)) > 1, F.lit(0.5)
        )
        .otherwise(F.lit(1.0))
    )

    components = {
        "identifier_completeness": identifier_score,
        "classification_completeness": classification_score,
        "exchange_validity": exchange_score,
        "country_validity": country_score,
        "currency_validity": currency_score,
        "duplicate_risk": duplicate_score,
    }

    out = df
    total = F.lit(0.0)
    for name, fraction in components.items():
        weight = _profile_column(profiles, name)
        contribution = F.round(fraction * weight, 4)
        out = out.withColumn("score_" + name, contribution)
        total = total + contribution

    out = out.withColumn("quality_score", F.round(total, 2))
    return out.withColumn(
        "quality_band",
        F.when(F.col("quality_score") >= TRUSTED_MIN, F.lit(BAND_TRUSTED))
        .when(F.col("quality_score") >= REVIEW_MIN, F.lit(BAND_REVIEW))
        .otherwise(F.lit(BAND_QUARANTINE)),
    )


# --------------------------------------------------------------------------
# Rule evaluation
# --------------------------------------------------------------------------


def with_quality_context(df: DataFrame, spark: SparkSession) -> DataFrame:
    """Add the derived columns the rules and scorer depend on.

    Reference lookups are broadcast joins against small in-memory dimensions,
    and the duplicate counters are window aggregates over the snapshot.
    """
    exchange_ref = ref.exchange_df(spark).select(
        F.col("exchange_code"),
        F.col("country").alias("exchange_country"),
        F.col("currency").alias("exchange_currency"),
        F.col("venue_type"),
        F.col("primary_rank"),
    )
    country_ref = ref.country_df(spark).select(
        F.col("country_name"), F.lit(True).alias("_country_known")
    )
    currency_ref = ref.currency_df(spark).select(
        F.col("currency_code"), F.lit(True).alias("_currency_known")
    )

    out = (
        df.join(
            F.broadcast(exchange_ref),
            F.upper(F.col("exchange")) == F.col("exchange_code"),
            "left",
        )
        .join(F.broadcast(country_ref), F.col("country") == F.col("country_name"), "left")
        .join(
            F.broadcast(currency_ref),
            F.col("currency") == F.col("currency_code"),
            "left",
        )
        .withColumn("exchange_is_known", F.col("exchange_code").isNotNull())
        .withColumn("country_is_known", F.coalesce(F.col("_country_known"), F.lit(False)))
        .withColumn(
            "currency_is_known", F.coalesce(F.col("_currency_known"), F.lit(False))
        )
        .withColumn(
            "is_non_trading_venue",
            F.coalesce(F.col("venue_type") != F.lit(ref.VENUE_EXCHANGE), F.lit(False)),
        )
        .drop("exchange_code", "country_name", "currency_code",
              "_country_known", "_currency_known")
    )

    # Duplicate counters are computed as grouped aggregates joined back on,
    # rather than as windows.  A window partitioned by ``isin`` would place
    # every one of the ~54k rows that carry no ISIN into a single partition and
    # collect_set over it, which is both needlessly expensive and meaningless.
    # Grouping lets the NULL keys drop out before the aggregation.
    out = out.withColumn(
        "symbol_exchange_occurrences",
        F.count(F.lit(1)).over(
            Window.partitionBy(F.upper(F.col("symbol")), F.upper(F.col("exchange")))
        ),
    )

    def _distinct_instruments_per(key: str, out_col: str) -> DataFrame:
        return (
            out.where(F.col(key).isNotNull())
            .groupBy(key)
            .agg(F.countDistinct("instrument_id").alias(out_col))
            .where(F.col(out_col) > 1)
        )

    isin_counts = _distinct_instruments_per("isin", "isin_instrument_count")
    figi_counts = _distinct_instruments_per("figi", "figi_instrument_count")

    classification_counts = (
        out.where(F.col("instrument_id").isNotNull())
        .groupBy("instrument_id")
        .agg(
            F.countDistinct(
                F.concat_ws(
                    "|",
                    F.coalesce(F.col("sector"), F.lit("")),
                    F.coalesce(F.col("industry_group"), F.lit("")),
                    F.coalesce(F.col("industry"), F.lit("")),
                )
            ).alias("classification_variants")
        )
        .where(F.col("classification_variants") > 1)
    )

    return (
        out.join(F.broadcast(isin_counts), "isin", "left")
        .join(F.broadcast(figi_counts), "figi", "left")
        .join(F.broadcast(classification_counts), "instrument_id", "left")
        .withColumn(
            "isin_instrument_count", F.coalesce(F.col("isin_instrument_count"), F.lit(1))
        )
        .withColumn(
            "figi_instrument_count", F.coalesce(F.col("figi_instrument_count"), F.lit(1))
        )
        .withColumn(
            "classification_variants",
            F.coalesce(F.col("classification_variants"), F.lit(1)),
        )
    )


def evaluate(df: DataFrame, rules: Sequence[Rule]) -> DataFrame:
    """Attach one boolean ``dq_<CODE>`` column per rule, plus a failure array.

    A rule that does not apply to a row's asset class evaluates to ``false``
    rather than NULL, so downstream aggregation never has to special-case it.
    """
    out = df
    flag_names: List[str] = []
    for rule in rules:
        flag = "dq_" + rule.code
        applies = F.col("asset_class").isin(*sorted(rule.applies_to))
        out = out.withColumn(
            flag, F.when(applies, F.coalesce(rule.predicate(), F.lit(False))).otherwise(F.lit(False))
        )
        flag_names.append(flag)

    quarantine_codes = [r.code for r in rules if r.severity == SEVERITY_QUARANTINE]

    # Collapse the per-rule flags into the list of codes that actually fired.
    # ``filter`` rather than ``array_remove(.., None)``: array_remove does not
    # strip NULLs, it removes elements equal to the given value.
    failure_array = F.filter(
        F.array(
            *[
                F.when(F.col("dq_" + r.code), F.lit(r.code)).otherwise(
                    F.lit(None).cast("string")
                )
                for r in rules
            ]
        ),
        lambda code: code.isNotNull(),
    )
    out = out.withColumn("dq_failures", failure_array)
    out = out.withColumn("dq_failure_count", F.size(F.col("dq_failures")))
    out = out.withColumn(
        "dq_quarantine_failures",
        F.array_intersect(F.col("dq_failures"), F.array(*[F.lit(c) for c in quarantine_codes]))
        if quarantine_codes
        else F.array().cast("array<string>"),
    )
    return out.withColumn(
        "is_quarantined", F.size(F.col("dq_quarantine_failures")) > 0
    )


def rules_df(spark: SparkSession, rules: Sequence[Rule]) -> DataFrame:
    """The rule catalogue, published so the dashboard can explain each failure."""
    rows = [
        (
            r.code,
            r.dimension,
            r.severity,
            r.column,
            r.description,
            sorted(r.applies_to),
        )
        for r in rules
    ]
    return spark.createDataFrame(
        rows,
        "rule_code string, dimension string, severity string, target_column string, "
        "description string, applies_to array<string>",
    )
