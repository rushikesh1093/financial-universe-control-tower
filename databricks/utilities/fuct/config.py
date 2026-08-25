"""Central configuration: catalog, schemas, paths, and the asset-class registry.

Every value here can be overridden by a notebook widget, so the same code runs
against a dev sandbox, a shared workspace, or a scheduled job without edits.
No credentials live in this file - secrets are read from a Databricks secret
scope backed by Azure Key Vault (see :func:`secret`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

# --------------------------------------------------------------------------
# Catalog / schema layout
# --------------------------------------------------------------------------

# The metastore in this workspace has no storage root, so a new catalog
# cannot be created from the CLI. "azurelearn" is the existing managed
# catalog and is used as the default; override with the `catalog` widget.
DEFAULT_CATALOG = "azurelearn"

SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
SCHEMA_QUARANTINE = "quarantine"
SCHEMA_AUDIT = "audit"

ALL_SCHEMAS = (
    SCHEMA_BRONZE,
    SCHEMA_SILVER,
    SCHEMA_GOLD,
    SCHEMA_QUARANTINE,
    SCHEMA_AUDIT,
)

# Default landing location for the raw CSV snapshots.  A Unity Catalog volume is
# the default because it needs no mount and no storage key; point ``source_root``
# at an ``abfss://`` path to read ADLS Gen2 directly instead.
DEFAULT_VOLUME = "landing"

# --------------------------------------------------------------------------
# Asset classes
# --------------------------------------------------------------------------

EQUITY = "EQUITY"
ETF = "ETF"
FUND = "FUND"
INDEX = "INDEX"
CURRENCY = "CURRENCY"
CRYPTO = "CRYPTO"
MONEY_MARKET = "MONEY_MARKET"

ASSET_CLASSES: Tuple[str, ...] = (
    EQUITY,
    ETF,
    FUND,
    INDEX,
    CURRENCY,
    CRYPTO,
    MONEY_MARKET,
)

# Asset classes that represent a claim on an issuing legal entity.  Only these
# take part in entity resolution - an FX pair or an index has no issuer, so
# forcing one would invent meaningless "companies".
ENTITY_BEARING = frozenset({EQUITY, ETF, FUND, MONEY_MARKET})

# Asset classes that actually trade on a venue. An index, an FX rate and a
# crypto pair are published by a calculator or aggregator rather than traded,
# so a missing exchange is normal for them and must not be treated as a
# blocking defect.
TRADABLE = frozenset({EQUITY, ETF, FUND, MONEY_MARKET})

# Asset classes that carry security identifiers (ISIN/CUSIP/FIGI) in the source.
IDENTIFIER_BEARING = frozenset({EQUITY, ETF})

# Asset classes carrying GICS-style sector / industry group / industry.
CLASSIFICATION_BEARING = frozenset({EQUITY})

# Asset classes whose classification is a category taxonomy rather than GICS.
CATEGORY_BEARING = frozenset({ETF, FUND, INDEX})


# --------------------------------------------------------------------------
# Asset-class detection by header signature
# --------------------------------------------------------------------------
#
# The source folders in this dataset are NOT reliable indicators of asset class.
# Profiling ``bronze/data`` showed:
#
#   bronze/data/funds/     -> equity schema  (sector, industry, isin, cusip, figi)
#   bronze/data/equities/  -> fund schema    (category_group, category, family)
#   bronze/data/eft/       -> ETF schema     (a spelling of "etf")
#
# Detecting the class from the column signature is therefore both more correct
# and more robust than trusting the directory name.  Ordered most-specific
# first; the first signature that is a subset of the file's header wins.

ASSET_CLASS_SIGNATURES: Sequence[Tuple[frozenset, str]] = (
    (frozenset({"sector", "industry", "isin", "cusip", "figi"}), EQUITY),
    (frozenset({"base_currency", "quote_currency"}), CURRENCY),
    (frozenset({"cryptocurrency"}), CRYPTO),
    (frozenset({"category_group", "category", "family", "isin"}), ETF),
    (frozenset({"category_group", "category", "family"}), FUND),
    (frozenset({"category_group", "category"}), INDEX),
    (frozenset({"family"}), MONEY_MARKET),
)


def detect_asset_class(header: Sequence[str]) -> Optional[str]:
    """Return the canonical asset class for a source header, or ``None``.

    >>> detect_asset_class(["symbol", "name", "cryptocurrency", "exchange"])
    'CRYPTO'
    """
    cols = frozenset(c.strip().lower() for c in header)
    for signature, asset_class in ASSET_CLASS_SIGNATURES:
        if signature <= cols:
            return asset_class
    return None


# --------------------------------------------------------------------------
# Ingestion metadata (the ADF "control table" equivalent)
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceDataset:
    """One row of the metadata-driven ingestion control table."""

    dataset: str
    source_glob: str
    fmt: str = "csv"
    active: bool = True
    #: Expected class.  Detection still runs; a mismatch raises a profiling
    #: warning rather than silently trusting either side.
    declared_asset_class: Optional[str] = None
    notes: str = ""


#: Canonical asset class -> the bronze dataset (and table) it lands in.
#: Ingestion groups files by their *detected* class using this map, so the
#: physical folder layout of the landing area is irrelevant.
DATASET_FOR_ASSET_CLASS: dict = {
    EQUITY: "equities",
    ETF: "etfs",
    FUND: "funds",
    INDEX: "indices",
    CURRENCY: "currencies",
    CRYPTO: "cryptocurrencies",
    MONEY_MARKET: "money_markets",
}


#: Retained for documentation and for the profiling cross-check only.
#: Ingestion no longer reads it: the landing folders have been renamed twice
#: (``eft`` -> ``efts``, ``equities`` -> ``funds_data``, ``funds`` ->
#: ``equities_data``) and at least one file does not match the class its folder
#: implies, so any folder-based mapping is a latent bug. Detection is per file.
SOURCE_REGISTRY: Tuple[SourceDataset, ...] = (
    SourceDataset(
        dataset="equities",
        source_glob="funds/*.csv",
        declared_asset_class=EQUITY,
        notes="Folder named 'funds' but carries the equity schema.",
    ),
    SourceDataset(
        dataset="etfs",
        source_glob="eft/*.csv",
        declared_asset_class=ETF,
        notes="Folder 'eft' is a spelling of 'etf'.",
    ),
    SourceDataset(
        dataset="funds",
        source_glob="equities/*.csv",
        declared_asset_class=FUND,
        notes="Folder named 'equities' but carries the fund schema.",
    ),
    SourceDataset(dataset="indices", source_glob="indices.csv", declared_asset_class=INDEX),
    SourceDataset(
        dataset="currencies", source_glob="currencies.csv", declared_asset_class=CURRENCY
    ),
    SourceDataset(
        dataset="cryptocurrencies", source_glob="cryptos.csv", declared_asset_class=CRYPTO
    ),
    SourceDataset(
        dataset="money_markets",
        source_glob="moneymarkets.csv",
        declared_asset_class=MONEY_MARKET,
    ),
)

SOURCE_URL = "https://github.com/JerBouma/FinanceDatabase"

#: Far-future sentinel for open-ended SCD2 rows.  A sentinel rather than NULL so
#: that ``BETWEEN effective_from AND effective_to`` point-in-time queries work
#: without special-casing the current row.
SCD_END_OF_TIME = "9999-12-31"


# --------------------------------------------------------------------------
# Runtime settings
# --------------------------------------------------------------------------


@dataclass
class Settings:
    """Resolved runtime configuration for a single pipeline run."""

    catalog: str = DEFAULT_CATALOG
    source_root: str = ""
    snapshot_date: str = ""
    pipeline_run_id: str = ""
    source_version: str = "main"
    volume: str = DEFAULT_VOLUME
    reprocess: bool = False
    #: When set, silver tables are created as EXTERNAL Delta tables rooted here
    #: (one folder per table) instead of as managed tables. Set it to keep the
    #: standardised data in the project's own ADLS container.
    silver_root: str = ""
    #: Same idea for the gold layer. Empty means "managed table".
    gold_root: str = ""
    #: And for quarantine. The brief lists quarantine as a lake folder beside
    #: bronze/silver/gold, so rejects belong in the project's own container
    #: rather than in Databricks-managed storage where a steward cannot see them.
    quarantine_root: str = ""

    def silver_location(self, name: str) -> Optional[str]:
        """External LOCATION for a silver table, or None for a managed table."""
        if not self.silver_root:
            return None
        return self.silver_root.rstrip("/") + "/" + name

    def gold_location(self, name: str) -> Optional[str]:
        if not self.gold_root:
            return None
        return self.gold_root.rstrip("/") + "/" + name

    def quarantine_location(self, name: str) -> Optional[str]:
        if not self.quarantine_root:
            return None
        return self.quarantine_root.rstrip("/") + "/" + name

    # --- table naming helpers -------------------------------------------
    def table(self, schema: str, name: str) -> str:
        return "`{0}`.`{1}`.`{2}`".format(self.catalog, schema, name)

    def bronze(self, name: str) -> str:
        return self.table(SCHEMA_BRONZE, name)

    def silver(self, name: str) -> str:
        return self.table(SCHEMA_SILVER, name)

    def gold(self, name: str) -> str:
        return self.table(SCHEMA_GOLD, name)

    def quarantine(self, name: str) -> str:
        return self.table(SCHEMA_QUARANTINE, name)

    def audit(self, name: str) -> str:
        return self.table(SCHEMA_AUDIT, name)

    @property
    def volume_root(self) -> str:
        return "/Volumes/{0}/{1}/{2}".format(self.catalog, SCHEMA_BRONZE, self.volume)


def resolve(dbutils) -> Settings:
    """Build :class:`Settings` from notebook widgets, applying defaults.

    Widgets are created when absent so a notebook runs interactively with no
    prior setup, and are overridden by job parameters when run from a bundle.
    """
    import uuid
    from datetime import date

    defaults = {
        "catalog": DEFAULT_CATALOG,
        "source_root": "",
        "snapshot_date": date.today().isoformat(),
        "pipeline_run_id": "",
        "source_version": "main",
        "volume": DEFAULT_VOLUME,
        "reprocess": "false",
        "silver_root": "",
        "gold_root": "",
        "quarantine_root": "",
    }
    for key, value in defaults.items():
        try:
            dbutils.widgets.text(key, value)
        except Exception:  # already defined with a different default
            pass

    def _get(key: str) -> str:
        try:
            return (dbutils.widgets.get(key) or "").strip()
        except Exception:
            return defaults[key]

    catalog = _get("catalog") or DEFAULT_CATALOG
    volume = _get("volume") or DEFAULT_VOLUME
    run_id = _get("pipeline_run_id") or _job_run_id(dbutils) or "manual-" + uuid.uuid4().hex[:12]

    return Settings(
        catalog=catalog,
        source_root=_get("source_root")
        or "/Volumes/{0}/{1}/{2}".format(catalog, SCHEMA_BRONZE, volume),
        snapshot_date=_get("snapshot_date") or defaults["snapshot_date"],
        pipeline_run_id=run_id,
        source_version=_get("source_version") or "main",
        volume=volume,
        reprocess=_get("reprocess").lower() in ("true", "1", "yes"),
        silver_root=_get("silver_root"),
        gold_root=_get("gold_root"),
        quarantine_root=_get("quarantine_root"),
    )


def _job_run_id(dbutils) -> str:
    """Return the Databricks job run id when running under a job, else ''."""
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        job_id = ctx.tags().get("jobId")
        run_id = ctx.tags().get("runId")
        job_id = job_id.get() if job_id.isDefined() else None
        run_id = run_id.get() if run_id.isDefined() else None
        if job_id and run_id:
            return "job-{0}-run-{1}".format(job_id, run_id)
    except Exception:
        pass
    return ""


def secret(dbutils, scope: str, key: str) -> str:
    """Read a secret from a Key Vault-backed Databricks secret scope.

    Credentials must never appear in source.  This is the only sanctioned way
    for the pipeline to obtain the Azure SQL password or storage credentials.
    """
    return dbutils.secrets.get(scope=scope, key=key)
