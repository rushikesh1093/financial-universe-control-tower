# Business Rules & Profiling Discoveries

## Executive Summary

Data profiling performed prior to architecture design revealed critical structural anomalies in the source dataset (`FinanceDatabase`). This document codifies all business rules, data cleaning transformations, non-standard value handling, and graph resolution safeguards implemented in the platform.

---

## 1. Data Profiling Discoveries & Structural Corrections

### 1.1 Folder Name Mislabeling Correction
Source profiling revealed that folder names in `FinanceDatabase` do not reflect their true contents:

| Directory Path | Actual Header Signature | Real Asset Class | Platform Routing Action |
|---|---|---|---|
| `funds/` | `sector, industry, isin, cusip, figi, market_cap` | **EQUITY** | Dynamic signature classifier routes to `bronze.equities` |
| `equities/` | `category_group, category, family` | **FUND** | Dynamic signature classifier routes to `bronze.funds` |
| `eft/` | `category_group, category, family, isin` | **ETF** | Dynamic signature classifier routes to `bronze.etfs` |

**Business Rule**: Never infer asset class from directory path or file name. Always inspect the CSV column header signature at runtime.

### 1.2 ISIN Cross-Listing Collapse Rule
Profiling revealed that 4,006 out of 7,025 unique ISINs appear across multiple exchanges.
- Example: ISIN `IE00B5KQNG97` trades on Berlin (`BER`), Frankfurt (`FRA`), Germany (`GER`), and London (`LSE`).
- **Business Rule**: An ISIN identifies a global financial security (`dim_instrument`), not a venue listing (`fact_listing`). Multiple venue listings sharing the same ISIN must resolve to a single canonical `instrument_id`.

---

## 2. Non-Standard Value Normalization Rules

### 2.1 Currency Normalization & Pence Conversion
- **`GBp` / `GBX` Handling**: In UK markets, prices are often quoted in Great British Pence (`GBp`). Treating `GBp` as a separate currency from British Pounds (`GBP`) distorts financial aggregation.
- **Rule**: `GBp` is normalized to `GBX` (ISO standard code for pence sterling).
- **Non-Currency Values (`MCE`, `KEW`)**:
  - Profiling identified non-currency exchange strings (e.g. `MCE` = Mercado Continuo Español, `KEW` = Keuper) placed incorrectly in the currency column.
  - **Rule**: `MCE` and `KEW` are stripped from `currency`, flagged in `failed_column = 'currency'`, and assigned `currency = NULL`.

### 2.2 Symbol & Exchange Cleaning Rules
- **Whitespace & Case**: Tickers and exchange codes are trimmed of leading/trailing whitespace and converted to uppercase (`upper(trim(symbol))`).
- **Delimiter Parsing**: Tickers containing exchange suffixes (e.g. `AA.N`, `AAPL.OQ`) are split into clean symbol (`AA`) and venue code (`N`) where venue is otherwise missing.

---

## 3. Entity Resolution & Graph Safety Guards

To prevent incorrect company merging during min-label propagation:

### 3.1 Generic Name Blocking Keys
Common corporate suffixes (e.g. `Inc`, `Corp`, `LLC`, `Ltd`, `PLC`, `Holdings`, `Group`, `Fund`, `Trust`, `ETF`) generate massive false-positive matches if used as entity link keys.
- **Rule**: Exclude generic corporate suffix terms from entity resolution blocking keys.

### 3.2 Component Size Threshold Guard
If an entity link key matches >50 distinct instruments, it indicates a dirty identifier or corrupted name rather than a single enterprise.
- **Rule**: Drop any edge candidate key associated with >50 instruments to prevent graph explosion.

### 3.3 Non-Issuer Asset Class Rule
- **Rule**: Financial instruments belonging to asset classes without corporate issuers (`INDEX`, `CURRENCY`, `CRYPTO`) must have `entity_id = NULL`. Invented dummy entities (e.g., "Bitcoin Corp") are strictly prohibited.

---

## 4. Asset-Class Specific Business Rules Summary

| Asset Class | Primary Key Basis | Required Identifiers | Entity Assignment | Primary Venue Logic |
|---|---|---|---|---|
| **EQUITY** | ISIN or `(symbol, exchange)` | ISIN, CUSIP, FIGI | Resolved via Graph | Largest market cap / Home country exchange |
| **ETF** | ISIN or `(symbol, exchange)` | ISIN, Composite FIGI | Resolved via Issuer | Primary listing venue |
| **FUND** | ISIN or `(symbol, exchange)` | ISIN | Resolved via Fund Family | Primary distribution venue |
| **INDEX** | `(symbol, provider)` | Provider Index Code | `NULL` | Primary publishing venue |
| **CURRENCY** | `(base_currency, quote_currency)` | ISO 4217 Currency Code | `NULL` | FX Market |
| **CRYPTO** | `(symbol, blockchain)` | Crypto Symbol / Contract | `NULL` | Primary Crypto Exchange |
| **MONEY_MARKET** | `(symbol, issuer, maturity)` | CUSIP / Instrument ID | Resolved via Issuer | OTC / Money Market Venue |
