"""Reference (conformed dimension) data for the Financial Universe.

Scope note
----------
Everything in this module is derived from **the values actually present in
``bronze/data``** - 73 exchange codes, 95 country names, and 182 currency codes
(48 from the ``currency`` column plus 179 from the FX ``base``/``quote`` columns,
overlapping) -
rather than from a global ISO catalogue.  That keeps the reference data
auditable: every row here exists because a source record referenced it.

When a future snapshot introduces a code that is not listed here, the pipeline
does not fail and does not silently accept it.  The value flows through as
``UNKNOWN_EXCHANGE`` / ``UNKNOWN_COUNTRY`` / ``UNKNOWN_CURRENCY``, is scored
down by the data-quality framework and surfaced in the quality dashboard, which
is the signal to extend this file.
"""

from __future__ import annotations

from typing import Dict, Optional

# --------------------------------------------------------------------------
# Venue types
# --------------------------------------------------------------------------

VENUE_EXCHANGE = "EXCHANGE"
VENUE_INDEX = "INDEX_VENUE"
VENUE_FX = "FX_AGGREGATOR"
VENUE_CRYPTO = "CRYPTO_AGGREGATOR"

#: Ranking used to choose a primary listing when country of domicile does not
#: disambiguate.  Lower wins.  Main national markets rank ahead of the German
#: regional venues, which mostly carry cross-listings of foreign securities.
RANK_PRIMARY_MARKET = 10
RANK_NATIONAL_MARKET = 20
RANK_REGIONAL_MARKET = 50
RANK_NON_TRADING_VENUE = 900


def _ex(name, mic, country, currency, venue_type=VENUE_EXCHANGE, rank=RANK_PRIMARY_MARKET,
        verified=True):
    return {
        "exchange_name": name,
        "mic": mic,
        "country": country,
        "currency": currency,
        "venue_type": venue_type,
        "primary_rank": rank,
        "is_verified": verified,
    }


#: exchange code -> attributes.  Codes are the Yahoo-style venue codes used by
#: FinanceDatabase.  ``mic`` values were cross-checked against the ``mic``
#: column in the source where the source supplied one.
EXCHANGE_REFERENCE: Dict[str, dict] = {
    # --- Europe ---------------------------------------------------------
    "AMS": _ex("Euronext Amsterdam", "XAMS", "Netherlands", "EUR"),
    "ATH": _ex("Athens Stock Exchange", "ASEX", "Greece", "EUR"),
    "BER": _ex("Berlin Stock Exchange", "XBER", "Germany", "EUR", rank=RANK_REGIONAL_MARKET),
    "BRU": _ex("Euronext Brussels", "XBRU", "Belgium", "EUR"),
    "BUD": _ex("Budapest Stock Exchange", "XBUD", "Hungary", "HUF"),
    "EBS": _ex("SIX Swiss Exchange", "XSWX", "Switzerland", "CHF"),
    "FRA": _ex("Frankfurt Stock Exchange", "XFRA", "Germany", "EUR", rank=RANK_REGIONAL_MARKET),
    "GER": _ex("XETRA", "XETR", "Germany", "EUR", rank=RANK_NATIONAL_MARKET),
    "ISE": _ex("Euronext Dublin", "XDUB", "Ireland", "EUR"),
    "IST": _ex("Borsa Istanbul", "XIST", "Turkey", "TRY"),
    "LIS": _ex("Euronext Lisbon", "XLIS", "Portugal", "EUR"),
    "LIT": _ex("Nasdaq Vilnius", "XLIT", "Lithuania", "EUR"),
    "LSE": _ex("London Stock Exchange", "XLON", "United Kingdom", "GBP"),
    "MCE": _ex("Bolsa de Madrid", "XMAD", "Spain", "EUR"),
    "MCX": _ex("Moscow Exchange", "MISX", "Russia", "RUB"),
    "MIL": _ex("Borsa Italiana", "XMIL", "Italy", "EUR"),
    "OSL": _ex("Oslo Bors", "XOSL", "Norway", "NOK"),
    "PAR": _ex("Euronext Paris", "XPAR", "France", "EUR"),
    "PRA": _ex("Prague Stock Exchange", "XPRA", "Czech Republic", "CZK"),
    "RIS": _ex("Nasdaq Riga", "XRIS", "Latvia", "EUR"),
    "STO": _ex("Nasdaq Stockholm", "XSTO", "Sweden", "SEK"),
    "STU": _ex("Stuttgart Stock Exchange", "XSTU", "Germany", "EUR", rank=RANK_REGIONAL_MARKET),
    "TAL": _ex("Nasdaq Tallinn", "XTAL", "Estonia", "EUR"),
    "VIE": _ex("Vienna Stock Exchange", "XWBO", "Austria", "EUR"),
    "ZRH": _ex("SIX Swiss Exchange", "XSWX", "Switzerland", "CHF"),
    # --- Americas -------------------------------------------------------
    "ASE": _ex("NYSE American", "XASE", "United States", "USD", rank=RANK_NATIONAL_MARKET),
    "BUE": _ex("Buenos Aires Stock Exchange", "XBUE", "Argentina", "ARS"),
    "CCS": _ex("Caracas Stock Exchange", "BVCA", "Venezuela", "VES"),
    "MEX": _ex("Bolsa Mexicana de Valores", "XMEX", "Mexico", "MXN"),
    "NAS": _ex("Nasdaq Stock Market", "XNAS", "United States", "USD"),
    "NYQ": _ex("New York Stock Exchange", "XNYS", "United States", "USD"),
    "NYS": _ex("New York Stock Exchange", "XNYS", "United States", "USD"),
    "SAO": _ex("B3 - Brasil Bolsa Balcao", "BVMF", "Brazil", "BRL"),
    "SGO": _ex("Santiago Stock Exchange", "XSGO", "Chile", "CLP"),
    "VAN": _ex("TSX Venture Exchange", "XTSX", "Canada", "CAD", rank=RANK_NATIONAL_MARKET),
    # --- Asia / Pacific -------------------------------------------------
    "ASX": _ex("Australian Securities Exchange", "XASX", "Australia", "AUD"),
    "BSE": _ex("BSE Limited", "XBOM", "India", "INR", rank=RANK_NATIONAL_MARKET),
    "HKG": _ex("Hong Kong Stock Exchange", "XHKG", "Hong Kong", "HKD"),
    "JKT": _ex("Indonesia Stock Exchange", "XIDX", "Indonesia", "IDR"),
    "JPX": _ex("Japan Exchange Group", "XJPX", "Japan", "JPY"),
    "KLS": _ex("Bursa Malaysia", "XKLS", "Malaysia", "MYR"),
    "KOE": _ex("KOSDAQ", "XKOS", "South Korea", "KRW", rank=RANK_NATIONAL_MARKET),
    "KSC": _ex("Korea Exchange", "XKRX", "South Korea", "KRW"),
    "NSE": _ex("National Stock Exchange of India", "XNSE", "India", "INR"),
    "NSI": _ex("National Stock Exchange of India", "XNSE", "India", "INR"),
    "NZE": _ex("New Zealand Exchange", "XNZE", "New Zealand", "NZD"),
    "OSA": _ex("Osaka Exchange", "XOSE", "Japan", "JPY", rank=RANK_NATIONAL_MARKET),
    "PHS": _ex("Philippine Stock Exchange", "XPHS", "Philippines", "PHP"),
    "SES": _ex("Singapore Exchange", "XSES", "Singapore", "SGD"),
    "SET": _ex("Stock Exchange of Thailand", "XBKK", "Thailand", "THB"),
    "SHH": _ex("Shanghai Stock Exchange", "XSHG", "China", "CNY"),
    "SHZ": _ex("Shenzhen Stock Exchange", "XSHE", "China", "CNY"),
    "TAI": _ex("Taiwan Stock Exchange", "XTAI", "Taiwan", "TWD"),
    "TWO": _ex("Taipei Exchange", "ROCO", "Taiwan", "TWD", rank=RANK_NATIONAL_MARKET),
    "CSE": _ex("Colombo Stock Exchange", "XCOL", "Sri Lanka", "LKR"),
    # --- Middle East / Africa -------------------------------------------
    "CAI": _ex("Egyptian Exchange", "XCAI", "Egypt", "EGP"),
    "DOH": _ex("Qatar Stock Exchange", "DSMD", "Qatar", "QAR"),
    "JNB": _ex("Johannesburg Stock Exchange", "XJSE", "South Africa", "ZAR"),
    "SAU": _ex("Saudi Exchange (Tadawul)", "XSAU", "Saudi Arabia", "SAR"),
    "TLV": _ex("Tel Aviv Stock Exchange", "XTAE", "Israel", "ILS"),
    # --- Non-trading venues ---------------------------------------------
    # Index calculators and price aggregators.  They are not places where an
    # instrument trades, so they are excluded from primary-listing selection
    # and from the exchange/country consistency rule.
    "CCC": _ex("CryptoCompare Aggregate", None, None, None, VENUE_CRYPTO,
               RANK_NON_TRADING_VENUE),
    "CCY": _ex("FX Rate Aggregate", None, None, None, VENUE_FX, RANK_NON_TRADING_VENUE),
    "DJI": _ex("Dow Jones Indices", None, "United States", "USD", VENUE_INDEX,
               RANK_NON_TRADING_VENUE),
    "SNP": _ex("S&P Custom Indices", None, "United States", "USD", VENUE_INDEX,
               RANK_NON_TRADING_VENUE),
    "NIM": _ex("Nasdaq Index Market", None, "United States", "USD", VENUE_INDEX,
               RANK_NON_TRADING_VENUE),
    "NYB": _ex("ICE Futures US", None, "United States", "USD", VENUE_INDEX,
               RANK_NON_TRADING_VENUE),
    "FGI": _ex("FTSE Global Indices", None, "United Kingdom", "GBP", VENUE_INDEX,
               RANK_NON_TRADING_VENUE),
    "FSI": _ex("FTSE Indices", None, "United Kingdom", "GBP", VENUE_INDEX,
               RANK_NON_TRADING_VENUE),
    "ENX": _ex("Euronext Indices", None, None, "EUR", VENUE_INDEX, RANK_NON_TRADING_VENUE),
    # Present in the source but not confidently attributable to a publisher.
    # Marked unverified so the quality layer can report them for follow-up
    # instead of the pipeline asserting something it cannot support.
    "OPI": _ex("Unattributed Index Venue (OPI)", None, None, None, VENUE_INDEX,
               RANK_NON_TRADING_VENUE, verified=False),
    "TSI": _ex("Unattributed Index Venue (TSI)", None, None, None, VENUE_INDEX,
               RANK_NON_TRADING_VENUE, verified=False),
    "WCB": _ex("Unattributed Index Venue (WCB)", None, None, None, VENUE_INDEX,
               RANK_NON_TRADING_VENUE, verified=False),
}

#: Venues where an instrument does not actually trade.
NON_TRADING_VENUES = frozenset(
    code for code, meta in EXCHANGE_REFERENCE.items()
    if meta["venue_type"] != VENUE_EXCHANGE
)


# --------------------------------------------------------------------------
# Countries - the 95 distinct values observed in bronze/data
# --------------------------------------------------------------------------

#: country name -> (ISO 3166-1 alpha-2, region).  Region is a coarse grouping
#: used for Power BI roll-ups only.
COUNTRY_REFERENCE: Dict[str, tuple] = {
    "Argentina": ("AR", "Americas"),
    "Australia": ("AU", "Asia Pacific"),
    "Austria": ("AT", "Europe"),
    "Azerbaijan": ("AZ", "Asia Pacific"),
    "Bahamas": ("BS", "Americas"),
    "Bangladesh": ("BD", "Asia Pacific"),
    "Belgium": ("BE", "Europe"),
    "Belize": ("BZ", "Americas"),
    "Bermuda": ("BM", "Americas"),
    "Brazil": ("BR", "Americas"),
    "British Virgin Islands": ("VG", "Americas"),
    "Bulgaria": ("BG", "Europe"),
    "Cambodia": ("KH", "Asia Pacific"),
    "Canada": ("CA", "Americas"),
    "Cayman Islands": ("KY", "Americas"),
    "Chile": ("CL", "Americas"),
    "China": ("CN", "Asia Pacific"),
    "Colombia": ("CO", "Americas"),
    "Costa Rica": ("CR", "Americas"),
    "Cyprus": ("CY", "Europe"),
    "Czech Republic": ("CZ", "Europe"),
    "Denmark": ("DK", "Europe"),
    "Estonia": ("EE", "Europe"),
    "Falkland Islands": ("FK", "Americas"),
    "Finland": ("FI", "Europe"),
    "France": ("FR", "Europe"),
    "French Guiana": ("GF", "Americas"),
    "Gabon": ("GA", "Middle East & Africa"),
    "Georgia": ("GE", "Europe"),
    "Germany": ("DE", "Europe"),
    "Gibraltar": ("GI", "Europe"),
    "Greece": ("GR", "Europe"),
    "Guernsey": ("GG", "Europe"),
    "Hong Kong": ("HK", "Asia Pacific"),
    "Hungary": ("HU", "Europe"),
    "Iceland": ("IS", "Europe"),
    "India": ("IN", "Asia Pacific"),
    "Indonesia": ("ID", "Asia Pacific"),
    "Ireland": ("IE", "Europe"),
    "Isle of Man": ("IM", "Europe"),
    "Israel": ("IL", "Middle East & Africa"),
    "Italy": ("IT", "Europe"),
    "Japan": ("JP", "Asia Pacific"),
    "Jersey": ("JE", "Europe"),
    "Kazakhstan": ("KZ", "Asia Pacific"),
    "Kenya": ("KE", "Middle East & Africa"),
    "Kuwait": ("KW", "Middle East & Africa"),
    "Kyrgyzstan": ("KG", "Asia Pacific"),
    "Latvia": ("LV", "Europe"),
    "Liechtenstein": ("LI", "Europe"),
    "Lithuania": ("LT", "Europe"),
    "Luxembourg": ("LU", "Europe"),
    "Macau": ("MO", "Asia Pacific"),
    "Macedonia": ("MK", "Europe"),
    "Malaysia": ("MY", "Asia Pacific"),
    "Malta": ("MT", "Europe"),
    "Marshall Islands": ("MH", "Asia Pacific"),
    "Mauritius": ("MU", "Middle East & Africa"),
    "Mexico": ("MX", "Americas"),
    "Monaco": ("MC", "Europe"),
    "Mongolia": ("MN", "Asia Pacific"),
    "Morocco": ("MA", "Middle East & Africa"),
    "Mozambique": ("MZ", "Middle East & Africa"),
    "Myanmar": ("MM", "Asia Pacific"),
    "Netherlands": ("NL", "Europe"),
    "Netherlands Antilles": ("AN", "Americas"),
    "New Zealand": ("NZ", "Asia Pacific"),
    "Nigeria": ("NG", "Middle East & Africa"),
    "Norway": ("NO", "Europe"),
    "Panama": ("PA", "Americas"),
    "Papua New Guinea": ("PG", "Asia Pacific"),
    "Peru": ("PE", "Americas"),
    "Philippines": ("PH", "Asia Pacific"),
    "Poland": ("PL", "Europe"),
    "Portugal": ("PT", "Europe"),
    "Puerto Rico": ("PR", "Americas"),
    "Romania": ("RO", "Europe"),
    "Russia": ("RU", "Europe"),
    "Singapore": ("SG", "Asia Pacific"),
    "Slovenia": ("SI", "Europe"),
    "South Africa": ("ZA", "Middle East & Africa"),
    "South Korea": ("KR", "Asia Pacific"),
    "Spain": ("ES", "Europe"),
    "Sweden": ("SE", "Europe"),
    "Switzerland": ("CH", "Europe"),
    "Taiwan": ("TW", "Asia Pacific"),
    "Thailand": ("TH", "Asia Pacific"),
    "Turkey": ("TR", "Europe"),
    "Ukraine": ("UA", "Europe"),
    "United Arab Emirates": ("AE", "Middle East & Africa"),
    "United Kingdom": ("GB", "Europe"),
    "United States": ("US", "Americas"),
    "Uruguay": ("UY", "Americas"),
    "Vietnam": ("VN", "Asia Pacific"),
    "Zambia": ("ZM", "Middle East & Africa"),
    # Referenced by EXCHANGE_REFERENCE but not by any instrument's country
    # column.  Included so the exchange -> country foreign key always resolves.
    "Egypt": ("EG", "Middle East & Africa"),
    "Qatar": ("QA", "Middle East & Africa"),
    "Saudi Arabia": ("SA", "Middle East & Africa"),
    "Sri Lanka": ("LK", "Asia Pacific"),
    "Venezuela": ("VE", "Americas"),
}


# --------------------------------------------------------------------------
# Currencies - the 48 distinct values observed in bronze/data
# --------------------------------------------------------------------------

CCY_FIAT = "FIAT"
CCY_CRYPTO = "CRYPTO"
CCY_MINOR = "MINOR_UNIT"
CCY_SYNTHETIC = "SYNTHETIC"

#: currency code -> (display name, kind).
CURRENCY_REFERENCE: Dict[str, tuple] = {
    "AED": ("UAE Dirham", CCY_FIAT),
    "AUD": ("Australian Dollar", CCY_FIAT),
    "BGN": ("Bulgarian Lev", CCY_FIAT),
    "BRL": ("Brazilian Real", CCY_FIAT),
    "CAD": ("Canadian Dollar", CCY_FIAT),
    "CHF": ("Swiss Franc", CCY_FIAT),
    "CLP": ("Chilean Peso", CCY_FIAT),
    "CNY": ("Chinese Yuan", CCY_FIAT),
    "COP": ("Colombian Peso", CCY_FIAT),
    "CZK": ("Czech Koruna", CCY_FIAT),
    "DKK": ("Danish Krone", CCY_FIAT),
    "EGP": ("Egyptian Pound", CCY_FIAT),
    "EUR": ("Euro", CCY_FIAT),
    "GBP": ("Pound Sterling", CCY_FIAT),
    "GBX": ("Pence Sterling", CCY_MINOR),
    "HKD": ("Hong Kong Dollar", CCY_FIAT),
    "HUF": ("Hungarian Forint", CCY_FIAT),
    "IDR": ("Indonesian Rupiah", CCY_FIAT),
    "ILS": ("Israeli New Shekel", CCY_FIAT),
    "INR": ("Indian Rupee", CCY_FIAT),
    "ISK": ("Icelandic Krona", CCY_FIAT),
    "JPY": ("Japanese Yen", CCY_FIAT),
    "KES": ("Kenyan Shilling", CCY_FIAT),
    "KRW": ("South Korean Won", CCY_FIAT),
    "KWD": ("Kuwaiti Dinar", CCY_FIAT),
    "LKR": ("Sri Lankan Rupee", CCY_FIAT),
    "MXN": ("Mexican Peso", CCY_FIAT),
    "MYR": ("Malaysian Ringgit", CCY_FIAT),
    "NOK": ("Norwegian Krone", CCY_FIAT),
    "NZD": ("New Zealand Dollar", CCY_FIAT),
    "PEN": ("Peruvian Sol", CCY_FIAT),
    "PHP": ("Philippine Peso", CCY_FIAT),
    "PKR": ("Pakistani Rupee", CCY_FIAT),
    "PLN": ("Polish Zloty", CCY_FIAT),
    "QAR": ("Qatari Riyal", CCY_FIAT),
    "RUB": ("Russian Ruble", CCY_FIAT),
    "SAR": ("Saudi Riyal", CCY_FIAT),
    "SEK": ("Swedish Krona", CCY_FIAT),
    "SGD": ("Singapore Dollar", CCY_FIAT),
    "THB": ("Thai Baht", CCY_FIAT),
    "TRY": ("Turkish Lira", CCY_FIAT),
    "TWD": ("New Taiwan Dollar", CCY_FIAT),
    "USD": ("US Dollar", CCY_FIAT),
    "ZAR": ("South African Rand", CCY_FIAT),
    # Crypto quote currencies observed on CRYPTO instruments.
    "BTC": ("Bitcoin", CCY_CRYPTO),
    "ETH": ("Ether", CCY_CRYPTO),
    # Referenced by EXCHANGE_REFERENCE only.
    "ARS": ("Argentine Peso", CCY_FIAT),
    "VES": ("Venezuelan Bolivar", CCY_FIAT),
}

#: Currency codes that appear only in the FX pair columns
#: (``base_currency`` / ``quote_currency``) of ``currencies.csv``. The
#: original reference was built from the ``currency`` column alone and held
#: 48 codes, so every FX pair quoting in one of the other ~130 scored as an
#: unknown currency and was pushed into the quarantine band.
#:
#: Display names are the codes themselves. Inventing full names for codes
#: that could not be verified would put unchecked assertions into a
#: reference table, and the code is what the platform actually joins on.
FX_CURRENCY_REFERENCE: Dict[str, tuple] = {
    "AED": ("AED", CCY_FIAT),
    "AFN": ("AFN", CCY_FIAT),
    "ALL": ("ALL", CCY_FIAT),
    "AMD": ("AMD", CCY_FIAT),
    "ANG": ("ANG", CCY_FIAT),
    "AOA": ("AOA", CCY_FIAT),
    "ARS": ("ARS", CCY_FIAT),
    "AUD": ("AUD", CCY_FIAT),
    "AUS": ("AUS (non-ISO FX series code)", CCY_SYNTHETIC),
    "AUX": ("AUX (non-ISO FX series code)", CCY_SYNTHETIC),
    "AWG": ("AWG", CCY_FIAT),
    "AZN": ("AZN", CCY_FIAT),
    "BAM": ("BAM", CCY_FIAT),
    "BBD": ("BBD", CCY_FIAT),
    "BDT": ("BDT", CCY_FIAT),
    "BGN": ("BGN", CCY_FIAT),
    "BHD": ("BHD", CCY_FIAT),
    "BIF": ("BIF", CCY_FIAT),
    "BMD": ("BMD", CCY_FIAT),
    "BND": ("BND", CCY_FIAT),
    "BOB": ("BOB", CCY_FIAT),
    "BRL": ("BRL", CCY_FIAT),
    "BRX": ("BRX (non-ISO FX series code)", CCY_SYNTHETIC),
    "BSD": ("BSD", CCY_FIAT),
    "BTN": ("BTN", CCY_FIAT),
    "BWP": ("BWP", CCY_FIAT),
    "BYN": ("BYN", CCY_FIAT),
    "BZD": ("BZD", CCY_FIAT),
    "CAD": ("CAD", CCY_FIAT),
    "CAX": ("CAX (non-ISO FX series code)", CCY_SYNTHETIC),
    "CDF": ("CDF", CCY_FIAT),
    "CHF": ("CHF", CCY_FIAT),
    "CLF": ("CLF", CCY_FIAT),
    "CLP": ("CLP", CCY_FIAT),
    "CNH": ("CNH", CCY_FIAT),
    "CNY": ("CNY", CCY_FIAT),
    "COP": ("COP", CCY_FIAT),
    "CRC": ("CRC", CCY_FIAT),
    "CUC": ("CUC", CCY_FIAT),
    "CUP": ("CUP", CCY_FIAT),
    "CVE": ("CVE", CCY_FIAT),
    "CZK": ("CZK", CCY_FIAT),
    "CZX": ("CZX (non-ISO FX series code)", CCY_SYNTHETIC),
    "DJF": ("DJF", CCY_FIAT),
    "DKK": ("DKK", CCY_FIAT),
    "DKX": ("DKX (non-ISO FX series code)", CCY_SYNTHETIC),
    "DOP": ("DOP", CCY_FIAT),
    "DZD": ("DZD", CCY_FIAT),
    "EGP": ("EGP", CCY_FIAT),
    "ERN": ("ERN", CCY_FIAT),
    "ETB": ("ETB", CCY_FIAT),
    "EUR": ("EUR", CCY_FIAT),
    "EUX": ("EUX (non-ISO FX series code)", CCY_SYNTHETIC),
    "FJD": ("FJD", CCY_FIAT),
    "FKP": ("FKP", CCY_FIAT),
    "GBP": ("GBP", CCY_FIAT),
    "GEL": ("GEL", CCY_FIAT),
    "GHS": ("GHS", CCY_FIAT),
    "GIP": ("GIP", CCY_FIAT),
    "GMD": ("GMD", CCY_FIAT),
    "GNF": ("GNF", CCY_FIAT),
    "GTQ": ("GTQ", CCY_FIAT),
    "GYD": ("GYD", CCY_FIAT),
    "HKD": ("HKD", CCY_FIAT),
    "HNL": ("HNL", CCY_FIAT),
    "HRK": ("HRK", CCY_FIAT),
    "HRX": ("HRX (non-ISO FX series code)", CCY_SYNTHETIC),
    "HTG": ("HTG", CCY_FIAT),
    "HUF": ("HUF", CCY_FIAT),
    "HUX": ("HUX (non-ISO FX series code)", CCY_SYNTHETIC),
    "IDR": ("IDR", CCY_FIAT),
    "ILS": ("ILS", CCY_FIAT),
    "INR": ("INR", CCY_FIAT),
    "IQD": ("IQD", CCY_FIAT),
    "IRR": ("IRR", CCY_FIAT),
    "ISK": ("ISK", CCY_FIAT),
    "ISX": ("ISX (non-ISO FX series code)", CCY_SYNTHETIC),
    "JMD": ("JMD", CCY_FIAT),
    "JOD": ("JOD", CCY_FIAT),
    "JPY": ("JPY", CCY_FIAT),
    "KES": ("KES", CCY_FIAT),
    "KGS": ("KGS", CCY_FIAT),
    "KHR": ("KHR", CCY_FIAT),
    "KMF": ("KMF", CCY_FIAT),
    "KPW": ("KPW", CCY_FIAT),
    "KRW": ("KRW", CCY_FIAT),
    "KWD": ("KWD", CCY_FIAT),
    "KYD": ("KYD", CCY_FIAT),
    "KZT": ("KZT", CCY_FIAT),
    "LAK": ("LAK", CCY_FIAT),
    "LBP": ("LBP", CCY_FIAT),
    "LKR": ("LKR", CCY_FIAT),
    "LRD": ("LRD", CCY_FIAT),
    "LSL": ("LSL", CCY_FIAT),
    "LYD": ("LYD", CCY_FIAT),
    "MAD": ("MAD", CCY_FIAT),
    "MDL": ("MDL", CCY_FIAT),
    "MGA": ("MGA", CCY_FIAT),
    "MKD": ("MKD", CCY_FIAT),
    "MMK": ("MMK", CCY_FIAT),
    "MNT": ("MNT", CCY_FIAT),
    "MOP": ("MOP", CCY_FIAT),
    "MRU": ("MRU", CCY_FIAT),
    "MUR": ("MUR", CCY_FIAT),
    "MVR": ("MVR", CCY_FIAT),
    "MWK": ("MWK", CCY_FIAT),
    "MXN": ("MXN", CCY_FIAT),
    "MXV": ("MXV", CCY_FIAT),
    "MXX": ("MXX (non-ISO FX series code)", CCY_SYNTHETIC),
    "MYR": ("MYR", CCY_FIAT),
    "MYX": ("MYX (non-ISO FX series code)", CCY_SYNTHETIC),
    "MZN": ("MZN", CCY_FIAT),
    "NAD": ("NAD", CCY_FIAT),
    "NGN": ("NGN", CCY_FIAT),
    "NIO": ("NIO", CCY_FIAT),
    "NOK": ("NOK", CCY_FIAT),
    "NPR": ("NPR", CCY_FIAT),
    "NZD": ("NZD", CCY_FIAT),
    "OMR": ("OMR", CCY_FIAT),
    "PAB": ("PAB", CCY_FIAT),
    "PEN": ("PEN", CCY_FIAT),
    "PGK": ("PGK", CCY_FIAT),
    "PHP": ("PHP", CCY_FIAT),
    "PKR": ("PKR", CCY_FIAT),
    "PLN": ("PLN", CCY_FIAT),
    "PLX": ("PLX (non-ISO FX series code)", CCY_SYNTHETIC),
    "PYG": ("PYG", CCY_FIAT),
    "QAR": ("QAR", CCY_FIAT),
    "RON": ("RON", CCY_FIAT),
    "RSD": ("RSD", CCY_FIAT),
    "RUB": ("RUB", CCY_FIAT),
    "RUX": ("RUX (non-ISO FX series code)", CCY_SYNTHETIC),
    "RWF": ("RWF", CCY_FIAT),
    "SAR": ("SAR", CCY_FIAT),
    "SBD": ("SBD", CCY_FIAT),
    "SCR": ("SCR", CCY_FIAT),
    "SDG": ("SDG", CCY_FIAT),
    "SEK": ("SEK", CCY_FIAT),
    "SGD": ("SGD", CCY_FIAT),
    "SHP": ("SHP", CCY_FIAT),
    "SLL": ("SLL", CCY_FIAT),
    "SOS": ("SOS", CCY_FIAT),
    "SRD": ("SRD", CCY_FIAT),
    "SSP": ("SSP", CCY_FIAT),
    "STN": ("STN", CCY_FIAT),
    "SVC": ("SVC", CCY_FIAT),
    "SYP": ("SYP", CCY_FIAT),
    "SZL": ("SZL", CCY_FIAT),
    "THB": ("THB", CCY_FIAT),
    "THX": ("THX (non-ISO FX series code)", CCY_SYNTHETIC),
    "TJS": ("TJS", CCY_FIAT),
    "TMT": ("TMT", CCY_FIAT),
    "TND": ("TND", CCY_FIAT),
    "TOP": ("TOP", CCY_FIAT),
    "TRY": ("TRY", CCY_FIAT),
    "TTD": ("TTD", CCY_FIAT),
    "TWD": ("TWD", CCY_FIAT),
    "TWI": ("TWI (non-ISO FX series code)", CCY_SYNTHETIC),
    "TZS": ("TZS", CCY_FIAT),
    "UAH": ("UAH", CCY_FIAT),
    "UGX": ("UGX", CCY_FIAT),
    "USD": ("USD", CCY_FIAT),
    "USY": ("USY (non-ISO FX series code)", CCY_SYNTHETIC),
    "UYU": ("UYU", CCY_FIAT),
    "UZS": ("UZS", CCY_FIAT),
    "VES": ("VES", CCY_FIAT),
    "VND": ("VND", CCY_FIAT),
    "VUV": ("VUV", CCY_FIAT),
    "WST": ("WST", CCY_FIAT),
    "XAF": ("XAF", CCY_FIAT),
    "XCD": ("XCD", CCY_FIAT),
    "XCU": ("XCU (non-ISO FX series code)", CCY_SYNTHETIC),
    "XDR": ("XDR", CCY_FIAT),
    "XOF": ("XOF", CCY_FIAT),
    "XPF": ("XPF", CCY_FIAT),
    "YER": ("YER", CCY_FIAT),
    "ZAC": ("South African Cent", CCY_MINOR),
    "ZAR": ("ZAR", CCY_FIAT),
    "ZMW": ("ZMW", CCY_FIAT),
}

# Merged in without overriding the curated entries above, which carry real
# display names.
for _code, _meta in FX_CURRENCY_REFERENCE.items():
    CURRENCY_REFERENCE.setdefault(_code, _meta)

#: Source spellings normalised before validation.  ``GBp`` is the Yahoo spelling
#: of pence sterling; folding it to upper case alone would lose that meaning.
CURRENCY_ALIASES: Dict[str, str] = {"GBp": "GBX"}

#: Values that appear in the source ``currency`` column but are not currencies.
#: ``MCE`` is the Madrid venue code leaking into the currency field and ``KEW``
#: is a corruption of ``KES``.  Listing them explicitly means the DQ layer
#: reports a precise reason instead of a generic "unknown code".
KNOWN_BAD_CURRENCY_CODES: Dict[str, str] = {
    "MCE": "Exchange code (Bolsa de Madrid) present in the currency column",
    "KEW": "Not a currency code; likely a corruption of KES (Kenyan Shilling)",
}


# --------------------------------------------------------------------------
# Lookup helpers
# --------------------------------------------------------------------------


def normalise_currency(code: Optional[str]) -> Optional[str]:
    """Apply alias folding to a raw source currency code."""
    if not code:
        return None
    code = code.strip()
    return CURRENCY_ALIASES.get(code, code.upper())


def exchange_country(code: Optional[str]) -> Optional[str]:
    meta = EXCHANGE_REFERENCE.get((code or "").strip().upper())
    return meta["country"] if meta else None


def is_known_exchange(code: Optional[str]) -> bool:
    return (code or "").strip().upper() in EXCHANGE_REFERENCE


def is_known_country(name: Optional[str]) -> bool:
    return (name or "").strip() in COUNTRY_REFERENCE


def is_known_currency(code: Optional[str]) -> bool:
    return normalise_currency(code) in CURRENCY_REFERENCE


# --------------------------------------------------------------------------
# Spark DataFrame builders
# --------------------------------------------------------------------------


def exchange_df(spark):
    """Reference exchange dimension as a Spark DataFrame."""
    rows = [
        (
            code,
            meta["exchange_name"],
            meta["mic"],
            meta["country"],
            meta["currency"],
            meta["venue_type"],
            int(meta["primary_rank"]),
            bool(meta["is_verified"]),
        )
        for code, meta in sorted(EXCHANGE_REFERENCE.items())
    ]
    return spark.createDataFrame(
        rows,
        "exchange_code string, exchange_name string, mic string, country string, "
        "currency string, venue_type string, primary_rank int, is_verified boolean",
    )


def country_df(spark):
    rows = [
        (name, iso2, region) for name, (iso2, region) in sorted(COUNTRY_REFERENCE.items())
    ]
    return spark.createDataFrame(
        rows, "country_name string, country_iso2 string, region string"
    )


def currency_df(spark):
    rows = [
        (code, name, kind) for code, (name, kind) in sorted(CURRENCY_REFERENCE.items())
    ]
    return spark.createDataFrame(
        rows, "currency_code string, currency_name string, currency_kind string"
    )
