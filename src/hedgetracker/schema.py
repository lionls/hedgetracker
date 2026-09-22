"""Strict schema enforcement for normalized 13F-HR holdings.

SEC XML filings are inconsistent: filers omit optional elements, pad values with
whitespace, and serialize missing values as the literal strings ``"nan"`` or
``"None"`` (which pandas otherwise happily carries around as real data). Every
frame that reaches the data lake is passed through :func:`enforce_schema`, which

* projects the frame onto :data:`EXPECTED_SCHEMA` (missing columns are created and
  filled with nulls, unexpected columns are dropped),
* restores leading zeros that pandas stripped from all-digit CUSIPs,
* resolves each holding's ``ticker`` from its CUSIP, and
* casts every column to its exact nullable dtype, mapping placeholder tokens to
  real nulls.

The information table states no filer — it holds only what the fund bought — so
``filer_name`` is the one column the extraction fills in from the filing itself,
which is what lets a reader name a fund instead of numbering it. It is null in a
lake extracted before the column existed; the next sweep of that window names it.

The result is byte-for-byte schema compatible, so a partition can be read back as
a single coherent table regardless of which filer produced which file.
"""

from __future__ import annotations

import re
from typing import Final

import pandas as pd
import pyarrow as pa

from hedgetracker.reference import tickers_for_cusips

#: Exact column -> pandas dtype contract for the 13F holdings data lake.
EXPECTED_SCHEMA: Final[dict[str, str]] = {
    "cik": "string",
    "report_period": "datetime64[ns]",
    "accession_number": "string",
    "filer_name": "string",  # The reporting fund, from EDGAR's index of the filing
    "nameOfIssuer": "string",
    "titleOfClass": "string",
    "cusip": "string",
    "ticker": "string",  # Derived from the CUSIP, not from the filing
    "value": "Int64",  # Pandas nullable integer
    "sshPrnamt": "Int64",  # Shares
    "sshPrnamtType": "string",
    "putCall": "string",  # "Put", "Call", or missing (null)
    "investmentDiscretion": "string",
    "otherManager": "string",
    "Sole": "Int64",  # Voting authority
    "Shared": "Int64",
    "None": "Int64",
}

#: edgartools normalizes the SEC XML element names before handing back a frame
#: (``Issuer`` instead of ``nameOfIssuer``, ``SharesPrnAmount`` instead of
#: ``sshPrnamt``, ...). Everything listed here is renamed into
#: :data:`EXPECTED_SCHEMA` before conformance; unmapped source columns are dropped.
#: Without this step a reindex onto :data:`EXPECTED_SCHEMA` would silently produce
#: an all-null frame, because not a single source column name matches.
SOURCE_COLUMN_MAP: Final[dict[str, str]] = {
    "Issuer": "nameOfIssuer",
    "Class": "titleOfClass",
    "Cusip": "cusip",
    "Ticker": "ticker",
    "Value": "value",
    "SharesPrnAmount": "sshPrnamt",
    "Type": "sshPrnamtType",
    "PutCall": "putCall",
    "InvestmentDiscretion": "investmentDiscretion",
    "OtherManager": "otherManager",
    "SoleVoting": "Sole",
    "SharedVoting": "Shared",
    "NonVoting": "None",
}

#: pyarrow types the schema maps onto; used to build :data:`ARROW_SCHEMA`.
_ARROW_TYPES: Final[dict[str, pa.DataType]] = {
    "string": pa.string(),
    "datetime64[ns]": pa.timestamp("ns"),
    "Int64": pa.int64(),
}

#: The Parquet schema every written file conforms to.
ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [pa.field(column, _ARROW_TYPES[dtype]) for column, dtype in EXPECTED_SCHEMA.items()]
)

#: Textual placeholders that mean "no value" rather than a value.
NULL_TOKENS: Final[frozenset[str]] = frozenset({"", "nan", "none", "null", "n/a", "na", "<na>"})

#: Characters that only appear as thousands/decimal separators in numeric elements.
_NUMERIC_NOISE: Final[re.Pattern[str]] = re.compile(r"[$,\s]")

_CUSIP_LENGTH: Final[int] = 9


def _as_nullable_string(series: pd.Series) -> pd.Series:
    """Trim text and turn placeholder tokens into real nulls."""
    text = series.astype("string").str.strip()
    placeholder = text.str.lower().isin(NULL_TOKENS)
    return text.mask(placeholder, pd.NA).astype("string")


def _as_cusip(series: pd.Series) -> pd.Series:
    """Normalize CUSIPs, restoring leading zeros stripped by numeric coercion."""
    text = _as_nullable_string(series)
    # A CUSIP such as "037833100" is all digits; pandas may have read it as a
    # float, which survives as "37833100.0" and would defeat the zero-padding.
    whole_number = text.str.fullmatch(r"\d+\.0")
    text = text.mask(whole_number, text.str.slice(stop=-2))
    return text.str.zfill(_CUSIP_LENGTH).astype("string")


def _as_nullable_int(series: pd.Series) -> pd.Series:
    """Coerce to ``Int64``; anything non-numeric or non-integral becomes null."""
    text = _as_nullable_string(series)
    numeric = pd.to_numeric(text.str.replace(_NUMERIC_NOISE, "", regex=True), errors="coerce")
    integral = numeric.where(numeric % 1 == 0)
    return integral.astype("Int64")


def _as_datetime(series: pd.Series) -> pd.Series:
    """Coerce to naive ``datetime64[ns]``; unparseable values become null."""
    values = pd.to_datetime(series, errors="coerce")
    if isinstance(values.dtype, pd.DatetimeTZDtype):
        values = values.dt.tz_convert("UTC").dt.tz_localize(None)
    return values.astype("datetime64[ns]")


_CASTERS: Final[dict[str, object]] = {
    "string": _as_nullable_string,
    "datetime64[ns]": _as_datetime,
    "Int64": _as_nullable_int,
}


def enforce_schema(holdings: pd.DataFrame) -> pd.DataFrame:
    """Project ``holdings`` onto :data:`EXPECTED_SCHEMA` with exact dtypes.

    Source columns are first renamed through :data:`SOURCE_COLUMN_MAP`. The frame
    is returned with a fresh ``RangeIndex`` and columns in schema order, so
    callers never have to reason about the incidental shape of the source filing.
    ``ticker`` is resolved from the CUSIP rather than taken from the source, so a
    filing whose information table edgartools does not enrich still gets one.
    """
    frame = holdings.rename(columns=SOURCE_COLUMN_MAP)
    frame = frame.loc[:, ~frame.columns.duplicated()]
    # Missing columns appear as NaN, unexpected columns are dropped.
    frame = frame.reindex(columns=list(EXPECTED_SCHEMA)).reset_index(drop=True)
    for column, dtype in EXPECTED_SCHEMA.items():
        caster = _CASTERS[dtype]
        if column == "cusip":
            frame[column] = _as_cusip(frame[column])
        elif dtype == "string":
            frame[column] = _as_nullable_string(frame[column])
        else:
            frame[column] = caster(frame[column])  # type: ignore[operator]
    frame["ticker"] = tickers_for_cusips(frame["cusip"])
    return frame


def to_arrow_table(holdings: pd.DataFrame) -> pa.Table:
    """Convert an :func:`enforce_schema` d frame to its Parquet representation.

    Raises ``pyarrow.ArrowInvalid`` if the frame does not match
    :data:`ARROW_SCHEMA`, which makes schema drift a hard failure instead of a
    silently mixed partition.
    """
    return pa.Table.from_pandas(holdings, schema=ARROW_SCHEMA, preserve_index=False)
