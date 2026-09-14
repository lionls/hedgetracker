"""SEC reference data that 13F holdings are enriched with.

The CUSIP -> ticker table ships inside edgartools (``ct.pq``, 68,830 CUSIPs), so
enrichment is offline: it costs no SEC request, and a lake extracted years ago can
be re-enriched without asking EDGAR for anything. It is also the only source that
covers every era — edgartools attaches a ticker only when it parses an XML
information table, which is the form 2013 and later filings carry.
"""

from __future__ import annotations

import pandas as pd
from edgar.reference.tickers import cusip_ticker_mapping


def tickers_for_cusips(cusips: pd.Series) -> pd.Series:
    """Return the ticker the bundled map knows for each CUSIP, null where it knows none.

    ``cusips`` must already be normalized, because the map is keyed by the
    9-character CUSIP exactly as the SEC writes it: one stripped leading zero and
    the lookup misses. The lookup itself upper-cases the key, because filers do
    write the letters in lower case (``29355a107`` is Enphase) and the map does not.
    """
    mapping = cusip_ticker_mapping(allow_duplicate_cusips=False)
    return cusips.str.upper().map(mapping["Ticker"]).astype("string")
