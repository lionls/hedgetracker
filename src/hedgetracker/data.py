"""SEC EDGAR access layer.

Isolates every edgartools call so the Prefect layer stays free of SEC specifics
and the extraction logic stays testable without a flow run.
"""

from __future__ import annotations

from typing import Final

import pandas as pd
from edgar import Filing, get_filings, set_identity
from edgar.attachments import Attachment

from hedgetracker.storage import cik_key

#: Quarterly holdings report. Amendments (``13F-HR/A``) are deliberately excluded;
#: see :func:`quarterly_13f_filings`.
FORM = "13F-HR"

#: How a filing's information table document is found, in edgartools' own order:
#: XML for 2013+ filings, then the table embedded in older submissions.
_INFORMATION_TABLE_SELECTORS: Final[tuple[str, ...]] = (
    "document_type=='INFORMATION TABLE' and document.lower().endswith('.xml')",
    "description=='FORM 13F' or description=='INFORMATION TABLE'",
)


class SECDocumentUnavailable(RuntimeError):
    """A document the extraction depends on could not be read.

    Raised for the SEC's transient failures. The API returns 503 (and times out)
    under load, which leaves edgartools with an empty filing index; it then
    reports the filing as having no information table and, two layers down,
    raises an unrelated ``AttributeError``. Failing loudly keeps those filings in
    Prefect's retries — and in a later re-run — rather than recording them as
    filers with no holdings.
    """


def configure_identity(user_email: str) -> None:
    """Declare the contact identity the SEC requires on every request."""
    set_identity(user_email)


def quarterly_13f_filings(year: int, quarter: int) -> tuple[list[Filing], int]:
    """Return the 13F-HR filings filed in the ``year``/``quarter`` EDGAR index window.

    Returns ``(originals, amendments_skipped)``.

    ``get_filings`` normally expands ``13F-HR`` to include ``13F-HR/A``
    amendments. Those are dropped here: an amendment restates the *same*
    ``(filer, report_period)`` pair as the filing it amends, so it maps to the
    same ``{CIK}.parquet`` path and the two would race to overwrite each other
    under ``.map()``. Extracting only originals keeps a run deterministic and
    every partition self-consistent. Amendment reconciliation is a separate
    concern that needs an explicit precedence rule, not a race.
    """
    filings = get_filings(form=FORM, year=year, quarter=quarter) or []
    originals = [filing for filing in filings if filing.form == FORM]
    return originals, len(filings) - len(originals)


def _information_table_document(filing: Filing) -> Attachment | None:
    """Return the document holding a filing's positions, or ``None`` if it has none."""
    attachments = filing.attachments
    if len(attachments) == 0:
        # Every 13F-HR submission carries at least its cover page document, so an
        # empty index means the submission itself could not be retrieved.
        raise SECDocumentUnavailable(
            f"Filing {filing.accession_no} (CIK {filing.cik}) has no readable attachment index"
        )
    for selector in _INFORMATION_TABLE_SELECTORS:
        matches = attachments.query(selector)
        if matches is not None and len(matches) > 0:
            return matches.get_by_index(0)
    return None


def _refetch(filing: Filing) -> Filing:
    """Return a fresh handle on the same filing, with no submission text cached.

    When the SEC answers a submission request with an error page, edgartools
    degrades it to a minimal submission built from the filing's homepage and
    caches that on the instance — so every later access, including a Prefect
    retry, sees the same empty index instead of asking the SEC again. A new
    instance makes the next attempt an actual retry.
    """
    return Filing(
        cik=filing.cik,
        company=filing.company,
        form=filing.form,
        filing_date=filing.filing_date,
        accession_no=filing.accession_no,
    )


def holdings_frame(filing: Filing) -> pd.DataFrame | None:
    """Return a filing's raw information table, or ``None`` if it has none.

    The frame keeps edgartools' column names and dtypes; conformance to the data
    lake schema happens later, in
    :func:`hedgetracker.schema.enforce_schema`.
    """
    try:
        document = _information_table_document(filing)
    except SECDocumentUnavailable:
        filing = _refetch(filing)
        document = _information_table_document(filing)
    if document is None:
        return None
    if not document.download():
        raise SECDocumentUnavailable(
            f"Information table {document.document!r} of filing {filing.accession_no} "
            f"(CIK {filing.cik}) could not be retrieved"
        )

    report = filing.obj()
    infotable = report.infotable
    if infotable is None or len(infotable) == 0:
        return None

    frame = infotable.copy()
    # Filing-level metadata the information table itself does not carry.
    frame["cik"] = cik_key(filing.cik)
    frame["report_period"] = report.report_period
    frame["accession_number"] = report.accession_number
    return frame
