"""Prefect extraction of quarterly SEC 13F-HR holdings into a Parquet data lake.

``extract_13f_holdings`` extracts a single filing; ``extract_quarterly_13f``
fans the task out across one EDGAR quarterly index window.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd
from edgar import Filing
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.concurrency.sync import rate_limit
from prefect.task_runners import ThreadPoolTaskRunner

from hedgetracker import data, settings, storage
from hedgetracker.schema import enforce_schema

#: Prefect global concurrency limit that meters every SEC request.
SEC_API_LIMIT = "sec-api"

#: The SEC answers with HTTP 503 above 10 requests/second, so the rate limit
#: replenishes 10 slots per second, with a burst of 10. edgartools throttles its
#: own client to 9 requests/second on top of that.
SEC_API_REQUESTS_PER_SECOND = 10

#: Filings extracted in parallel. Matches the request budget: a wider pool would
#: only queue threads behind the rate limit, a narrower one would idle it.
MAX_CONCURRENT_FILINGS = SEC_API_REQUESTS_PER_SECOND


def ensure_sec_rate_limit(requests_per_second: int = SEC_API_REQUESTS_PER_SECOND) -> None:
    """Declare the SEC rate limit that :func:`rate_limit` acquires slots from.

    ``rate_limit`` only meters requests when a matching decay-based limit exists
    on the server; without one it logs a warning and lets traffic through, so the
    flow configures it up front. The call is an upsert and therefore safe to
    repeat.
    """
    from prefect.client.orchestration import get_client

    with get_client(sync_client=True) as client:
        client.upsert_global_concurrency_limit_by_name(
            name=SEC_API_LIMIT,
            limit=requests_per_second,
            # Slots (not each individual slot) decay at this rate per second, so
            # this is the sustained request rate; `limit` is the burst size.
            slot_decay_per_second=float(requests_per_second),
        )


@task(
    name="extract-13f-holdings",
    retries=3,
    retry_delay_seconds=15,
    tags=[SEC_API_LIMIT],
    # Extraction writes to the lake and must never be skipped. Caching would also
    # have Prefect hash the Filing input, which cannot always be serialized.
    cache_policy=NO_CACHE,
)
def extract_13f_holdings(filing: Filing, base_dir: str) -> dict[str, Any] | None:
    """Extract one 13F-HR filing and write it to the data lake.

    Returns a record describing what was written, ``None`` for a filing with no
    holdings table (a notice filing), or a record with ``skipped`` set for a filing
    whose quarter is already in the lake — an accepted filing's holdings and report
    period never change, because amendments are separate filings and they are
    filtered out before this point, so an existing file is the current one. The
    quarter partition is taken from the filing's own report period, not from the
    index window it was found in: 13F-HR reports are filed up to 45 days after
    quarter end, so most filings in a window describe the previous quarter, and
    some describe far older ones.
    """
    logger = get_run_logger()
    # Prefect 3's rate_limit is a one-shot slot acquisition against a decay-based
    # limit, not a context manager: slots are never released, they replenish.
    rate_limit(SEC_API_LIMIT, occupy=1)

    # An already-extracted filing is recognised from its index page, which is a
    # fraction of the submission the full extraction downloads, so re-running a
    # quarter costs one small request per filer instead of a submission download
    # and an information-table parse each.
    if (indexed_period := data.report_period_from_index_page(filing)) is not None:
        year, quarter = storage.year_quarter(indexed_period)
        extracted = storage.holdings_path(base_dir, year, quarter, filing.cik)
        if extracted.exists():
            logger.info(
                "Skipping filing %s (CIK %s): %s already holds its %s holdings",
                filing.accession_no,
                filing.cik,
                extracted,
                indexed_period.date().isoformat(),
            )
            return {
                "cik": storage.cik_key(filing.cik),
                "accession_number": filing.accession_no,
                "report_period": indexed_period.date().isoformat(),
                "year": year,
                "quarter": quarter,
                "rows": 0,
                "path": str(extracted),
                "skipped": True,
            }

    frame = data.holdings_frame(filing)

    if frame is None:
        logger.info(
            "Skipping filing %s (CIK %s): no information table",
            filing.accession_no,
            filing.cik,
        )
        return None

    report_period = pd.to_datetime(frame["report_period"].iloc[0], errors="coerce")
    if pd.isna(report_period):
        raise ValueError(
            f"Filing {filing.accession_no} (CIK {filing.cik}) has no usable report period"
        )

    holdings = enforce_schema(frame)
    year, quarter = storage.year_quarter(report_period)
    path = storage.write_holdings(
        holdings, storage.holdings_path(base_dir, year, quarter, filing.cik)
    )
    logger.info(
        "Wrote %d holdings from %s (CIK %s, period %s) to %s",
        len(holdings),
        filing.accession_no,
        filing.cik,
        report_period.date().isoformat(),
        path,
    )
    return {
        "cik": storage.cik_key(filing.cik),
        "accession_number": filing.accession_no,
        "report_period": report_period.date().isoformat(),
        "year": year,
        "quarter": quarter,
        "rows": len(holdings),
        "path": str(path),
        "skipped": False,
    }


@flow(
    name="13F-HR Extraction Pipeline",
    task_runner=ThreadPoolTaskRunner(max_workers=MAX_CONCURRENT_FILINGS),
)
def extract_quarterly_13f(
    user_email: str,
    year: int,
    quarter: int,
    base_dir: str | Path | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Extract every 13F-HR filed in one EDGAR index window.

    ``year``/``quarter`` select the index window to scan; each filing is stored
    under the quarter of its own report period. ``limit`` caps how many filings
    are extracted, which keeps smoke runs cheap. Filings whose quarter is already
    in the lake are skipped, so re-running a window only reads what is missing. A
    run that loses even one filing to a permanent error fails, after every other
    filing has been processed, so an incomplete quarter is never mistaken for a
    complete one.
    """
    logger = get_run_logger()
    lake = settings.base_dir(base_dir)
    logger.info("13F-HR extraction started: window=%dQ%d base_dir=%s", year, quarter, lake)

    data.configure_identity(user_email)
    ensure_sec_rate_limit()

    filings, amendments_skipped = data.quarterly_13f_filings(year, quarter)
    if limit is not None:
        filings = filings[:limit]
    logger.info(
        "EDGAR %dQ%d: %d 13F-HR filings to extract (%d amendments skipped)",
        year,
        quarter,
        len(filings),
        amendments_skipped,
    )

    states = extract_13f_holdings.map(filings, base_dir=str(lake), return_state=True)

    written: list[dict[str, Any]] = []
    skipped_existing: list[dict[str, Any]] = []
    failed: list[str] = []
    for filing, state in zip(filings, states, strict=True):
        if state.is_failed():
            failed.append(f"{filing.accession_no}: {state.result(raise_on_failure=False)}")
        elif (record := state.result()) is not None:
            (skipped_existing if record["skipped"] else written).append(record)

    total_rows = sum(record["rows"] for record in written)
    logger.info(
        "13F-HR extraction finished: %d files written, %d holdings rows, %d filings without "
        "holdings, %d already extracted, %d failures",
        len(written),
        total_rows,
        len(states) - len(written) - len(skipped_existing) - len(failed),
        len(skipped_existing),
        len(failed),
    )
    if failed:
        for failure in failed:
            logger.error("Filing failed: %s", failure)
        raise RuntimeError(
            f"{len(failed)} of {len(states)} 13F-HR filings failed to extract "
            f"(first: {failed[0]}); re-run the flow to fill the gaps — filings already in the "
            "lake are skipped, so only the missing ones are read again"
        )

    return {
        "year": year,
        "quarter": quarter,
        "base_dir": str(lake),
        "filings": len(states),
        "amendments_skipped": amendments_skipped,
        "written": len(written),
        "skipped_existing": len(skipped_existing),
        "skipped_no_holdings": len(states) - len(written) - len(skipped_existing),
        "holdings_rows": total_rows,
    }


@flow(name="13F-HR Backfill")
def backfill_13f(
    user_email: str,
    start_year: int = data.FIRST_XML_INFORMATION_TABLE_YEAR,
    end_year: int | None = None,
    quarters: Sequence[int] = data.QUARTERS,
    base_dir: str | Path | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Extract every closed EDGAR index window from ``start_year`` to ``end_year``.

    Each window runs as its own subflow, so a backfill shows up in Prefect as one
    child run per quarter: progress is visible, and a quarter that failed can be
    re-run on its own with the ``13f-quarterly`` deployment. Windows run one after
    another because the SEC rate limit is global — one window already keeps it
    saturated with filings in flight, so parallel windows would only queue behind
    it. Every window is attempted even when an earlier one failed, and the flow
    then raises with the failures, so a partial backfill never looks complete.
    Filings already in the lake are skipped, which is what makes re-running a range
    cheap: only the windows that are missing something reach EDGAR for real work.

    ``end_year`` defaults to the current year. ``start_year`` defaults to the
    first year of XML information tables; pass ``1993`` to also read the tables
    embedded in older submissions.
    """
    logger = get_run_logger()
    lake = settings.base_dir(base_dir)
    windows = data.quarter_windows(start_year, end_year, quarters)
    if not windows:
        raise ValueError(
            f"no closed EDGAR index window between {start_year} and {end_year or 'today'}; "
            "the current quarter is still open"
        )
    logger.info(
        "13F-HR backfill started: %d windows, %dQ%d through %dQ%d, base_dir=%s",
        len(windows),
        *windows[0],
        *windows[-1],
        lake,
    )

    swept: list[dict[str, Any]] = []
    failed: list[str] = []
    for year, quarter in windows:
        try:
            summary = extract_quarterly_13f(
                user_email=user_email,
                year=year,
                quarter=quarter,
                base_dir=lake,
                limit=limit,
            )
        except Exception as error:
            # One window must not abandon the rest: the windows already written are
            # idempotent, so re-running from here only redoes this one.
            failed.append(f"{year}Q{quarter}: {error}")
            logger.error("Window %dQ%d failed: %s", year, quarter, error)
            continue
        swept.append(summary)
        logger.info(
            "Window %dQ%d wrote %d files (%d holdings rows), %d filings already extracted",
            year,
            quarter,
            summary["written"],
            summary["holdings_rows"],
            summary["skipped_existing"],
        )

    holdings_rows = sum(summary["holdings_rows"] for summary in swept)
    logger.info(
        "13F-HR backfill finished: %d of %d windows swept, %d files, %d holdings rows, "
        "%d filings already extracted, %d windows failed",
        len(swept),
        len(windows),
        sum(summary["written"] for summary in swept),
        holdings_rows,
        sum(summary["skipped_existing"] for summary in swept),
        len(failed),
    )
    if failed:
        for failure in failed:
            logger.error("Window failed: %s", failure)
        raise RuntimeError(
            f"{len(failed)} of {len(windows)} index windows failed (first: {failed[0]}); "
            "re-run those windows with the 13f-quarterly deployment — filings already in the "
            "lake are skipped, so only what is missing is read again"
        )

    return {
        "base_dir": str(lake),
        "start_year": windows[0][0],
        "end_year": windows[-1][0],
        "windows": len(windows),
        "windows_swept": len(swept),
        "filings": sum(summary["filings"] for summary in swept),
        "written": sum(summary["written"] for summary in swept),
        "skipped_existing": sum(summary["skipped_existing"] for summary in swept),
        "holdings_rows": holdings_rows,
        "per_window": [
            {
                "year": summary["year"],
                "quarter": summary["quarter"],
                "written": summary["written"],
                "skipped_existing": summary["skipped_existing"],
                "holdings_rows": summary["holdings_rows"],
            }
            for summary in swept
        ],
    }


if __name__ == "__main__":
    # Worked example from the PRD: every 13F-HR filed in Q3 2024, partitioned by
    # report period. Identity and destination come from the environment.
    extract_quarterly_13f(
        user_email=settings.sec_identity_email(),
        year=2024,
        quarter=3,
        base_dir=settings.base_dir(),
    )
