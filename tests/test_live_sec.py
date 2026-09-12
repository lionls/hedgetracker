"""Opt-in integration tests against the real SEC EDGAR API (``pytest -m live``).

These fetch the SEC quarterly index (tens of megabytes) and a handful of live
filings, so they are excluded from the default run.
"""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest

from hedgetracker import settings, storage
from hedgetracker.flows import sec_13f
from hedgetracker.schema import ARROW_SCHEMA

pytestmark = pytest.mark.live

#: Q3 2024: the window used in the PRD, filed by the end of 2024 and therefore stable.
WINDOW = {"year": 2024, "quarter": 3}


def test_live_extraction_writes_conforming_partitions(tmp_path):
    summary = sec_13f.extract_quarterly_13f(
        user_email=settings.sec_identity_email(),
        base_dir=tmp_path,
        limit=3,
        **WINDOW,
    )

    assert summary["filings"] == 3
    assert summary["written"] >= 1, summary

    for path in sorted(tmp_path.rglob("*.parquet")):
        assert pq.read_schema(path) == ARROW_SCHEMA
        holdings = pq.read_table(path).to_pandas()
        assert len(holdings) > 0
        assert (holdings["cik"] == path.stem).all()
        year, quarter = storage.year_quarter(holdings["report_period"].iloc[0])
        assert path.parent.parent.name == f"year={year}"
        assert path.parent.name == f"quarter={quarter}"
        # Every row of a filing carries that filing's period and accession.
        assert holdings["report_period"].nunique() == 1
        assert holdings["accession_number"].nunique() == 1
