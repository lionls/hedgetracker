"""Partition layout and idempotent Parquet writes."""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest

from hedgetracker.schema import ARROW_SCHEMA, enforce_schema
from hedgetracker.storage import holdings_path, partition_dir, write_holdings, year_quarter


def test_partition_path_is_hive_style_with_a_padded_cik(tmp_path):
    path = holdings_path(tmp_path, 2024, 2, 1661222)

    assert path == tmp_path / "13f_holdings" / "year=2024" / "quarter=2" / "0001661222.parquet"


@pytest.mark.parametrize(
    ("report_period", "expected"),
    [
        ("2024-06-30", (2024, 2)),
        ("2023-12-31", (2023, 4)),
        ("2024-01-01", (2024, 1)),
        ("2024-09-30", (2024, 3)),
    ],
)
def test_year_quarter_comes_from_the_report_period(report_period, expected):
    assert year_quarter(report_period) == expected


def test_write_creates_the_partition_and_lands_on_the_declared_schema(tmp_path, infotable):
    path = write_holdings(enforce_schema(infotable()), holdings_path(tmp_path, 2024, 2, 1661222))

    assert path.exists()
    assert pq.read_schema(path) == ARROW_SCHEMA
    assert len(pq.read_table(path)) == 1


def test_rewriting_a_partition_replaces_it_without_leaving_temporaries(tmp_path, infotable):
    path = holdings_path(tmp_path, 2024, 2, 1661222)
    write_holdings(enforce_schema(infotable()), path)

    write_holdings(enforce_schema(infotable(Issuer="MICROSOFT CORP")), path)

    rewritten = pq.read_table(path).to_pandas()
    assert len(rewritten) == 1
    assert rewritten.loc[0, "nameOfIssuer"] == "MICROSOFT CORP"
    assert list(partition_dir(tmp_path, 2024, 2).glob(".*")) == []
