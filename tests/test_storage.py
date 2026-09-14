"""Partition layout and idempotent Parquet writes."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hedgetracker.schema import ARROW_SCHEMA, enforce_schema
from hedgetracker.storage import (
    conform_holdings,
    holdings_path,
    partition_dir,
    write_holdings,
    year_quarter,
)


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


def test_conform_rewrites_the_files_extracted_before_the_ticker_column(tmp_path, infotable):
    # A file from the earlier schema: same rows, no ticker column.
    legacy_schema = pa.schema([field for field in ARROW_SCHEMA if field.name != "ticker"])
    legacy = enforce_schema(infotable()).drop(columns=["ticker"])
    paths = [
        holdings_path(tmp_path, 2024, 2, 1661222),
        holdings_path(tmp_path, 2023, 4, 1958250),
    ]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(legacy, schema=legacy_schema), path)

    summary = conform_holdings(tmp_path)

    assert (summary["files"], summary["rewritten"], summary["skipped"]) == (2, 2, 0)
    assert summary["rows"] == 2
    for path in paths:
        assert pq.read_schema(path) == ARROW_SCHEMA
        upgraded = pq.read_table(path).to_pandas()
        # Only the schema changed: the same holding, plus the ticker it implies.
        assert upgraded.loc[0, "nameOfIssuer"] == "AMAZON.COM INC"
        assert upgraded.loc[0, "cusip"] == "023135106"
        assert upgraded.loc[0, "sshPrnamt"] == 83760
        assert upgraded.loc[0, "ticker"] == "AMZN"


def test_conform_leaves_a_file_that_already_conforms_untouched(tmp_path, infotable):
    path = holdings_path(tmp_path, 2024, 2, 1661222)
    write_holdings(enforce_schema(infotable()), path)
    written = path.read_bytes()

    summary = conform_holdings(tmp_path)

    assert (summary["files"], summary["rewritten"], summary["skipped"]) == (1, 0, 1)
    assert summary["rows"] == 0
    assert path.read_bytes() == written


def test_force_conform_re_derives_a_column_whose_values_have_changed(tmp_path, infotable):
    # A file on the current schema whose ticker was computed by older code: the
    # metadata comparison cannot see that, which is what --force is for.
    stale = enforce_schema(infotable())
    stale["ticker"] = "ZZZZ"
    path = holdings_path(tmp_path, 2024, 2, 1661222)
    write_holdings(stale, path)

    untouched = conform_holdings(tmp_path)
    forced = conform_holdings(tmp_path, force=True)

    assert (untouched["rewritten"], untouched["skipped"]) == (0, 1)
    assert (forced["rewritten"], forced["skipped"], forced["rows"]) == (1, 0, 1)
    assert forced["force"] is True
    assert pq.read_table(path).to_pandas().loc[0, "ticker"] == "AMZN"


def test_conform_refuses_a_base_dir_that_holds_no_holdings(tmp_path):
    with pytest.raises(FileNotFoundError):
        conform_holdings(tmp_path)
