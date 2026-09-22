"""Partition layout and idempotent Parquet writes."""

from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hedgetracker.schema import ARROW_SCHEMA, enforce_schema
from hedgetracker.storage import (
    compact_holdings,
    compacted_path,
    conform_holdings,
    extracted_path,
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


_LINES = (
    ("AMAZON.COM INC", "023135106", "AMZN"),
    ("APPLE INC", "037833100", "AAPL"),
    ("MICROSOFT CORP", "594918104", "MSFT"),
)


def _extract(
    tmp_path,
    infotable,
    cik,
    *,
    year=2024,
    quarter=2,
    report_period="2024-06-30",
    lines=_LINES,
    **overrides,
):
    """One filer's quarter in the lake: every line of its table, as extraction writes it."""
    frame = enforce_schema(
        pd.concat(
            [
                infotable(Issuer=issuer, Cusip=cusip, Ticker=ticker, **overrides)
                for issuer, cusip, ticker in lines
            ],
            ignore_index=True,
        )
    )
    frame["cik"] = cik
    frame["report_period"] = pd.Timestamp(report_period)
    return write_holdings(frame, holdings_path(tmp_path, year, quarter, cik))


def _parquet_names(tmp_path, year, quarter):
    return sorted(path.name for path in partition_dir(tmp_path, year, quarter).glob("*.parquet"))


def test_compaction_merges_a_quarter_into_one_file(tmp_path, infotable):
    for cik in ("0001661222", "0001067983", "0002034595"):
        _extract(tmp_path, infotable, cik)

    summary = compact_holdings(tmp_path)

    assert (summary["partitions"], summary["rewritten"], summary["files_removed"]) == (1, 1, 3)
    # Nine lines. A filer's lines share its CIK and report period, so none of
    # them is a duplicate of another and the merge has to keep all of them.
    assert (summary["rows"], summary["rows_superseded"]) == (9, 0)
    assert _parquet_names(tmp_path, 2024, 2) == ["holdings.parquet"]
    merged = pq.read_table(compacted_path(tmp_path, 2024, 2))
    assert merged.schema == ARROW_SCHEMA
    pairs = zip(merged.column("cik").to_pylist(), merged.column("ticker").to_pylist(), strict=True)
    assert sorted(pairs) == sorted(
        (cik, ticker)
        for cik in ("0001661222", "0001067983", "0002034595")
        for ticker in ("AMZN", "AAPL", "MSFT")
    )


def test_compaction_lets_a_part_extracted_since_the_last_merge_win(tmp_path, infotable):
    _extract(tmp_path, infotable, "0001661222")
    compact_holdings(tmp_path)
    # The same filer's table again, one line restated and one gone: what an
    # extraction run after the merge leaves in the partition.
    _extract(
        tmp_path,
        infotable,
        "0001661222",
        lines=(("AMAZON.COM INC", "023135106", "AMZN"), ("TESLA INC", "88160R101", "TSLA")),
    )

    summary = compact_holdings(tmp_path)

    # The filer's file is rewritten whole, so the part replaces its three lines.
    assert (summary["files_removed"], summary["rows_superseded"]) == (1, 3)
    merged = pq.read_table(compacted_path(tmp_path, 2024, 2)).to_pandas()
    assert sorted(merged["ticker"]) == ["AMZN", "TSLA"]


def test_compaction_leaves_an_already_merged_quarter_alone(tmp_path, infotable):
    _extract(tmp_path, infotable, "0001661222")
    compact_holdings(tmp_path)
    target = compacted_path(tmp_path, 2024, 2)
    written = target.stat().st_mtime_ns

    summary = compact_holdings(tmp_path)

    assert (summary["partitions"], summary["rewritten"], summary["skipped"]) == (1, 0, 1)
    assert (summary["files_removed"], summary["rows"]) == (0, 3)
    # A second merge would move the timestamp.
    assert target.stat().st_mtime_ns == written


def test_compaction_merges_only_the_quarters_in_its_scope(tmp_path, infotable):
    _extract(tmp_path, infotable, "0001661222", year=2024, quarter=2)
    _extract(tmp_path, infotable, "0001067983", year=2024, quarter=3, report_period="2024-09-30")
    _extract(tmp_path, infotable, "0002034595", year=2023, quarter=4, report_period="2023-12-31")

    summary = compact_holdings(tmp_path, years=(2024,), quarters=(3,))

    assert (summary["partitions"], summary["rewritten"]) == (1, 1)
    assert _parquet_names(tmp_path, 2024, 3) == ["holdings.parquet"]
    assert _parquet_names(tmp_path, 2024, 2) == ["0001661222.parquet"]
    assert _parquet_names(tmp_path, 2023, 4) == ["0002034595.parquet"]


def test_compaction_refuses_a_lake_that_holds_no_holdings(tmp_path):
    with pytest.raises(FileNotFoundError):
        compact_holdings(tmp_path)


def test_compaction_refuses_a_scope_the_lake_has_nothing_in(tmp_path, infotable):
    _extract(tmp_path, infotable, "0001661222", year=2024, quarter=2)

    with pytest.raises(ValueError, match=r"years \[2020\]"):
        compact_holdings(tmp_path, years=(2020,))


def test_extracted_path_finds_a_filer_in_a_merged_quarter(tmp_path, infotable):
    part = _extract(tmp_path, infotable, "0001661222")

    assert extracted_path(tmp_path, 2024, 2, "0001661222") == part
    assert extracted_path(tmp_path, 2024, 2, "0001067983") is None

    compact_holdings(tmp_path)

    assert extracted_path(tmp_path, 2024, 2, "0001661222") == compacted_path(tmp_path, 2024, 2)
    # A filer the merge never held is still unknown, so it is extracted rather
    # than being left out of the lake.
    assert extracted_path(tmp_path, 2024, 2, "0001067983") is None


def test_extracted_path_notices_a_filer_merged_after_it_last_looked(tmp_path, infotable):
    _extract(tmp_path, infotable, "0001661222")
    compact_holdings(tmp_path)
    assert extracted_path(tmp_path, 2024, 2, "0001067983") is None

    _extract(tmp_path, infotable, "0001067983")
    compact_holdings(tmp_path)

    assert extracted_path(tmp_path, 2024, 2, "0001067983") == compacted_path(tmp_path, 2024, 2)
