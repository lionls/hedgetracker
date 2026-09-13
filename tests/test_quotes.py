"""Folding upstream one-minute bars into daily bars.

The fake upstream files are local Parquet files, so every test here runs without
network access; only :func:`hedgetracker.quotes.list_source_months` and the Hub listing
it reads are stubbed out.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hedgetracker import quotes

#: Column order of the upstream minute files, and of the ``rows`` a test publishes.
MINUTE_COLUMNS = ("ticker", "timestamp", "open", "high", "low", "close", "volume")

MINUTE_SCHEMA = pa.schema(
    [
        pa.field("timestamp", pa.timestamp("ns", tz="UTC")),
        pa.field("ticker", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.float64()),
    ]
)


def utc(*parts: int) -> datetime:
    """A UTC timestamp, spelled out in tests as ``utc(2024, 7, 5, 13, 30)``."""
    return datetime(*parts, tzinfo=timezone.utc)


@dataclass
class FakeSource:
    """A local stand-in for the Hugging Face dataset, with the listing patched out."""

    root: Path
    published: list[quotes.Month] = field(default_factory=list)

    def publish(
        self, month: quotes.Month, rows: Sequence[tuple[str, datetime, float, ...]]
    ) -> None:
        """Write one month of minute bars and add it to the stubbed listing."""
        table = pa.Table.from_pylist(
            [dict(zip(MINUTE_COLUMNS, row, strict=True)) for row in rows],
            schema=MINUTE_SCHEMA,
        )
        pq.write_table(table, self.root / f"ohlcv_{month}.parquet")
        self.published.append(month)

    def build(self, months: Iterable[quotes.Month], **overrides: object) -> dict[str, object]:
        """Run :func:`hedgetracker.quotes.build_daily` against this fake upstream."""
        options: dict[str, object] = {
            "work_dir": self.root / "shards",
            "destination": self.root / "quotes_daily.parquet",
            "workers": 1,
            "threads": 2,
            "memory_limit": "1GB",
            "keep_shards": True,
            "proxy": "",
        }
        options.update(overrides)
        return quotes.build_daily(tuple(months), **options)  # type: ignore[arg-type]

    def bars(self) -> list[dict[str, object]]:
        """Rows of the merged daily dataset, in file order."""
        return pq.read_table(self.root / "quotes_daily.parquet").to_pylist()

    def daily(self, month: quotes.Month) -> Path:
        """Path of the daily shard written for ``month``."""
        return quotes.shard_path(self.root / "shards", month)


@pytest.fixture
def source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeSource:
    """A fake upstream dataset whose URI and listing are wired into the module."""
    fake = FakeSource(tmp_path / "upstream")
    fake.root.mkdir()
    # configure_connection drops the ambient proxy variables; monkeypatch puts them back.
    for name in quotes.PROXY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        quotes, "source_uri", lambda month: str(fake.root / f"ohlcv_{month}.parquet")
    )
    monkeypatch.setattr(quotes, "list_source_months", lambda **_: tuple(sorted(fake.published)))
    return fake


def test_the_ambient_proxy_is_applied_and_cleared_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://user:secret@proxy.example:8080")
    con = duckdb.connect()
    try:
        quotes.configure_connection(con, proxy=None)
        assert con.execute(
            "SELECT current_setting('http_proxy'), current_setting('http_proxy_username'), "
            "current_setting('http_proxy_password')"
        ).fetchone() == ("proxy.example:8080", "user", "secret")
    finally:
        con.close()
    # DuckDB re-reads the ambient variable on every request, so it must be gone.
    assert "HTTPS_PROXY" not in os.environ


def test_regular_session_bars_fold_into_one_daily_bar(source: FakeSource) -> None:
    july = quotes.Month(2024, 7)
    source.publish(
        july,
        [
            # New York is on daylight time: 09:30 ET == 13:30 UTC.
            ("AAA", utc(2024, 7, 5, 13, 30), 10.0, 11.0, 9.5, 10.5, 100.0),
            ("AAA", utc(2024, 7, 5, 13, 31), 10.5, 12.0, 10.4, 11.9, 200.0),
            ("AAA", utc(2024, 7, 5, 19, 59), 11.9, 12.5, 11.8, 12.4, 300.0),
            # 12:00 UTC is 08:00 ET, pre-market.
            ("AAA", utc(2024, 7, 5, 12, 0), 97.0, 97.0, 97.0, 97.0, 7.0),
            # 20:00 UTC is 16:00 ET, the first bar after the close.
            ("AAA", utc(2024, 7, 5, 20, 0), 99.0, 99.0, 99.0, 99.0, 7.0),
            # 00:30 UTC on the 6th is 20:30 ET on the 5th, after hours.
            ("AAA", utc(2024, 7, 6, 0, 30), 98.0, 98.0, 98.0, 98.0, 7.0),
        ],
    )

    summary = source.build([july])

    assert source.bars() == [
        {
            "ticker": "AAA",
            "date": date(2024, 7, 5),
            "open": 10.0,
            "high": 12.5,
            "low": 9.5,
            "close": 12.4,
            "volume": 600.0,
        }
    ]
    assert summary["rows"] == 1
    assert summary["tickers"] == 1
    assert summary["first_date"] == "2024-07-05"
    assert list(source.daily(july).parent.glob(".*")) == []


def test_a_null_in_the_opening_or_closing_minute_does_not_blank_the_day(
    source: FakeSource,
) -> None:
    july = quotes.Month(2024, 7)
    source.publish(
        july,
        [
            ("BBB", utc(2024, 7, 5, 13, 30), None, 5.0, 4.0, 4.5, 10.0),
            ("BBB", utc(2024, 7, 5, 13, 31), 5.1, 5.2, 5.0, 5.15, 20.0),
            ("BBB", utc(2024, 7, 5, 19, 58), 5.2, 5.3, 5.1, 5.25, 30.0),
            ("BBB", utc(2024, 7, 5, 19, 59), 5.25, 5.4, 5.2, None, 40.0),
        ],
    )

    source.build([july])

    (bar,) = source.bars()
    assert (bar["open"], bar["close"]) == (5.1, 5.25)


@pytest.mark.parametrize(
    ("month", "pre_market", "session_open", "session_close"),
    [
        # New York is on standard time in January (UTC-5) and daylight time in July (UTC-4).
        (
            quotes.Month(2024, 1),
            utc(2024, 1, 16, 13, 30),
            utc(2024, 1, 16, 14, 30),
            utc(2024, 1, 16, 21, 0),
        ),
        (
            quotes.Month(2024, 7),
            utc(2024, 7, 5, 12, 30),
            utc(2024, 7, 5, 13, 30),
            utc(2024, 7, 5, 20, 0),
        ),
    ],
)
def test_session_bounds_follow_exchange_wall_clock(
    source: FakeSource,
    month: quotes.Month,
    pre_market: datetime,
    session_open: datetime,
    session_close: datetime,
) -> None:
    source.publish(
        month,
        [
            ("AAA", pre_market, 1.0, 1.0, 1.0, 1.0, 1.0),
            ("AAA", session_open, 2.0, 3.0, 2.0, 2.5, 10.0),
            ("AAA", session_close, 4.0, 4.0, 4.0, 4.0, 100.0),
        ],
    )

    source.build([month])

    (bar,) = source.bars()
    assert bar["date"] == session_open.date()
    assert (bar["open"], bar["volume"]) == (2.0, 10.0)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("data/ohlcv_1992-01.parquet", quotes.Month(1992, 1)),
        ("ohlcv_2026-03.parquet", quotes.Month(2026, 3)),
        ("data/ohlcv_2024-13.parquet", None),
        ("data/README.md", None),
        ("data/ohlcv_2024-01.parquet.bak", None),
    ],
)
def test_only_monthly_file_names_are_listed_as_months(
    name: str, expected: quotes.Month | None
) -> None:
    assert quotes.month_from_file_name(name) == expected


def test_requested_months_expand_over_a_year_range_and_reject_nonsense() -> None:
    assert quotes.iter_months(2024, 2024, [2, 12]) == (
        quotes.Month(2024, 2),
        quotes.Month(2024, 12),
    )
    assert len(quotes.iter_months(1992, 1993)) == 24
    with pytest.raises(ValueError):
        quotes.iter_months(2024, 2024, [13])


def test_shards_are_reused_until_the_run_is_forced(source: FakeSource) -> None:
    july = quotes.Month(2024, 7)
    source.publish(july, [("AAA", utc(2024, 7, 5, 13, 30), 1.0, 1.0, 1.0, 1.0, 1.0)])

    first = source.build([july])
    second = source.build([july])

    assert (first["months_aggregated"], first["months_reused"]) == (1, 0)
    assert (second["months_aggregated"], second["months_reused"]) == (0, 1)

    forced = source.build([july], force=True, keep_shards=False)

    assert forced["months_aggregated"] == 1
    assert not (source.root / "shards").exists()


def test_a_shard_is_enough_to_merge_again_without_upstream(source: FakeSource) -> None:
    july = quotes.Month(2024, 7)
    source.publish(july, [("AAA", utc(2024, 7, 5, 13, 30), 1.0, 1.0, 1.0, 1.0, 1.0)])
    source.build([july])
    (source.root / f"ohlcv_{july}.parquet").unlink()

    summary = source.build([july])

    assert summary["months_reused"] == 1
    assert summary["rows"] == 1


def test_a_month_that_cannot_be_read_fails_the_run_without_merging(source: FakeSource) -> None:
    june, july = quotes.Month(2024, 6), quotes.Month(2024, 7)
    source.publish(june, [("AAA", utc(2024, 6, 3, 13, 30), 1.0, 1.0, 1.0, 1.0, 1.0)])
    # Listed upstream but never written: the run must not pretend that month worked.
    source.published.append(july)

    with pytest.raises(quotes.QuotesSourceError, match="2024-07"):
        source.build([june, july])

    assert not (source.root / "quotes_daily.parquet").exists()
    assert source.daily(june).exists()


def test_months_outside_the_published_range_are_skipped_not_fatal(source: FakeSource) -> None:
    january, july = quotes.Month(2024, 1), quotes.Month(2024, 7)
    source.publish(january, [("AAA", utc(2024, 1, 16, 14, 30), 1.0, 1.0, 1.0, 1.0, 1.0)])
    source.publish(july, [("AAA", utc(2024, 7, 5, 13, 30), 2.0, 2.0, 2.0, 2.0, 2.0)])

    before = source.build([quotes.Month(2023, 12), january])
    after = source.build([july, quotes.Month(2024, 8)])

    assert before["months_absent_upstream"] == ["2023-12"]
    assert before["first_month"] == "2024-01"
    assert after["months_absent_upstream"] == ["2024-08"]
    assert after["last_month"] == "2024-07"

    # A hole between two published months is an upstream defect, not a silent gap.
    with pytest.raises(quotes.QuotesSourceError, match="2024-02"):
        source.build([quotes.Month(2024, 2)])


def test_the_merged_dataset_is_typed_and_ordered_by_ticker_then_date(
    source: FakeSource,
) -> None:
    january, july = quotes.Month(2024, 1), quotes.Month(2024, 7)
    source.publish(january, [("BBB", utc(2024, 1, 16, 14, 30), 1.0, 1.0, 1.0, 1.0, 1.0)])
    source.publish(july, [("AAA", utc(2024, 7, 5, 13, 30), 2.0, 2.0, 2.0, 2.0, 2.0)])

    summary = source.build([january, july])

    table = pq.read_table(source.root / "quotes_daily.parquet")
    assert table.schema.names == list(quotes.DAILY_COLUMNS)
    assert [(field.name, str(field.type)) for field in table.schema] == [
        ("ticker", "string"),
        ("date", "date32[day]"),
        ("open", "double"),
        ("high", "double"),
        ("low", "double"),
        ("close", "double"),
        ("volume", "double"),
    ]
    assert [(bar["ticker"], str(bar["date"])) for bar in source.bars()] == [
        ("AAA", "2024-07-05"),
        ("BBB", "2024-01-16"),
    ]
    assert summary["rows"] == 2
    assert summary["output_bytes"] == (source.root / "quotes_daily.parquet").stat().st_size
