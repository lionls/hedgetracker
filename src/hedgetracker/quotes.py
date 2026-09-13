"""Daily OHLCV bars folded out of the Hugging Face ``mito0o852/OHLCV-1m`` dataset.

Upstream publishes one Parquet file per calendar month of one-minute bars
(``data/ohlcv_YYYY-MM.parquet``, 1992-01 through 2026-03, about 82 GB in total) with a
UTC ``timestamp``, one row per ticker and minute. This module streams those files
through DuckDB's ``httpfs`` extension, folds each month into daily bars, and
concatenates the monthly shards into one daily dataset. The minute bars never land on
disk.

Three properties of the upstream data shape the aggregation:

* Bars are stamped in UTC but belong to a New York trading day, so both the trading
  date and the session bounds are evaluated as ``America/New_York`` wall-clock time.
  Grouping by UTC date instead would move every after-hours bar (16:00 ET and later,
  which is 20:00 or 21:00 UTC) onto the following day, and a fixed UTC offset would
  break twice a year at the DST switch.
* Only regular-session bars (09:30 up to but not including 16:00 ET) are kept.
  Pre-market and after-hours prints would otherwise inflate the daily high, low and
  volume. Early closes need no special handling: the last bar that exists wins.
* ``open`` and ``close`` come from the first and last bar *that has a value*
  (``arg_min``/``arg_max`` skip nulls), so a null in the opening minute does not blank
  a whole day.

Because the files are partitioned by UTC month and only regular-session bars are kept,
every trading day is complete inside its own month: a New York session never crosses a
UTC month boundary (13:30–21:00 UTC in both DST regimes, on the same UTC date). The
upstream month filter therefore never truncates a session, and the only bars a
requested month might miss are after-hours prints of the previous month's last day,
which the session filter drops anyway.

Shards make the multi-hour download restartable: an interrupted run resumes at the
first month whose shard is missing, and a run that fails on any month refuses to merge,
so a partial dataset is never published. Missing upstream months are reported rather
than silently swallowed: the file listing is read from the Hugging Face Hub, so "this
month does not exist" is a known fact instead of a caught exception.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import count
from pathlib import Path
from typing import Any, Final

import duckdb

#: Hugging Face dataset repository holding the one-minute bars.
SOURCE_REPO: Final[str] = "mito0o852/OHLCV-1m"

#: Directory inside the repository that holds the monthly Parquet files.
SOURCE_DIRECTORY: Final[str] = "data"

#: First month published upstream, used as the default sweep start.
FIRST_SOURCE_MONTH: Final[tuple[int, int]] = (1992, 1)

#: Exchange timezone; trading dates and session bounds are New York wall-clock time.
EXCHANGE_TIMEZONE: Final[str] = "America/New_York"

#: Regular-session bounds in exchange time, half-open: 09:30 ET through 15:59 ET.
SESSION_START: Final[str] = "09:30"
SESSION_CLOSE: Final[str] = "16:00"

#: Columns of the daily dataset, in order, with the type each one is cast to.
DAILY_COLUMNS: Final[dict[str, str]] = {
    "ticker": "VARCHAR",
    "date": "DATE",
    "open": "DOUBLE",
    "high": "DOUBLE",
    "low": "DOUBLE",
    "close": "DOUBLE",
    "volume": "DOUBLE",
}

#: Directory inside the data lake that holds the daily dataset and its shards.
DATASET: Final[str] = "market"

#: File name of the merged daily dataset.
DAILY_FILE_NAME: Final[str] = "quotes_daily.parquet"

#: Directory inside :data:`DATASET` that holds the per-month shards while a run is going.
SHARDS_DIRECTORY: Final[str] = "shards"

#: Hub endpoint listing every file of the dataset, used to learn which months exist.
HUB_TREE_URL: Final[str] = f"https://huggingface.co/api/datasets/{SOURCE_REPO}/tree/main"

#: Proxy environment variables DuckDB would read on its own, in priority order.
PROXY_ENV_VARS: Final[tuple[str, ...]] = (
    "HTTPS_PROXY",
    "https_proxy",
    "HTTP_PROXY",
    "http_proxy",
)

_SOURCE_FILE_NAME: Final[re.Pattern[str]] = re.compile(r"^ohlcv_(\d{4})-(\d{2})\.parquet$")

_TRANSIENT_HTTP_STATUS: Final[frozenset[int]] = frozenset({408, 429, 500, 502, 503, 504})


class QuotesSourceError(RuntimeError):
    """The upstream dataset could not be listed or read, or a month failed to fold."""


@dataclass(frozen=True, order=True)
class Month:
    """One calendar month of the upstream dataset."""

    year: int
    month: int

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def iter_months(
    start_year: int, end_year: int, months: Iterable[int] | None = None
) -> tuple[Month, ...]:
    """Every requested month from ``start_year`` to ``end_year``, both inclusive."""
    wanted = frozenset(range(1, 13) if months is None else months)
    unknown = sorted(month for month in wanted if month not in range(1, 13))
    if unknown:
        raise ValueError(f"months must be between 1 and 12, got {unknown}")
    return tuple(
        Month(year, month)
        for year in range(start_year, end_year + 1)
        for month in range(1, 13)
        if month in wanted
    )


def source_uri(month: Month) -> str:
    """DuckDB URI of one month of upstream minute bars."""
    return f"hf://datasets/{SOURCE_REPO}/{SOURCE_DIRECTORY}/ohlcv_{month}.parquet"


def month_from_file_name(name: str) -> Month | None:
    """Month encoded in an upstream file name, or ``None`` for anything else."""
    match = _SOURCE_FILE_NAME.match(Path(name).name)
    if match is None:
        return None
    year, month = int(match.group(1)), int(match.group(2))
    return Month(year, month) if 1 <= month <= 12 else None


def shard_path(work_dir: str | Path, month: Month) -> Path:
    """Path of the daily shard that holds one aggregated month."""
    return Path(work_dir) / f"quotes_{month}.parquet"


def ambient_proxy() -> str:
    """Proxy URL from the environment, or ``""`` when none is configured."""
    for name in PROXY_ENV_VARS:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def _sql_literal(value: str | Path) -> str:
    """Single-quoted SQL string literal for ``value``."""
    return "'" + str(value).replace("'", "''") + "'"


def _sql_literal_list(values: Iterable[str | Path]) -> str:
    """SQL list literal of string literals, for ``read_parquet([...])``."""
    return "[" + ", ".join(_sql_literal(value) for value in values) + "]"


def _request_opener(proxy: str, token: str | None) -> urllib.request.OpenerDirector:
    """Opener that reaches the Hub through ``proxy`` when one is configured."""
    handlers: list[urllib.request.BaseHandler] = []
    if proxy:
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    opener = urllib.request.build_opener(*handlers)
    if token:
        # See https://huggingface.co/docs/hub/security-tokens.
        opener.addheaders = [("Authorization", f"Bearer {token}")]
    return opener


def _hub_json(url: str, *, proxy: str, attempts: int = 3, timeout: float = 60.0) -> Any:
    """GET ``url`` and decode the JSON body, retrying transient failures."""
    opener = _request_opener(proxy, os.environ.get("HF_TOKEN"))
    for remaining in range(attempts, 0, -1):
        try:
            with opener.open(url, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code not in _TRANSIENT_HTTP_STATUS or remaining == 1:
                raise QuotesSourceError(f"GET {url} failed with HTTP {error.code}") from error
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            if remaining == 1:
                raise QuotesSourceError(f"GET {url} failed: {error}") from error
        time.sleep(2.0)
    raise AssertionError("unreachable")


def list_source_months(*, proxy: str | None = None, attempts: int = 3) -> tuple[Month, ...]:
    """Months that exist upstream, earliest first, read from the Hub file listing."""
    listing = _hub_json(
        f"{HUB_TREE_URL}/{SOURCE_DIRECTORY}?recursive=true",
        proxy=ambient_proxy() if proxy is None else proxy,
        attempts=attempts,
    )
    if not isinstance(listing, list):
        raise QuotesSourceError(f"unexpected listing payload: {type(listing).__name__}")
    months = {
        month
        for entry in listing
        if isinstance(entry, dict) and entry.get("type") == "file"
        if (month := month_from_file_name(str(entry.get("path", "")))) is not None
    }
    if not months:
        raise QuotesSourceError(f"no monthly files found under {HUB_TREE_URL}")
    return tuple(sorted(months))


def _ensure_extension(con: duckdb.DuckDBPyConnection, name: str) -> None:
    """Load an extension, installing it only when it is not on disk yet."""
    try:
        con.execute(f"LOAD {name}")
    except duckdb.Error:
        con.execute(f"INSTALL {name}")
        con.execute(f"LOAD {name}")


def _configure_proxy(con: duckdb.DuckDBPyConnection, proxy: str) -> None:
    """Apply a proxy URL to DuckDB, which cannot parse a credentialed one itself."""
    if not proxy:
        return
    parts = urllib.parse.urlsplit(proxy)
    if not parts.hostname:
        raise QuotesSourceError(f"proxy {proxy!r} has no host")
    con.execute(f"SET http_proxy={_sql_literal(f'{parts.hostname}:{parts.port or 80}')}")
    if parts.username:
        con.execute(f"SET http_proxy_username={_sql_literal(urllib.parse.unquote(parts.username))}")
    if parts.password:
        con.execute(f"SET http_proxy_password={_sql_literal(urllib.parse.unquote(parts.password))}")


def configure_connection(
    con: duckdb.DuckDBPyConnection,
    *,
    threads: int = 4,
    memory_limit: str = "4GB",
    temp_directory: str | Path | None = None,
    proxy: str | None = None,
) -> None:
    """Load the extensions and settings every quotes query depends on.

    DuckDB reads proxy environment variables on its own but cannot parse a credentialed
    proxy URL, and a value it cannot parse makes it stall instead of failing. The
    ambient variables are therefore dropped here and the proxy is applied explicitly;
    callers that still need the proxy afterwards (Hub listings) pass it themselves.
    """
    resolved = ambient_proxy() if proxy is None else proxy
    for name in PROXY_ENV_VARS:
        os.environ.pop(name, None)
    _configure_proxy(con, resolved)
    _ensure_extension(con, "httpfs")
    _ensure_extension(con, "icu")
    con.execute(f"SET threads={int(threads)}")
    con.execute(f"SET memory_limit={_sql_literal(memory_limit)}")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET http_retries=5")
    if temp_directory is not None:
        spill = Path(temp_directory)
        spill.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory={_sql_literal(spill)}")


def _exchange_time(expression: str) -> str:
    """Exchange-time form of ``expression``, used for both grouping and filtering."""
    return f"timezone({_sql_literal(EXCHANGE_TIMEZONE)}, {expression})"


def aggregate_sql() -> str:
    """The month-to-day folding query; ``$uri`` binds to one upstream file."""
    session_time = f"{_exchange_time('timestamp')}::TIME"
    folds = {
        "ticker": "ticker",
        "date": f"{_exchange_time('timestamp')}::DATE",
        # arg_min/arg_max skip nulls, so a missing opening or closing print keeps the day.
        "open": "arg_min(open, timestamp)",
        "high": "max(high)",
        "low": "min(low)",
        "close": "arg_max(close, timestamp)",
        "volume": "sum(volume)",
    }
    projections = [
        f"CAST({folds[name]} AS {DAILY_COLUMNS[name]}) AS {name}" for name in DAILY_COLUMNS
    ]
    return (
        "SELECT\n       "
        + ",\n       ".join(projections)
        + f"\nFROM read_parquet($uri)\nWHERE {session_time} >= TIME {_sql_literal(SESSION_START)}"
        + f"\n  AND {session_time} < TIME {_sql_literal(SESSION_CLOSE)}"
        + "\nGROUP BY ticker, date"
    )


def _write_parquet(
    con: duckdb.DuckDBPyConnection,
    query: str,
    destination: str | Path,
    parameters: dict[str, Any] | None = None,
) -> Path:
    """Run ``query`` into ``destination`` atomically, leaving no temporary behind."""
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        con.execute(
            f"COPY ({query}) TO {_sql_literal(temporary)} (FORMAT PARQUET, COMPRESSION ZSTD)",
            parameters,
        )
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def aggregate_month(con: duckdb.DuckDBPyConnection, uri: str, destination: str | Path) -> Path:
    """Fold one month of minute bars into daily bars written to ``destination``."""
    return _write_parquet(con, aggregate_sql(), destination, {"uri": uri})


def merge_shards(
    con: duckdb.DuckDBPyConnection,
    shards: Sequence[str | Path],
    destination: str | Path,
) -> Path:
    """Concatenate daily shards into the final dataset, ordered by ticker then date."""
    if not shards:
        raise ValueError("merging needs at least one shard")
    query = (
        f"SELECT {', '.join(DAILY_COLUMNS)}\n"
        f"FROM read_parquet({_sql_literal_list(shards)})\n"
        "ORDER BY ticker, date"
    )
    return _write_parquet(con, query, destination)


def inspect_dataset(path: str | Path) -> dict[str, Any]:
    """Rows, tickers, date span and null counts of a written daily dataset."""
    con = duckdb.connect()
    try:
        row = con.execute(
            f"""SELECT count(*) AS rows,
                       count(DISTINCT ticker) AS tickers,
                       min(date) AS first_date,
                       max(date) AS last_date,
                       count(*) FILTER (open IS NULL) AS null_opens,
                       count(*) FILTER (close IS NULL) AS null_closes
                FROM read_parquet({_sql_literal(path)})"""
        ).fetchone()
    finally:
        con.close()
    keys = ("rows", "tickers", "first_date", "last_date", "null_opens", "null_closes")
    summary: dict[str, Any] = dict(zip(keys, row, strict=True))
    summary["first_date"] = str(summary["first_date"])
    summary["last_date"] = str(summary["last_date"])
    return summary


def _log(message: str) -> None:
    """Progress line for the operator; the run summary is the machine-readable output."""
    print(message, file=sys.stderr, flush=True)


def build_daily(
    months: Sequence[Month],
    *,
    work_dir: str | Path,
    destination: str | Path,
    force: bool = False,
    workers: int = 1,
    threads: int = 4,
    memory_limit: str = "4GB",
    keep_shards: bool = False,
    proxy: str | None = None,
    attempts: int = 3,
) -> dict[str, Any]:
    """Aggregate ``months`` into ``destination``, caching one daily shard per month.

    Shards already on disk are reused unless ``force``. A month that fails raises before
    anything is merged, so ``destination`` only ever holds a complete dataset, and the
    shards stay on disk for the next attempt to resume from.
    """
    if workers < 1:
        raise ValueError(f"workers must be at least 1, got {workers}")
    started = time.monotonic()
    shards_dir = Path(work_dir)
    shards_dir.mkdir(parents=True, exist_ok=True)
    resolved_proxy = ambient_proxy() if proxy is None else proxy

    upstream = list_source_months(proxy=resolved_proxy, attempts=attempts)
    available = set(upstream)
    requested = tuple(sorted(set(months)))
    missing = tuple(month for month in requested if month not in available)
    # A month inside the published span that is absent is a hole; one outside it is just
    # not published (yet), like a quarter EDGAR has not closed.
    holes = tuple(month for month in missing if upstream[0] <= month < upstream[-1])
    if holes:
        raise QuotesSourceError(
            f"upstream publishes up to {upstream[-1]} but is missing requested "
            + ", ".join(str(month) for month in holes)
        )
    present = [month for month in requested if month in available]
    if not present:
        raise QuotesSourceError(
            f"none of the {len(requested)} requested month(s) are published upstream "
            f"(upstream ends at {upstream[-1]})"
        )
    todo = [month for month in present if force or not shard_path(shards_dir, month).exists()]
    reused = len(present) - len(todo)
    _log(
        f"quotes: {len(todo)} month(s) to aggregate, {reused} shard(s) reused, "
        f"{len(missing)} month(s) beyond the published range"
    )

    def connect() -> duckdb.DuckDBPyConnection:
        connection = duckdb.connect()
        configure_connection(
            connection,
            threads=threads,
            memory_limit=memory_limit,
            temp_directory=shards_dir / "tmp",
            proxy=resolved_proxy,
        )
        return connection

    worker_state = threading.local()
    opened: list[duckdb.DuckDBPyConnection] = []
    progress = count(1)
    failures: dict[str, str] = {}

    def aggregate_one(month: Month) -> None:
        connection = getattr(worker_state, "connection", None)
        if connection is None:
            connection = connect()
            worker_state.connection = connection
            opened.append(connection)
        target = aggregate_month(connection, source_uri(month), shard_path(shards_dir, month))
        size = target.stat().st_size
        _log(f"[{next(progress)}/{len(todo)}] {month} -> {target.name} ({size} bytes)")

    try:
        if workers == 1:
            for month in todo:
                try:
                    aggregate_one(month)
                except Exception as error:  # noqa: BLE001 - reported per month, raised below
                    failures[str(month)] = f"{type(error).__name__}: {error}"
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(aggregate_one, month): month for month in todo}
                for future, month in futures.items():
                    try:
                        future.result()
                    except Exception as error:  # noqa: BLE001 - reported per month, raised below
                        failures[str(month)] = f"{type(error).__name__}: {error}"
        if failures:
            raise QuotesSourceError(
                f"{len(failures)} month(s) failed: "
                + "; ".join(f"{month} ({reason})" for month, reason in sorted(failures.items()))
            )

        output = Path(destination)
        merger = connect()
        try:
            merge_shards(merger, [shard_path(shards_dir, month) for month in present], output)
        finally:
            merger.close()
    finally:
        for connection in opened:
            connection.close()

    summary: dict[str, Any] = {
        "source": SOURCE_REPO,
        "months_requested": len(requested),
        "months_aggregated": len(todo),
        "months_reused": reused,
        "months_absent_upstream": [str(month) for month in missing],
        "first_month": str(present[0]),
        "last_month": str(present[-1]),
        "output": str(output),
        "output_bytes": output.stat().st_size,
        "shards_kept": bool(keep_shards),
        "seconds": round(time.monotonic() - started, 1),
    }
    summary.update(inspect_dataset(output))
    if not keep_shards:
        shutil.rmtree(shards_dir, ignore_errors=True)
    return summary
