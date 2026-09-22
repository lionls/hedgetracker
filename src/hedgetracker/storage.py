"""Hive-partitioned Parquet storage for the 13F data lake.

Layout: ``{base_dir}/13f_holdings/year={YYYY}/quarter={Q}/{CIK}.parquet``, or —
once :func:`compact_holdings` has merged a quarter — a single
``…/quarter={Q}/holdings.parquet`` in place of that quarter's per-filer files.

Paths are plain filesystem semantics, so ``base_dir`` can point at a mounted
S3/GCS prefix without changing any caller.
"""

from __future__ import annotations

import os
import time
from collections.abc import Sequence
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from hedgetracker.schema import ARROW_SCHEMA, enforce_schema, to_arrow_table

#: Top-level directory of the dataset inside ``base_dir``.
DATASET = "13f_holdings"

#: Width of the SEC's canonical zero-padded CIK.
CIK_WIDTH = 10

#: File holding a whole compacted quarter, written beside the per-filer files it
#: replaces. Ten digits of CIK cannot collide with it.
COMPACTED_NAME = "holdings.parquet"


def cik_key(cik: str | int) -> str:
    """Canonical zero-padded 10-digit CIK, used for both the column and the file name."""
    return str(cik).strip().zfill(CIK_WIDTH)


def year_quarter(report_period: datetime | date | pd.Timestamp) -> tuple[int, int]:
    """Calendar year and quarter that a holdings report belongs to."""
    timestamp = pd.Timestamp(report_period)
    return timestamp.year, (timestamp.month - 1) // 3 + 1


def partition_dir(base_dir: str | Path, year: int, quarter: int) -> Path:
    """Directory holding one quarter of holdings, in Hive partitioning style."""
    return Path(base_dir) / DATASET / f"year={year}" / f"quarter={quarter}"


def holdings_path(base_dir: str | Path, year: int, quarter: int, cik: str | int) -> Path:
    """Full path of the single Parquet file that holds one filer's quarter."""
    return partition_dir(base_dir, year, quarter) / f"{cik_key(cik)}.parquet"


def compacted_path(base_dir: str | Path, year: int, quarter: int) -> Path:
    """Full path of the single Parquet file that holds a compacted quarter."""
    return partition_dir(base_dir, year, quarter) / COMPACTED_NAME


def partition_parts(base_dir: str | Path, year: int, quarter: int) -> tuple[Path, ...]:
    """The per-filer files of one quarter, in filename order.

    A part is what one filer's extraction writes and what a compaction merges
    away, so the compacted file is not one of them: a compacted quarter has no
    parts left.
    """
    files = partition_dir(base_dir, year, quarter).glob("*.parquet")
    return tuple(sorted(path for path in files if path.name != COMPACTED_NAME))


@lru_cache(maxsize=64)
def _compacted_ciks(path: str, modified_ns: int, size: int) -> frozenset[str]:
    """The CIKs one compacted quarter holds, re-read whenever the file changes.

    Keyed on the file's identity rather than its path alone: a compaction
    rewrites it, and a cache that outlived that rewrite would go on reporting a
    filer as extracted whose file the compaction had just merged.
    """
    return frozenset(pq.read_table(path, columns=["cik"]).column("cik").to_pylist())


def extracted_path(base_dir: str | Path, year: int, quarter: int, cik: str | int) -> Path | None:
    """The lake file that already holds ``cik``'s ``quarter``, or ``None``.

    This is the extraction's "already extracted" test, and it answers for either
    layout a partition can be in: a filer is recognised from the name of its own
    file, or — once :func:`compact_holdings` has merged the quarter — from the
    ``cik`` column of the single file that replaced them.
    """
    part = holdings_path(base_dir, year, quarter, cik)
    if part.exists():
        return part
    compacted = compacted_path(base_dir, year, quarter)
    try:
        stat = compacted.stat()
    except FileNotFoundError:
        return None
    if cik_key(cik) in _compacted_ciks(str(compacted), stat.st_mtime_ns, stat.st_size):
        return compacted
    return None


def write_holdings(holdings: pd.DataFrame, path: str | Path) -> Path:
    """Write ``holdings`` to ``path`` as Parquet, creating partitions as needed.

    The write is atomic (temp file + rename) and always replaces any existing
    file, which is what makes re-running a quarter idempotent: a filer's
    partition holds exactly the most recent extraction of that filing.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    table = to_arrow_table(holdings)
    # Same directory so the rename stays on one filesystem and is atomic.
    temporary = target.with_name(f".{target.name}.tmp")
    try:
        pq.write_table(table, temporary, compression="snappy")
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def holdings_files(base_dir: str | Path) -> tuple[Path, ...]:
    """Every holdings file in the lake, in partition order.

    Raises ``FileNotFoundError`` when the lake holds nothing, which is a mistyped
    ``base_dir`` rather than a lake that is already up to date.
    """
    root = Path(base_dir) / DATASET
    files = tuple(sorted(root.glob("year=*/quarter=*/*.parquet")))
    if not files:
        raise FileNotFoundError(f"no holdings files under {root}")
    return files


def conform_holdings(base_dir: str | Path, *, force: bool = False) -> dict[str, Any]:
    """Rewrite the holdings files whose Parquet schema is not the current one.

    Reading Parquet metadata is enough to recognise a file that already conforms,
    so an up-to-date lake costs one small read per file and nothing is written. A
    file that predates a schema addition — a lake extracted before the
    CUSIP-derived ``ticker`` existed, say — is read, projected through
    :func:`hedgetracker.schema.enforce_schema`, and written back to the same path.
    That is what makes the lake a single coherent table again, offline, without
    re-extracting a single filing.

    ``force`` rewrites every file instead, which is what a change to a *derived*
    column needs: the schema is unchanged, so the metadata comparison cannot see
    that the values in the file were computed by older code.
    """
    started = time.monotonic()
    files = holdings_files(base_dir)
    rewritten = 0
    rows = 0
    for path in files:
        if not force and pq.read_schema(path) == ARROW_SCHEMA:
            continue
        holdings = enforce_schema(pq.read_table(path).to_pandas())
        write_holdings(holdings, path)
        rewritten += 1
        rows += len(holdings)
    return {
        "base_dir": str(base_dir),
        "files": len(files),
        "rewritten": rewritten,
        "skipped": len(files) - rewritten,
        "rows": rows,
        "force": force,
        "seconds": round(time.monotonic() - started, 1),
    }


def partitions(base_dir: str | Path) -> tuple[tuple[int, int], ...]:
    """Every ``(year, quarter)`` the lake holds a partition for, oldest first.

    Raises ``FileNotFoundError`` when the lake holds no partition at all, which
    is a mistyped ``base_dir`` rather than a lake that needs nothing done to it.
    """
    root = Path(base_dir) / DATASET
    found: list[tuple[int, int]] = []
    for path in root.glob("year=*/quarter=*"):
        try:
            year = int(path.parent.name.removeprefix("year="))
            quarter = int(path.name.removeprefix("quarter="))
        except ValueError:
            continue
        found.append((year, quarter))
    if not found:
        raise FileNotFoundError(f"no holdings partitions under {root}")
    return tuple(sorted(found))


def compact_holdings(
    base_dir: str | Path,
    *,
    years: Sequence[int] | None = None,
    quarters: Sequence[int] | None = None,
) -> dict[str, Any]:
    """Merge each quarter's per-filer files into the one file that replaces them.

    The extraction writes a file per filer per quarter, which is what keeps a
    single filing's write atomic and a re-run cheap, but a real quarter is a few
    thousand files and a lake of a few years is tens of thousands, every one of
    which a reader has to open. This walks the lake and merges each quarter into
    ``holdings.parquet`` — the file those filers' rows would have been written to
    in the first place: the same rows, in the same partition, one file instead of
    thousands.

    The merge is convergent, which is what makes it safe to repeat. A filer's file
    is the extraction's unit of truth — one file per filer and quarter, written
    whole — so a part written since the last compaction supersedes that filer's
    rows in the compacted file rather than being merged with them: the compacted
    file is read first, those rows are dropped, and the parts are appended. Every
    line a part holds is kept. A 13F information table holds one line per
    security and a filer may report the same security more than once — under
    different managers, as a put and as a call — so lines are never deduplicated
    against each other; only a whole filer's file is replaced. The merged file is
    written atomically and only then are the parts removed: interrupted in
    between, the partition is one the next run merges to the same result, because
    a part that is still there still supersedes the same rows.

    Each partition is read, merged and written whole, so one quarter of rows is
    the working set. ``years`` and ``quarters`` narrow the walk; a scope that
    matches no partition of the lake is an error, not an empty success.

    The extraction keeps working across a compaction: a filing written afterwards
    is a new part in the same partition, and :func:`extracted_path` answers for
    both layouts. Run this when the extraction is idle — a part written *while* a
    quarter is being merged is not in the merged file, so it stays a part until
    the next compaction, and editing a part that a running merge already read is
    the one way to lose a row.
    """
    started = time.monotonic()
    wanted_years = None if years is None else set(years)
    wanted_quarters = None if quarters is None else set(quarters)
    selected = [
        (year, quarter)
        for year, quarter in partitions(base_dir)
        if (wanted_years is None or year in wanted_years)
        and (wanted_quarters is None or quarter in wanted_quarters)
    ]
    if not selected:
        in_years = sorted(wanted_years) if wanted_years else "any"
        in_quarters = sorted(wanted_quarters) if wanted_quarters else "any"
        raise ValueError(
            f"{base_dir} holds no partition in years {in_years} and quarters {in_quarters}"
        )

    per_partition: list[dict[str, Any]] = []
    for year, quarter in selected:
        target = compacted_path(base_dir, year, quarter)
        parts = partition_parts(base_dir, year, quarter)
        if not parts:
            # Nothing to merge: a compacted quarter, or one whose every filer
            # filed nothing.
            per_partition.append(
                {
                    "year": year,
                    "quarter": quarter,
                    "parts": 0,
                    "rows": pq.read_metadata(target).num_rows if target.exists() else 0,
                    "rows_superseded": 0,
                    "rewritten": False,
                }
            )
            continue

        part_frames = [pq.read_table(part).to_pandas() for part in parts]
        # One file per filer and quarter, rewritten whole, so a filer's part
        # supersedes every row the compacted file holds for that filer.
        filers = {cik for frame in part_frames for cik in frame["cik"].tolist()}
        frames = part_frames
        superseded = 0
        if target.exists():
            carried = pq.read_table(target).to_pandas()
            superseded = int(carried["cik"].isin(filers).sum())
            frames = [carried[~carried["cik"].isin(filers)], *part_frames]
        # The projection the extraction writes through, so a part extracted under
        # an older schema — before the CUSIP-derived ticker existed — is upgraded
        # here rather than poisoning the file the whole quarter merges into.
        merged = enforce_schema(pd.concat(frames, ignore_index=True))
        write_holdings(merged, target)
        for part in parts:
            part.unlink()
        per_partition.append(
            {
                "year": year,
                "quarter": quarter,
                "parts": len(parts),
                "rows": len(merged),
                "rows_superseded": superseded,
                "rewritten": True,
            }
        )

    return {
        "base_dir": str(base_dir),
        "partitions": len(per_partition),
        "rewritten": sum(entry["rewritten"] for entry in per_partition),
        "skipped": sum(not entry["rewritten"] for entry in per_partition),
        "files_removed": sum(entry["parts"] for entry in per_partition),
        "rows": sum(entry["rows"] for entry in per_partition),
        "rows_superseded": sum(entry["rows_superseded"] for entry in per_partition),
        "seconds": round(time.monotonic() - started, 1),
        "per_partition": per_partition,
    }
