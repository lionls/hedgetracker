"""Hive-partitioned Parquet storage for the 13F data lake.

Layout: ``{base_dir}/13f_holdings/year={YYYY}/quarter={Q}/{CIK}.parquet``

Paths are plain filesystem semantics, so ``base_dir`` can point at a mounted
S3/GCS prefix without changing any caller.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from hedgetracker.schema import ARROW_SCHEMA, enforce_schema, to_arrow_table

#: Top-level directory of the dataset inside ``base_dir``.
DATASET = "13f_holdings"

#: Width of the SEC's canonical zero-padded CIK.
CIK_WIDTH = 10


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
