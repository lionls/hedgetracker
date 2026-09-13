"""Command line entry point for the 13F extraction pipeline."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from hedgetracker import data, quotes, settings
from hedgetracker.flows.sec_13f import backfill_13f, extract_quarterly_13f


def _quarters(text: str) -> tuple[int, ...]:
    """Parse ``--quarters 3,4`` into the quarter numbers to sweep."""
    try:
        quarters = tuple(int(part) for part in text.split(","))
    except ValueError:
        raise argparse.ArgumentTypeError(f"quarters must be numbers, got {text!r}") from None
    unknown = [quarter for quarter in quarters if quarter not in data.QUARTERS]
    if unknown:
        raise argparse.ArgumentTypeError(f"quarters must be in {data.QUARTERS}, got {unknown}")
    return quarters


def _add_base_dir_option(parser: argparse.ArgumentParser) -> None:
    """Add the data lake root option, resolved by :func:`settings.base_dir`."""
    parser.add_argument(
        "--base-dir",
        default=None,
        help="data lake root (default: "
        f"${settings.BASE_DIR_ENV_VAR} or {settings.DEFAULT_BASE_DIR})",
    )


def _add_extraction_options(parser: argparse.ArgumentParser) -> None:
    """Add the destination and identity options that every extraction command shares."""
    _add_base_dir_option(parser)
    parser.add_argument(
        "--email",
        default=None,
        help=f"SEC contact identity (default: ${settings.EMAIL_ENV_VAR})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="extract only the first N filings of each EDGAR index window",
    )


def _months(text: str) -> tuple[int, ...]:
    """Parse ``--months 1,12`` into the month numbers to fold."""
    try:
        months = tuple(int(part) for part in text.split(","))
    except ValueError:
        raise argparse.ArgumentTypeError(f"months must be numbers, got {text!r}") from None
    unknown = [month for month in months if month not in range(1, 13)]
    if unknown:
        raise argparse.ArgumentTypeError(f"months must be between 1 and 12, got {unknown}")
    return months


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hedgetracker",
        description="Extract SEC 13F-HR holdings into a partitioned Parquet data lake.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    run = subcommands.add_parser("run", help="extract one EDGAR quarterly index window")
    run.add_argument("--year", type=int, required=True, help="EDGAR index year to scan")
    run.add_argument(
        "--quarter", type=int, required=True, choices=data.QUARTERS, help="EDGAR index quarter"
    )
    _add_extraction_options(run)

    backfill = subcommands.add_parser(
        "backfill", help="extract every closed EDGAR quarterly index window in a range of years"
    )
    backfill.add_argument(
        "--start-year",
        type=int,
        default=data.FIRST_XML_INFORMATION_TABLE_YEAR,
        help=f"first year to sweep (default: {data.FIRST_XML_INFORMATION_TABLE_YEAR}, the first "
        "year of XML information tables; 1993 also reads the tables embedded in older filings)",
    )
    backfill.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="last year to sweep (default: the current year)",
    )
    backfill.add_argument(
        "--quarters",
        type=_quarters,
        default=data.QUARTERS,
        metavar="1,2,3,4",
        help=f"quarters to sweep in every year (default: {','.join(map(str, data.QUARTERS))})",
    )
    _add_extraction_options(backfill)

    quotes_command = subcommands.add_parser(
        "quotes",
        help="fold the Hugging Face one-minute OHLCV files into a daily OHLCV dataset",
        description="Stream every monthly Parquet file of the mito0o852/OHLCV-1m "
        "dataset through DuckDB, fold the one-minute bars of each New York regular "
        "session into a daily bar, and merge the months into one Parquet file.",
    )
    quotes_command.add_argument(
        "--start-year",
        type=int,
        default=quotes.FIRST_SOURCE_MONTH[0],
        help=f"first year to fold (default: {quotes.FIRST_SOURCE_MONTH[0]})",
    )
    quotes_command.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="last year to fold (default: the current year)",
    )
    quotes_command.add_argument(
        "--months",
        type=_months,
        default=None,
        metavar="1,2,12",
        help="months to fold in every year of the range (default: all twelve)",
    )
    quotes_command.add_argument(
        "--out",
        default=None,
        help=f"destination Parquet file (default: "
        f"{{base_dir}}/{quotes.DATASET}/{quotes.DAILY_FILE_NAME})",
    )
    quotes_command.add_argument(
        "--work-dir",
        default=None,
        help="directory holding the resumable per-month shards (default: "
        f"{{base_dir}}/{quotes.DATASET}/{quotes.SHARDS_DIRECTORY}); removed after a "
        "successful merge unless --keep-shards",
    )
    quotes_command.add_argument(
        "--force",
        action="store_true",
        help="re-fold months whose shard is already on disk",
    )
    quotes_command.add_argument(
        "--workers",
        type=int,
        default=2,
        help="months folded in parallel (default: 2)",
    )
    quotes_command.add_argument(
        "--threads",
        type=int,
        default=4,
        help="DuckDB threads per connection (default: 4)",
    )
    quotes_command.add_argument(
        "--memory-limit",
        default="4GB",
        help="DuckDB memory limit per connection (default: 4GB)",
    )
    quotes_command.add_argument(
        "--keep-shards",
        action="store_true",
        help="keep the per-month shards after a successful merge",
    )
    _add_base_dir_option(quotes_command)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the pipeline and print the run summary as JSON."""
    args = _parser().parse_args(argv)
    if args.command == "run":
        summary = extract_quarterly_13f(
            user_email=settings.sec_identity_email(args.email),
            year=args.year,
            quarter=args.quarter,
            base_dir=settings.base_dir(args.base_dir),
            limit=args.limit,
        )
    elif args.command == "backfill":
        summary = backfill_13f(
            user_email=settings.sec_identity_email(args.email),
            start_year=args.start_year,
            end_year=args.end_year,
            quarters=args.quarters,
            base_dir=settings.base_dir(args.base_dir),
            limit=args.limit,
        )
    elif args.command == "quotes":
        lake = settings.base_dir(args.base_dir)
        summary = quotes.build_daily(
            quotes.iter_months(args.start_year, args.end_year or date.today().year, args.months),
            work_dir=Path(args.work_dir)
            if args.work_dir
            else lake / quotes.DATASET / quotes.SHARDS_DIRECTORY,
            destination=Path(args.out)
            if args.out
            else lake / quotes.DATASET / quotes.DAILY_FILE_NAME,
            force=args.force,
            workers=args.workers,
            threads=args.threads,
            memory_limit=args.memory_limit,
            keep_shards=args.keep_shards,
        )
    else:
        raise AssertionError(f"unhandled command {args.command!r}")
    json.dump(summary, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
