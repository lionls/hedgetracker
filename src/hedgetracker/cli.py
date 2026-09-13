"""Command line entry point for the 13F extraction pipeline."""

from __future__ import annotations

import argparse
import json
import sys

from hedgetracker import data, settings
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


def _add_extraction_options(parser: argparse.ArgumentParser) -> None:
    """Add the destination and identity options that every extraction command shares."""
    parser.add_argument(
        "--base-dir",
        default=None,
        help="data lake root (default: "
        f"${settings.BASE_DIR_ENV_VAR} or {settings.DEFAULT_BASE_DIR})",
    )
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
    else:
        raise AssertionError(f"unhandled command {args.command!r}")
    json.dump(summary, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
