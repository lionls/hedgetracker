"""Command line entry point for the 13F extraction pipeline."""

from __future__ import annotations

import argparse
import json
import sys

from hedgetracker import settings
from hedgetracker.flows.sec_13f import extract_quarterly_13f


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hedgetracker",
        description="Extract SEC 13F-HR holdings into a partitioned Parquet data lake.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    run = subcommands.add_parser("run", help="extract one EDGAR quarterly index window")
    run.add_argument("--year", type=int, required=True, help="EDGAR index year to scan")
    run.add_argument("--quarter", type=int, required=True, choices=(1, 2, 3, 4))
    run.add_argument(
        "--base-dir",
        default=None,
        help="data lake root (default: "
        f"${settings.BASE_DIR_ENV_VAR} or {settings.DEFAULT_BASE_DIR})",
    )
    run.add_argument(
        "--email",
        default=None,
        help=f"SEC contact identity (default: ${settings.EMAIL_ENV_VAR})",
    )
    run.add_argument("--limit", type=int, default=None, help="extract only the first N filings")
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
        json.dump(summary, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    raise AssertionError(f"unhandled command {args.command!r}")


if __name__ == "__main__":
    raise SystemExit(main())
