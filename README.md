# hedgetracker

Prefect pipelines that track hedge fund fund flow by extracting quarterly
13F-HR holdings from SEC EDGAR into a partitioned Parquet data lake.

See [`PRD.md`](PRD.md) for the product requirements.

## What it does

```
EDGAR quarterly index  ──►  get_filings(form="13F-HR", year, quarter)
        │
        ▼
extract_13f_holdings (task, .map over every filing)
        │  rate_limit("sec-api") · 3 retries · 15 s backoff
        │  filing.obj() → infotable → normalize → enforce_schema
        ▼
{base_dir}/13f_holdings/year={YYYY}/quarter={Q}/{CIK}.parquet
```

* **Idempotent** — one Parquet file per filer, overwritten atomically on re-run.
* **Strict schema** — every file conforms to `EXPECTED_SCHEMA` (PyArrow), missing
  source columns are created as nulls, and `"nan"`/`"None"` placeholders become
  real nulls. A partition always reads back as one coherent table.
* **Fund-flow ready** — `report_period` is always present, so quarter-over-quarter
  position deltas can be computed per filer.

## Layout

```
src/hedgetracker/
├── schema.py            # EXPECTED_SCHEMA, enforce_schema(), ARROW_SCHEMA
├── storage.py           # Hive-style partition paths + atomic Parquet writes
├── settings.py          # env-backed configuration
├── cli.py               # `hedgetracker run ...`
├── data.py              # edgartools access helpers (identity, filing lookup)
└── flows/
    └── sec_13f.py       # extract_13f_holdings task + extract_quarterly_13f flow
tests/                   # offline unit tests + `-m live` SEC integration tests
```

## Setup

Requires Python 3.10+ and [`uv`](https://docs.astral.sh/uv/).

```bash
uv sync                      # create .venv and install dependencies
cp .env.example .env         # then set SEC_IDENTITY_EMAIL
```

The SEC requires a real contact address in the `User-Agent` of every request;
`SEC_IDENTITY_EMAIL` is that address and `edgartools` sends it on your behalf.
The default (`mark@gmail.com`) is a test identity — set your own before any
production run.

## Running

```bash
# Q3 2024, the PRD's worked example
uv run python -m hedgetracker.flows.sec_13f

# or through the CLI
uv run hedgetracker run --year 2024 --quarter 3

# smoke run: first 25 filings only, into a scratch lake
uv run hedgetracker run --year 2024 --quarter 4 --limit 25 --base-dir data/smoke
```

| Flag | Required | Meaning |
| --- | --- | --- |
| `--year` | yes | EDGAR index year to scan |
| `--quarter` | yes | EDGAR index quarter to scan (1-4) |
| `--base-dir` | no | Data lake root (default `$HEDGETRACKER_BASE_DIR` or `data/lake`) |
| `--email` | no | SEC identity (default `$SEC_IDENTITY_EMAIL` or the test identity) |
| `--limit` | no | Process only the first N filings (smoke tests) |

### Partitioning

`year` / `quarter` in the flow arguments select **which EDGAR quarterly index to
scan** — the filing window. The Parquet partition is derived from each filing's
own `report_period` (the holdings as-of date), so a Q2 position report filed
during Q3 lands in `quarter=2`, and late or amended filings never pollute the
wrong quarter.

Files are written as `{base_dir}/13f_holdings/year={YYYY}/quarter={Q}/{CIK}.parquet`
with the CIK zero-padded to the SEC's canonical 10 digits. The layout is
Hive-partitioned and path-based only, so the base directory can be swapped for an
S3/GCS prefix without touching flow code.

### SEC load, rate limits and failures

EDGAR throttles aggressively, and `edgartools` reacts to throttling in ways the
pipeline has to compensate for.

* `edgartools` caps its own requests at 9/s process-wide
  (`EDGAR_RATE_LIMIT_PER_SEC`) with a 30 s per-request timeout. Bulk runs should
  raise it — `EDGAR_HTTP_TIMEOUT=60` is what the verification runs below used.
* The flow adds a Prefect concurrency limit (`sec-api`: 10 slots decaying at 10/s,
  i.e. a sustained 10 requests/s) via `rate_limit("sec-api", occupy=1)` before each
  filing, and processes at most `MAX_CONCURRENT_FILINGS` filings at a time.
* When the SEC answers a submission request with an error page (HTTP 503 under
  load), `edgartools` degrades the filing to a stub built from its homepage index
  and caches that stub on the `Filing` instance. The degraded stub has an **empty
  attachment index** and no information table, so a naive extraction either records
  the filer as holding nothing or crashes with
  `AttributeError: 'NoneType' object has no attribute 'empty'` from deep inside
  `edgartools`' legacy text fallback.

The extraction therefore distinguishes three outcomes per filing:

| Outcome | Meaning | Effect |
| --- | --- | --- |
| frame written | information table parsed | `{CIK}.parquet` in the `report_period` partition |
| `None` | filing indexes no information table, or an empty one | counted as `skipped_no_holdings`, no file written |
| `SECDocumentUnavailable` | the submission, its index or the table document could not be read | task retries (3 attempts, 15 s apart) |

Because `edgartools` caches the degraded submission, a retry on the same object
would re-read the stub instead of asking the SEC again; the extraction re-fetches
each retried filing through a fresh handle so the retry is a real retry. Failures
that survive all attempts still fail the run: every filing is attempted, all
successful filings are written, and the flow then raises
`RuntimeError: N of M 13F-HR filings failed to extract (first: ...)`. An
incomplete quarter therefore never looks like a completed one, and because writes
are atomic per filer, re-running the same window is the fix — it is idempotent and
overwrites each file with identical bytes.

## Testing

```bash
uv run pytest                 # offline: schema, partitioning, retries, idempotency
uv run pytest -m live         # adds real SEC EDGAR extraction
uv run ruff check . && uv run ruff format --check .
```

`-m live` tests hit the SEC over the network and are excluded from the default
run.

### Verification runs

The offline suite passes (31 tests) and the live smoke run
(`--year 2024 --quarter 3 --limit 25`, scratch lake) wrote 25 files / 3069 holding
rows with no failures, spread over 11 report-period partitions
(`2021Q4`-`2024Q2` — a Q3 2024 index window, so the partition is the filer's own
as-of date, not the filing date). Every file's Parquet schema equals
`ARROW_SCHEMA` exactly, every `cik` column matches its file name, every partition
matches its `report_period`, and string placeholders (`""`, `"nan"`, `"None"`)
read back as real nulls. Running the same command twice rewrote all 25 files
byte-identically (21 s warm, against 149 s for the cold run that populated the
cache).
