# hedgetracker

Prefect pipelines that track hedge fund fund flow by extracting quarterly
13F-HR holdings from SEC EDGAR into a partitioned Parquet data lake, plus the
daily price bars those holdings are tracked against.

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

* **Idempotent** — one Parquet file per filer, written atomically, and a filing
  whose quarter is already in the lake is skipped rather than rewritten: a re-run
  reads only what is missing.
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
├── cli.py               # `hedgetracker run|backfill|quotes ...`
├── data.py              # edgartools access helpers (identity, filing lookup)
├── quotes.py            # Hugging Face minute bars → daily OHLCV bars
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

### Backfilling every year and quarter

`backfill` sweeps a range of EDGAR index windows in a single run, oldest first:

```bash
uv run hedgetracker backfill                    # 2013 Q1 through the last closed quarter
uv run hedgetracker backfill --start-year 1993  # from EDGAR's first indexed year
uv run hedgetracker backfill --start-year 2024 --end-year 2024 --quarters 2,4
uv run hedgetracker backfill --limit 25         # 25 filings per window, smoke scale
```

| Flag | Required | Meaning |
| --- | --- | --- |
| `--start-year` | no | First year to sweep (default `2013`, the first year of XML information tables) |
| `--end-year` | no | Last year to sweep (default: the current year) |
| `--quarters` | no | Quarters to sweep in every year (default `1,2,3,4`) |
| `--base-dir` | no | Data lake root, as for `run` |
| `--email` | no | SEC identity, as for `run` |
| `--limit` | no | First N filings of *each* window (smoke tests) |

`2013` is the default start because that is when 13F-HR filings began carrying
their information table as XML. The extraction also reads the table embedded in
older submissions, so `--start-year 1993` — EDGAR's first indexed year — works
too, just against a less structured era. A year before 1993 has no index to read
and is an error rather than an empty run.

Windows that have not closed yet are skipped: EDGAR's quarterly index is complete
only once the quarter is over, so a quarter still in progress would contribute
just the filings that have already arrived. `--end-year 2026` run in September
2026 therefore sweeps 2013 Q1 through 2026 Q2, and a range with nothing closed in
it (`--end-year 2026` in January 2026) is an error.

Each window runs as a subflow of its own, so a backfill shows up in the Prefect UI
as one child run per quarter: progress is visible, and a quarter that failed can
be re-run on its own, exactly like a normal window. The windows themselves run one
after another rather than in parallel — the SEC rate limit is global and a single
window already keeps it saturated, so parallel windows would only queue behind it.
Every window is attempted even if an earlier one failed, and the run then raises
with the list of failures, so a partial backfill never looks complete. Writes are
idempotent and filings already in the lake are skipped, so re-running a range — or
just the quarter that failed — costs only what it has not extracted yet. To force
a rewrite after changing the extraction itself, delete the partition and run again.

A sweep from 2013 is ~54 windows of up to a few thousand filings each, hours of
wall-clock time and a lot of SEC traffic: it is a background job, not a smoke
test. `--start-year`, `--quarters` and `--limit` keep trial runs small. The same
sweep is available as a Prefect deployment (`13f-backfill`, see
[Docker Compose](#docker-compose)).

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

The extraction therefore distinguishes four outcomes per filing:

| Outcome | Meaning | Effect |
| --- | --- | --- |
| already extracted | the filing's own file is in the lake, read from its index page | counted as `skipped_existing`; nothing downloaded, parsed or written |
| frame written | information table parsed | `{CIK}.parquet` in the `report_period` partition |
| `None` | filing indexes no information table, or an empty one | counted as `skipped_no_holdings`, no file written |
| `SECDocumentUnavailable` | the submission, its index or the table document could not be read | task retries (3 attempts, 15 s apart) |

An accepted filing's holdings and report period never change — amendments are
separate filings and are filtered out before extraction — so an existing file is
the current one. That is what lets the extraction recognise a filing from its
index page alone: the page is a small HTML document that states the same period
the information table does, so a re-run costs one small request per filer instead
of a submission download and an information-table parse each. A page that states
nothing, or does not answer, is not a reason to skip: the filing is then extracted
the slow way. To force a rewrite after changing the extraction itself, delete the
file or its partition and run again.

Because `edgartools` caches the degraded submission, a retry on the same object
would re-read the stub instead of asking the SEC again; the extraction re-fetches
each retried filing through a fresh handle so the retry is a real retry. Failures
that survive all attempts still fail the run: every filing is attempted, all
successful filings are written, and the flow then raises
`RuntimeError: N of M 13F-HR filings failed to extract (first: ...)`. An
incomplete quarter therefore never looks like a completed one. Re-running the
window is the fix, and it is a cheap one: filings already in the lake are skipped,
so the re-run reads and writes only the gaps.

## Daily price bars

13F holdings are quarterly snapshots, so anything that measures what a position
did between two filings needs prices. `quotes` folds the
[`mito0o852/OHLCV-1m`](https://huggingface.co/datasets/mito0o852/OHLCV-1m) dataset
— one Parquet file of **one-minute** US OHLCV bars per month, 411 files from
`ohlcv_1992-01.parquet` to `ohlcv_2026-03.parquet`, 82 GB in total — into **one
daily bar per ticker and session**, in a single Parquet file:

```bash
uv run hedgetracker quotes                        # 1992-01 through the current year
uv run hedgetracker quotes --start-year 2024 --end-year 2024 --months 1,2
uv run hedgetracker quotes --out /tmp/quotes.parquet --work-dir /tmp/quote-shards
```

| Flag | Required | Meaning |
| --- | --- | --- |
| `--start-year` | no | First year to fold (default `1992`, the first year upstream) |
| `--end-year` | no | Last year to fold (default: the current year) |
| `--months` | no | Months to fold in every year of the range, e.g. `1,2,12` (default: all twelve) |
| `--out` | no | Destination Parquet file (default `{base_dir}/market/quotes_daily.parquet`) |
| `--work-dir` | no | Directory holding the per-month shards (default `{base_dir}/market/shards`) |
| `--force` | no | Re-fold months whose shard is already on disk |
| `--workers` | no | Months folded in parallel (default `2`) |
| `--threads` | no | DuckDB threads per connection (default `4`) |
| `--memory-limit` | no | DuckDB memory limit per connection (default `4GB`) |
| `--keep-shards` | no | Keep the per-month shards after a successful merge |
| `--base-dir` | no | Data lake root, as for `run` |

### What a daily bar means

| Column | Type | Value |
| --- | --- | --- |
| `ticker` | `VARCHAR` | upstream symbol, as published |
| `date` | `DATE` | the **New York** calendar date of the session |
| `open` | `DOUBLE` | first print of the regular session — `arg_min(open, timestamp)` |
| `high` | `DOUBLE` | `max(high)` over the regular session |
| `low` | `DOUBLE` | `min(low)` over the regular session |
| `close` | `DOUBLE` | last print of the regular session — `arg_max(close, timestamp)` |
| `volume` | `DOUBLE` | upstream `volume` summed over the regular session |

Four decisions are baked into that, and they are the ones that are easy to get
wrong:

* **Regular session only** — bars are kept when the exchange-local time is
  `09:30:00` through `15:59:59` ET, so pre-market and after-hours prints (which
  the source does carry, and which a UTC-date rollover would attribute to the
  next day) never enter a daily bar. Volume is therefore regular-session volume,
  not the full-day figure a data vendor would quote.
* **Exchange-local dates** — rows are grouped by
  `timezone('America/New_York', timestamp)::DATE`, not by the UTC date of the
  timestamp. Both differ for after-hours prints, and a fixed UTC offset breaks
  twice a year: 09:30 ET is 14:30 UTC under EST and 13:30 UTC under EDT. Because
  the source partitions by UTC month while a session runs `13:30`-`21:00` UTC on
  one UTC date in either regime, no session ever straddles a month boundary and
  folding each month on its own is exact.
* **Null-safe ends** — `arg_min`/`arg_max` skip nulls, so a month whose first or
  last in-session print has a null `open`/`close` still produces that day's bar
  instead of blanking it. A day whose every in-session print lacks the field
  stays null.
* **One row per ticker and session** — `GROUP BY ticker, date`, so a ticker with
  a halt, a gap, or a single print still yields exactly one bar.

A ticker that does not trade in a month simply has no rows for it; the dataset is
sparse and has no forward-filled bars.

### Streaming the source

The source is 82 GB and is never copied: DuckDB reads the Hub Parquet files over
HTTPS through its `httpfs` extension, one month at a time, and only the daily
bars survive. The first run downloads that extension (and `icu`, if the DuckDB
build has no timezone support of its own); later runs load it from disk.

* **One shard per month**, `{work-dir}/quotes_YYYY-MM.parquet`, written
  atomically. A month whose shard is already there is reused, so a run that was
  interrupted — or a range that was folded earlier — pays only for the months it
  does not have. `--force` re-folds them anyway, which is what a change to the
  folding rules requires.
* **Never a partial dataset**: every requested month is folded first and the
  shards are merged only if all of them succeeded. A failure is reported as
  `QuotesSourceError: N month(s) failed: <month> (<reason>)` with the shards left
  on disk, so the rerun resumes; the destination keeps the previous dataset.
* **Months upstream does not publish** are not an error as long as they are outside
  the published span — the source runs `1992-01` to `2026-03`, so asking for
  `1991-06` or `2026-04` reports them in `months_absent_upstream` and folds the
  rest. A month missing *inside* the span, or a run whose every requested month is
  unpublished, is an error: a gap in the input never becomes a silent gap in the
  output.
* The source carries a handful of exact `(ticker, timestamp)` duplicates per month
  (6 in the 26,063,374 rows of `2024-01`). They are folded as they are, so such a
  minute counts twice in that day's volume.
* **After a successful merge** the work directory is removed, unless
  `--keep-shards`.
* **The merge streams; it does not re-sort.** The shards are concatenated in date
  order on a single thread. A global `ORDER BY ticker, date` over a hundred million
  bars needs several GB of memory, or a scratch directory as large again as the
  shards — the first attempt here spilled until the disk filled and the merge died
  with an `IOException`. The dataset is therefore month-major, ordered by ticker
  and date inside each month.
* **Proxies are applied by hand.** DuckDB reads `HTTP_PROXY`/`HTTPS_PROXY` itself
  but cannot parse a proxy URL that carries credentials, and a value it cannot
  parse makes the request stall instead of failing (observed here: no answer for
  over 25 minutes). The ambient variables are therefore dropped and the proxy is
  set on the connection the way DuckDB expects — host and port as `http_proxy`,
  credentials as `http_proxy_username`/`http_proxy_password`. A missing or
  unparseable proxy still means the direct connection.

Because files are partitioned by UTC month and a session never crosses a UTC
month boundary, `--months` is free to select any subset.

The whole 1992-2026 sweep is a background job: 82 GB over HTTPS, and the rate
depends mostly on the size of each month's file (`--workers 2` overlaps the
downloads; the Hub, not the CPU, is the bottleneck). Progress goes to stderr, one
line per month, and the JSON summary on stdout is the same shape as the 13F
commands'. Stopping a sweep mid-1994 and running the same command again reported
`387 month(s) to aggregate, 24 shard(s) reused, 9 month(s) beyond the published
range` — the finished months cost nothing the second time.

## Docker Compose

`docker-compose.yml` runs the whole harness — Prefect server, worker and flows —
from one image built out of this repo, so the server, the worker and the CLI all
share the `uv.lock` versions of Prefect and `edgartools`. That image is a purely
local tag (`hedgetracker:dev`) — nothing is pulled from a registry, and
`pull_policy: build` keeps Compose from going looking for it on Docker Hub — so
`docker compose up -d --build` is the whole setup.

| Service | Role |
| --- | --- |
| `prefect-server` | Prefect 3 API + UI on <http://localhost:4200>, backed by SQLite on the `prefect-data` volume |
| `worker` | creates the `process-pool` work pool, deploys `prefect.yaml`, then polls the pool |
| `cli` | one-shot `hedgetracker` run; only started through the `cli` profile |

```bash
cp .env.example .env             # optional: set SEC_IDENTITY_EMAIL here
docker compose up -d --build     # server + worker
open http://localhost:4200       # Prefect UI
```

The worker registers both deployments (`13f-quarterly` and `13f-backfill`) on
startup, so a run is one command (or one click in the UI):

```bash
docker compose exec worker \
  prefect deployment run '13F-HR Extraction Pipeline/13f-quarterly' --param limit=2
```

The whole range is one more command — one subflow run per quarter, oldest first
(see [Backfilling every year and quarter](#backfilling-every-year-and-quarter)):

```bash
docker compose exec worker \
  prefect deployment run '13F-HR Backfill/13f-backfill' \
  --param start_year=2024 --param end_year=2024 --param limit=2
```

`13f-quarterly` defaults to `limit: 5` to stay polite to EDGAR; any parameter of
either deployment can be overridden per run with `--param` (`limit`,
`year`/`quarter`, `start_year`/`end_year`, `quarters` as a JSON list such as
`--param quarters='[2,4]'`, `user_email`). Runs land
in the `lake` volume (`/data/lake` in the containers, Hive partitions as
described above; see [Inspecting the lake from the
host](#inspecting-the-lake-from-the-host) to read them) and `edgartools`' HTTP
cache lives on the `edgar-cache` volume, so re-running the same window is fast —
and it extracts nothing: filings already in the lake are skipped.

`.env` supplies `SEC_IDENTITY_EMAIL` and `EDGAR_HTTP_TIMEOUT` (the same variables
and defaults as `settings.py`) to the worker and the one-shot CLI. A *deployment*
run takes its parameters from `prefect.yaml`, which ships the test identity — pass
`--param user_email=you@example.com` when running one for real, and see
[`PRD.md`](PRD.md) for the SEC contact-address requirement.

To run without the server at all:

```bash
docker compose --profile cli run --rm cli run --year 2024 --quarter 3 --limit 5
```

`docker compose down` stops the stack; add `-v` to also drop the lake, the EDGAR
cache and the Prefect database.

### Running against another Prefect instance

The worker and the one-shot CLI take the API they talk to from `PREFECT_API_URL`
(plus `PREFECT_API_KEY` for an authenticated one), both interpolated from the
environment — see `.env.example` — so the same image can run the flows on Prefect
Cloud or any other Prefect 3 instance. The local `prefect-server` is not needed
for that, which is what `--no-deps` is for:

```bash
export PREFECT_API_URL=https://api.prefect.cloud/api/accounts/<account>/workspaces/<workspace>
export PREFECT_API_KEY=pnu_...
docker compose up -d --no-deps worker

# one-off runs use the same two variables
docker compose --profile cli run --rm cli backfill --start-year 2024 --end-year 2024 --limit 1
```

The worker still creates the `process-pool` work pool and runs `prefect deploy
--all` on startup, so pointing it at an instance that has never seen this repo
registers both deployments there. Runs are then scheduled and reported by that
instance — `prefect deployment run '13F-HR Backfill/13f-backfill' --param ...`
with the same `PREFECT_API_URL`, or its UI — while the Parquet lands in the
worker's own `lake` volume, because the worker does the writing. The API key needs
permission to create work pools and register deployments on the target workspace.
Once the worker points there, shipping a change is a rebuild and a restart of that
worker — see [Updating and redeploying](#updating-and-redeploying).

#### Updating and redeploying

Nothing in the stack is bind-mounted: the flows, `uv.lock` and `prefect.yaml` are
baked into the image at build time, and the worker re-runs `prefect deploy --all`
every time it starts (the `successfully created` lines in its log). An update is
therefore always the same two steps, and they apply to whichever instance the
worker points at:

```bash
docker compose build                  # new flows, new prefect.yaml, new uv.lock
docker compose up -d --no-deps worker # recreated because the image changed,
                                      # and re-registers both deployments
```

Against another instance those are the commands above with `PREFECT_API_URL` (and
`PREFECT_API_KEY`) exported; an exported variable wins over `.env`. A parameter
default, description, tag or entrypoint changed in `prefect.yaml` reaches the
target the same way — `deploy --all` updates the deployment in place, same id, new
`updated` timestamp, rather than adding a second one under the same name.

To re-register the deployments without restarting a worker — after deleting one on
the target, or to push the current definitions somewhere the worker is not pointed
at — run the deploy step once in a `cli` container, which is also how to see what
the target has:

```bash
export PREFECT_API_URL=... PREFECT_API_KEY=...
docker compose --profile cli run --rm --entrypoint prefect cli deploy --all
docker compose --profile cli run --rm --entrypoint prefect cli deployment ls
```

Two things `deploy --all` will not clean up for you:

- Deployments are matched by name (`<flow name>/<deployment name>`). Renaming one
  in `prefect.yaml` registers the new name and leaves the old deployment on the
  target — `prefect deployment delete '<flow name>/<old name>'`, from a `cli`
  container as above or in the target's UI.
- The work pool is created with `|| true`, so restarting the worker never changes
  an existing pool's settings. That takes `prefect work-pool update`.

#### Restarting while runs are in flight

The worker starts each flow run as a child of its own container, so recreating
that container strands whatever was running: the process goes away with the
worker while the run keeps its last state — `Running` on the target, or
`Cancelling` if you cancel it — and nothing finishes it, because no worker is left
to report on it. There is no drain, and the restarted worker takes new runs as
soon as it is up, so restart when the pool is idle; for runs already stranded,
`prefect flow-run cancel <id>` and `prefect flow-run delete <id>` (or the target's
UI) get them out of the way.

Re-running the window or the range afterwards is the recovery, and it is cheap:
filings already in the lake are skipped, so a sweep cut off part-way writes only
what it had not written yet.

### Inspecting the lake from the host

The lake is a named volume, so it is not a host directory. Read it inside the
image, where `pandas`/`pyarrow` are already installed:

```bash
docker compose exec worker find /data/lake -name '*.parquet'
docker compose exec worker python -c "import pandas as pd; print(pd.read_parquet('/data/lake'))"
```

Or copy it out — the copy keeps the `year=`/`quarter=` layout, so the repo's own
environment can read it, and the files land owned by you:

```bash
docker compose cp worker:/data/lake/. ./data/lake
uv run python -c "import pandas as pd; print(pd.read_parquet('data/lake'))"
```

To skip the copy and have runs write into the repo, mount the lake over the volume
with an override file that leaves `docker-compose.yml` untouched:

```yaml
# docker-compose.override.yml — optional: keep the lake on the host
services:
  worker:
    volumes:
      - ./data/lake:/data/lake
  cli:
    volumes:
      - ./data/lake:/data/lake
```

```bash
docker compose up -d             # recreates the worker with the extra mount
```

Runs then land in `data/lake` — the same directory the local CLI uses by default,
and `data/` is already gitignored. Containers write as root, so on Linux
`sudo chown -R "$USER" data/lake` is needed before running the CLI against that
directory; Docker Desktop maps the files to your own user, so macOS needs nothing.
A bind-mounted lake is also outside Compose's control: `down -v` drops the
volumes, not that directory.

## Testing

```bash
uv run pytest                 # offline: schema, partitioning, retries, idempotency
uv run pytest -m live         # adds real SEC EDGAR extraction
uv run ruff check . && uv run ruff format --check .
```

`-m live` tests hit the SEC over the network and are excluded from the default
run.

### Verification runs

The offline suite passes (58 tests) and the live smoke run
(`--year 2024 --quarter 3 --limit 25`, scratch lake) wrote 25 files / 3069 holding
rows with no failures, spread over 11 report-period partitions
(`2021Q4`-`2024Q2` — a Q3 2024 index window, so the partition is the filer's own
as-of date, not the filing date). Every file's Parquet schema equals
`ARROW_SCHEMA` exactly, every `cik` column matches its file name, every partition
matches its `report_period`, and string placeholders (`""`, `"nan"`, `"None"`)
read back as real nulls.

Running the same command again wrote nothing: all 25 filings were recognised from
their index pages (`written: 0`, `skipped_existing: 25`, `holdings_rows: 0`) in
28 s, against 149 s for the cold run that also populated the EDGAR cache, and
every file kept its sha256 and mtime.

The sweep was verified end to end on a second Prefect server — the same image with
`PREFECT_API_URL` pointed at it: the 2024 backfill ran its four windows as
subflows of one parent run on that instance, and a second sweep of the same range
wrote nothing (`0 files, 4 filings already extracted`, every window reported
complete) and left the lake byte-identical, checksums and timestamps included.

The `quotes` fold was verified against the live dataset rather than a fixture:
1992-01 and 1992-02 (two 31 MB files) folded in 10.5 s, and 2024-01 — 319 MB,
26,063,374 minute rows, 20,221 tickers — in 37.7 s, giving 294,847 daily bars.
Those three months were then recomputed from the upstream files with an
independent query (correlated `ORDER BY ... LIMIT 1` subqueries where the module
uses `arg_min`/`arg_max`) and compared bar for bar against the shards: 518,046
daily bars, zero mismatches. The `AAPL` bars for 2024-01-02 and 2024-01-03 match a
direct read of the minutes (`187.17/188.44/183.89/185.525` and
`184.29/185.87/183.43/184.24`, volumes 63,793,826 and 45,009,448), and the fold
reproduces that reading's independent totals for the month exactly: 294,847 bars
over 20,221 tickers and 21 sessions. Reading 2024-01's timestamps in exchange time
also shows how much the session filter removes: 749,073 prints before 09:30 ET,
286,634 in the 16:00 ET hour, and 264,350 after it, none of which reach a bar.

A two-worker sweep folds the 1990s files at roughly ten months per minute (about
five seconds each, ~2 MB of shard) and the 2020s files — 319 to 442 MB each — at
two to three months per minute, so the 411 months of the source take a bit over
two hours.

The whole sweep was then run against the live dataset: 411 months folded, the nine
months of 2026 that upstream has not published yet reported as absent, and a merge
that took 36 s and wrote 964 MB. The finished dataset holds **77,811,153 daily bars
for 80,843 tickers, from 1992-01-02 to 2026-03-31**, with no null opens or closes.
Its row count equals the sum of the 411 shard row counts exactly, every bar of
1992-01 and 2024-01 matches its shard bar for bar, and the file's 630 row groups
carry non-decreasing dates — which is what shows the merge streamed the months in
order rather than shuffling them.
