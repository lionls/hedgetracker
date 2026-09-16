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

* **Idempotent** — one Parquet file per filer, written atomically, and a filing
  whose quarter is already in the lake is skipped rather than rewritten: a re-run
  reads only what is missing.
* **Strict schema** — every file conforms to `EXPECTED_SCHEMA` (PyArrow), missing
  source columns are created as nulls, and `"nan"`/`"None"` placeholders become
  real nulls. A partition always reads back as one coherent table.
* **Ticker per holding** — `ticker` is resolved from each holding's CUSIP through
  the reference map bundled with `edgartools` (68,830 CUSIPs). That lookup is
  offline, so it costs no SEC request and covers every era — edgartools itself
  annotates only the XML information tables of 2013 and later — and `ticker` is
  the key the daily bars join on (see [Market data](#market-data) and the
  [market explorer](#market-explorer)). A CUSIP the map does not know stays null
  rather than being guessed at.
* **Fund-flow ready** — `report_period` is always present, so quarter-over-quarter
  position deltas can be computed per filer.

## Layout

```
src/hedgetracker/
├── schema.py            # EXPECTED_SCHEMA, enforce_schema(), ARROW_SCHEMA
├── reference.py         # bundled CUSIP → ticker lookup
├── storage.py           # Hive-style partition paths + atomic Parquet writes
├── settings.py          # env-backed configuration
├── cli.py               # `hedgetracker run|backfill|conform ...`
├── data.py              # edgartools access helpers (identity, filing lookup)
└── flows/
    └── sec_13f.py       # extract_13f_holdings task + extract_quarterly_13f flow
tests/                   # offline unit tests + `-m live` SEC integration tests
server/                  # Go + DuckDB backend for the market explorer
├── main.go              # flags, boot-time warm, graceful shutdown
├── duck.go              # DuckDB DSN, proxy handling, httpfs
├── dataset.go           # ticker index, profiles, bars, fundamentals, response caches
└── api.go               # JSON endpoints + the built frontend
web/                     # React frontend (Vite, lightweight-charts)
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
a rewrite after changing the extraction itself, delete the partition and run again;
a schema-only change needs no re-extraction at all, just `conform` (below).

A sweep from 2013 is ~54 windows of up to a few thousand filings each, hours of
wall-clock time and a lot of SEC traffic: it is a background job, not a smoke
test. `--start-year`, `--quarters` and `--limit` keep trial runs small. The same
sweep is available as a Prefect deployment (`13f-backfill`, see
[Docker Compose](#docker-compose)).

### Bringing an older lake onto the current schema

`ticker` was added to the schema after the first lakes had been extracted, and a
holding's ticker is a pure function of its CUSIP, so a lake extracted under an
older schema can be brought up to date without asking EDGAR for anything:

```bash
uv run hedgetracker conform                     # the default lake
uv run hedgetracker conform --base-dir data/smoke
```

| Flag | Required | Meaning |
| --- | --- | --- |
| `--base-dir` | no | Data lake root, as for `run` |
| `--force` | no | Rewrite every file, not only the ones whose schema is out of date |

`conform` reads each file's Parquet metadata and rewrites only the files whose
schema differs from the one this version writes, so an up-to-date lake costs one
small read per file and nothing is written. Every file it does read is re-projected
through `enforce_schema` — the same function the extraction writes through — and
replaced atomically in place, so the result is the file the current code would
have produced. Rows, and every column they already had, are untouched.

The metadata comparison sees schema changes, not *value* changes: if a derived
column's values were computed by older code — a ticker the reference map has since
learned, a lookup that no longer misses on a lower-case CUSIP — the file is already
on the current schema and is skipped. `--force` rewrites the lot, which is what
re-derives those columns across an existing lake.

This is the answer to "the schema changed, now what". A re-crawl is not: filings
already in the lake are skipped rather than rewritten, so a full backfill would
leave the old files exactly as they are. A `base_dir` with no holdings files at
all is an error rather than a no-op, because it usually means a typo.

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

## Market data

A holding's `ticker` is the key the daily bars join on, so pricing a position — or
measuring what a filer did between two quarters — is a join between the lake and a
source of daily bars. Two sources are in play, and the second supplements the
first rather than replacing it.

### Daily bars folded from minute data

`quotes` folds [`mito0o852/OHLCV-1m`](https://huggingface.co/datasets/mito0o852/OHLCV-1m)
— 82 GB of one-minute US bars in 411 monthly Parquet files, 1992-01 through
2026-03 — into one daily bar per ticker and session. The 82 GB is read over HTTPS
and never copied; only the fold is written:

| | |
| --- | --- |
| Destination | `{base_dir}/market/quotes_daily.parquet` |
| Bars | 77,811,153 |
| Tickers | 80,843 |
| Sessions | 1992-01-02 through 2026-03-31 |
| Columns | `ticker`, `date`, `open`, `high`, `low`, `close`, `volume` |

That command lives on the `feat/quotes-daily` branch and is not in `main` yet. It
stays as it is — the deeper history, folded from minute bars.

### The supplementary source

[`defeatbeta/yahoo-finance-data`](https://huggingface.co/datasets/defeatbeta/yahoo-finance-data)
publishes 15 Parquet tables under `data/US/`, built from Yahoo Finance, Nasdaq and
US Treasury data for research and educational use, licensed ODC-BY, and refreshed
daily: `spec.json` sits at the repo root, one level above `data/`, carrying the
`update_time` — `2026-09-15T05:10:40Z` when the numbers below were taken — and a
sha256 per file; the revision itself is `6d603343ced0d83114a529bed4872a12ae0b7c8b`.
Its daily bars are a finished table, so there is nothing to fold and — for now —
nothing to cache: each table is read over HTTPS, straight off the Hub. The tables
moved under a country directory at some point, so an older `resolve/main/data/…`
path that used to work — as the first revision of this section had it — returns
404 and the prefix is `data/US/` everywhere below.

```sql
SELECT symbol, report_date, close, volume
FROM 'https://huggingface.co/datasets/defeatbeta/yahoo-finance-data/resolve/main/data/US/stock_prices.parquet'
WHERE symbol = 'KO'
ORDER BY report_date DESC
LIMIT 5;
```

Every table follows the same shape, `resolve/main/data/US/<table>.parquet`, with
the SEC's `company_tickers.json` (1.4 MB, 10,432 tickers) sitting beside them in
`data/US/` even though it is not one of the 15. DuckDB reads the Parquet footer
first and then only the column chunks a query names, so a
filtered query touches a fraction of a 445 MiB file. Two things to remember when
running it from a sandbox like this one: DuckDB reads `HTTP_PROXY`/`HTTPS_PROXY`
itself but cannot parse a proxy URL that carries credentials, and a value it
cannot parse fails the query, so drop those variables and set the proxy through
`SET http_proxy` / `http_proxy_username` / `http_proxy_password` instead, exactly
as `quotes.py` does; and keep `temp_directory` out of the repo, because a spill
there is written inside `data/` and has filled the disk before.

#### Tables worth joining

| Table | Size | Rows | Contents |
| --- | --- | --- | --- |
| `stock_prices` | 445 MiB | 36,701,230 | `symbol`, `report_date`, `open`, `close`, `high`, `low`, `volume` — `DECIMAL(16,4)` prices, `BIGINT` volume |
| `stock_split_events` | 67 KiB | 9,947 | `split_factor` as a ratio string (`4:1`) |
| `stock_dividend_events` | 700 KiB | 305,713 | cash amount per share, by ex-date |
| `stock_shares_outstanding` | 5.7 MiB | 1,169,934 | shares outstanding, by `report_date` |
| `stock_profile` | 2.5 MiB | 11,358 | sector, industry, employees, address |
| `exchange_rate` | 3.1 MiB | 243,276 | `EUR=X`-style pairs with OHLC |
| `stock_sec_filing` | 87 MiB | 7,832,949 | `cik`, `accession_number`, `form_type`, `filing_date`, `filing_url`, 13F-HR included |

Sizes and row counts read from the live files on 2026-09-15, at revision
`6d603343`; the day's refresh added 22,624 bars, 9 symbols and 8 profiles to a
table that had 36,678,606 bars for 12,289 symbols the day before.

The rest are financials and text — `stock_statement` (112 MiB), `stock_news`
(1.1 GiB), `stock_earning_call_transcripts` (2.1 GiB), `stock_tailing_eps`,
`stock_officers`, `stock_revenue_breakdown`, `stock_earning_calendar`,
`daily_treasury_yield` — and none of them is needed to price a holding.

#### What `stock_prices` holds

Verified against the live table on 2026-09-15: 36,701,230 bars for 12,298 symbols,
1994-11-30 through 2026-09-14, with no duplicate `symbol`/`report_date` pair — the
join key is unique — and every `report_date` an ISO `YYYY-MM-DD` string, so cast
it before joining. A symbol's history starts where the symbol starts (`ENPH` from
2012-03-30, its listing) and any symbol already trading before 1994-11-30 (`AON`,
`GILD`, `KO`) begins on that first date instead. Prices are **split-adjusted**:
AAPL closes at 126.52 on 2020-08-26, the session before its 4:1 split, rather than
the ~$500 an unadjusted series would show. Corporate actions are their own rows,
so a total-return figure needs `stock_split_events` and `stock_dividend_events`
alongside the bars.

#### What it does not cover

`stock_prices.symbol` is Yahoo's ticker, while the lake's `ticker` comes from the
CUSIP map bundled with `edgartools`, and the two spell share classes differently:
edgartools writes `BRKB` where Yahoo writes `BRK-B`. Compare separator-stripped
(`replace(upper(symbol), '-', '')`) rather than verbatim. Measured over the 3,069
holding rows and $5.54B of reported value in `data/smoke/13f_holdings` —
re-measured on 2026-09-15: 25 Parquet files, 1,098 distinct resolved tickers, none
the map cannot resolve — with each CUSIP resolved the way `conform` resolves it:

| Join | Rows priced | Reported value priced |
| --- | --- | --- |
| verbatim | 1,974 (64.3%) | $2.75B (49.7%) |
| separator-stripped | 1,988 (64.8%) | $2.79B (50.4%) |

Normalization is worth 14 rows and $38.8M of that, all of it Berkshire's class B.
What stays unpriced is mostly funds: the lake's largest unmatched positions are
`VNQ`, `VTEB`, `SHV`, `SCHO`, `DFAC`, `BIL`, `IEF`, `AVUS`, `XLP` and `SCHB`, and
the only ETFs the table carries are the largest in the market (`SPY`, `QQQ`,
`IVV`). Across the map as a whole, 10,303 of its 55,145 distinct tickers — 18.7% —
have bars: the map reaches every SEC-registered security, including funds, foreign
ordinaries, units and rights, while this table covers listed equities.

The gap that matters most for 13F work is **delisted symbols**: `TWTR` and `ATVI`
return no rows, so a position that has since been acquired, merged away or
delisted cannot be priced here — a 2021 filing's Twitter stake has no bars to join
to, whatever it was worth at the time. Anything survivorship-sensitive needs a
second source for those names.

#### Next

The first use is enrichment: join each holding to the bar for its filing date to
get a price and a position value, carry that quarter over quarter, and take a
sector from `stock_profile` for grouping. Caching the Parquet files locally is
deferred until that work needs it — `stock_prices` is 445 MiB and reads fine over
HTTPS. The [market explorer](#market-explorer) below is the first consumer of
these tables: it fetches a symbol's whole history in one query, which is the shape
the join wants, and it makes the gap between a ticker the map knows and a ticker
that has bars visible before any of that work starts.

## Market explorer

`server/` and `web/` are a browser for those tables: a ticker search, a company
card, a candlestick chart with volume, and a second page that lists operating
companies with their financial statements. The Go process answers every request
straight from the Parquet files on the Hub — no local copy of a table, no database
file, nothing written outside DuckDB's spill directory in `/tmp` — and the React
app is two pages with no router and no state library, because the whole app is six
endpoints and a handful of pieces of state. Both pages are one component with a
`page` prop, chosen from `location.pathname`; navigation is two `<a href>`s that
reload the bundle, which is cheaper than a router for two pages of a local app.

| | |
| --- | --- |
| Backend | Go 1.26, `github.com/marcboeker/go-duckdb/v2` (cgo, statically linked DuckDB) |
| Frontend | React 19, Vite 8, `lightweight-charts` 5 |
| Source of truth | `data/US/` on the Hub, read through DuckDB's `httpfs` |
| Binary | 70 MB; 17 s to build cold, 2.5 s warm |

### Endpoints

| Route | Answer |
| --- | --- |
| `GET /api/health` | `{"status":"ok","tables":"data/US/","symbols":11908,"priced":12298,"pricedDone":true}` |
| `GET /api/search?q=appl&limit=25&stocks=1` | `{"query":"appl","results":[{"symbol":"AAPL","name":"Apple Inc.","sector":"Technology","priced":true}]}` — `limit` is capped at 200, and `stocks=1` skips every indexed symbol that is not an operating company, which is what the stocks page searches: `spy` loses `SPY` and `SPYU` but keeps `SGP` and `SYRE` |
| `GET /api/stocks` | `{"stocks":[{"symbol":"AAPL","name":"Apple Inc.","sector":"Technology"},…]}` — all 8,643 operating companies, alphabetical, in one response |
| `GET /api/fundamentals/KO` | `{"symbol":"KO","periods":["2025-12-31",…],"groups":[{"name":"Income statement","rows":[{"key":"total_revenue","label":"Revenue","unit":"usd","values":[47941000000,…]},…]},…],"shares":4303000000,"trailingEps":3.3191}`; `404 {"error":"no fundamentals for symbol"}` for a symbol the dataset filed no annual statements for (every fund), and `400` for a symbol that is not a ticker |
| `GET /api/company/AAPL` | `{"symbol":"AAPL","name":"Apple Inc.","sector":"Technology","industry":"Consumer Electronics","employees":150000,"website":"https://www.apple.com","city":"Cupertino","country":"United States"}`; `404 {"error":"unknown symbol"}` |
| `GET /api/bars/AAPL?range=1y\|5y\|max` | `{"symbol":"AAPL","range":"5y","dates":[…],"open":[…],"high":[…],"low":[…],"close":[…],"volume":[…]}`; `range` defaults to `5y` and any other value is a `400` |
| `GET /*` | `web/dist`, falling back to `index.html` |

The history travels as one array per field instead of one object per bar, which
makes the JSON — and its gzip — roughly three times smaller, and every `/api/`
response is gzipped. Price columns are `DECIMAL(16,4)` in the file and cast to
`DOUBLE` in SQL, volume to `BIGINT`.

### 13F dashboard endpoints

The explorer serves the other half of the dataset: the 13F holdings lake the
extractor writes — the one table it reads from disk instead of the Hub — joined to
the same dataset's prices and split events and folded into the four views
`server/thirteenf.sql` defines. The lake says what funds held; the dataset supplies
the quarterly VWAP that turns a share change into an estimated capital flow.

| View | What it is |
| --- | --- |
| `holdings_normalized` | one row per (filer, report period, CUSIP): shares, value, portfolio weight, and how many filing lines it was collapsed from |
| `market_quarterly_vwap` | per ticker and quarter: trading days, first and last trade date, volume-weighted close, low and high |
| `fund_quarterly_flows` | one row per position a fund changed between consecutive *filings*, with split-adjusted deltas and an action — NEW, ADDED, TRIMMED, EXITED or HELD |
| `conviction_scores` | the flows joined to the VWAP: `estCapitalFlow`, and a signal — HIGH_CONVICTION_BUY, STANDARD_BUY, PASSIVE_REBALANCE, CONVICTION_DUMP, MAINTAINED or ROUTINE_ADJUSTMENT |

Three properties of the serving path matter to a client:

* **The tables are materialised, not queried live.** One conviction query reads the
  whole lake and the whole price table, which is minutes of work no dashboard
  request can wait for. The server builds the four tables in the background at
  startup, reports that build on `/api/13f/status`, keeps serving the previous build
  while a new one runs, and answers every data endpoint `503` — with the status in
  the body — until the first build is ready. `POST /api/13f/refresh` starts one by
  hand and answers `202`, or `409` while a build is already running.
* **Rows are objects here, not column arrays.** A fund-quarter is hundreds of rows
  and a dashboard wants named fields, so the 13F endpoints return one object per
  row; `/api/bars` keeps its array-per-field shape because it carries thousands of
  bars.
* **The cache survives a redeploy.** The tables and a manifest live under
  `-13f-cache`, and a manifest whose recorded view hash does not match the running
  binary is rebuilt whatever `-13f-max-age` says. Until that rebuild succeeds the
  server reports `stale` and answers every data endpoint `503` rather than serving
  tables another version of the views built; a rebuild that fails over a cache of
  the *same* views leaves the previous tables serving, with the reason in
  `lastError`.

| Route | Answer |
| --- | --- |
| `GET /api/13f/status` | `{"state":"ready","builtAt":"2026-09-16T20:06:09Z","buildSeconds":212.7,"lakeFiles":25,"filings":25,"funds":9,"positions":2422,"flows":1153,"signals":1153,"quarters":["2021-12-31",…,"2024-06-30"]}`; `state` is `ready`, `building`, `stale`, `failed` or `unconfigured`, `lastError` carries the last failed build, and `/api/health` carries `thirteenF` |
| `GET /api/13f/funds` | `{"funds":[{"cik":"0000891943","quarters":1,"firstPeriod":"2024-06-30","latestPeriod":"2024-06-30","latestValueUsd":1501802000,"latestPositions":794},…],"total":9,"limit":500}` — one row per filer, its newest portfolio, largest first |
| `GET /api/13f/holdings?cik=2038506&period=2024Q2` | `{"cik":"0002038506","period":"2024-06-30","quarters":[…11 periods…],"portfolioValueUsd":131104358,"total":67,"holdings":[{"cusip":"464288679","ticker":"SHV","issuer":"ISHARES TR","classTitle":"SHORT TREAS BD","shares":127843,"valueUsd":14126679,"weightPct":10.7751,"reportedLines":1},…]}`; `period` takes a date, a quarter label or `latest`, and defaults to the filer's newest filing; the weights of a fund-quarter sum to 100% |
| `GET /api/13f/flows?cik=2038506&action=NEW,ADDED` | `{"cik":"0002038506","period":"","actions":[…],"total":389,"flows":[{"cik":"0002038506","period":"2024-06-30","prevPeriod":"2024-03-31","quartersBetween":1,"cusip":"595112103","ticker":"MU","issuer":"MICRON TECHNOLOGY INC","action":"NEW","shares":17554,"valueUsd":2308878,"weightPct":1.7611,"prevShares":0,"deltaShares":17554,"deltaSharesPct":null,"deltaWeightPct":1.7611,"splitFactor":1,"splitAdjusted":false},…]}`; without `period` it reports the fund's whole history, newest first |
| `GET /api/13f/signals?period=2024Q2&signal=HIGH_CONVICTION_BUY` | the flow shape plus `signal`, `quarterlyVwap`, `quarterlyLow`, `quarterlyHigh`, `estCapitalFlow`. The filters are `cik`, `ticker`, `period`, `action` and `signal`, and `period=latest` is the default; with no filters it answers the lake's newest quarter, largest estimated flow first, and for one fund and ticker it is the position: `?cik=2038506&period=2024Q2&ticker=NVDA` is `{"action":"TRIMMED","shares":20139,"prevShares":21720,"deltaShares":-1581,"splitFactor":10,"splitAdjusted":true,"weightPct":1.8977,"signal":"PASSIVE_REBALANCE","quarterlyVwap":100.3169,"estCapitalFlow":-158601}` |
| `GET /api/13f/vwap?ticker=NVDA` | `{"ticker":"NVDA","quarters":[{"symbol":"NVDA","year":2024,"quarter":2,"tradingDays":63,"firstTradeDate":"2024-04-01","lastTradeDate":"2024-06-28","totalVolume":27164691100,"vwap":100.3169,"low":75.606,"high":140.76},…]}` |
| `POST /api/13f/refresh` | `202` with the status body, or `409` with it while a build is running |
| `GET /*` | `web/dist`, falling back to `index.html` |

The panels are these six calls and no more: a fund picker is `/funds`; a holdings
table is `/holdings?cik=…` with its quarter selector filled from `quarters`; a
"what changed" chart is `/flows?cik=…`, filtered by `action`; the dashboard's
opening view — what conviction moved this quarter — is `/signals?period=…`; the
drill-down from a signal row is `/signals?cik=…&ticker=…`, with the price context
beside it from `/vwap?ticker=…`; and the refresh button is `POST /refresh`.

Five things the numbers mean, which the SQL file argues in full:

* **A position is a CUSIP, and a filing can repeat one.** 184 (filer, period, CUSIP)
  groups in this lake carry more than one line item — up to five — and the views
  sum them into one position, so 3,069 filing lines become 2,422 positions.
  Without that collapse, joining a position to its previous quarter would inflate
  every weight and delta by the same factor.
* **Share deltas are split-adjusted, values are not** — a split does not change
  market value — and `splitFactor` says what was applied. Over 2024Q1→Q2, CIK
  0002038506's NVDA position reads as a 17,967-share increase from the raw filings
  and as a 1,581-share trim once the 10:1 split of 2024-06-10 is undone; CIK
  0002032121's reads as +13,395 before and +3,603 after. `splitAdjusted: false`
  also covers "this lake has no split data", which is why the factor is exposed
  next to it.
* **Flows compare a fund's consecutive filings, not consecutive quarters.**
  `quartersBetween` is 1 in the clean case; a fund that files sparsely gets its
  deltas across the gap, and a position missing from the earlier filing is NEW
  rather than ADDED.
* **The VWAP is a daily-close proxy**, `SUM(close*volume)/SUM(volume)` over the
  quarter's bars — the dataset ships `close` and `volume` and no intraday prices.
  `estCapitalFlow` is the split-adjusted share delta times it, so it is `null` for
  the third of positions whose ticker has no bars.
* **The lake carries no filer name**, only the CIK and the issuer fields, so the
  fund picker shows CIKs. A `filerName` column in the extractor's schema is the fix.

`-13f-offline` builds the tables from the lake alone: the flows, the actions and
the signals are unchanged, `estCapitalFlow` and the VWAP columns are `null`, and a
build takes a quarter of a second instead of three and a half minutes.

### The stocks page

`/stocks` is the same app in a second mode: same chart, same company card, same
search, restricted to operating companies, with the selected symbol's financial
statements between the chart and the company card. It exists because the index
does not distinguish an equity from a SPAC unit, a preferred class or an ETF, and
because none of the statement tables were reachable from the page at all.

**What counts as a stock.** The dataset states no `quote_type`, so the filter is
what the profile table allows: a symbol survives if `stock_profile` gives it a
sector, its industry is not `Shell Companies`, and its symbol has no class suffix.
Of the 11,358 profile rows, 9,805 carry a non-empty sector — the missing 1,553 are
the funds, which report nothing but a name — and the sector alone removes every
ETF in the table, `SPY` and `QQQ` included, along with every SPAC unit and warrant
that has no other detail. 828 rows report `Shell Companies`, which is what a
blank-check vehicle files, and that removes the units that do carry a sector. Of
the 363 dash-suffixed symbols left, the tails `-P*` (preferred classes), `-WI`
(when-issued), `-UN`/`-U` (units), `-WS`/`-WT` (warrants) and `-RT`/`-CL` (rights)
are dropped; a plain class like `BRK-B` or `AGM-A` has no such tail and stays.
11,908 indexed symbols go in, 8,643 come out, across 11 sectors.

The rule is a heuristic over a table that was never meant to answer this question,
and it errs in one direction: a security that files statements and reports a
sector is kept, so a closed-end fund (`QQQX`, `NBB`) stays in the list where an
ETF does not. Telling those apart needs a field the dataset does not carry.

**Browsing.** The page fetches `/api/stocks` once — the entire list, 634 KB,
114 KB gzipped, 4 ms out of the in-process index — and then filters in the
browser: a sector `<select>` over those 11 sectors, and a *Show 100 more* button,
because rendering ten thousand rows costs more than drawing the chart does.
Typing hands the sidebar back to `/api/search` with `stocks=1`, so the page never
lists a symbol its own browse list would not.

**The panel.** `GET /api/fundamentals/<symbol>` answers with the annual rows of
`stock_statement` — revenue, gross profit, operating income, net income, diluted
EPS, total assets, total liabilities, shareholders' equity, total debt, cash and
equivalents, operating cash flow, free cash flow, capital expenditure — for the
five most recent fiscal years, grouped into income statement, balance sheet and
cash flow. The browser adds the two figures the statements imply but do not state
(net margin, revenue growth) and the two that need a price (market cap and P/E,
from the share count, trailing EPS and latest close that ride along in the same
response, so nothing in the panel waits on the chart's own bars). Annual periods
only: the dataset also files a trailing-twelve-month row per symbol, under the
literal
`report_date = 'TTM'`, and mixing it into the fiscal years would label a
twelve-month window as a year. A symbol with no statements — every fund — says so
instead of showing an empty table.

The panel sits between the chart and the company card rather than at the bottom of
the column, which is a layout decision with a number behind it: at the end of the
column it began 651 px down on a 1440×768 window — chart at its 380 px minimum,
then the company card — which is past the fold entirely on a 600 px-tall window,
and a column that scrolls without a scrollbar gives no hint that anything is
there. Above the card it starts at 479 px, so the heading, the figures and the
first table are on screen without scrolling at every window height tested from 600
to 1100 px.

The same sweep found the second layout, which had the same fault: below 900 px
wide the grid collapses to one column and the sidebar is stacked over the chart,
which put the figures at 795 px — 27 px past the fold of a 768 px window. The
sidebar is capped at 32vh there and the chart states a 280 px height in place of
its 380 px minimum; a minimum cannot shrink it, because the canvas is laid out at
its initial height and holds the panel open. In that layout the figures sit at
664 px in the same window, and the panel is the first thing under the chart in
both layouts rather than the last thing in the column.

The read is per symbol and cached in the process like the bars are, so a second
visitor to a symbol pays 0.5 ms. The first one pays for the statements and one
query that unions three one-row reads — the latest share count, the latest
trailing EPS and the latest close — which together cost about as much as the
chart's own first read; that is why the panel and the chart appear together. A
figures read that fails is served with dashes but not cached, so a retry fills it
in rather than pinning the dashes on the symbol for the life of the process.

### Running it

```sh
cd web    && npm install && npm run build   # writes web/dist, 0.8 s
cd server && go build -o marketdata .       # 70 MB, 17 s cold
cd server && ./marketdata                   # :8080, serving web/dist
```

For frontend work `npm run dev` in `web/` serves the page on `:5173` with `/api`
proxied to `:8080`, leaving the Go process running as it is. `-addr` and `-web`
move the defaults, and `-temp-dir` (default `/tmp/hedgetracker-marketdata`) is
where DuckDB spills — it must stay outside the repo.

### Running it on a server

`Dockerfile.explorer` builds the whole app in one image — the Vite bundle in a
`node:22-bookworm-slim` stage, the cgo binary in `golang:1.26-bookworm`, both
copied into a `debian:bookworm-slim` runtime — and `docker-compose.yml` carries it
as an `explorer` service behind a profile, so a bare `docker compose up -d` still
means the Prefect harness and nothing else:

```sh
docker compose up -d --build explorer    # build, start, publish :8080
docker compose logs -f explorer          # index warm, listening, then the priced scan
docker compose --profile explorer down   # stops and removes the container, keeps the image
```

| Setting | Default | What it does |
| --- | --- | --- |
| `EXPLORER_PORT` | `8080` | Published host port; the container always listens on `8080` |
| `EXPLORER_MEMORY` | `2GB` | DuckDB's memory limit; the boot scan is the one query that wants more |
| `EXPLORER_THREADS` | `4` | DuckDB threads |
| `EXPLORER_HTTPS_PROXY` | empty | Sets `HTTPS_PROXY` in the container, for a host that reaches the Hub only through a proxy |
| `EXPLORER_NO_PROXY` | empty | Hosts to skip for that proxy |
| `EXPLORER_13F_MAX_AGE` | `24h` | Rebuild the materialised 13F tables when they are older than this; `0` reuses them until the mounted lake changes |

The explorer also carries the 13F dashboard's flags — `-13f-lake` (the mounted
holdings lake, `/data/lake/13f_holdings`), `-13f-cache` (where the materialised
tables go, `/tmp/marketdata/13f-dashboard` by default), `-13f-prices`/`-13f-splits`
(override the price or split source with a path or URL) and `-13f-offline` (build
without prices at all). `-13f-max-age 0` means "reuse the tables until the lake
changes", which is what a redeploy wants: the price scan behind a build is minutes,
so it should not be repeated for an unchanged lake.

Debian, not Alpine, because `go-duckdb` links DuckDB's C++ static library against
glibc. Besides that library and `ca-certificates` — the two things `ldd
marketdata` asks for beyond libc — the image installs nothing. It runs as uid
`10001`, and the only path it can write to is `/tmp/marketdata` (the
`marketdata-temp` volume), where DuckDB spills and where it also unpacks the
`httpfs` extension on first use. Nothing of the dataset is ever on disk, so the
image is the same 144 MB wherever it runs and a redeploy changes nothing about
the data.

Two consequences of reading Parquet over HTTPS. The container needs egress to
`huggingface.co` for every query, which is why `restart: unless-stopped` is there
to bring it back after a transient failure — and why the healthcheck is the
binary itself (`-health`, since the image has no `curl`): it stays red until the
dataset index actually loads. Everything else belongs to the reverse proxy.
`api.go` gzips `/api/` responses but serves the frontend as-is, so terminate TLS
and compress the bundle in front of the container if that matters. Only
`linux/amd64` was built and run here; the Dockerfile is architecture-neutral, and
`go-duckdb` ships the static DuckDB library for `linux/arm64` too.

### Measured

Against the running server on 2026-09-15, over the proxy this sandbox uses:

| Step | Measured |
| --- | --- |
| Boot: index warm — 10,432 SEC names + 11,358 profiles → 11,908 symbols | 5.1 s cold, 3.4 s with DuckDB's HTTP metadata cache already warm |
| Boot: `SELECT DISTINCT symbol`, on a background goroutine | 38.9 s, 12,298 symbols |
| `GET /api/search?q=appl` | 4 ms |
| `GET /api/bars/AAPL?range=5y`, first read of the file | 2.65 s |
| `GET /api/bars/AAPL?range=max`, same symbol, 7,999 bars | 38–48 ms |
| `GET /api/bars/AAPL?range=5y` again, from the in-process cache | 0.98 ms |
| `GET /api/bars/KO?range=5y`, first read of the file | 3.22 s |
| `GET /api/bars/MSFT?range=5y`, gzipped | 21,556 B (56,830 B raw) |
| `GET /api/bars/AAPL?range=max`, gzipped | 126,521 B (407,912 B raw) |
| `GET /api/stocks`, the whole browse list | 634,481 B (114,217 B gzipped), 3.8 ms — served from the in-process index |
| `GET /api/fundamentals/KO`, first read | 6.78 s, 1,991 B — five fiscal years, thirteen metrics, share count and trailing EPS |
| `GET /api/fundamentals/KO` again, from the in-process cache | 0.51 ms |
| The two reads behind that response, on a fresh DuckDB with the server's settings | 3.4 s (`stock_shares_outstanding` + `stock_tailing_eps`) + 3.0 s (`stock_statement`); 1.5 s + 1.5 s for the next symbol, whose file footers are already cached |
| `npm run build` | 0.21 s — 124.77 kB gzipped JS, 1.56 kB gzipped CSS |

### Why it is shaped this way

**One query per symbol, sliced in the browser.** Fetching a symbol's row group
costs 2–3 s; every further range within a row group the process has already read
costs about 40 ms. So the page asks for `range=max` once and slices `1Y`/`5Y`/`MAX`
client-side — switching the range never touches the network, and a symbol is
loaded once. The server still accepts `range` for anything else that wants it.

**The index is built at boot.** `stock_profile` (2.5 MiB) and the SEC's
`company_tickers.json` (1.4 MB) are read once at startup and merged into 11,908
searchable symbols; search is then a linear scan with no I/O and answers in ~4 ms.
The 39 s `SELECT DISTINCT symbol` that marks which symbols actually have price
history is *not* on the boot path: the server starts listening first and fills
`priced` in the background, so an early search still works and simply reports
`priced: false`.

**Search ranking without popularity data.** Nothing in these tables says which
Apple is *the* Apple, so the ordering leans on structure and then on heuristics:
exact symbol, symbol prefix, symbol substring or name word-start, then name
substring; ties go to symbols that have price history, then to the shorter company
name (Apple Inc. over Applied Industrial Technologies), then to the shorter
symbol. `appl` lands on `AAPL`, `coca` on `KO`, `johnson` on `JNJ` — and
mid-word matches like "PINE**APPL**E" sort below word-starts instead of above them.

**One index, filtered twice.** The stocks page gets no ranking machinery of its
own: `Search` takes a `stocksOnly` flag and skips the entries the filter rejects,
and `/api/stocks` is that same index in full, so a company the page can browse to
is one it can search to and both orders come out of the same comparison. The list
crosses the wire once and is then filtered and paged in the browser — the index is
already in memory, and a round trip per sector click would cost more than the
filter does.

**Fundamentals are shaped server-side.** The browser is never told what yfinance
calls a line item: `stock_statement`'s `item_name` values are mapped to keys,
labels and units in one table in `dataset.go`, so a rename upstream breaks one line
and not a page. The statements, the share count and the trailing EPS travel in one
response, and market cap and P/E are composed in the browser from the close the
chart has already loaded — one read per symbol, not three.

**Chart.** `lightweight-charts` draws candles in pane 0 and volume in pane 1 with
an autosizing canvas; the theme is dark to match the page. Each series gets
`setData` and `fitContent` when the symbol changes, and the chart instance is
removed on unmount.

**Caching.** Bars are cached per `symbol|range` (256 entries, FIFO) for the life of
the process, and DuckDB's own object and metadata caches make a re-read of a file
it has already touched roughly 70× cheaper (2.65 s → 38 ms). The Parquet files
themselves are never cached locally — that is still the deferred step below.

**Proxy.** DuckDB reads `HTTP_PROXY`/`HTTPS_PROXY` but cannot parse a proxy URL
that carries credentials, and a value it cannot parse fails the query, so the four
variables are read once, unset, and handed to DuckDB as `http_proxy`,
`http_proxy_username` and `http_proxy_password` — and to the Go HTTP client that
fetches the ticker file.

### Verification runs

`go vet ./...` is clean and the binary builds from scratch in 17 s. The API was
checked with `curl` against the running server: `AAPL` 5y returns 1,254 bars from
2021-09-15 through 2026-09-14 and `max` returns 7,999, a delisted symbol (`TWTR`)
returns zero bars rather than an error, an unknown symbol is a `404`, and
`range=2y` is a `400`.

The page was driven in headless Chromium. Searching `appl` lists `AAPL | Apple
Inc.` first and `coca` lists `KO | COCA COLA CO`; clicking a result swaps the card
(the quote moved from `333.08 / +0.81 (+0.24%)` to `89.35 / +1.06 (+1.20%)`) and
refetches only that symbol; the range buttons rewrite the change figure
(`+59.90%` at 5Y against `+599.07%` at `MAX`) without a request. The chart was
confirmed to actually paint, without anyone looking at it: read back off the price
pane's canvas, `AAPL`'s largest pane holds 3,794 candle pixels in the up colour
and 3,805 in the down colour, and switching `KO` from `1Y` to `MAX` grows the
painted area from 6,942 to 7,927 pixels.

The stocks page was checked the same way, against the same server. `/api/stocks`
answers with 8,643 symbols — `AAPL`, `KO`, `BRK-A`, `BRK-B` and `AGM-A` in;
`SPY`, `VOO`, `TLT` and `AHT-PD` out — and `spy` searched with `stocks=1` loses
`SPY` and `SPYU` while keeping `SGP` and `SYRE`, where the same query without the
flag returns `SPY` first. On the page: the sidebar lists `A`, `AA`, `AACG` under
the count `8,643 stocks`, sector `Technology` narrows that to 1,146 rows, *Show
100 more* takes the rendered list from 100 to 200, and `aht` — eight results on
`/` — returns two. Clicking a row of the browse list selects it (`AADX`, market
cap `$2.01B`, P/E `—` because its trailing EPS is negative), and `coca` selects
`KO`, whose panel reads `Market cap $384.47B` (`4.30B × 89.35`), `Trailing EPS
3.32`, `P/E 26.92` and three statement tables of 7, 5 and 3 rows, `Revenue
$47.94B $47.06B $45.75B $43B $38.66B` in the first. The panel starts 479 px down
the column, so the heading, the figures and the first table are on screen without
scrolling at 600, 768, 900 and 1100 px of window height. Switching symbols brings
the figures with the statements rather than after them: `AAGH` reads `Market cap
$4.23M` and `Shares outstanding 21.15B` from the same response, with dashes for
trailing EPS and P/E, which is what the dataset holds for it — 36 of 39 sampled
stocks carry a trailing EPS, and the gap sits in the small listings. The narrow
layout was swept too, and the figures are fully in view at 900, 820, 768 and 700
px wide on a 768 px window, 664 px down; at 900×600 they need a small scroll. The
chart keeps painting
beside it — 2,487 up and 2,664 down pixels in its candle pane at 1440×768, with
volume and the time axis on their own canvases — and the explorer page was
rechecked for regressions: `/` still returns `AHT-PD` for `aht`, has no sector
filter, no count and no fundamentals panel, keeps its headline, chart, company
card order, and its chart still fills the column (497 px at 1440×768).
`npm run dev` on `:5173` serves the page
and proxies `/api` to the Go process, checked with `curl` through the dev server.

The image was then built and run the same way. `docker compose up -d --build
explorer` takes 48 s here once the base images are pulled — `npm ci` 8 s, `go mod
download` 24 s and `go build` 21 s, the Go stages in parallel with the frontend —
and produces a 144 MB image whose binary is 60 MB. Inside the container the API
answers exactly as it does on the host: `11,908` symbols and `12,298` priced ones
reported by `/api/health`, `AAPL` 5y in 3.57 s on the first read and 5 ms from the
cache, `max` in 41 ms, 5y gzipped to 21,077 B. It reported `healthy` 10 s after
start, with the `HEALTHCHECK` the image carries; the page loaded against the
running container, `appl` listed `AAPL | Apple Inc.` first, and searching it
painted the chart — 2,985 candle pixels in the up colour and 2,973 in the down
colour in the largest pane. The dataset index warms in 4.5–5.3 s
and the priced scan runs on behind it, so `/api/health` reads `"pricedDone": true`
about a minute after a start with the default memory limit — 58 s here against the
host's 39 s, which asks DuckDB for the whole machine; `EXPLORER_MEMORY` is the
knob if boot time matters.

The image was rebuilt with the stocks page in it and the check repeated. It serves
the same numbers as the host: `/api/stocks` returns 8,643 symbols, 634,481 B,
114,219 B gzipped, in 4.3 ms; `/api/fundamentals/KO` takes 7.8 s on its first read
with the priced scan still running, then 0.4 ms, with the same five periods,
`$384.47B` market cap and `26.92` P/E on the page; `SPY` is a `404`; `/stocks` is
a `200`. The image was rebuilt and checked once more after the close moved into
the fundamentals response and the narrow layout was fixed: it serves the current
bundle (`index-dAgGe8Hr.js`), `AAGH` comes back with `close 0.0002` and no
trailing EPS — `$4.23M` market cap, dashes for EPS and P/E on the page — and the
panel's figures sit 504 px down the column at 1440×768 and 664 px at 900×768,
above the fold in both, exactly as on the host.
`docker compose --profile explorer down` removes the container and leaves
the image — without the profile the command silently matches nothing, which is why
that line carries it.

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

The offline suite passes (49 tests). The live smoke run
(`--year 2024 --quarter 4 --limit 25`, scratch lake) wrote 25 files / 1975 holding
rows with no failures, in 24 s, spread over 8 report-period year partitions
(`2017`-`2024` — a Q4 2024 index window, so the partition is the filer's own
as-of date, not the filing date). Every file's Parquet schema equals
`ARROW_SCHEMA` exactly, every `cik` column matches its file name, every partition
matches its `report_period`, and string placeholders (`""`, `"nan"`, `"None"`)
read back as real nulls.

1953 of the 1975 rows (416 distinct CUSIPs) came out with a `ticker`. The
reference map has no entry for 6 of those CUSIPs — 22 rows, among them Siemens A G
New Ord and a Schwab money-market fund — and those stay null rather than being
guessed at. Two resolved only because the lookup upper-cases the key: the filers
had written `g0403h108` (Aon) and `29355a107` (Enphase) in lower case, which the
map itself does not.

Running the same command again wrote nothing: all 25 filings were recognised from
their index pages (`written: 0`, `skipped_existing: 25`, `holdings_rows: 0`) in
34 s wall clock, and every file kept its sha256.

`conform` was verified against a lake extracted under the previous schema — a copy
of the 25-file, 3069-row smoke lake (11 report-period partitions, 1101 distinct
CUSIPs). It rewrote all 25 files in 1.0 s, and every pre-existing column came back
with an identical content digest, file by file, so nothing but the schema changed.
All 3069 rows got a ticker: the map knows every one of those 1101 CUSIPs. Running
it again rewrote nothing (`rewritten: 0`, `skipped: 25`) in 0.0 s, because the
schema comparison reads Parquet metadata only. `--force` (1.0 s for the same 25
files) re-derived the column on the ticker-era lake too, which is how the two
lower-case CUSIPs above were picked up after the fact.

The sweep was verified end to end on a second Prefect server — the same image with
`PREFECT_API_URL` pointed at it: the 2024 backfill ran its four windows as
subflows of one parent run on that instance, and a second sweep of the same range
wrote nothing (`0 files, 4 filings already extracted`, every window reported
complete) and left the lake byte-identical, checksums and timestamps included.
