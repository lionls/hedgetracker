-- 13F dashboard views.
--
-- Adapted from the standalone SQL written for the dashboard. The four views
-- keep their original names (holdings_normalized, market_quarterly_vwap,
-- fund_quarterly_flows, conviction_scores) so they stay recognisable; the
-- deviations are listed below and each one is backed by a measurement on a
-- real lake.
--
-- The caller creates three raw views before running this file, because their
-- sources are configuration rather than SQL:
--
--   raw_holdings  every holdings file of the lake, as written by the extractor
--   raw_prices    daily bars (symbol, report_date, close, volume, low, high);
--                 empty when no price source is configured
--   raw_splits    split events (symbol, report_date, split_factor); empty when
--                 no price source is configured
--
-- Each view is materialised to Parquet once (see thirteenf.go): a live
-- conviction view scans the whole remote price table and re-joins the lake,
-- which is far too slow to answer a dashboard request.
--
-- Deviations from the standalone SQL:
--
-- 1. No CUSIP -> ticker mapping table. The lake carries a `ticker` column
--    derived by the extractor from the same bundled CUSIP map, so joining a
--    second copy would be a duplicate source of truth. Unknown CUSIPs fall
--    back to the CUSIP itself, preserving COALESCE(m.ticker, h.cusip).
--
-- 2. Positions are collapsed to (filer, report period, CUSIP) first. A filing
--    reports one CUSIP on several lines - separate discretionary/voting
--    accounts, and option lines. In the reference lake 3,069 raw rows are
--    2,422 positions. The standalone window functions and the flows self-join
--    fan out over those rows, so portfolio weights and share deltas come out
--    inflated; grouping first is what makes the joins one-to-one.
--
-- 3. `value` is whole dollars for every report period in the lake (2021Q4 to
--    2024Q2): the median implied price per share - value / sshPrnamt - is
--    $74-146 in every quarter, so no thousands-to-dollars rescaling is
--    applied. A lake written from pre-2023 era filings should be re-checked
--    with that same query before trusting the portfolio totals.
--
-- 4. Share deltas are split-adjusted. Shares are as reported, but the price
--    dataset is split-adjusted, so an unadjusted comparison reads a 10:1 split
--    (NVDA, 2024-06-10) as a tenfold ADDED position. Prev-period shares are
--    multiplied by every split factor between the two report dates. Reported
--    values need no adjustment: a split does not change market value. When no
--    split data is available the factor is 1 and `split_adjusted` is false, so
--    a consumer can tell the two cases apart. Both `split_factor` and
--    `split_adjusted` are carried through, so a dashboard can say which split a
--    restated share count was adjusted for.
--
-- 5. `adj_close` does not exist in the price dataset - it ships `close`
--    (split-adjusted) and `volume`. The VWAP is therefore close-weighted:
--    SUM(close * volume) / SUM(volume), a daily-close proxy, not an intraday
--    VWAP.
--
-- 6. Flows compare a fund's consecutive *filings* rather than a LAG() over
--    each position, and they carry prev_period and quarters_between. A lake
--    that is missing quarters would otherwise silently compare non-adjacent
--    periods, and a consumer could not tell that from a real quarterly move.
--
-- 7. Conviction thresholds are unchanged from the standalone SQL:
--    NEW/ADDED with weight >= 5% is a HIGH_CONVICTION_BUY, NEW/ADDED below it
--    a STANDARD_BUY, TRIMMED with |delta weight| <= 0.5 a PASSIVE_REBALANCE,
--    TRIMMED with delta weight < -2 or a full exit a CONVICTION_DUMP, an
--    unchanged position MAINTAINED and anything else a ROUTINE_ADJUSTMENT.
--
-- Options are excluded (putCall IS NULL), as in the standalone SQL: one line
-- in the reference lake. sshPrnamtType is 'Shares' throughout, so no filtering
-- on it is needed; principal-amount rows would be summed alongside shares
-- otherwise.
--
-- 8. Portfolio totals are a grouped aggregate joined back onto the positions,
--    not SUM(value_usd) OVER (PARTITION BY cik, report_period). A window keeps
--    its partitions in memory for the whole scan and cannot spill, so
--    materialising holdings_normalized over a real lake (4.9M raw rows, 40k
--    filings) dies on the memory limit; the grouped form spills and its build
--    side is one row per filing.
--
-- 9. The positions stage is split in two: positions_base collapses the lake's
--    reporting lines to one row per filer, period and CUSIP with sums and a
--    count, and positions joins the security's names onto it. The names are
--    arg_max over a VARCHAR - variable-length aggregate state, which DuckDB
--    holds in a string heap that cannot spill - so carrying them through the
--    collapse is what ran the memory limit out once the lake was large enough:
--    on the same 484k-position lake at a 64MB limit the string form dies while
--    the identical GROUP BY with fixed-width payloads completes. Collapsed per
--    CUSIP the names cost one row per security instead of one per position, and
--    positions_base is materialised on its own stage so the join that adds them
--    reads two Parquet relations instead of sharing a pipeline with the
--    aggregate. The names are then read off the CUSIP rather than off the
--    reporting filer; measured on the reference lake (3,069 raw rows) both
--    forms agree on every position, and the ticker is the bundled CUSIP map
--    either way.

-- The collapse of the lake's reporting lines to one row per filer, period and
-- CUSIP. Its aggregate state is fixed width - two sums and a count - which is
-- what lets DuckDB spill it; the security's names are added in positions below,
-- because a variable-length aggregate state is not spillable.
CREATE OR REPLACE VIEW positions_base AS
WITH reported AS (
  -- report_period is normalised before it is grouped on: the lake stores it as
  -- text, and grouping on the raw column would split a filing in two if the
  -- same date ever arrived in two spellings.
  SELECT
    cik,
    CAST(report_period AS DATE) AS report_period,
    cusip,
    sshPrnamt,
    value
  FROM raw_holdings
  WHERE putCall IS NULL
)
SELECT
  cik,
  report_period,
  cusip,
  -- Sums are cast to DOUBLE so the materialised Parquet keeps the type: an
  -- integer SUM is a HUGEINT, which Parquet has no column type for, and the
  -- stages below read this table back from Parquet with a view that would no
  -- longer match its own definition.
  CAST(SUM(sshPrnamt) AS DOUBLE) AS shares,
  CAST(SUM(value) AS DOUBLE)     AS value_usd,
  count(*)                       AS reported_lines
FROM reported
GROUP BY cik, report_period, cusip;

CREATE OR REPLACE VIEW security_names AS
SELECT
  cusip,
  arg_max(nameOfIssuer, value)                         AS issuer,
  arg_max(titleOfClass, value)                         AS class_title,
  COALESCE(NULLIF(arg_max(ticker, value), ''), cusip)  AS ticker
FROM raw_holdings
WHERE putCall IS NULL
GROUP BY cusip;

CREATE OR REPLACE VIEW positions AS
-- Every CUSIP of the lake is in security_names, because both sides read the
-- same raw rows through the same option filter.
SELECT
  c.cik,
  c.report_period,
  c.cusip,
  n.issuer,
  n.class_title,
  n.ticker,
  c.shares,
  c.value_usd,
  c.reported_lines
FROM positions_base c
JOIN security_names n ON n.cusip = c.cusip;

CREATE OR REPLACE VIEW holdings_normalized AS
WITH portfolio_totals AS (
  SELECT
    cik,
    report_period,
    SUM(value_usd) AS portfolio_value_total
  FROM positions
  GROUP BY cik, report_period
)
SELECT
  p.cik,
  p.report_period,
  year(p.report_period)    AS report_year,
  quarter(p.report_period) AS report_quarter,
  p.cusip,
  p.issuer,
  p.class_title,
  p.ticker,
  p.shares,
  p.value_usd,
  p.reported_lines,
  t.portfolio_value_total,
  ROUND(100.0 * p.value_usd / NULLIF(t.portfolio_value_total, 0), 4) AS portfolio_weight_pct
FROM positions p
JOIN portfolio_totals t
  ON  t.cik = p.cik
  AND t.report_period = p.report_period;

CREATE OR REPLACE VIEW market_quarterly_vwap AS
SELECT
  symbol,
  year(CAST(report_date AS DATE))    AS period_year,
  quarter(CAST(report_date AS DATE)) AS period_quarter,
  count(*)                           AS trading_days,
  MIN(CAST(report_date AS DATE))     AS first_trade_date,
  MAX(CAST(report_date AS DATE))     AS last_trade_date,
  CAST(SUM(volume) AS DOUBLE)        AS total_volume,
  ROUND(SUM(close * volume) / NULLIF(SUM(volume), 0), 4) AS quarterly_vwap,
  MIN(low)                           AS quarterly_low,
  MAX(high)                          AS quarterly_high
FROM raw_prices
WHERE close IS NOT NULL
GROUP BY symbol, period_year, period_quarter;

CREATE OR REPLACE VIEW fund_quarterly_flows AS
WITH filing_periods AS (
  SELECT
    cik,
    report_period,
    LAG(report_period) OVER (PARTITION BY cik ORDER BY report_period) AS prev_period
  FROM (SELECT DISTINCT cik, report_period FROM holdings_normalized)
),
split_factors AS (
  SELECT
    symbol,
    CAST(report_date AS DATE) AS split_date,
    TRY_CAST(regexp_extract(split_factor, '^([0-9.]+):', 1) AS DOUBLE)
      / NULLIF(TRY_CAST(regexp_extract(split_factor, ':([0-9.]+)$', 1) AS DOUBLE), 0) AS factor
  FROM raw_splits
),
current_positions AS (
  SELECT
    fp.cik, fp.report_period, fp.prev_period,
    h.cusip, h.ticker, h.issuer,
    h.shares, h.value_usd, h.portfolio_weight_pct, h.portfolio_value_total
  FROM filing_periods fp
  JOIN holdings_normalized h
    ON h.cik = fp.cik AND h.report_period = fp.report_period
),
previous_positions AS (
  SELECT
    fp.cik, fp.report_period, fp.prev_period,
    h.cusip, h.ticker, h.issuer,
    h.shares, h.value_usd, h.portfolio_weight_pct, h.portfolio_value_total
  FROM filing_periods fp
  JOIN holdings_normalized h
    ON h.cik = fp.cik AND h.report_period = fp.prev_period
),
changed_positions AS (
  SELECT
    COALESCE(cur.cik, prev.cik)                     AS cik,
    COALESCE(cur.report_period, prev.report_period)  AS report_period,
    COALESCE(cur.prev_period, prev.prev_period)      AS prev_period,
    COALESCE(cur.cusip, prev.cusip)                  AS cusip,
    COALESCE(cur.ticker, prev.ticker)                AS ticker,
    COALESCE(cur.issuer, prev.issuer)                AS issuer,
    COALESCE(cur.shares, 0)                          AS shares,
    COALESCE(cur.value_usd, 0)                       AS value_usd,
    COALESCE(cur.portfolio_weight_pct, 0)            AS portfolio_weight_pct,
    COALESCE(prev.shares, 0)                         AS prev_shares_raw,
    COALESCE(prev.value_usd, 0)                      AS prev_value_usd,
    COALESCE(prev.portfolio_weight_pct, 0)           AS prev_portfolio_weight_pct,
    COALESCE(cur.portfolio_value_total, prev.portfolio_value_total) AS portfolio_value_total
  FROM current_positions cur
  FULL JOIN previous_positions prev
    ON  prev.cik = cur.cik
    AND prev.report_period = cur.report_period
    AND prev.prev_period = cur.prev_period
    AND prev.cusip = cur.cusip
  -- The fund's first filing in the lake has nothing to compare against; it is
  -- excluded rather than reported as a portfolio of brand-new positions. The
  -- test runs on the joined rows because a position that was exited has no
  -- current row at all.
  WHERE COALESCE(cur.prev_period, prev.prev_period) IS NOT NULL
)
SELECT
  cik,
  report_period,
  year(report_period)                                                   AS report_year,
  quarter(report_period)                                                AS report_quarter,
  prev_period,
  (4 * year(report_period) + quarter(report_period))
    - (4 * year(prev_period) + quarter(prev_period))                    AS quarters_between,
  cusip,
  ticker,
  issuer,
  CASE
    WHEN prev_shares_raw = 0 AND shares > 0 THEN 'NEW'
    WHEN shares = 0 AND prev_shares_raw > 0 THEN 'EXITED'
    WHEN shares > prev_shares_adjusted THEN 'ADDED'
    WHEN shares < prev_shares_adjusted THEN 'TRIMMED'
    ELSE 'HELD'
  END                                                                   AS action,
  shares,
  value_usd,
  ROUND(portfolio_weight_pct, 4)                                        AS portfolio_weight_pct,
  prev_shares_adjusted                                                  AS prev_shares,
  prev_value_usd,
  ROUND(prev_portfolio_weight_pct, 4)                                   AS prev_portfolio_weight_pct,
  ROUND(shares - prev_shares_adjusted, 4)                               AS delta_shares,
  ROUND(100.0 * (shares - prev_shares_adjusted) / NULLIF(prev_shares_adjusted, 0), 4) AS delta_shares_pct,
  value_usd - prev_value_usd                                            AS delta_value_usd,
  ROUND(portfolio_weight_pct - prev_portfolio_weight_pct, 4)            AS delta_weight_pct,
  portfolio_value_total,
  split_factor,
  split_factor <> 1                                                     AS split_adjusted
FROM (
  SELECT
    changed_positions.*,
    ROUND(prev_shares_raw * COALESCE(f.factor, 1), 4) AS prev_shares_adjusted,
    COALESCE(f.factor, 1)                             AS split_factor
  FROM changed_positions
  LEFT JOIN LATERAL (
    SELECT product(sf.factor) AS factor
    FROM split_factors sf
    WHERE sf.symbol = changed_positions.ticker
      AND sf.split_date > changed_positions.prev_period
      AND sf.split_date <= changed_positions.report_period
  ) f ON TRUE
);

CREATE OR REPLACE VIEW conviction_scores AS
SELECT
  f.cik,
  f.report_period,
  f.report_year,
  f.report_quarter,
  f.prev_period,
  f.quarters_between,
  f.cusip,
  f.ticker,
  f.issuer,
  f.action,
  f.shares,
  f.value_usd,
  f.portfolio_weight_pct,
  f.prev_shares,
  f.prev_value_usd,
  f.prev_portfolio_weight_pct,
  f.delta_shares,
  f.delta_shares_pct,
  f.delta_value_usd,
  f.delta_weight_pct,
  f.portfolio_value_total,
  f.split_factor,
  f.split_adjusted,
  m.quarterly_vwap,
  m.quarterly_low,
  m.quarterly_high,
  m.trading_days,
  ROUND(f.delta_shares * m.quarterly_vwap, 0)                           AS est_capital_flow,
  CASE
    WHEN f.action IN ('NEW', 'ADDED') AND f.portfolio_weight_pct >= 5 THEN 'HIGH_CONVICTION_BUY'
    WHEN f.action IN ('NEW', 'ADDED')                                 THEN 'STANDARD_BUY'
    WHEN f.action = 'TRIMMED' AND ABS(f.delta_weight_pct) <= 0.5      THEN 'PASSIVE_REBALANCE'
    WHEN f.action = 'TRIMMED' AND f.delta_weight_pct < -2             THEN 'CONVICTION_DUMP'
    WHEN f.action = 'EXITED'                                          THEN 'CONVICTION_DUMP'
    WHEN f.action = 'HELD'                                            THEN 'MAINTAINED'
    ELSE 'ROUTINE_ADJUSTMENT'
  END                                                                   AS signal
FROM fund_quarterly_flows f
LEFT JOIN market_quarterly_vwap m
  ON  m.symbol = f.ticker
  AND m.period_year = f.report_year
  AND m.period_quarter = f.report_quarter;
