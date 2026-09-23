import { useEffect, useMemo, useRef, useState } from 'react';
import FundPicker from './FundPicker.jsx';
import HoldingsPie from './HoldingsPie.jsx';
import PositionBars from './PositionBars.jsx';
import QuarterChart from './QuarterChart.jsx';
import { Badge, ACTION_TONE } from './Badge.jsx';
import { thirteenFund } from './api.js';
import {
  count,
  dollars,
  period,
  percent,
  plural,
  signedCount,
  signedDollars,
  signedPercent,
} from './numbers.js';

// POSITION_STEP is how much of the quarter's table grows per click: a 794-position
// filing is one request but not one screen.
const POSITION_STEP = 25;

// The rankings the endpoint offers, in the words the select shows. The values are
// the server's whitelist, so these two lists are one contract.
const SORTS = [
  { value: 'value', label: 'largest positions' },
  { value: 'gain', label: 'biggest gains' },
  { value: 'loss', label: 'biggest losses' },
  { value: 'weight', label: 'heaviest weight' },
];

const UP = 'rgba(38, 166, 154, 0.55)';
const DOWN = 'rgba(239, 83, 80, 0.55)';

// One description per panel, held outside the component: the options are what the
// chart is created with, and only the points change afterwards.
const VALUE_SERIES = {
  kind: 'area',
  options: {
    lineColor: '#6ab0f3',
    topColor: 'rgba(106, 176, 243, 0.18)',
    bottomColor: 'rgba(106, 176, 243, 0.02)',
    priceLineVisible: false,
  },
};
const PNL_SERIES = { kind: 'histogram', options: { priceLineVisible: false, base: 0 } };
const CUMULATIVE_SERIES = {
  kind: 'line',
  pane: 1,
  options: { color: '#6ab0f3', lineWidth: 2, priceLineVisible: false },
};
const OPENED_SERIES = { kind: 'histogram', options: { color: UP, priceLineVisible: false, base: 0 } };
const CLOSED_SERIES = { kind: 'histogram', options: { color: DOWN, priceLineVisible: false, base: 0 } };

// points is one chart point per filing, and a filing the estimate cannot answer
// for is left out rather than drawn at zero: the library rejects a null, and a
// zero would claim a quarter that went nowhere when what happened is that the
// price dataset does not cover the position.
function points(series, pick) {
  const out = [];
  for (const row of series) {
    const value = pick(row);
    if (value === null || value === undefined) continue;
    out.push({ time: row.period, value });
  }
  return out;
}

// bars is points() with the sign carried by the colour: the axis says how much
// moved, the colour says which way.
function bars(series, pick) {
  return points(series, pick).map((point) => ({ ...point, color: point.value < 0 ? DOWN : UP }));
}

// The quarterly figures, built once per response: every panel reads the same
// series, and rebuilding the arrays on an unrelated render would feed the charts
// the data they already have.
function marks(series) {
  return {
    value: [{ ...VALUE_SERIES, data: points(series, (row) => row.portfolioValueUsd) }],
    pnl: [
      { ...PNL_SERIES, data: bars(series, (row) => row.pnlUsd) },
      { ...CUMULATIVE_SERIES, data: points(series, (row) => row.cumulativePnlUsd) },
    ],
    cash: [
      { ...OPENED_SERIES, data: points(series, (row) => row.purchasedUsd) },
      { ...CLOSED_SERIES, data: points(series, (row) => (row.soldUsd === null ? null : -row.soldUsd)) },
    ],
    moves: [
      {
        ...OPENED_SERIES,
        data: points(series, (row) =>
          row.prevPeriod ? row.newPositions + row.addedPositions : null,
        ),
      },
      {
        ...CLOSED_SERIES,
        data: points(series, (row) =>
          row.prevPeriod ? -(row.trimmedPositions + row.exitedPositions) : null,
        ),
      },
    ],
  };
}

// The fund analytics page: one fund's filings over time and the quarter under the
// cursor. The estimate the whole page rests on is spelled out at the bottom —
// a 13F carries share counts, not what was paid for them.
export default function Funds() {
  const [status, setStatus] = useState(null);
  // A link from the directory or the comparison carries the CIK it wants to open:
  // the page is served without a router, so a row click is a document load with
  // ?cik= on it, and this is the one place that reads it. Without one the picker
  // seeds the largest fund in the lake, as it does on every other fund page.
  const [fund, setFund] = useState(() => {
    const cik = new URLSearchParams(window.location.search).get('cik');
    return cik ? { cik: cik.trim() } : null;
  });
  const [wanted, setWanted] = useState('');
  const [sort, setSort] = useState(SORTS[0].value);
  const [payload, setPayload] = useState(null);
  const [failure, setFailure] = useState('');
  const [visible, setVisible] = useState(POSITION_STEP);
  const [version, setVersion] = useState(0);
  const cik = fund?.cik || '';

  // A new fund or quarter is a new page and the panels blank rather than show the
  // previous filing under the new headline. A new sort is not: it is the same
  // answer in a different order, and only the table moves.
  const selection = `${cik}|${wanted}`;
  const lastSelection = useRef(selection);

  useEffect(() => {
    if (!cik) return undefined;
    let active = true;
    if (lastSelection.current !== selection) {
      lastSelection.current = selection;
      setPayload(null);
    }
    setFailure('');
    setVisible(POSITION_STEP);
    thirteenFund(cik, wanted, sort).then(
      (payload) => active && setPayload(payload),
      (error) => active && setFailure(error.message),
    );
    return () => {
      active = false;
    };
  }, [selection, sort, version]);

  const charts = useMemo(() => (payload ? marks(payload.series) : null), [payload]);
  const quarters = payload?.quarters || [];
  // The selected quarter is a row of the series, so the panel's figures and the
  // charts cannot disagree: there is one answer per filing, read twice.
  const row = payload ? payload.series.find((entry) => entry.period === payload.period) : null;
  const rows = payload?.positions || [];
  const shown = rows.slice(0, visible);
  const filings = payload?.series.length || 0;
  const spanning = payload ? `${period(payload.series[0].period)} to ${period(payload.period)}` : '';

  function pickFund(entry) {
    setFund(entry);
    // The next fund opens on its own newest filing: a quarter filed by the fund
    // being left is a 404 for the fund being opened.
    setWanted('');
  }

  function tone(value) {
    if (value === null || value === undefined || value === 0) return '';
    return value < 0 ? 'down' : 'up';
  }

  return (
    <div className="app">
      {/* A fund named on the query string is this page's opening view: the picker's
          own opening fund is the largest in the lake, and seeding it here would
          open that one under the CIK the link asked for. */}
      <FundPicker
        page="funds"
        tagline="fund analytics"
        fund={fund}
        onFund={pickFund}
        onStatus={setStatus}
        version={version}
        onVersion={() => setVersion((value) => value + 1)}
        seed={!fund}
      >
        {status && !status.priceSource ? (
          <p className="note down">
            no price dataset is configured, so no P&amp;L can be estimated on this page
          </p>
        ) : null}
      </FundPicker>

      <main className="main">
        <header className="headline">
          <div className="identity">
            <h1>
              {payload?.filerName || cik || '—'}
              <span className="company-name">
                {payload
                  ? `${payload.filerName ? `CIK ${cik} · ` : ''}${dollars(
                      row?.portfolioValueUsd,
                    )} portfolio`
                  : cik
                    ? `CIK ${cik}`
                    : ''}
              </span>
            </h1>
            {payload ? (
              <div className="meta">
                {plural(row?.positions, 'position')} · {period(payload.period)} ·{' '}
                {plural(filings, 'filing')} in the lake
              </div>
            ) : (
              <div className="meta">reading the filings…</div>
            )}
          </div>
          {quarters.length > 0 ? (
            <select
              className="control"
              value={wanted || payload?.period || ''}
              title={`${quarters.length} quarters filed`}
              onChange={(event) => setWanted(event.target.value)}
            >
              {quarters.map((day) => (
                <option key={day} value={day}>
                  {period(day)}
                </option>
              ))}
            </select>
          ) : null}
        </header>

        {failure ? <p className="error">{failure}</p> : null}

        {payload && row ? (
          <section className="section">
            <header>
              <h2>{period(payload.period)}</h2>
              <dl className="figures">
                <div>
                  <dt>Quarter P&amp;L</dt>
                  <dd className={tone(row.pnlUsd)}>{signedDollars(row.pnlUsd)}</dd>
                </div>
                <div title={`summed over every quarter from ${period(payload.series[0].period)} on`}>
                  <dt>P&amp;L since then</dt>
                  <dd className={tone(row.cumulativePnlUsd)}>
                    {signedDollars(row.cumulativePnlUsd)}
                  </dd>
                </div>
                <div title="positions that could be marked, of those that moved since the previous filing">
                  <dt>Marked</dt>
                  <dd>
                    {count(row.positionsWithPnl)} of {count(row.movedPositions)}
                    <span className="muted"> · {percent(row.coveragePct)} of value</span>
                  </dd>
                </div>
                <div
                  title={`${plural(row.purchasedPositions, 'position')} bought, ${plural(
                    row.soldPositions,
                    'position',
                  )} sold`}
                >
                  <dt>Bought / sold</dt>
                  <dd>
                    {dollars(row.purchasedUsd)} <span className="muted">/</span>{' '}
                    {dollars(row.soldUsd)}
                  </dd>
                </div>
                {row.quartersBetween > 1 ? (
                  <div
                    title={`the previous filing is ${period(row.prevPeriod)}, ${count(
                      row.quartersBetween,
                    )} quarters back`}
                  >
                    <dt>Gap since {period(row.prevPeriod)}</dt>
                    <dd>
                      {count(row.quartersBetween)} quarters
                      <span className="muted"> · the whole gap is marked here</span>
                    </dd>
                  </div>
                ) : null}
              </dl>
            </header>
          </section>
        ) : null}

        {payload ? (
          <section className="section">
            <header>
              <h2>Over time</h2>
              <span className="muted">
                {plural(filings, 'filing')} · {spanning} · quarterly VWAP marks
              </span>
            </header>
            <div className="chart-grid">
              <QuarterChart
                title="Portfolio value"
                caption="reported at each filing"
                series={charts.value}
              />
              {charts.pnl[0].data.length > 0 ? (
                <QuarterChart
                  title="Quarterly P&L"
                  caption="bars: the quarter's mark · line: summed from the first filing"
                  series={charts.pnl}
                  tall
                />
              ) : (
                <figure className="chart-panel">
                  <figcaption>
                    <h3>Quarterly P&amp;L</h3>
                  </figcaption>
                  <p className="muted">
                    {filings > 1
                      ? 'the price dataset covers none of these positions, so no quarter could be marked'
                      : 'a fund needs two filings to have a change to mark'}
                  </p>
                </figure>
              )}
              {charts.cash[0].data.length > 0 ? (
                <QuarterChart
                  title="Money in and out"
                  caption="bought above the axis, sold below, at the quarter's VWAP"
                  series={charts.cash}
                />
              ) : (
                <figure className="chart-panel">
                  <figcaption>
                    <h3>Money in and out</h3>
                  </figcaption>
                  <p className="muted">no priced move to value</p>
                </figure>
              )}
              {charts.moves[0].data.length > 0 ? (
                <QuarterChart
                  title="Positions opened and closed"
                  caption="opened or added above the axis, trimmed or exited below"
                  series={charts.moves}
                />
              ) : (
                <figure className="chart-panel">
                  <figcaption>
                    <h3>Positions opened and closed</h3>
                  </figcaption>
                  <p className="muted">no previous filing to compare against</p>
                </figure>
              )}
            </div>
          </section>
        ) : null}

        {payload ? (
          <section className="section">
            <header>
              <h2>Holdings</h2>
              <span className="muted">
                the filing's own book at {period(payload.period)} · by reported value, largest first
              </span>
            </header>
            <HoldingsPie holdings={payload.holdings} />
          </section>
        ) : null}

        {payload ? (
          <section className="section">
            <header>
              <h2>P&amp;L by position</h2>
              <span className="muted">
                biggest marks of {period(payload.period)} · gains right of the axis, losses left
              </span>
            </header>
            <PositionBars rows={rows} />
          </section>
        ) : null}

        {payload ? (
          <section className="section">
            <header>
              <h2>Positions</h2>
              <span className="muted">
                {rows.length === 0
                  ? `${plural(row.positions, 'position')} filed · ${period(payload.period)}`
                  : `${count(shown.length)} of ${count(payload.total)} positions · ${period(
                      payload.period,
                    )}`}
              </span>
              {rows.length > 0 ? (
                <div className="filters">
                  <select
                    className="control"
                    value={sort}
                    title="the ranking of the table below"
                    onChange={(event) => setSort(event.target.value)}
                  >
                    {SORTS.map((entry) => (
                      <option key={entry.value} value={entry.value}>
                        {entry.label}
                      </option>
                    ))}
                  </select>
                </div>
              ) : null}
            </header>
            {rows.length === 0 ? (
              <p className="muted">
                The lake holds one filing for this fund, so there is no earlier quarter to compare
                its positions against: what changed is measured between two filings, and a quarter
                of nothing but opening positions would say nothing.
              </p>
            ) : (
              <>
                <table className="statements">
                  <thead>
                    <tr>
                      <th scope="col">Ticker</th>
                      <th scope="col">Issuer</th>
                      <th scope="col">Action</th>
                      <th scope="col">Shares</th>
                      <th scope="col">Δ shares</th>
                      <th scope="col">VWAP then</th>
                      <th scope="col">VWAP now</th>
                      <th scope="col">Value</th>
                      <th scope="col">Weight</th>
                      <th scope="col">P&amp;L</th>
                      <th scope="col">P&amp;L %</th>
                      <th scope="col">P&amp;L since then</th>
                    </tr>
                  </thead>
                  <tbody>
                    {shown.map((position) => (
                      <tr key={position.cusip}>
                        <th scope="row" title={position.cusip}>
                          {position.ticker || position.cusip}
                        </th>
                        <td className="issuer">{position.issuer}</td>
                        <td>
                          <Badge
                            tone={ACTION_TONE[position.action] || 'flat'}
                            title={
                              position.quartersBetween > 1
                                ? `since ${period(row.prevPeriod)}, ${
                                    position.quartersBetween
                                  } quarters back`
                                : undefined
                            }
                          >
                            {position.action.toLowerCase()}
                          </Badge>
                        </td>
                        <td title={`${count(position.prevShares)} shares at the previous filing`}>
                          {count(position.shares)}
                        </td>
                        <td
                          className={tone(position.deltaShares)}
                          title={
                            position.splitAdjusted
                              ? `adjusted for a ${count(position.splitFactor)}:1 split since the previous filing`
                              : undefined
                          }
                        >
                          {signedCount(position.deltaShares)}
                          {position.splitAdjusted ? <span className="split"> split</span> : null}
                        </td>
                        <td>{dollars(position.prevVwap)}</td>
                        <td>{dollars(position.vwap)}</td>
                        <td>{dollars(position.valueUsd)}</td>
                        <td>{percent(position.weightPct)}</td>
                        <td className={tone(position.pnlUsd)}>{signedDollars(position.pnlUsd)}</td>
                        <td className={tone(position.pnlPct)}>
                          {signedPercent(position.pnlPct)}
                        </td>
                        <td
                          className={tone(position.cumulativePnlUsd)}
                          title={`the same marks summed from the position's first filing in the lake through ${period(
                            payload.period,
                          )} — a position held for several quarters carries all of them, and one the fund has bought back carries the spell before it too`}
                        >
                          {signedDollars(position.cumulativePnlUsd)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {rows.length > visible ? (
                  <button
                    type="button"
                    className="more"
                    onClick={() => setVisible(visible + POSITION_STEP)}
                  >
                    Show {Math.min(POSITION_STEP, rows.length - visible)} more
                  </button>
                ) : null}
              </>
            )}
          </section>
        ) : null}

        {payload ? (
          <p className="note">
            A 13F reports what a fund holds, not what it paid, so the P&amp;L on this page is an
            estimate: each position is marked at the change in its quarter&apos;s volume-weighted
            mean close, on the split-adjusted shares held at the previous filing — bought and sold
            at the VWAP the filing is marked at. A position opened this quarter is marked at what
            it is held at, so it contributes nothing; an exit is valued at the quarter&apos;s VWAP
            rather than at the price it was sold at, which the filing does not carry. P&amp;L since
            then sums those marks per position over the fund&apos;s filings through the selected
            quarter — per CUSIP, so a position the fund sold and later bought back carries both
            spells — which is the fund&apos;s own running total asked per row: the rows of this
            table cannot add up to that total, because a position the fund exited before this
            quarter is in the total and not in the table. Positions the price dataset does not
            cover are counted rather than zeroed: this quarter&apos;s marks
            cover {percent(row.coveragePct)} of the reported value, and a quarter between two
            filings further apart than three months carries the whole gap&apos;s mark. No fees,
            dividends or intraday prices are in any of it.
          </p>
        ) : null}
      </main>
    </div>
  );
}
