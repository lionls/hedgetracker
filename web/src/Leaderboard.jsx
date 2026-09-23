import { useEffect, useState } from 'react';
import FundPicker from './FundPicker.jsx';
import { Badge } from './Badge.jsx';
import { thirteenLeaderboard } from './api.js';
import {
  count,
  dollars,
  period,
  percent,
  plural,
  signedDollars,
  signedPercent,
} from './numbers.js';

// The columns are also the sort control, and the ranking each one asks the server
// for comes back in the direction recorded here: the server's whitelist ranks
// every figure largest first and the name alphabetically, so `dir` is what the
// arrow the stylesheet appends is allowed to claim. A column with no value is not
// a ranking the endpoint offers, and its heading stays text rather than a button
// that would 400.
const COLUMNS = [
  {
    value: 'name',
    dir: 'asc',
    label: 'Fund',
    title: 'the filer the SEC has on record for this CIK',
  },
  {
    value: 'aum',
    dir: 'desc',
    label: 'AUM',
    title: "the value the fund's newest filing reports",
  },
  {
    label: 'Positions',
    title: 'the positions in the newest filing',
  },
  {
    value: 'concentration',
    dir: 'desc',
    label: 'Top 10',
    title: "the ten largest positions as a share of that filing's value",
  },
  {
    value: 'turnover',
    dir: 'desc',
    label: 'Turnover',
    title:
      "the quarter's purchases and sales marked at the quarter's VWAP, halved, over the filing — a 13F reports positions, not transactions, so this is an estimate",
  },
  {
    value: 'quarter',
    dir: 'desc',
    label: 'Quarter P&L',
    title: 'the quarter\'s estimated mark against the share of the book the price dataset could price',
  },
  {
    value: 'year',
    dir: 'desc',
    label: '1Y P&L',
    title: "the last four filings' estimated marks against the newest filing's covered value",
  },
  {
    value: 'threeyear',
    dir: 'desc',
    label: '3Y P&L',
    title: "the last twelve filings' estimated marks against the newest filing's covered value",
  },
  {
    label: 'Latest filing',
    title: 'the newest filing the fund has in the lake, and whether it is the lake\'s newest quarter',
  },
  {
    label: 'Coverage',
    title: 'the share of the moved value the price dataset could mark',
  },
];

// The directory over the whole lake: one row per filer, ranked by the server
// rather than in the browser, so the same nine rows are in the same order for
// every reader and a deeper lake stays a slice rather than a resort.
export default function Leaderboard() {
  const [status, setStatus] = useState(null);
  const [sort, setSort] = useState('aum');
  const [limit, setLimit] = useState(50);
  const [payload, setPayload] = useState(undefined);
  const [failure, setFailure] = useState('');
  const [version, setVersion] = useState(0);

  useEffect(() => {
    let active = true;
    setFailure('');
    thirteenLeaderboard(sort, limit).then(
      (payload) => active && setPayload(payload),
      (error) => active && setFailure(error.message),
    );
    return () => {
      active = false;
    };
  }, [sort, limit, version]);

  const funds = payload?.funds || [];
  const total = payload?.total || 0;
  const ranked = COLUMNS.find((column) => column.value === sort) || COLUMNS[0];

  return (
    <div className="app">
      <FundPicker
        page="leaderboard"
        tagline="fund directory"
        fund={null}
        onFund={(entry) => window.location.assign(`/funds?cik=${entry.cik}`)}
        onStatus={setStatus}
        version={version}
        onVersion={() => setVersion((value) => value + 1)}
        seed={false}
      >
        {status && !status.priceSource ? (
          <p className="note down">
            no price dataset is configured, so no price-marked figure on this page can be estimated
          </p>
        ) : null}
      </FundPicker>

      <main className="main">
        <header className="headline">
          <div className="identity">
            <h1>
              Leaderboard
              <span className="company-name">
                {payload ? `the lake's ${count(total)} funds, ranked` : 'reading the lake…'}
              </span>
            </h1>
            {payload ? (
              <div className="meta">
                {payload.period ? `newest filing ${period(payload.period)} · ` : ''}
                {count(funds.length)} shown
              </div>
            ) : (
              <div className="meta">reading the filings…</div>
            )}
          </div>
        </header>

        {failure ? <p className="error">{failure}</p> : null}

        {payload ? (
          <section className="section">
            <header>
              <h2>Funds</h2>
              <span className="muted">
                ranked by {ranked.label} · the column headings are the control
              </span>
            </header>
            {total === 0 ? (
              <p className="muted">
                the lake holds no filers yet, so there is no fund to rank — a rebuild that finds
                holdings is what fills this directory
              </p>
            ) : (
              <>
                <table className="statements">
                  <thead>
                    <tr>
                      {COLUMNS.map((column) => (
                        <th
                          scope="col"
                          key={column.label}
                          title={column.value ? undefined : column.title}
                        >
                          {column.value ? (
                            <button
                              type="button"
                              className={`sort${
                                sort === column.value ? ` active ${column.dir}` : ''
                              }`}
                              onClick={() => setSort(column.value)}
                              title={column.title}
                            >
                              {column.label}
                            </button>
                          ) : (
                            column.label
                          )}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {funds.map((row) => (
                      <tr key={row.cik}>
                        <td>
                          <a
                            className="link"
                            href={`/funds?cik=${row.cik}`}
                            title={`CIK ${row.cik}`}
                          >
                            {row.filerName || `CIK ${row.cik}`}
                          </a>
                        </td>
                        <td
                          title={`${plural(row.quarters, 'filing')} in the lake, ${period(
                            row.firstPeriod,
                          )} to ${period(row.latestPeriod)}`}
                        >
                          {dollars(row.latestValueUsd)}
                        </td>
                        <td title="positions in the newest filing">
                          {count(row.latestPositions)}
                        </td>
                        <td title="the ten largest positions as a share of that filing's value">
                          {percent(row.top10Pct)}
                        </td>
                        <td title="the quarter's purchases and sales marked at the quarter's VWAP, halved, over the filing — a 13F reports positions, not transactions, so this is an estimate">
                          {percent(row.turnoverPct)}
                        </td>
                        <td
                          className={tone(row.quarterPnlPct)}
                          title={`${signedDollars(
                            row.quarterPnlUsd,
                          )} over the share of the book the price dataset could mark`}
                        >
                          {signedPercent(row.quarterPnlPct)}
                        </td>
                        <td
                          className={tone(row.yearPnlPct)}
                          title={`${signedDollars(row.yearPnlUsd)} · ${plural(
                            row.yearFilings,
                            'filing',
                          )} of the window could be marked`}
                        >
                          {signedPercent(row.yearPnlPct)}
                        </td>
                        <td
                          className={tone(row.threeYearPnlPct)}
                          title={`${signedDollars(row.threeYearPnlUsd)} · ${plural(
                            row.threeYearFilings,
                            'filing',
                          )} of the window could be marked`}
                        >
                          {signedPercent(row.threeYearPnlPct)}
                        </td>
                        <td>
                          {period(row.latestPeriod)}{' '}
                          <Badge
                            tone={row.status === 'current' ? 'up' : 'flat'}
                            title={
                              row.status === 'stale'
                                ? `${plural(row.quartersSinceLatest, 'quarter')} behind the lake`
                                : undefined
                            }
                          >
                            {row.status}
                          </Badge>
                        </td>
                        <td title="the share of the moved value the price dataset could mark">
                          {percent(row.coveragePct)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {limit < total ? (
                  <button
                    type="button"
                    className="more"
                    onClick={() => setLimit(limit + 50)}
                  >
                    Show {Math.min(50, total - limit)} more
                  </button>
                ) : null}
              </>
            )}
            {total > 0 ? (
              <p className="note">
                A 13F files the positions a fund holds, never the trades it made, so the turnover
                column and the capital behind every mark here are estimates: the quarter&apos;s
                reported change is valued at the quarter&apos;s VWAP, and a purchase and a sale at
                the same weight cancel rather than book. The quarter&apos;s P&amp;L is measured
                against the share of the book the price dataset could price — the coverage column,
                whose holes are left out of the mark rather than counted as zero — and the 1Y and 3Y
                percentages are those quarters&apos; summed estimated P&amp;L against the newest
                filing&apos;s covered value: a record of the marks the lake can make, not a return
                on capital. Each window also counts, in the cell&apos;s title, how many of its
                filings could be marked at all, so a fund the lake holds three filings of does not
                read as a three-year record. A fund marked stale filed nothing in the lake&apos;s
                newest quarter, so every figure on its row describes its own newest filing, which is
                older than the lake&apos;s. No fees, dividends or intraday prices are in any of it.
              </p>
            ) : null}
          </section>
        ) : null}
      </main>
    </div>
  );
}

// A delta carries its direction in the cell class and its sign in the text, and a
// mark the price dataset could not make is neither up nor down.
function tone(value) {
  if (value === null || value === undefined || value === 0) return '';
  return value < 0 ? 'down' : 'up';
}
