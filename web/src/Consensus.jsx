import { useEffect, useRef, useState } from 'react';
import ConvictionBoard from './ConvictionBoard.jsx';
import FundPicker from './FundPicker.jsx';
import InstitutionalOwnership from './InstitutionalOwnership.jsx';
import { thirteenConsensus } from './api.js';
import {
  compact,
  count,
  dollars,
  period,
  percent,
  plural,
  precisePercent,
  signedCount,
  signedDollars,
} from './numbers.js';

// The board reads one slice of every list at a time and the server is what ranks
// and cuts it, so growing a list is a new request rather than a re-sort here.
const STEP = 25;
// The crowded radar ranks the quarter's 100 largest positions before it has a
// market share count to divide by, so past 100 there is nothing more to name.
const CEILING = 100;

// tone is the delta convention the dashboard shares: the sign lives in the text
// and the direction in the class, and a zero or a hole is neither.
function tone(value) {
  if (value === null || value === undefined || value === 0) return '';
  return value < 0 ? 'down' : 'up';
}

// largest reads the biggest figure a list put up, skipping the rows the price
// dataset could not mark: the number describes what could be measured, and a
// null is not a zero.
function largest(rows, pick) {
  let best = null;
  for (const row of rows) {
    const value = pick(row);
    if (value === null || value === undefined) continue;
    if (best === null || value > best) best = value;
  }
  return best;
}

// unmarked is the title a valued cell carries when part of what it sums had no
// price to be valued at. The hole is counted rather than zeroed, because a zero
// would claim a mark the dataset never made.
function unmarked(n, noun) {
  if (!n) return undefined;
  return `${plural(n, noun)} the price dataset could not mark`;
}

// aShare is the price a quarter's opening is marked at — the quarter's VWAP — and
// null when there is no marked money to divide by the shares.
function aShare(row) {
  if (row.boughtUsd === null || row.boughtUsd === undefined || !row.shares) return null;
  return row.boughtUsd / row.shares;
}

// Cut is the "Shown" figure every list carries: the slice the server returned,
// printed only when it is smaller than the quarter's own count, because a list
// that shows everything has no second number to report.
function Cut({ total, shown }) {
  if (shown === 0 || shown >= total) return null;
  return (
    <div title={`of the ${plural(total, 'name')} in the quarter`}>
      <dt>Shown</dt>
      <dd>{count(shown)}</dd>
    </div>
  );
}

// Ticker is the drill every board row carries: a lake-wide table can say what the
// cohort did to a company, and the ownership panel below says which funds did it.
function Ticker({ row, onDrill }) {
  return (
    <button type="button" className="drill" onClick={() => onDrill(row.ticker)} title={row.issuer}>
      {row.ticker}
    </button>
  );
}

// The consensus board: one quarter of every filer in the lake, fund against fund.
// The four lists are the questions a single fund's dossier cannot ask — which
// companies the cohort moved into together, which it left, where the quarter's
// money went, and what the tracked funds own a large share of.
export default function Consensus() {
  const [status, setStatus] = useState(null);
  // '' is the lake's newest quarter: the board opens on the filing most funds
  // have in common rather than on a date the reader has to guess.
  const [wanted, setWanted] = useState('');
  const [limit, setLimit] = useState(STEP);
  const [drilled, setDrilled] = useState('');
  const [payload, setPayload] = useState(undefined);
  const [failure, setFailure] = useState('');
  const [version, setVersion] = useState(0);
  const drill = useRef(null);

  useEffect(() => {
    let active = true;
    setFailure('');
    thirteenConsensus(wanted, limit).then(
      (result) => active && setPayload(result),
      (error) => active && setFailure(error.message),
    );
    return () => {
      active = false;
    };
  }, [wanted, limit, version]);

  // The ownership panel sits at the end of a long page, so a ticker click has to
  // bring the answer to the reader rather than leave them at the row they clicked.
  useEffect(() => {
    if (drilled && drill.current) drill.current.scrollIntoView({ block: 'start' });
  }, [drilled]);

  const totals = payload?.totals || {};
  const quarters = payload?.quarters || [];
  const accumulations = payload?.newAccumulations || [];
  const liquidations = payload?.liquidations || [];
  const netVolume = payload?.netVolume || [];
  const crowded = payload?.crowded || [];
  const quarter = period(payload?.period);
  const inQuarter = quarter ? ` in ${quarter}` : '';
  // The two sides of the flow are the rows on the screen summed, not the quarter:
  // the figure's title says so and the number is the slice's own.
  const buyers = netVolume.reduce((sum, row) => sum + row.buyers, 0);
  const sellers = netVolume.reduce((sum, row) => sum + row.sellers, 0);

  return (
    <div className="app">
      <FundPicker
        page="consensus"
        tagline="consensus board"
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
              Consensus
              <span className="company-name">
                {payload ? `${quarter} · fund against fund` : ''}
              </span>
            </h1>
            {payload ? (
              <div className="meta">
                {count(totals.accumulations)} companies opened ·{' '}
                {count(totals.liquidations)} left entirely · {count(totals.netVolume)} moved
              </div>
            ) : (
              <div className="meta">reading the filings…</div>
            )}
          </div>
          {quarters.length > 0 ? (
            <select
              className="control"
              value={wanted || payload?.period || ''}
              title={`${quarters.length} quarters in the lake`}
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

        {payload ? (
          <section className="section">
            <header>
              <h2>Top new accumulations</h2>
              <dl className="figures">
                <div title="companies at least one fund opened this quarter">
                  <dt>Names</dt>
                  <dd>{count(totals.accumulations)}</dd>
                </div>
                <Cut total={totals.accumulations} shown={accumulations.length} />
                <div title="the largest opening on the page, marked at the quarter's VWAP">
                  <dt>Largest</dt>
                  <dd>{dollars(largest(accumulations, (row) => row.boughtUsd))}</dd>
                </div>
              </dl>
            </header>
            {accumulations.length === 0 ? (
              <p className="muted">no fund opened a company{inQuarter}</p>
            ) : (
              <table className="statements">
                <thead>
                  <tr>
                    <th scope="col">Ticker</th>
                    <th scope="col">Company</th>
                    <th scope="col">Funds</th>
                    <th scope="col">Shares</th>
                    <th scope="col">Value</th>
                    <th scope="col">Bought</th>
                    <th scope="col">A share</th>
                    <th scope="col">Largest weight</th>
                  </tr>
                </thead>
                <tbody>
                  {accumulations.map((row) => {
                    const perShare = aShare(row);
                    return (
                      <tr key={row.ticker}>
                        <th scope="row">
                          <Ticker row={row} onDrill={setDrilled} />
                        </th>
                        <td className="issuer">{row.issuer}</td>
                        <td>{count(row.funds)}</td>
                        <td>{count(row.shares)}</td>
                        <td>{dollars(row.valueUsd)}</td>
                        <td className="up" title={unmarked(row.unpriced, 'opening')}>
                          {dollars(row.boughtUsd)}
                        </td>
                        <td
                          title={
                            perShare === null
                              ? undefined
                              : "the quarter's volume-weighted mean close, which is what the opening is marked at"
                          }
                        >
                          {perShare === null ? null : count(perShare)}
                        </td>
                        <td title="the largest weight any of the funds that opened it gave the position">
                          {percent(row.largestWeightPct)}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </section>
        ) : null}

        {payload ? (
          <section className="section">
            <header>
              <h2>Top liquidations</h2>
              <dl className="figures">
                <div title="companies at least one fund left entirely this quarter">
                  <dt>Names</dt>
                  <dd>{count(totals.liquidations)}</dd>
                </div>
                <Cut total={totals.liquidations} shown={liquidations.length} />
                <div title="the largest exit on the page, marked at the quarter's VWAP">
                  <dt>Largest</dt>
                  <dd>{dollars(largest(liquidations, (row) => row.soldUsd))}</dd>
                </div>
              </dl>
            </header>
            {liquidations.length === 0 ? (
              <p className="muted">no fund left a company entirely{inQuarter}</p>
            ) : (
              <table className="statements">
                <thead>
                  <tr>
                    <th scope="col">Ticker</th>
                    <th scope="col">Company</th>
                    <th scope="col">Funds</th>
                    <th scope="col">Held before</th>
                    <th scope="col">Worth then</th>
                    <th scope="col">Proceeds</th>
                  </tr>
                </thead>
                <tbody>
                  {liquidations.map((row) => (
                    <tr key={row.ticker}>
                      <th scope="row">
                        <Ticker row={row} onDrill={setDrilled} />
                      </th>
                      <td className="issuer">{row.issuer}</td>
                      <td>{count(row.funds)}</td>
                      <td title="the shares the cohort still held at the previous filing">
                        {count(row.prevShares)}
                      </td>
                      <td>{dollars(row.prevValueUsd)}</td>
                      <td className="down" title={unmarked(row.unpriced, 'exit')}>
                        {dollars(row.soldUsd)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            <p className="note">
              A 13F files positions, never transactions, so an exit is the shares the cohort still
              held at the previous filing, and the proceeds column is that position marked at this
              quarter&apos;s volume-weighted mean close — the value that came out of the name over
              the quarter, not the price it was sold at, which the filing does not carry. An exit
              the price dataset covers none of is counted rather than zeroed.
            </p>
          </section>
        ) : null}

        {payload ? (
          <section className="section">
            <header>
              <h2>Net institutional volume</h2>
              <dl className="figures">
                <div title="companies the quarter moved at all">
                  <dt>Names</dt>
                  <dd>{count(totals.netVolume)}</dd>
                </div>
                <Cut total={totals.netVolume} shown={netVolume.length} />
                <div title="buyers summed across the rows shown, not the whole quarter">
                  <dt>Buyers</dt>
                  <dd className="up">{count(buyers)}</dd>
                </div>
                <div title="sellers summed across the rows shown, not the whole quarter">
                  <dt>Sellers</dt>
                  <dd className="down">{count(sellers)}</dd>
                </div>
              </dl>
            </header>
            {netVolume.length === 0 ? (
              <p className="muted">no company changed hands{inQuarter}</p>
            ) : (
              <table className="statements">
                <thead>
                  <tr>
                    <th scope="col">Ticker</th>
                    <th scope="col">Company</th>
                    <th scope="col">Funds</th>
                    <th scope="col">Buyers</th>
                    <th scope="col">Sellers</th>
                    <th scope="col">Net shares</th>
                    <th scope="col">Net flow</th>
                  </tr>
                </thead>
                <tbody>
                  {netVolume.map((row) => (
                    <tr key={row.ticker}>
                      <th scope="row">
                        <Ticker row={row} onDrill={setDrilled} />
                      </th>
                      <td className="issuer">{row.issuer}</td>
                      <td>{count(row.funds)}</td>
                      <td className="up" title="funds that added to the name this quarter">
                        {count(row.buyers)}
                      </td>
                      <td className="down" title="funds that trimmed or left the name this quarter">
                        {count(row.sellers)}
                      </td>
                      <td className={tone(row.netShares)}>{signedCount(row.netShares)}</td>
                      <td
                        className={tone(row.netUsd)}
                        title={unmarked(row.unpriced, 'move')}
                      >
                        {signedDollars(row.netUsd)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            <p className="note">
              The list is ordered by the size of the flow, so it runs from the quarter&apos;s
              largest net purchase at the top to its largest net sale at the bottom. Positive is a
              net buy. A 13F files positions, never transactions, so the flow is the change in the
              cohort&apos;s filed shares marked at the quarter&apos;s volume-weighted mean close,
              and a row whose moves the price dataset could not mark shows — and counts them in
              unpriced.
            </p>
          </section>
        ) : null}

        {payload ? (
          <section className="section">
            <header>
              <h2>Crowded trades radar</h2>
              <dl className="figures">
                <div title="companies the tracked funds hold a share of this quarter">
                  <dt>Names</dt>
                  <dd>{count(totals.crowded)}</dd>
                </div>
                <Cut total={totals.crowded} shown={crowded.length} />
              </dl>
            </header>
            {crowded.length === 0 ? (
              <p className="muted">no company&apos;s stake could be measured against a share count{inQuarter}</p>
            ) : (
              <table className="statements">
                <thead>
                  <tr>
                    <th scope="col">Ticker</th>
                    <th scope="col">Company</th>
                    <th scope="col">Funds</th>
                    <th scope="col">Shares held</th>
                    <th scope="col">Value</th>
                    <th scope="col">Shares outstanding</th>
                    <th scope="col">Share of shares outstanding</th>
                  </tr>
                </thead>
                <tbody>
                  {crowded.map((row) => (
                    <tr key={row.ticker}>
                      <th scope="row">
                        <Ticker row={row} onDrill={setDrilled} />
                      </th>
                      <td className="issuer">{row.issuer}</td>
                      <td>{count(row.funds)}</td>
                      <td>{count(row.shares)}</td>
                      <td>{dollars(row.valueUsd)}</td>
                      <td
                        title={
                          row.outstandingShares === undefined
                            ? undefined
                            : "the company's own share count in the market dataset at this date"
                        }
                      >
                        {row.outstandingShares === undefined
                          ? '—'
                          : compact(row.outstandingShares)}
                      </td>
                      <td>{precisePercent(row.ownedPct)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            <p className="note">
              The share is the tracked funds&apos; filed shares over the company&apos;s own share
              count at that date — a share of the shares outstanding, which is the only denominator
              either source carries, <strong>not a share of a float</strong> — and it counts only
              the funds in this lake, so it is what this lake sees and not the company&apos;s whole
              register. A share count the dataset does not carry leaves the row with no denominator,
              which is why that column is blank rather than zero.
            </p>
          </section>
        ) : null}

        {payload && limit < CEILING ? (
          <button
            type="button"
            className="more"
            onClick={() => setLimit(limit + STEP)}
            title="the crowded radar can only rank the quarter's 100 largest positions, so 100 is the end of every list"
          >
            Show {STEP} more of each list
          </button>
        ) : null}

        <ConvictionBoard
          quarters={payload?.quarters || []}
          version={version}
          onTicker={setDrilled}
        />

        {drilled ? (
          <div ref={drill}>
            <InstitutionalOwnership symbol={drilled} />
          </div>
        ) : null}
      </main>
    </div>
  );
}
