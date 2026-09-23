import { useEffect, useState } from 'react';
import { ACTION_TONE, Badge } from './Badge.jsx';
import { thirteenOwners } from './api.js';
import { compact, count, dollars, period, plural, precisePercent, signedCount, signedPoints } from './numbers.js';

// The institutional half of a company's page: which of the lake's 13F filers hold
// the symbol, what the quarter did to their positions, and what the cohort's
// stake adds up to. The panels above and below are market data and this one is
// filings, which is the whole point of it — the shares come from the forms and the
// denominator of the ownership badge from the market table, so the two halves of
// the app meet in one row.
//
// What it cannot say, it does not: a filing states no cost basis, so the column is
// an estimate and says so, and the market dataset carries no float, so the
// ownership badge names the denominator it actually divides by.

// actionBadge marks the quarter's move. An empty action is not HELD: the fund's
// first filing in the lake is this quarter, so there is no previous book to have
// moved from, and the panel says that instead of calling it unchanged.
function actionBadge(holder) {
  if (!holder.action) {
    return (
      <span className="muted" title="the fund's first filing in the lake — a flow needs two">
        —
      </span>
    );
  }
  const move = holder.action === 'HELD'
    ? 'held through the quarter, unchanged'
    : `${signedCount(holder.deltaShares)} shares, ${signedPoints(holder.deltaWeightPct)} of the fund's book`;
  return (
    <Badge tone={ACTION_TONE[holder.action]} title={`${move}${holder.splitAdjusted ? ' · split-adjusted' : ''}`}>
      {holder.action}
    </Badge>
  );
}

// basisTitle explains the estimate the column shows: the average of the VWAPs of
// the quarters this fund bought the position in, the window it covers, and how it
// compares with the price the filing itself values the shares at.
function basisTitle(holder) {
  if (holder.estCostPerShare === null) {
    return "nothing to price it from: the fund filed no purchase of this position in the quarters the lake covers, or no market VWAP for them";
  }
  // The estimate only has a filing price to sit beside when the filing stated
  // both a share count and a value: where it stated neither, or a count of zero,
  // the tooltip says that rather than dividing by a count the filing never gave.
  if (!holder.shares || holder.valueUsd === null) {
    return `${count(holder.estCostPerShare)} a share: the average VWAP of the ${plural(holder.buyQuarters, 'quarter')} this fund bought in, which the filing's own figures leave nothing to set against`;
  }
  const filed = holder.valueUsd / holder.shares;
  return `${count(holder.estCostPerShare)} a share: the average VWAP of the ${plural(holder.buyQuarters, 'quarter')} this fund bought in, against the ${count(filed)} a share the filing values the position at`;
}

export default function InstitutionalOwnership({ symbol }) {
  // undefined while loading, and a payload whose period is empty is a lake with no
  // filings in it: both are states the panel prints rather than errors. The error
  // state is the lake itself missing, building or stale, which the server answers
  // with a 503 whose message is the reason.
  const [payload, setPayload] = useState(undefined);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    setPayload(undefined);
    setError('');
    thirteenOwners(symbol).then(
      (result) => active && setPayload(result),
      (failure) => active && setError(failure.message),
    );
    return () => {
      active = false;
    };
  }, [symbol]);

  const summary = payload?.summary;
  const rows = payload?.holders || [];
  const quarter = payload ? period(payload.period) : '';
  const figures = payload && payload.period
    ? [
      ['Tracked shares', compact(summary.shares), `across ${plural(summary.holders, 'fund')} in ${quarter}`],
      [
        'Share of tracked AUM',
        precisePercent(summary.aumPct),
        `${dollars(summary.valueUsd)} as filed, of the ${dollars(summary.trackedAumUsd)} every tracked fund reports holding in ${quarter}`,
      ],
      [
        'Net QoQ flow',
        `${signedCount(summary.netShares)} shares`,
        `${plural(summary.buyingFunds, 'buying fund')}, ${plural(summary.sellingFunds, 'selling fund')}, ${plural(summary.exits, 'exit')} in ${quarter}`,
      ],
      [
        'Share of shares outstanding',
        precisePercent(summary.ownedPct),
        summary.outstandingShares
          ? `${count(summary.shares)} shares of the ${compact(summary.outstandingShares)} the company reported for ${quarter}`
          : `the market dataset reports no share count for ${symbol} at ${quarter}, so there is nothing to divide by`,
      ],
    ]
    : [];

  return (
    <section className="section">
      <header>
        <h2>Institutional ownership</h2>
        <span className="muted">
          {error
            ? ''
            : payload === undefined
              ? 'reading the 13F filings…'
              : payload.period
                ? `${plural(summary.holders, 'tracked fund')} holding ${symbol} in ${quarter}`
                : 'no filings in the lake'}
        </span>
        {figures.length > 0 ? (
          <dl className="figures">
            {figures.map(([label, value, title]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd title={title}>{value}</dd>
              </div>
            ))}
          </dl>
        ) : null}
      </header>
      {error ? <p className="error">{error}</p> : null}
      {payload && !payload.period ? (
        <p className="muted">
          The 13F lake holds no filings, so there is nothing to say about who owns {symbol}.
        </p>
      ) : null}
      {payload && payload.period && rows.length === 0 ? (
        <p className="muted">
          None of the funds the lake tracks held {symbol} in {quarter}, its newest quarter. The
          cohort is a fixed set of filers, so this is a thin list rather than an error — {symbol} is
          simply not in their books.
        </p>
      ) : null}
      {rows.length > 0 ? (
        <>
          <table className="statements">
            <thead>
              <tr>
                <th scope="col">Fund</th>
                <th scope="col">Shares</th>
                <th scope="col" title="the position's share of the fund's own reported book">
                  Position weight
                </th>
                <th scope="col" title="what this fund's own previous filing makes of the position">
                  QoQ action
                </th>
                <th scope="col" title="the average price the quarters this fund bought in imply">
                  Est. cost basis
                </th>
              </tr>
            </thead>
            <tbody>
              {rows.map((holder) => (
                <tr key={holder.cik}>
                  <th scope="row" className="issuer" title={`CIK ${holder.cik}`}>
                    {holder.filerName || `CIK ${holder.cik}`}
                  </th>
                  <td title={`${dollars(holder.valueUsd)} as filed`}>{count(holder.shares)}</td>
                  <td>{precisePercent(holder.weightPct)}</td>
                  <td>{actionBadge(holder)}</td>
                  <td title={basisTitle(holder)}>
                    {holder.estCostUsd === null ? '—' : dollars(holder.estCostUsd)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {payload.total > rows.length ? (
            <p className="muted">
              The {rows.length} largest positions of {count(payload.total)}, by the weight they are
              of the fund holding them.
            </p>
          ) : null}
          <p className="note">
            The lake is a fixed set of 13F filers, so this is that cohort&apos;s book rather than the
            market: shares and values are as filed for {quarter}, and the roster is the funds still
            holding the name, a fund that left it counted in the flow and not in the table. The
            ownership badge divides their shares by the count the market dataset reports at that
            quarter&apos;s end — the market table carries shares outstanding and no float, so no
            figure here is a float percentage. A 13F files no cost basis either: the estimate is
            what the quarters a fund bought in imply at their VWAP, over the window the lake covers
            and ignoring what it sold
            {summary.unpricedFunds > 0
              ? `, which is why ${summary.unpricedFunds} of the ${rows.length} holders here show a dash`
              : ''}
            .
          </p>
        </>
      ) : null}
    </section>
  );
}
