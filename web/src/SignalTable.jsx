import { count, dollars, percent, signedCount, signedDollars, signedPercent, signedPoints } from './numbers.js';

// The six classes conviction_scores emits, in the three tones the stylesheet
// has: a buy, a dump, and the bookkeeping in between. A rebalance of a passive
// index fund and a fund that did not move at all are both answers, not noise,
// so neither is coloured.
const SIGNAL_TONE = {
  HIGH_CONVICTION_BUY: 'up',
  STANDARD_BUY: 'up',
  CONVICTION_DUMP: 'down',
  PASSIVE_REBALANCE: 'flat',
  MAINTAINED: 'flat',
  ROUTINE_ADJUSTMENT: 'flat',
};

// The five actions fund_quarterly_flows emits. A trim and a dump are the same
// direction of travel, so they share a colour.
const ACTION_TONE = { NEW: 'up', ADDED: 'up', TRIMMED: 'down', EXITED: 'down', HELD: 'flat' };

function Badge({ tone, title, children }) {
  return (
    <span className={`badge ${tone}`} title={title}>
      {children}
    </span>
  );
}

// SignalTable is the conviction table: one fund's quarter when `fund` is set,
// the whole lake's quarter otherwise — the only difference between the two
// panels that show it.
export default function SignalTable({ rows, fund = false, onTicker }) {
  return (
    <table className="statements">
      <thead>
        <tr>
          <th scope="col">Ticker</th>
          {fund ? null : <th scope="col">Fund</th>}
          <th scope="col">Action</th>
          <th scope="col">Shares</th>
          <th scope="col">Δ shares</th>
          <th scope="col">Weight</th>
          <th scope="col">Δ weight</th>
          <th scope="col">Est. flow</th>
          <th scope="col">Signal</th>
          <th scope="col">Quarter VWAP</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => {
          const name = row.ticker || row.cusip;
          return (
            <tr key={`${row.cik}:${row.cusip}:${row.period}`}>
              <th scope="row" title={row.issuer}>
                {onTicker && row.ticker ? (
                  <button type="button" className="drill" onClick={() => onTicker(row.ticker)}>
                    {name}
                  </button>
                ) : (
                  name
                )}
              </th>
              {fund ? null : <td title={`CIK ${row.cik}`}>{row.filerName || row.cik}</td>}
              <td>
                <Badge tone={ACTION_TONE[row.action] || 'flat'}>{row.action}</Badge>
              </td>
              <td
                title={`${count(row.prevShares)} shares on ${
                  row.prevPeriod || 'the previous filing'
                }, ${row.quartersBetween} quarter(s) earlier`}
              >
                {count(row.shares)}
              </td>
              <td className={row.deltaShares >= 0 ? 'up' : 'down'}>
                {signedCount(row.deltaShares)}
                {row.splitAdjusted ? (
                  <span className="split">
                    <Badge
                      tone="flat"
                      title={`share delta adjusted for a ${count(
                        row.splitFactor,
                      )}:1 split between the two filings; values are not adjusted, a split does not change market value`}
                    >
                      ×{count(row.splitFactor)}
                    </Badge>
                  </span>
                ) : null}
                {row.deltaSharesPct === null || row.deltaSharesPct === undefined ? null : (
                  <small className="muted"> {signedPercent(row.deltaSharesPct)}</small>
                )}
              </td>
              <td>{percent(row.weightPct)}</td>
              <td className={row.deltaWeightPct >= 0 ? 'up' : 'down'}>
                {signedPoints(row.deltaWeightPct)}
              </td>
              <td
                className={(row.estCapitalFlow || 0) >= 0 ? 'up' : 'down'}
                title="split-adjusted share delta × the quarter's VWAP; null when the ticker has no daily bars"
              >
                {signedDollars(row.estCapitalFlow)}
              </td>
              <td>
                <Badge tone={SIGNAL_TONE[row.signal] || 'flat'}>{row.signal}</Badge>
              </td>
              <td
                title={
                  row.quarterlyLow === null || row.quarterlyLow === undefined
                    ? 'no daily price bars for this ticker'
                    : `${dollars(row.quarterlyLow)} – ${dollars(row.quarterlyHigh)} over the quarter`
                }
              >
                {dollars(row.quarterlyVwap)}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
