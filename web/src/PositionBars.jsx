import { count, signedDollars, signedPercent } from './numbers.js';

// How much of each side of the axis the panel draws. The point is the shape of a
// quarter rather than the whole book, and the table underneath carries every
// position with its figures.
const SIDE = 8;

// PositionBars is one quarter's P&L per position: gains to the right of the axis,
// losses to the left, both scaled to the largest mark in view so the lengths are
// comparable. The bars are elements rather than a canvas, which keeps the labels
// crisp and the ranking readable at any width. A position the price dataset does
// not cover has no mark to place — it is counted in the caption instead of being
// drawn as a flat bar.
export default function PositionBars({ rows }) {
  const marked = rows.filter((row) => row.pnlUsd !== null);
  const gains = marked
    .filter((row) => row.pnlUsd > 0)
    .sort((left, right) => right.pnlUsd - left.pnlUsd)
    .slice(0, SIDE);
  const losses = marked
    .filter((row) => row.pnlUsd < 0)
    .sort((left, right) => left.pnlUsd - right.pnlUsd)
    .slice(0, SIDE);
  const shown = [...gains, ...losses];
  if (shown.length === 0) {
    return (
      <p className="muted">
        no position of this quarter could be marked
        {marked.length === 0 && rows.length > 0
          ? ` — the price dataset covers none of the ${count(rows.length)} positions filed`
          : ''}
      </p>
    );
  }

  const scale = shown.reduce((largest, row) => Math.max(largest, Math.abs(row.pnlUsd)), 0);

  return (
    <ul className="pnl-bars">
      {shown.map((row) => {
        const gain = row.pnlUsd > 0;
        const width = `${(50 * Math.abs(row.pnlUsd)) / scale}%`;
        return (
          <li key={row.cusip}>
            <span className="pnl-name" title={`${row.issuer} · ${row.action.toLowerCase()}`}>
              {row.ticker || row.cusip}
            </span>
            <span className="pnl-track">
              <span className={gain ? 'pnl-fill up' : 'pnl-fill down'} style={gain ? { left: '50%', width } : { right: '50%', width }} />
            </span>
            <span className={gain ? 'pnl-value up' : 'pnl-value down'}>
              {signedDollars(row.pnlUsd)}
            </span>
            <span className="pnl-pct muted">{signedPercent(row.pnlPct)}</span>
          </li>
        );
      })}
    </ul>
  );
}
