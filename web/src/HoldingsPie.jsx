import { count, dollars, percent, plural } from './numbers.js';

// The colours a named slice can be drawn in, and the one the tail gets. The
// palette walks from the page's own blue around to teal, so a dozen slices stay
// apart without any of them reading as a gain or a loss: this chart is
// composition, not P&L, and the page has spent red and green on marks already.
const COLORS = [
  '#5aa9f0',
  '#7b8ef0',
  '#a07bf0',
  '#c47ae6',
  '#e07ad0',
  '#f0709f',
  '#f0785a',
  '#e8a054',
  '#d9c05a',
  '#b4cc5c',
  '#6cc79a',
  '#58c2c0',
];
const TAIL_COLOR = 'rgba(139, 150, 165, 0.35)';

// One ring, drawn as one stroked circle per slice: a slice is its share of the
// circumference, placed by shifting the dash pattern to where the slice starts.
// Arc paths would be the other way to draw it and cannot draw a book that is one
// position — that arc ends where it begins, which is not an arc.
const RADIUS = 42;
const THICKNESS = 15;
const CIRCUMFERENCE = 2 * Math.PI * RADIUS;
// The gap between slices, in circumference units: without it two neighbours read
// as one slice, and the ring stops saying how many there are.
const GAP = 1.5;

// HoldingsPie is the fund's book at the quarter the page is on: the largest
// positions by reported value, and everything the response did not name as one
// tail slice. The centre carries the total, so the ring answers what the fund
// holds and how spread it is before a legend row is read.
export default function HoldingsPie({ holdings }) {
  const largest = holdings?.largest || [];
  const positions = holdings?.positions || 0;
  // The ring can only draw a position the filing put a value on, because an arc
  // needs a size and a zero-size arc would claim the fund held nothing: a row
  // with no value is left to the tail, which counts it without pricing it. A
  // book whose values are all unstated has no ring to draw, and says that rather
  // than drawing a filing worth nothing.
  const valued = largest.filter((row) => typeof row.valueUsd === 'number');
  const total = holdings?.valueUsd;
  if (positions === 0) {
    return <p className="muted">this filing reports no positions</p>;
  }
  if (typeof total !== 'number' || total <= 0 || valued.length === 0) {
    return <p className="muted">this filing states no values for its positions</p>;
  }

  const drawn = valued.reduce((sum, row) => sum + row.valueUsd, 0);
  const named = valued.reduce((sum, row) => sum + row.weightPct, 0);
  const rest = Math.max(0, total - drawn);
  const slices = valued.map((row, index) => ({
    key: row.cusip,
    label: row.ticker || row.cusip,
    title: `${row.issuer} · ${percent(row.weightPct)} of the filing · ${dollars(row.valueUsd)}`,
    value: row.valueUsd,
    weight: row.weightPct,
    color: COLORS[index % COLORS.length],
  }));
  if (rest > 0) {
    const others = Math.max(0, positions - valued.length);
    slices.push({
      key: 'rest',
      label: others > 0 ? `${count(others)} others` : 'the rest',
      title: `${plural(others, 'position')}, ${dollars(rest)} in all`,
      value: rest,
      // The tail takes what the named slices leave of the book, so the column
      // adds up to the filing rather than to one rounding error short of it.
      weight: Math.max(0, 100 - named),
      color: TAIL_COLOR,
      tail: true,
    });
  }

  let cursor = 0;
  const arcs = slices.map((slice) => {
    const share = slice.value / total;
    const length = slices.length > 1 ? Math.max(share * CIRCUMFERENCE - GAP, 0.5) : CIRCUMFERENCE;
    const arc = { ...slice, dash: `${length} ${CIRCUMFERENCE - length}`, start: cursor };
    cursor += share * CIRCUMFERENCE;
    return arc;
  });

  return (
    <figure className="chart-panel">
      <div className="holdings">
        <div className="donut">
          <svg
            viewBox="0 0 100 100"
            role="img"
            aria-label={`${plural(positions, 'position')}, ${dollars(
              total,
            )} reported, the largest ${valued.length} of them named`}
          >
            <circle
              className="donut-track"
              cx="50"
              cy="50"
              r={RADIUS}
              fill="none"
              strokeWidth={THICKNESS}
            />
            <g transform="rotate(-90 50 50)">
              {arcs.map((arc) => (
                <circle
                  key={arc.key}
                  className="donut-slice"
                  cx="50"
                  cy="50"
                  r={RADIUS}
                  fill="none"
                  stroke={arc.color}
                  strokeWidth={THICKNESS}
                  strokeDasharray={arc.dash}
                  strokeDashoffset={-arc.start}
                >
                  <title>{arc.title}</title>
                </circle>
              ))}
            </g>
          </svg>
          <div className="donut-centre">
            <strong>{dollars(total)}</strong>
            <span className="muted">{plural(positions, 'position')}</span>
          </div>
        </div>
        <ul className="donut-legend">
          {slices.map((slice) => (
            <li key={slice.key}>
              <span className="donut-swatch" style={{ background: slice.color }} />
              <span
                className={slice.tail ? 'donut-name muted' : 'donut-name'}
                title={slice.title}
              >
                {slice.label}
              </span>
              <span className="donut-share muted">{percent(slice.weight)}</span>
              <span className="donut-value">{dollars(slice.value)}</span>
            </li>
          ))}
        </ul>
      </div>
    </figure>
  );
}
