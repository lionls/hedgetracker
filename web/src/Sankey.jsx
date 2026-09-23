import { count, dollars, period, percent, plural } from './numbers.js';

// The four kinds of band a position crosses the quarter in. `carried` is the grey
// the donut gives everything it does not name, bought and sold are the page's own
// up and down, and the market's move is the page's blue: green and red say a trade
// happened, blue says the value moved without one. There is no fifth colour for
// "unpriced" — a band drawn is a band the response could measure, and the rest is
// in the caption.
const BANDS = {
  carried: { label: 'carried', color: 'rgba(139, 150, 165, 0.35)' },
  bought: { label: 'bought', color: 'rgba(38, 166, 154, 0.55)' },
  sold: { label: 'sold', color: 'rgba(239, 83, 80, 0.55)' },
  market: { label: 'the market', color: 'rgba(106, 176, 243, 0.45)' },
};
const ORDER = ['carried', 'bought', 'sold', 'market'];

// The drawing's box, in viewBox units: two label gutters, two node columns, and
// the rest ribbons. The body grows with the taller column, so a fund whose two
// books share no position at all still gets rows a pointer can find, and the label
// gutter holds a CUSIP fallback beside its value. The row is what sets the panel's
// height, and the panel shares a scrolling column with the page's other sections,
// so it is sized to leave the whole picture in the panel at a desktop height.
const WIDTH = 1000;
const GUTTER = 96;
const COLUMN = 11;
const NODE_GAP = 7;
const TOP = 46;
const BOTTOM = 24;
const MIN_BODY = 300;
const ROW = 22;
const LEFT_X = GUTTER;
const RIGHT_X = WIDTH - GUTTER - COLUMN;
// Below this a node is a line rather than a row and the figure leaves its name to
// the tooltip. The floor is set by the label's own height against the closest two
// rows can sit: a labelled row is at least this tall, its neighbour is another
// NODE_GAP below, and two 11-unit labels cannot meet across that.
const LABEL_FLOOR = 10;

// round keeps the coordinates in the DOM at a hundredth of a unit: the paths are
// read back to check that the bands tile their nodes, and a full float in an
// attribute is noise no one reads.
const round = (value) => Math.round(value * 100) / 100;

// band is the arithmetic of one position over the two filings, and the reason the
// diagram is worth drawing: the change in reported value is split into the trade
// the flow view measured (`delta_shares` at the quarter's volume-weighted mean
// close) and what is left over, which is the market's move. A position's two
// values are as filed, so its bands add up to them exactly — carried plus what
// grew is its value now, carried plus what shrank is its value then — and the
// bands of both books therefore add up to both filings.
//
// Where the ticker has no daily bars there is no trade to measure, and the
// position's own action decides: shares that went up were bought, shares that went
// down were sold, and a position held without a price is the market's move. The
// tail has no action, so it is never credited with a trade it cannot show.
function band(row) {
  const carried = Math.min(row.prevValueUsd, row.valueUsd);
  const growth = Math.max(row.valueUsd - row.prevValueUsd, 0);
  const shrink = Math.max(row.prevValueUsd - row.valueUsd, 0);
  const flow = row.estFlowUsd;
  const traded = (up) => {
    if (flow !== null && flow !== undefined) return Math.min(growth || shrink, Math.max(up ? flow : -flow, 0));
    const action = row.action;
    if (action === 'NEW' || action === 'ADDED') return up ? growth : 0;
    if (action === 'TRIMMED' || action === 'EXITED') return up ? 0 : shrink;
    return 0;
  };
  const bought = growth > 0 ? traded(true) : 0;
  const sold = shrink > 0 ? traded(false) : 0;
  return {
    carried,
    bought,
    sold,
    market: growth + shrink - bought - sold,
    growth,
    shrink,
  };
}

// Sankey is the fund's book over two filings: the previous filing's positions in
// the left column, the selected quarter's in the right, and one band per position
// per kind of change. A position is a node on either side only if it is held on
// that side, so an exit leaves a node on the left and nothing on the right, and an
// entry the reverse; what arrived from outside the book (bought and the market
// going up) is the node the left column feeds from, and what left it (sold and the
// market going down) is the node the right column drains into. Both columns hold
// the same total, which is what makes the picture a rectangle rather than a
// funnel: the added node is exactly the amount the book did not have before and
// the removed node exactly what it no longer has.
export default function Sankey({ flow }) {
  if (!flow) return null;
  const previous = flow.previous;
  if (!previous) {
    const first = flow.quarters?.[1];
    return (
      <figure className="chart-panel">
        <p className="muted">
          {period(flow.period)} is this fund&apos;s first filing in the lake, so there is no book to
          flow from
          {first ? ` — the first transition the lake can draw is ${period(first)}` : ''}
        </p>
      </figure>
    );
  }

  const named = flow.positions.map((row) => ({ key: row.cusip, label: row.ticker || row.cusip, row, ...band(row) }));
  const rest = { key: 'others', label: 'the rest', row: flow.others, ...band({ ...flow.others, action: null }) };
  const held = named.filter((source) => source.carried + source.growth + source.shrink > 0);
  const sources = flow.others.prevValueUsd > 0 || flow.others.valueUsd > 0 ? [...held, rest] : held;

  const sum = (pick) => sources.reduce((total, source) => total + pick(source), 0);
  const added = sum((source) => source.growth);
  const removed = sum((source) => source.shrink);
  const total = Math.max(
    sum((source) => source.carried + source.shrink) + added,
    sum((source) => source.carried + source.growth) + removed,
  );

  const bands = [];
  for (const source of sources) {
    if (source.carried > 0) {
      bands.push({ key: `${source.key}|carried`, kind: 'carried', value: source.carried, from: source.key, to: source.key });
    }
    if (source.bought > 0) {
      bands.push({ key: `${source.key}|bought`, kind: 'bought', value: source.bought, from: 'added', to: source.key });
    }
    if (source.sold > 0) {
      bands.push({ key: `${source.key}|sold`, kind: 'sold', value: source.sold, from: source.key, to: 'removed' });
    }
    if (source.market > 0) {
      bands.push({
        key: `${source.key}|market`,
        kind: 'market',
        value: source.market,
        from: source.growth > 0 ? 'added' : source.key,
        to: source.growth > 0 ? source.key : 'removed',
      });
    }
  }
  if (bands.length === 0 || total <= 0) {
    return (
      <figure className="chart-panel">
        <p className="muted">
          neither {period(flow.prevPeriod)} nor {period(flow.period)} reports a position, so there is
          nothing to draw between them
        </p>
      </figure>
    );
  }

  // The right column is the book as it stands now, largest position first, with
  // what left the book under it; the left column is ordered against it by the
  // weighted mean of where its bands land, which is one barycentre pass and enough
  // to keep most ribbons from crossing.
  const columns = {
    left: sources
      .filter((source) => source.carried + source.shrink > 0)
      .map((source) => ({ key: source.key, label: source.label, value: source.carried + source.shrink })),
    right: sources
      .filter((source) => source.carried + source.growth > 0)
      .map((source) => ({ key: source.key, label: source.label, value: source.carried + source.growth })),
  };
  if (added > 0) columns.left.push({ key: 'added', label: 'added', value: added });
  if (removed > 0) columns.right.push({ key: 'removed', label: 'removed', value: removed });

  const tallest = Math.max(columns.left.length, columns.right.length);
  const body = Math.max(MIN_BODY, ROW * tallest);
  const scale = (body - NODE_GAP * (tallest - 1)) / total;
  const place = (column) => {
    const heights = column.map((node) => node.value * scale);
    const content =
      heights.reduce((tall, height) => tall + height, 0) + NODE_GAP * Math.max(column.length - 1, 0);
    let y = TOP + (body - content) / 2;
    return column.map((node, index) => {
      const placed = { ...node, y, height: heights[index], centre: y + heights[index] / 2 };
      y += heights[index] + NODE_GAP;
      return placed;
    });
  };

  const placedRight = place([
    ...columns.right.filter((node) => node.key !== 'removed').sort((a, b) => b.value - a.value || a.label.localeCompare(b.label)),
    ...columns.right.filter((node) => node.key === 'removed'),
  ]);
  const rightOf = new Map(placedRight.map((node) => [node.key, node]));
  const outflow = new Map();
  for (const link of bands) {
    const target = rightOf.get(link.to);
    if (!target) continue;
    if (!outflow.has(link.from)) outflow.set(link.from, []);
    outflow.get(link.from).push({ value: link.value, centre: target.centre });
  }
  const barycentre = (key) => {
    const out = outflow.get(key) || [];
    const weight = out.reduce((weight, entry) => weight + entry.value, 0);
    return weight > 0 ? out.reduce((mean, entry) => mean + entry.value * entry.centre, 0) / weight : Infinity;
  };
  const placedLeft = place([
    ...columns.left.filter((node) => node.key !== 'added').sort((a, b) => barycentre(a.key) - barycentre(b.key)),
    ...columns.left.filter((node) => node.key === 'added'),
  ]);

  const leftOf = new Map(placedLeft.map((node) => [node.key, node]));
  const links = bands
    .map((link) => ({ ...link, from: leftOf.get(link.from), to: rightOf.get(link.to) }))
    .filter((link) => link.from && link.to)
    // The books are most of the picture and the trades are the thin bands across
    // them, so the trades are painted last and stay visible where ribbons cross.
    .sort((a, b) => ORDER.indexOf(a.kind) - ORDER.indexOf(b.kind));
  // Bands leave and enter a node ordered by where their other end sits, so the
  // ribbons of one node fan out without pairing off against each other.
  for (const node of placedLeft) {
    let cursor = node.y;
    for (const link of links.filter((link) => link.from.key === node.key).sort((a, b) => a.to.centre - b.to.centre)) {
      link.y0 = cursor;
      cursor += link.value * scale;
    }
  }
  for (const node of placedRight) {
    let cursor = node.y;
    for (const link of links.filter((link) => link.to.key === node.key).sort((a, b) => a.from.centre - b.from.centre)) {
      link.y1 = cursor;
      cursor += link.value * scale;
    }
  }

  const ribbon = (link) => {
    const x0 = LEFT_X + COLUMN;
    const x1 = RIGHT_X;
    const xm = (x0 + x1) / 2;
    const t = link.value * scale;
    const top = [link.y0, link.y1];
    const bottom = [link.y0 + t, link.y1 + t];
    return [
      `M${round(x0)} ${round(top[0])}`,
      `C${round(xm)} ${round(top[0])} ${round(xm)} ${round(top[1])} ${round(x1)} ${round(top[1])}`,
      `L${round(x1)} ${round(bottom[1])}`,
      `C${round(xm)} ${round(bottom[1])} ${round(xm)} ${round(bottom[0])} ${round(x0)} ${round(bottom[0])}`,
      'Z',
    ].join(' ');
  };

  const title = (source) => {
    if (source.key === 'others') {
      const unpriced = flow.others.unpriced > 0 ? `, ${plural(flow.others.unpriced, 'position')} without price bars` : '';
      return `every position the diagram does not name · ${plural(flow.others.prevPositions, 'position')} at ${period(
        flow.prevPeriod,
      )} (${dollars(flow.others.prevValueUsd)}) → ${plural(flow.others.positions, 'position')} at ${period(
        flow.period,
      )} (${dollars(flow.others.valueUsd)})${unpriced}`;
    }
    const row = source.row;
    return `${row.issuer} · ${row.action.toLowerCase()} · ${percent(row.prevWeightPct)} of the book at ${period(
      flow.prevPeriod,
    )} → ${percent(row.weightPct)} at ${period(flow.period)}`;
  };
  const sourcesOf = new Map(sources.map((source) => [source.key, source]));
  const label = (node) =>
    node.key === 'added' ? 'added' : node.key === 'removed' ? 'removed' : sourcesOf.get(node.key)?.label || node.label;
  const heights = new Map([...placedLeft, ...placedRight].map((node) => [node.key, node.height]));
  const nodeTitle = (node) => {
    if (node.key === 'added') {
      return `value this quarter's positions gained over the previous filing, whether the fund bought it or the market moved`;
    }
    if (node.key === 'removed') {
      return `value this quarter's positions no longer hold, whether the fund sold it or the market moved`;
    }
    return title(sourcesOf.get(node.key));
  };

  // How many of the named positions the price dataset does not cover: their band
  // follows their shares rather than a measured trade, which is the one place the
  // picture states less than it looks like it says.
  const unpriced = named.filter(
    (source) => source.row.estFlowUsd === null || source.row.estFlowUsd === undefined,
  ).length;

  // A band drawn but not named in the legend is a colour with no meaning: the
  // legend is the bands that are in the picture, with what they add up to.
  const legend = ORDER.map((kind) => ({
    kind,
    ...BANDS[kind],
    value: bands.reduce((total, link) => total + (link.kind === kind ? link.value : 0), 0),
  })).filter((entry) => entry.value > 0);

  const draw = (node, x, side) => {
    const boundary = node.key === 'added' || node.key === 'removed';
    if (node.height < LABEL_FLOOR && !boundary) return null;
    const anchor = side === 'left' ? 'end' : 'start';
    const tx = side === 'left' ? x - 10 : x + COLUMN + 10;
    // What entered and what left the book are two of the four things the figure
    // exists to name, and they are drawn small whenever the book barely moved, so
    // they keep their label below the block rather than lose it to the floor. Both
    // are the last row of their column, so the space under them is free.
    const ty = node.height < LABEL_FLOOR ? node.y + node.height + 10 : node.centre + 4;
    return (
      <text key={`${node.key}-label`} className="sankey-label" x={tx} y={ty} textAnchor={anchor}>
        {label(node)} <tspan className="sankey-label-value">{dollars(node.value)}</tspan>
      </text>
    );
  };

  return (
    <figure className="chart-panel">
      <figcaption>
        <ul className="sankey-legend">
          {legend.map((entry) => (
            <li key={entry.kind}>
              <span className="sankey-swatch" style={{ background: entry.color }} />
              <span>{entry.label}</span>
              <span className="sankey-value">{dollars(entry.value)}</span>
            </li>
          ))}
        </ul>
        <span className="muted">
          the gap between the columns is the quarter: a band is what a position was worth at the
          previous filing on the left and at this one on the right
        </span>
      </figcaption>
      <svg
        className="sankey"
        viewBox={`0 0 ${WIDTH} ${TOP + body + BOTTOM}`}
        role="img"
        aria-label={`${plural(previous.positions, 'position')} at ${period(flow.prevPeriod)} and ${plural(
          flow.current.positions,
          'position',
        )} at ${period(flow.period)}, as ${plural(links.length, 'band')}`}
      >
        <text className="sankey-column" x={LEFT_X} y={26} textAnchor="start">
          {period(flow.prevPeriod)} · {plural(previous.positions, 'position')} · {dollars(previous.valueUsd)}
        </text>
        <text className="sankey-column" x={RIGHT_X + COLUMN} y={26} textAnchor="end">
          {period(flow.period)} · {plural(flow.current.positions, 'position')} · {dollars(flow.current.valueUsd)}
        </text>
        <g className="sankey-links">
          {links.map((link) => (
            <path key={link.key} className="sankey-link" d={ribbon(link)} fill={BANDS[link.kind].color}>
              <title>{`${label(link.from)} → ${label(link.to)} · ${BANDS[link.kind].label} · ${dollars(link.value)}`}</title>
            </path>
          ))}
        </g>
        {[...placedLeft.map((node) => [node, LEFT_X, 'left']), ...placedRight.map((node) => [node, RIGHT_X, 'right'])].map(
          ([node, x, side]) => (
            <g key={`${side}-${node.key}`}>
              <rect
                x={x}
                y={round(node.y)}
                width={COLUMN}
                height={round(node.height)}
                rx={2}
                fill={node.key === 'added' || node.key === 'removed' ? 'var(--line)' : 'rgba(139, 150, 165, 0.7)'}
              >
                <title>{nodeTitle(node)}</title>
              </rect>
              {draw(node, x, side)}
            </g>
          ),
        )}
      </svg>
      <p className="muted sankey-note">
        the {flow.slices} largest positions by each side&apos;s reported value are named and every
        other one is drawn as {rest.label}: {plural(flow.others.prevPositions, 'position')} worth{' '}
        {dollars(flow.others.prevValueUsd)} at {period(flow.prevPeriod)},{' '}
        {plural(flow.others.positions, 'position')} worth {dollars(flow.others.valueUsd)} at{' '}
        {period(flow.period)}
        {flow.others.unpriced > 0
          ? `, ${count(flow.others.unpriced)} of them with no price bars`
          : ''}
        .
        {/* The positions whose band is their action rather than a measured trade are
            worth counting here: they are the ones the picture states most loosely,
            and the lake's own coverage figure is what the funds page reports. */}
        {unpriced > 0
          ? ` ${count(unpriced)} of the named positions have no price bars either, so their bands follow their shares.`
          : ''}
      </p>
    </figure>
  );
}
