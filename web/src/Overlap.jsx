import { useEffect, useRef, useState } from 'react';
import FundPicker from './FundPicker.jsx';
import { Badge, ACTION_TONE } from './Badge.jsx';
import { thirteenCompare, thirteenFunds } from './api.js';
import { count, dollars, period, percent, plural, signedCount, signedPoints } from './numbers.js';

// The Venn's one figure the page cannot print: two circles, one per book, with the
// lens between them the capital both funds hold. A circle's area is its book, so
// its radius is the book's square root, the larger book drawing at 80 px; the
// centres are then placed by bisecting the standard two-circle intersection area
// for the shared value at that same dollars-per-area scale. The shape is
// proportional, the figures beside it are exact — the drawing says two books
// overlap a little, the numbers say how much.
const VENN_RADIUS = 80;
const VENN_WIDTH = 320;
const VENN_HEIGHT = 220;
const VENN_CX = VENN_WIDTH / 2;
const VENN_CY = VENN_HEIGHT / 2;

// lensArea is the area two circles of these radii share when their centres are d
// apart: the two circular segments the common chord cuts off. It runs from the
// smaller circle's whole area, when one circle sits inside the other, to zero when
// they only touch — which is what makes the bisection below converge.
function lensArea(rA, rB, d) {
  if (d <= Math.abs(rA - rB)) return Math.PI * Math.min(rA, rB) ** 2;
  if (d >= rA + rB) return 0;
  const cosA = (d * d + rA * rA - rB * rB) / (2 * d * rA);
  const cosB = (d * d + rB * rB - rA * rA) / (2 * d * rB);
  const chord = Math.sqrt(
    Math.max(0, (-d + rA + rB) * (d + rA - rB) * (d - rA + rB) * (d + rA + rB)),
  );
  return rA * rA * Math.acos(cosA) + rB * rB * Math.acos(cosB) - chord / 2;
}

// separation is the centre distance whose lens area is target, bisected between
// the closest the two circles can be drawn and the point where they touch: the
// area falls monotonically across that span, so sixty halvings land on the
// distance to within a fraction of a pixel.
function separation(rA, rB, target) {
  let near = Math.abs(rA - rB);
  let far = rA + rB;
  if (lensArea(rA, rB, near) <= target) return near;
  for (let step = 0; step < 60; step += 1) {
    const mid = (near + far) / 2;
    if (lensArea(rA, rB, mid) > target) near = mid;
    else far = mid;
  }
  return (near + far) / 2;
}

// venn is the whole geometry of the picture, from the three figures the payload
// carries: the radii the two book values imply, the distance the shared capital
// implies at the same scale, and where that puts the two centres.
function venn(aValue, bValue, shared) {
  const peak = Math.max(aValue, bValue);
  const rA = VENN_RADIUS * Math.sqrt(aValue / peak);
  const rB = VENN_RADIUS * Math.sqrt(bValue / peak);
  const target = (Math.PI * VENN_RADIUS * VENN_RADIUS * shared) / peak;
  const d = separation(rA, rB, target);
  // Each set names itself at its own centre, offset into its own half: two books
  // that overlap almost entirely draw two circles at almost the same place, and
  // labels at the centre would land on top of each other. The offset is half a
  // radius, capped so a large circle's label stays near the middle and a small
  // one's stays inside it.
  const aGap = Math.min(40, rA / 2);
  const bGap = Math.min(40, rB / 2);
  return {
    rA,
    rB,
    aCx: VENN_CX - d / 2,
    bCx: VENN_CX + d / 2,
    aLabelY: VENN_CY - aGap,
    bLabelY: VENN_CY + bGap,
  };
}

// fundName is how every panel names a side. This lake leaves filer_name empty, so
// a fund is its CIK — and a panel that says "the larger book" would be saying
// nothing when it can say which fund it means.
function fundName(side) {
  return side ? side.filerName || `CIK ${side.cik}` : '—';
}

// The sign is the delta's direction and the class is its tone: zero moved nowhere
// and null never moved, so neither is coloured.
function tone(value) {
  if (value === null || value === undefined || value === 0) return '';
  return value < 0 ? 'down' : 'up';
}

// mover is what one side of a row did, in the words its badge's title uses. An
// empty action is not HELD: it means the fund has no earlier filing in the lake,
// so there is no previous book for this quarter's position to have moved from.
function mover(fund, action, deltaShares, deltaWeightPct) {
  if (!action) {
    return `${fund} has no earlier filing in the lake, so this position has no move to read`;
  }
  if (action === 'HELD') return `${fund} held through the quarter, unchanged`;
  return `${fund}: ${signedCount(deltaShares)} shares, ${signedPoints(deltaWeightPct)} of its own book`;
}

// basis says what the column's estimate is made of: the average VWAP of the
// quarters this fund bought the CUSIP in, the same estimate the ownership panel
// makes. A fund that only held has no purchase to average, and a purchase the
// price dataset cannot mark leaves the average with nothing to stand on.
function basis(fund, value) {
  if (value === null || value === undefined) {
    return `no estimated basis: ${fund} filed no purchase of this position in the quarters the lake covers, or the price dataset has no VWAP for them`;
  }
  return `${fund}'s estimated basis, a share: the average VWAP of the quarters it bought this CUSIP in — an estimate from the price dataset, not a price the filing carries`;
}

// The overlap page: two funds of the lake side by side for one quarter, what the
// two books share, and where one of them is buying what the other sells. Every
// figure is the filings' own for that quarter; the two estimates on the page — the
// basis a purchase implies and the shape of the Venn — say so where they are used.
export default function Overlap() {
  const [status, setStatus] = useState(null);
  const [version, setVersion] = useState(0);
  const [funds, setFunds] = useState([]);
  const [a, setA] = useState('');
  const [b, setB] = useState('');
  const [wanted, setWanted] = useState('');
  const [payload, setPayload] = useState(null);
  const [failure, setFailure] = useState('');
  // The pair is seeded once, from the lake's list. A rebuilt lake is a new list
  // rather than a new pair, so it refreshes the options and leaves the choice.
  const seeded = useRef(false);
  const choice = `${a}|${b}|${wanted}`;
  const lastChoice = useRef(choice);

  useEffect(() => {
    let active = true;
    thirteenFunds('', 500).then(
      (list) => {
        if (!active) return;
        setFunds(list.funds);
        if (seeded.current) return;
        // The opening pair is the two smallest funds that have a second filing:
        // two flagship books side by side print a page of rows nobody reads and
        // bury the shared ground this page exists to show, and a fund whose only
        // filing is this quarter has no move to report in either table. A lake
        // without two such funds falls back to its two smallest books, and a lake
        // with fewer than two funds has no pair to seed at all.
        const comparable = list.funds.filter((entry) => entry.quarters > 1).slice(-2);
        const pair = comparable.length === 2 ? comparable : list.funds.slice(-2);
        if (pair.length < 2) return;
        seeded.current = true;
        setA(pair[0].cik);
        setB(pair[1].cik);
      },
      (error) => active && setFailure(error.message),
    );
    return () => {
      active = false;
    };
  }, [version]);

  useEffect(() => {
    if (!a || !b) return undefined;
    let active = true;
    // A new side or a new quarter is a different comparison, and the panels blank
    // rather than relabel: the response names the quarter it settled on, and the
    // previous answer under the new pair would be two funds' figures under two
    // other funds' names.
    if (lastChoice.current !== choice) {
      lastChoice.current = choice;
      setPayload(null);
    }
    setFailure('');
    thirteenCompare(a, b, wanted).then(
      (result) => active && setPayload(result),
      (error) => active && setFailure(error.message),
    );
    return () => {
      active = false;
    };
  }, [choice, version]);

  // A quarter is a property of the pair: the selector offers the quarters both
  // funds filed, so a new side goes back to the newest of those rather than to a
  // quarter it may never have filed.
  function pick(side, cik) {
    if (side === 'a') setA(cik);
    else setB(cik);
    setWanted('');
  }

  // The answer only counts for the pair on screen. The headline names two funds,
  // and a payload that answered a different pair would print one pair's figures
  // under another pair's names — the state a shrink of the lake or a choice the
  // server refuses leaves behind.
  const answer = payload && payload.a.cik === a && payload.b.cik === b ? payload : null;
  const quarters = answer?.quarters || [];
  const overlap = answer?.overlap;
  const shared = answer?.shared || [];
  const contrarian = answer?.contrarian || [];
  const sideA = answer?.a || funds.find((entry) => entry.cik === a) || null;
  const sideB = answer?.b || funds.find((entry) => entry.cik === b) || null;
  // inPeriod false is an unread book rather than an empty one: the fund filed
  // nothing that quarter, so there is nothing to put in a table and no table.
  const absent = answer
    ? [answer.a, answer.b].filter((side) => !side.inPeriod).map(fundName)
    : [];
  const compared = Boolean(answer && answer.period !== '' && absent.length === 0);
  // The circles need a value for each book and a price for what the two share: a
  // side the filing left unvalued has no size, and a shared set nothing could be
  // priced for has no overlap area, so the picture would be a claim no filing
  // made. The figures beside the picture are the filings' own either way.
  const shape =
    compared && sideA.valueUsd > 0 && sideB.valueUsd > 0 && overlap.sharedValueUsd != null
      ? venn(sideA.valueUsd, sideB.valueUsd, overlap.sharedValueUsd)
      : null;

  return (
    <div className="app">
      <FundPicker
        page="overlap"
        tagline="fund comparison"
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
              Overlap
              <span className="company-name">
                {fundName(sideA)} vs {fundName(sideB)}
              </span>
            </h1>
            <div className="meta">
              {!answer
                ? 'reading the two filings…'
                : answer.period
                  ? `${period(answer.period)} · ${plural(
                      answer.a.positions,
                      'position',
                    )} against ${plural(answer.b.positions, 'position')}`
                  : 'no quarter both funds filed'}
            </div>
          </div>
          {quarters.length > 0 ? (
            <select
              className="control"
              value={wanted}
              title={`${plural(quarters.length, 'quarter')} both funds filed`}
              onChange={(event) => setWanted(event.target.value)}
            >
              <option value="">newest quarter</option>
              {quarters.map((day) => (
                <option key={day} value={day}>
                  {period(day)}
                </option>
              ))}
            </select>
          ) : null}
        </header>

        {failure ? <p className="error">{failure}</p> : null}

        <section className="section">
          <header>
            <h2>Shared ground</h2>
            <span className="muted">
              {!answer
                ? 'reading the filings…'
                : compared
                  ? `one quarter of both books · ${period(answer.period)}`
                  : 'no quarter to compare'}
            </span>
            <div className="filters">
              <span className="muted">A</span>
              <select
                className="control"
                value={a}
                title={fundName(sideA)}
                onChange={(event) => pick('a', event.target.value)}
              >
                {funds.map((entry) => (
                  <option key={entry.cik} value={entry.cik}>
                    {entry.filerName || `CIK ${entry.cik}`}
                  </option>
                ))}
              </select>
              <span className="muted">B</span>
              <select
                className="control"
                value={b}
                title={fundName(sideB)}
                onChange={(event) => pick('b', event.target.value)}
              >
                {funds.map((entry) => (
                  <option key={entry.cik} value={entry.cik}>
                    {entry.filerName || `CIK ${entry.cik}`}
                  </option>
                ))}
              </select>
            </div>
          </header>

          {!a || !b ? (
            <p className="muted">
              This page puts two books side by side, so it needs two funds: choose one in A and one
              in B. A lake that holds a single filer has no pair to compare.
            </p>
          ) : !answer ? (
            failure ? null : <p className="muted">reading the two filings…</p>
          ) : answer.period === '' ? (
            <p className="muted">
              {fundName(answer.a)} and {fundName(answer.b)} filed no quarter in common, so there is
              nothing to compare: a comparison is one quarter of both books, and a quarter only one
              of them filed is one book.
            </p>
          ) : absent.length > 0 ? (
            <p className="muted">
              {absent.join(' and ')} filed nothing in {period(answer.period)}, so one side of this
              comparison has no book to read — that is an unread book, not an empty one. The quarter
              selector offers the quarters both funds filed.
            </p>
          ) : (
            <>
              <figure className="chart-panel">
                <div className="holdings">
                  {shape ? (
                    <svg
                      className="venn"
                      viewBox={`0 0 ${VENN_WIDTH} ${VENN_HEIGHT}`}
                      role="img"
                      aria-label={`${fundName(answer.a)} holds ${dollars(
                        sideA.valueUsd,
                      )}, ${fundName(answer.b)} holds ${dollars(
                        sideB.valueUsd,
                      )}, and ${dollars(overlap.sharedValueUsd)} of that is in both books`}
                    >
                      <circle className="venn-set a" cx={shape.aCx} cy={VENN_CY} r={shape.rA} />
                      <circle className="venn-set b" cx={shape.bCx} cy={VENN_CY} r={shape.rB} />
                      <text className="venn-label" x={shape.aCx} y={shape.aLabelY} textAnchor="middle">
                        {fundName(answer.a)}
                      </text>
                      <text className="venn-sub" x={shape.aCx} y={shape.aLabelY + 16} textAnchor="middle">
                        {dollars(sideA.valueUsd)}
                      </text>
                      <text className="venn-label" x={shape.bCx} y={shape.bLabelY} textAnchor="middle">
                        {fundName(answer.b)}
                      </text>
                      <text className="venn-sub" x={shape.bCx} y={shape.bLabelY + 16} textAnchor="middle">
                        {dollars(sideB.valueUsd)}
                      </text>
                      {overlap.sharedValueUsd > 0 ? (
                        <text className="venn-sub" x={VENN_CX} y={128} textAnchor="middle">
                          {dollars(overlap.sharedValueUsd)} shared
                        </text>
                      ) : null}
                    </svg>
                  ) : (
                    <p className="muted">
                      The books cannot be drawn at {period(answer.period)}: a side of the comparison,
                      or the positions the two share, is one the filing states no value for. The
                      figures beside this line are still the filings&apos; own.
                    </p>
                  )}
                  <dl className="figures">
                    <div title="positions matched on CUSIP: the intersection over the union">
                      <dt>Shared positions</dt>
                      <dd>
                        {count(overlap.sharedPositions)} of {count(overlap.unionPositions)}
                        <span className="muted"> · {percent(overlap.jaccardPct)} of the union</span>
                      </dd>
                    </div>
                    <div title="the smaller of the two values in each shared position — the part both funds hold">
                      <dt>Shared capital</dt>
                      <dd>{dollars(overlap.sharedValueUsd)}</dd>
                    </div>
                    <div title={`${fundName(answer.a)}'s book minus the part both funds hold`}>
                      <dt>Only A</dt>
                      <dd>{dollars(overlap.aOnlyValueUsd)}</dd>
                    </div>
                    <div title={`${fundName(answer.b)}'s book minus the part both funds hold`}>
                      <dt>Only B</dt>
                      <dd>{dollars(overlap.bOnlyValueUsd)}</dd>
                    </div>
                    <div title="each fund's book at this quarter">
                      <dt>Shared is</dt>
                      <dd>
                        {percent(overlap.sharedPctOfA)} of A
                        <span className="muted"> · </span>
                        {percent(overlap.sharedPctOfB)} of B
                      </dd>
                    </div>
                  </dl>
                </div>
              </figure>
              <p className="note">
                Positions are matched on CUSIP, the identity a filing uses rather than the ticker, so
                a company held through two lines is two positions here and one held under two
                spellings of a ticker is one. Nothing on this page is a share of the shares
                outstanding: the market dataset&apos;s count is not used, because no ownership
                percentage is shown — every figure is the two filings&apos; own reported value, both
                read at {period(answer.period)}.
              </p>
            </>
          )}
        </section>

        {compared ? (
          <section className="section">
            <header>
              <h2>Shared high conviction</h2>
              <span className="muted">
                {plural(shared.length, 'position')} both books hold ·{' '}
                {period(answer.period)}
              </span>
            </header>
            {shared.length === 0 ? (
              <p className="muted">The two hold nothing in common this quarter.</p>
            ) : (
              <>
                <table className="statements">
                  <thead>
                    <tr>
                      <th scope="col">Ticker</th>
                      <th scope="col">Company</th>
                      <th scope="col" title={`as filed by ${fundName(answer.a)}`}>
                        A shares
                      </th>
                      <th
                        scope="col"
                        title={`the position's share of ${fundName(answer.a)}'s own book`}
                      >
                        A weight
                      </th>
                      <th scope="col" title={`as filed by ${fundName(answer.b)}`}>
                        B shares
                      </th>
                      <th
                        scope="col"
                        title={`the position's share of ${fundName(answer.b)}'s own book`}
                      >
                        B weight
                      </th>
                      <th
                        scope="col"
                        title="the smaller of the two values — the part both funds hold"
                      >
                        Shared
                      </th>
                      <th scope="col" title="what this fund's own previous filing makes of it">
                        A action
                      </th>
                      <th scope="col" title="what this fund's own previous filing makes of it">
                        B action
                      </th>
                      <th
                        scope="col"
                        title="the average VWAP of the quarters this fund bought the CUSIP in, a share"
                      >
                        A est. basis
                      </th>
                      <th
                        scope="col"
                        title="the average VWAP of the quarters this fund bought the CUSIP in, a share"
                      >
                        B est. basis
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {shared.map((row) => (
                      <tr key={row.cusip}>
                        <th scope="row" title={row.issuer}>
                          {row.ticker || row.cusip}
                        </th>
                        <td className="issuer">{row.issuer}</td>
                        <td title={`${dollars(row.aValueUsd)} as filed`}>
                          {count(row.aShares)}
                        </td>
                        <td>{percent(row.aWeightPct)}</td>
                        <td title={`${dollars(row.bValueUsd)} as filed`}>
                          {count(row.bShares)}
                        </td>
                        <td>{percent(row.bWeightPct)}</td>
                        <td title="the smaller of the two values in this position — the part both funds hold">
                          {row.aValueUsd === null || row.bValueUsd === null
                            ? '—'
                            : dollars(Math.min(row.aValueUsd, row.bValueUsd))}
                        </td>
                        <td>
                          <Badge
                            tone={ACTION_TONE[row.aAction] || 'flat'}
                            title={mover(
                              fundName(answer.a),
                              row.aAction,
                              row.aDeltaShares,
                              row.aDeltaWeightPct,
                            )}
                          >
                            {row.aAction || '—'}
                          </Badge>{' '}
                          <span className={tone(row.aDeltaShares)}>
                            {signedCount(row.aDeltaShares)}
                          </span>
                        </td>
                        <td>
                          <Badge
                            tone={ACTION_TONE[row.bAction] || 'flat'}
                            title={mover(
                              fundName(answer.b),
                              row.bAction,
                              row.bDeltaShares,
                              row.bDeltaWeightPct,
                            )}
                          >
                            {row.bAction || '—'}
                          </Badge>{' '}
                          <span className={tone(row.bDeltaShares)}>
                            {signedCount(row.bDeltaShares)}
                          </span>
                        </td>
                        <td title={basis(fundName(answer.a), row.aEstCostPerShare)}>
                          {row.aEstCostPerShare === null ? '—' : count(row.aEstCostPerShare)}
                        </td>
                        <td title={basis(fundName(answer.b), row.bEstCostPerShare)}>
                          {row.bEstCostPerShare === null ? '—' : count(row.bEstCostPerShare)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <p className="note">
                  The estimated basis is a share price and not a filing&apos;s own: it is the average
                  VWAP of the quarters a fund bought the CUSIP in — the same estimate the ownership
                  panel makes — and not a price any filing carries:
                  a fund that only held has no purchase to average, and a purchase the price dataset
                  cannot mark leaves the average empty, which the column prints as —. An action is
                  that fund&apos;s own move in its own book: a 13F reports long positions only, so a
                  trim or an exit is a sale of the shares it held, never a short. Share counts,
                  values and weights are as filed for {period(answer.period)}. Order is the
                  server&apos;s: the positions that carry the most weight in either book first.
                </p>
              </>
            )}
          </section>
        ) : null}

        {compared ? (
          <section className="section">
            <header>
              <h2>Contrarian bets</h2>
              <span className="muted">
                {plural(answer.contrarianTotal, 'position')} · {period(answer.period)}
              </span>
            </header>
            {contrarian.length === 0 ? (
              <p className="muted">
                Neither fund is buying what the other is selling this quarter.
              </p>
            ) : (
              <>
                <table className="statements">
                  <thead>
                    <tr>
                      <th scope="col">Ticker</th>
                      <th scope="col">Company</th>
                      <th scope="col" title="the fund on the buying side of the row">
                        Buyer
                      </th>
                      <th
                        scope="col"
                        title={`the move ${fundName(answer.a)} made in this position, and its delta in shares and in weight`}
                      >
                        A
                      </th>
                      <th
                        scope="col"
                        title={`the move ${fundName(answer.b)} made in this position, and its delta in shares and in weight`}
                      >
                        B
                      </th>
                      <th
                        scope="col"
                        title={`the position's share of ${fundName(answer.a)}'s own book`}
                      >
                        A weight
                      </th>
                      <th
                        scope="col"
                        title={`the position's share of ${fundName(answer.b)}'s own book`}
                      >
                        B weight
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {contrarian.map((row) => (
                      <tr key={row.cusip}>
                        <th scope="row" title={row.issuer}>
                          {row.ticker || row.cusip}
                        </th>
                        <td className="issuer">{row.issuer}</td>
                        <td>
                          <Badge
                            tone="up"
                            title={`${fundName(answer[row.buyer])} opened or added to this position in ${period(
                              answer.period,
                            )}`}
                          >
                            {fundName(answer[row.buyer])}
                          </Badge>
                        </td>
                        <td>
                          <Badge
                            tone={ACTION_TONE[row.aAction] || 'flat'}
                            title={mover(
                              fundName(answer.a),
                              row.aAction,
                              row.aDeltaShares,
                              row.aDeltaWeightPct,
                            )}
                          >
                            {row.aAction || '—'}
                          </Badge>{' '}
                          <span className={tone(row.aDeltaShares)}>
                            {signedCount(row.aDeltaShares)}
                          </span>{' '}
                          <span className={tone(row.aDeltaWeightPct)}>
                            {signedPoints(row.aDeltaWeightPct)}
                          </span>
                        </td>
                        <td>
                          <Badge
                            tone={ACTION_TONE[row.bAction] || 'flat'}
                            title={mover(
                              fundName(answer.b),
                              row.bAction,
                              row.bDeltaShares,
                              row.bDeltaWeightPct,
                            )}
                          >
                            {row.bAction || '—'}
                          </Badge>{' '}
                          <span className={tone(row.bDeltaShares)}>
                            {signedCount(row.bDeltaShares)}
                          </span>{' '}
                          <span className={tone(row.bDeltaWeightPct)}>
                            {signedPoints(row.bDeltaWeightPct)}
                          </span>
                        </td>
                        <td>{percent(row.aWeightPct)}</td>
                        <td>{percent(row.bWeightPct)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <p className="note">
                  A row is contrarian when the two funds moved the same position in opposite
                  directions — one opening or adding while the other trimmed or left — which is what
                  a disagreement between two books looks like when the only thing a 13F reports is
                  where it ended the quarter. The two tables answer different questions, so a company
                  can be in both: held by both funds, with one of them adding while the other trims.
                  A 13F reports long positions only, so there is no short book anywhere on this page
                  — a TRIMMED or EXITED side is that fund&apos;s own sale of its own shares, never a
                  short. A position a side exited has no row in that quarter&apos;s book, so its
                  shares, value and weight read zero there while the move stays in the record; a side
                  that held the position but filed no share count or value for it shows a dash
                  instead, because the filing stated nothing to size. Rows are ranked by how much of
                  a book each side moved.
                </p>
              </>
            )}
          </section>
        ) : null}
      </main>
    </div>
  );
}
