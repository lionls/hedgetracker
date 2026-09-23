import { useEffect, useRef, useState } from 'react';
import FundPicker from './FundPicker.jsx';
import Sankey from './Sankey.jsx';
import { thirteenFlow } from './api.js';
import { period, plural } from './numbers.js';

// The flow page: one fund's book over two filings, drawn as bands rather than
// listed as rows. It is the same pair of filings the funds page marks — the
// previous one and the selected quarter — so the picture and the P&L table answer
// for one interval, and the quarter selector walks the whole series a transition
// at a time.
export default function Flow() {
  const [status, setStatus] = useState(null);
  const [fund, setFund] = useState(null);
  const [wanted, setWanted] = useState('');
  const [payload, setPayload] = useState(null);
  const [failure, setFailure] = useState('');
  const [version, setVersion] = useState(0);
  const cik = fund?.cik || '';

  // A new fund or quarter is a new picture and the panel blanks rather than redraw
  // the previous flow under the new headline.
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
    thirteenFlow(cik, wanted).then(
      (payload) => active && setPayload(payload),
      (error) => active && setFailure(error.message),
    );
    return () => {
      active = false;
    };
  }, [selection, version]);

  function pickFund(entry) {
    setFund(entry);
    // The next fund opens on its own newest filing, as the funds page does: a
    // quarter the fund being left filed is not a quarter the next one did.
    setWanted('');
  }

  const quarters = payload?.quarters || [];
  const flow = payload;
  return (
    <div className="app">
      <FundPicker
        page="flow"
        tagline="book flow"
        fund={fund}
        onFund={pickFund}
        onStatus={setStatus}
        version={version}
        onVersion={() => setVersion((value) => value + 1)}
      >
        {status && !status.priceSource ? (
          <p className="note down">
            no price dataset is configured, so no band here can be told apart by price: each position
            is drawn from its own action instead
          </p>
        ) : null}
      </FundPicker>

      <main className="main">
        <header className="headline">
          <div className="identity">
            <h1>
              {flow?.filerName || cik || '—'}
              <span className="company-name">
                {flow
                  ? `${flow.filerName ? `CIK ${cik} · ` : ''}${period(flow.prevPeriod)} → ${period(
                      flow.period,
                    )}`
                  : cik
                    ? `CIK ${cik}`
                    : ''}
              </span>
            </h1>
            {flow ? (
              <div className="meta">
                {flow.previous
                  ? `${plural(flow.positions.length, 'position')} named · ${plural(
                      flow.quartersBetween,
                      'quarter',
                    )} between the filings`
                  : `${period(flow.period)} is the first filing in the lake`}
              </div>
            ) : (
              <div className="meta">reading the filings…</div>
            )}
          </div>
          {quarters.length > 0 ? (
            <select
              className="control"
              value={wanted || flow?.period || ''}
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

        {flow ? (
          <section className="section">
            <header>
              <h2>Book flow</h2>
              <span className="muted">
                {flow.previous
                  ? `${period(flow.prevPeriod)} → ${period(flow.period)} · each band is a position on both sides of the quarter`
                  : `no previous filing to flow from`}
              </span>
            </header>
            <Sankey flow={flow} />
          </section>
        ) : null}

        {flow?.previous ? (
          <p className="note">
            Both columns are the books the filings report: the positions of the previous filing on
            the left, sized by what they were worth then, and the positions of {period(flow.period)}{' '}
            on the right. A band is one position on both sides of the quarter — carried, plus
            whatever it was bought or sold for and however the market moved it. The trade is the
            only part a 13F cannot be read for directly, so it is estimated the way the funds
            page estimates P&amp;L: the change in the filing&apos;s share count at the quarter&apos;s
            volume-weighted mean close, which caps what a band can call bought or sold and leaves
            the rest to the market. A position the price dataset does not cover has no such
            estimate, so its band follows its shares: more shares were bought, fewer were sold, and
            a position whose shares did not change is the market&apos;s move. The two nodes the book
            does not hold — added on the left, removed on the right — are what arrived from outside
            and what left, so the two columns are the same height and the picture is not a funnel.
            A filer that skipped quarters has all of the gap in this one filing
            {flow.quartersBetween > 1
              ? `: the previous filing is ${period(flow.prevPeriod)}, ${plural(
                  flow.quartersBetween,
                  'quarter',
                )} back`
              : ''}
            . No fees, dividends or intraday prices are in any of it.
          </p>
        ) : null}

        {flow && !flow.previous ? (
          <p className="note">
            A flow needs two filings, and {period(flow.period)} is the first this fund has in the
            lake
            {flow.quarters.length > 1
              ? `. Any of the later ${plural(flow.quarters.length - 1, 'filing')} of this fund's series can be selected above.`
              : ' — this fund has filed once, so there is no transition to draw yet.'}
          </p>
        ) : null}
      </main>
    </div>
  );
}
