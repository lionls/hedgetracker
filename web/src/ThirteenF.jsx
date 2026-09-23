import { useEffect, useState } from 'react';
import ConvictionBoard from './ConvictionBoard.jsx';
import FundMovements from './FundMovements.jsx';
import FundPicker from './FundPicker.jsx';
import { thirteenHoldings, thirteenVWAP } from './api.js';
import { count, dollars, period, percent } from './numbers.js';

// HOLDINGS_STEP is how much of a fund's portfolio the table grows per click: a
// 794-position filing is one request but not one screen.
const HOLDINGS_STEP = 25;

// The 13F dashboard. Three drill-downs share one page: a fund's quarter, a
// ticker's price, and the whole lake's convictions. They are all reads of the
// materialised tables, so a rebuild is the only thing that changes an answer,
// and that is what the version counter is for — the sidebar that owns the status
// behind a rebuild is FundPicker, which hands the version back here.
export default function ThirteenF() {
  const [status, setStatus] = useState(null);
  // The selection is the fund itself rather than its CIK: the picker searches on
  // the server, so the selected fund can drop out of the list it was picked from,
  // and the name the heading shows has to survive that.
  const [fund, setFund] = useState(null);
  const [wanted, setWanted] = useState('');
  const [holdings, setHoldings] = useState(null);
  const [quarters, setQuarters] = useState([]);
  const [holdingsError, setHoldingsError] = useState('');
  const [vwap, setVwap] = useState(null);
  const [visible, setVisible] = useState(HOLDINGS_STEP);
  const [chosen, setChosen] = useState(null);
  const [version, setVersion] = useState(0);

  const cik = fund?.cik || '';

  // Holdings own the quarter: an empty period asks for the filer's newest, and
  // the response says which one that was. The headline, the quarter selector and
  // the two panels below read that answer rather than each deciding for
  // themselves — the selector lists every quarter the filer filed, but the one it
  // shows selected is the one being displayed.
  useEffect(() => {
    if (!cik) return undefined;
    let active = true;
    setHoldings(null);
    setHoldingsError('');
    thirteenHoldings(cik, wanted).then(
      (payload) => {
        if (!active) return;
        setHoldings(payload);
        setQuarters(payload.quarters);
        setVisible(HOLDINGS_STEP);
      },
      (error) => active && setHoldingsError(error.message),
    );
    return () => {
      active = false;
    };
  }, [cik, wanted, version]);

  const rows = holdings?.holdings || [];
  // The price panel follows the last ticker clicked, and opens on the fund's
  // largest holding so the panel has something to say before anything is
  // clicked.
  const drill = chosen ?? rows[0]?.ticker ?? '';

  useEffect(() => {
    if (!drill) return undefined;
    let active = true;
    setVwap(null);
    thirteenVWAP(drill).then(
      (payload) => active && setVwap(payload.quarters),
      () => active && setVwap([]),
    );
    return () => {
      active = false;
    };
  }, [drill, version]);

  function pickFund(entry) {
    setFund(entry);
    setWanted('');
    setChosen(null);
    setQuarters([]);
  }

  function pickQuarter(day) {
    setWanted(day);
    setChosen(null);
  }

  const shown = rows.slice(0, visible);

  return (
    <div className="app">
      <FundPicker
        page="13f"
        tagline="13f dashboard"
        fund={fund}
        onFund={pickFund}
        onStatus={setStatus}
        version={version}
        onVersion={() => setVersion((value) => value + 1)}
      />

      <main className="main">
        <header className="headline">
          <div className="identity">
            <h1>
              {fund?.filerName || cik || '—'}
              <span className="company-name">
                {holdings
                  ? `${fund?.filerName ? `CIK ${cik} · ` : ''}${dollars(
                      holdings.portfolioValueUsd,
                    )} portfolio`
                  : cik
                    ? `CIK ${cik}`
                    : ''}
              </span>
            </h1>
            {holdings ? (
              <div className="meta">
                {count(holdings.total)} positions · {period(holdings.period)}
              </div>
            ) : (
              <div className="meta">reading the filings…</div>
            )}
          </div>
          {quarters.length > 0 ? (
            <select
              className="control"
              value={wanted || holdings?.period || ''}
              title={`${quarters.length} quarters filed`}
              onChange={(event) => pickQuarter(event.target.value)}
            >
              {quarters.map((day) => (
                <option key={day} value={day}>
                  {period(day)}
                </option>
              ))}
            </select>
          ) : null}
        </header>

        {holdingsError ? <p className="error">{holdingsError}</p> : null}

        {holdings ? (
          <FundMovements
            cik={cik}
            reportPeriod={holdings.period}
            version={version}
            onTicker={setChosen}
          />
        ) : null}

        {holdings ? (
          <section className="section">
            <header>
              <h2>Holdings</h2>
              <span className="muted">
                {count(shown.length)} of {count(holdings.total)} positions, worth{' '}
                {dollars(holdings.portfolioValueUsd)} · {period(holdings.period)}
              </span>
            </header>
            <table className="statements">
              <thead>
                <tr>
                  <th scope="col">Ticker</th>
                  <th scope="col">Issuer</th>
                  <th scope="col">Shares</th>
                  <th scope="col">Value</th>
                  <th scope="col">Weight</th>
                </tr>
              </thead>
              <tbody>
                {shown.map((row) => (
                  <tr key={row.cusip}>
                    <th scope="row">
                      {row.ticker ? (
                        <button
                          type="button"
                          className="drill"
                          onClick={() => setChosen(row.ticker)}
                          title={`${row.cusip} · price context below`}
                        >
                          {row.ticker}
                        </button>
                      ) : (
                        row.cusip
                      )}
                    </th>
                    <td className="issuer">
                      {row.issuer}
                      {row.classTitle ? <span className="muted"> · {row.classTitle}</span> : null}
                    </td>
                    <td
                      title={
                        row.reportedLines > 1
                          ? `${row.reportedLines} line items in the filing, summed into one position`
                          : undefined
                      }
                    >
                      {count(row.shares)}
                    </td>
                    <td>{dollars(row.valueUsd)}</td>
                    <td>{percent(row.weightPct)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {rows.length > visible ? (
              <button
                type="button"
                className="more"
                onClick={() => setVisible(visible + HOLDINGS_STEP)}
              >
                Show {Math.min(HOLDINGS_STEP, rows.length - visible)} more
              </button>
            ) : null}
          </section>
        ) : null}

        {drill ? (
          <section className="section">
            <header>
              <h2>Price context</h2>
              <span className="muted">
                {drill} · volume-weighted mean close, quarter by quarter
              </span>
            </header>
            {vwap && vwap.length === 0 ? (
              <p className="muted">
                the price dataset has no daily bars for {drill}, so its estimated flows are null too
              </p>
            ) : null}
            {vwap && vwap.length > 0 ? (
              <table className="statements">
                <thead>
                  <tr>
                    <th scope="col">Quarter</th>
                    <th scope="col">Trading days</th>
                    <th scope="col">VWAP</th>
                    <th scope="col">Low</th>
                    <th scope="col">High</th>
                  </tr>
                </thead>
                <tbody>
                  {vwap.map((row) => (
                    <tr key={`${row.year}Q${row.quarter}`}>
                      <th scope="row">
                        {period(row.lastTradeDate) || `${row.year} Q${row.quarter}`}
                      </th>
                      <td title={`${row.firstTradeDate || '—'} to ${row.lastTradeDate || '—'}`}>
                        {count(row.tradingDays)}
                      </td>
                      <td>{dollars(row.vwap)}</td>
                      <td>{dollars(row.low)}</td>
                      <td>{dollars(row.high)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : null}
            <p className="note">
              A quarter&apos;s VWAP is the mean daily close weighted by volume, which is what the
              price dataset carries — it does not know the intraday range. Est. flow above is the
              split-adjusted share delta times this number.
            </p>
          </section>
        ) : null}

        <ConvictionBoard quarters={status?.quarters || []} version={version} onTicker={setChosen} />
      </main>
    </div>
  );
}
