import { useEffect, useRef, useState } from 'react';
import Pages from './Pages.jsx';
import { thirteenFunds, thirteenRefresh, thirteenStatus } from './api.js';
import { dollars, period, plural, stamp } from './numbers.js';

// The sidebar the two fund pages share: the fund list and its search, the lake's
// health line, and the rebuild button. It owns the status it follows — read
// once, then polled while a build runs — and hands every reading up through
// onStatus, because the panels below the headline read the same build, while the
// version counter it bumps when a build lands is what makes those panels reread
// the tables that just changed under them.
export default function FundPicker({
  page,
  tagline,
  fund,
  onFund,
  onStatus,
  version,
  onVersion,
  children,
}) {
  const [status, setStatus] = useState(null);
  const [funds, setFunds] = useState([]);
  const [query, setQuery] = useState('');
  const [failure, setFailure] = useState('');
  const [busy, setBusy] = useState(false);
  const wasBuilding = useRef(false);
  const seeded = useRef(false);
  const cik = fund?.cik || '';
  const building = status?.state === 'building';

  function read(payload) {
    setStatus(payload);
    onStatus(payload);
  }

  useEffect(() => {
    let active = true;
    thirteenStatus().then(
      (payload) => active && read(payload),
      () => {},
    );
    return () => {
      active = false;
    };
  }, [version]);

  useEffect(() => {
    if (!building) return undefined;
    const timer = setTimeout(() => thirteenStatus().then(read, () => {}), 2000);
    return () => clearTimeout(timer);
  }, [building, status]);

  useEffect(() => {
    if (wasBuilding.current && !building) onVersion();
    wasBuilding.current = building;
  }, [building]);

  // The picker follows the search box: the server matches a name or a CIK, so a
  // fund can be found without the page holding the whole list. The delay is what
  // keeps a typed name from being one request per keystroke.
  useEffect(() => {
    let active = true;
    const timer = setTimeout(
      () => {
        thirteenFunds(query).then(
          (payload) => {
            if (!active) return;
            setFunds(payload.funds);
            // The first fund the lake lists is the opening view, and it is
            // offered once: a search is how the next fund is found, not a reset,
            // and a selection that drops out of a filtered list stays selected.
            if (!seeded.current) {
              seeded.current = true;
              onFund(payload.funds[0] || null);
            }
          },
          (error) => active && setFailure(error.message),
        );
      },
      query ? 150 : 0,
    );
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [version, query]);

  async function refresh() {
    setBusy(true);
    setFailure('');
    try {
      read(await thirteenRefresh());
    } catch (error) {
      setFailure(error.message);
    } finally {
      setBusy(false);
    }
  }

  const statusError = status?.error || status?.lastError || '';

  return (
    <aside className="sidebar">
      <div className="brand">
        hedgetracker
        <span>{tagline}</span>
      </div>
      <Pages page={page} />
      <input
        className="search"
        type="search"
        value={query}
        placeholder="Fund name or CIK"
        spellCheck={false}
        onChange={(event) => setQuery(event.target.value)}
        onKeyDown={(event) => {
          // Enter opens the largest fund the search matched, the fund the list
          // puts first.
          if (event.key === 'Enter' && funds[0]) onFund(funds[0]);
        }}
      />
      {query && funds.length === 0 ? <p className="note">no fund matches “{query}”</p> : null}
      <ul className="results funds">
        {funds.map((entry) => (
          <li key={entry.cik}>
            <button
              type="button"
              className={entry.cik === cik ? 'result active' : 'result'}
              onClick={() => onFund(entry)}
              title={`CIK ${entry.cik} · ${entry.quarters} filings, ${period(
                entry.firstPeriod,
              )} to ${period(entry.latestPeriod)}`}
            >
              <span className="ticker">{entry.filerName || entry.cik}</span>
              <span className="name">{dollars(entry.latestValueUsd)}</span>
              <span className="sector">
                {plural(entry.latestPositions, 'position')} · {period(entry.latestPeriod)}
              </span>
            </button>
          </li>
        ))}
      </ul>
      <p className="count">
        {status
          ? `${status.state}${status.builtAt ? ` · ${stamp(status.builtAt)}` : ''}`
          : 'reading the lake…'}
      </p>
      <button type="button" className="more" onClick={refresh} disabled={busy}>
        {busy
          ? 'rebuilding…'
          : `rebuild tables${status?.buildSeconds ? ` (${status.buildSeconds.toFixed(1)} s)` : ''}`}
      </button>
      {status ? (
        <p className="note">
          {plural(status.filings, 'filing')} · {plural(status.funds, 'fund')} ·{' '}
          {plural(status.positions, 'position')} · {plural(status.signals, 'signal')}
        </p>
      ) : null}
      {statusError ? <p className="note down">{statusError}</p> : null}
      {failure ? <p className="note down">{failure}</p> : null}
      {children}
    </aside>
  );
}
