import { useEffect, useMemo, useState } from 'react';
import Chart from './Chart.jsx';
import Fundamentals from './Fundamentals.jsx';
import { companyFor, historyFor, searchSymbols, sliceHistory, stocksList } from './api.js';

const RANGES = ['1y', '5y', 'max'];

// BROWSE_STEP is how much of the stocks list grows per click: rendering the
// whole ten-thousand-row list costs more than the chart does.
const BROWSE_STEP = 100;

export default function App({ page = 'explorer' }) {
  const stocksOnly = page === 'stocks';
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [stocks, setStocks] = useState(null);
  const [sector, setSector] = useState('');
  const [visible, setVisible] = useState(BROWSE_STEP);
  const [symbol, setSymbol] = useState('AAPL');
  const [range, setRange] = useState('5y');
  const [history, setHistory] = useState(null);
  const [company, setCompany] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!stocksOnly) return undefined;
    let active = true;
    stocksList().then(
      (list) => active && setStocks(list),
      () => active && setStocks([]),
    );
    return () => {
      active = false;
    };
  }, [stocksOnly]);

  useEffect(() => {
    // Browsing stocks: the list is already in memory, so nothing is asked of
    // the search endpoint until there is something to search for.
    if (stocksOnly && !query) {
      setResults([]);
      return undefined;
    }
    const controller = new AbortController();
    const timer = setTimeout(
      () => {
        searchSymbols(query, controller.signal, stocksOnly).then(setResults, () => {});
      },
      query ? 150 : 0,
    );
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [query, stocksOnly]);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError('');
    companyFor(symbol).then(
      (profile) => active && setCompany(profile),
      () => active && setCompany(null),
    );
    historyFor(symbol).then(
      (payload) => {
        if (!active) return;
        setHistory(payload);
        setLoading(false);
      },
      (failure) => {
        if (!active) return;
        setHistory(null);
        setError(failure.message);
        setLoading(false);
      },
    );
    return () => {
      active = false;
    };
  }, [symbol]);

  const view = useMemo(() => sliceHistory(history, range), [history, range]);

  // Browsing is a plain filter over a list that is already in memory; ranking
  // stays on the server, where the empty-query case never reaches.
  const sectors = useMemo(() => {
    if (!stocks) return [];
    const names = new Set();
    for (const stock of stocks) {
      if (stock.sector) names.add(stock.sector);
    }
    return [...names].sort();
  }, [stocks]);

  const browsed = useMemo(() => {
    if (!stocks) return [];
    return sector ? stocks.filter((stock) => stock.sector === sector) : stocks;
  }, [stocks, sector]);

  useEffect(() => setVisible(BROWSE_STEP), [sector, stocksOnly]);

  const listed = stocksOnly && !query ? browsed.slice(0, visible) : results;

  const stats = useMemo(() => {
    const { candles } = view;
    if (candles.length === 0) return null;
    const last = candles[candles.length - 1];
    const previous = candles.length > 1 ? candles[candles.length - 2] : last;
    const first = candles[0];
    return {
      last,
      change: last.close - previous.close,
      changePercent: previous.close ? ((last.close - previous.close) / previous.close) * 100 : 0,
      rangePercent: first.close ? ((last.close - first.close) / first.close) * 100 : 0,
    };
  }, [view]);

  const rising = stats ? stats.change >= 0 : true;

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          hedgetracker
          <span>{stocksOnly ? 'stocks' : 'market explorer'}</span>
        </div>
        <nav className="pages">
          <a className={stocksOnly ? 'page' : 'page active'} href="/">
            Explorer
          </a>
          <a className={stocksOnly ? 'page active' : 'page'} href="/stocks">
            Stocks
          </a>
        </nav>
        <input
          className="search"
          type="search"
          value={query}
          autoFocus
          placeholder="Ticker or company"
          spellCheck={false}
          onChange={(event) => setQuery(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter' && listed[0]) setSymbol(listed[0].symbol);
          }}
        />
        {stocksOnly && !query ? (
          <select className="sector" value={sector} onChange={(event) => setSector(event.target.value)}>
            <option value="">All sectors</option>
            {sectors.map((sectorName) => (
              <option key={sectorName} value={sectorName}>
                {sectorName}
              </option>
            ))}
          </select>
        ) : null}
        {stocksOnly ? (
          <p className="count">
            {query ? `${results.length} matches` : `${browsed.length.toLocaleString('en-US')} stocks`}
          </p>
        ) : null}
        <ul className="results">
          {listed.map((result) => (
            <li key={result.symbol}>
              <button
                type="button"
                className={result.symbol === symbol ? 'result active' : 'result'}
                onClick={() => setSymbol(result.symbol)}
              >
                <span className="ticker">{result.symbol}</span>
                <span className="name">{result.name}</span>
                {result.sector ? <span className="sector">{result.sector}</span> : null}
              </button>
            </li>
          ))}
        </ul>
        {stocksOnly && !query && browsed.length > visible ? (
          <button type="button" className="more" onClick={() => setVisible(visible + BROWSE_STEP)}>
            Show {Math.min(BROWSE_STEP, browsed.length - visible).toLocaleString('en-US')} more
          </button>
        ) : null}
      </aside>

      <main className="main">
        <header className="headline">
          <div className="identity">
            <h1>
              {symbol}
              {company?.name ? <span className="company-name">{company.name}</span> : null}
            </h1>
            <div className="meta">
              {[company?.sector, company?.industry].filter(Boolean).join(' · ') || '—'}
            </div>
          </div>
          <div className={rising ? 'quote up' : 'quote down'}>
            {stats ? (
              <>
                <strong>{stats.last.close.toFixed(2)}</strong>
                <span>
                  {rising ? '+' : ''}
                  {stats.change.toFixed(2)} ({rising ? '+' : ''}
                  {stats.changePercent.toFixed(2)}%)
                </span>
                <small>{stats.last.time}</small>
              </>
            ) : (
              <span className="muted">{loading ? 'loading…' : 'no price history'}</span>
            )}
          </div>
          <div className="ranges">
            {RANGES.map((option) => (
              <button
                key={option}
                type="button"
                className={option === range ? 'range active' : 'range'}
                onClick={() => setRange(option)}
              >
                {option.toUpperCase()}
              </button>
            ))}
          </div>
        </header>

        {error ? <p className="error">{error}</p> : null}

        <section className="panel">
          <Chart candles={view.candles} volume={view.volume} />
          {loading ? <div className="overlay">loading {symbol}…</div> : null}
        </section>

        {stocksOnly ? <Fundamentals symbol={symbol} /> : null}

        <section className="company">
          <dl>
            <div>
              <dt>Sector</dt>
              <dd>{company?.sector || '—'}</dd>
            </div>
            <div>
              <dt>Industry</dt>
              <dd>{company?.industry || '—'}</dd>
            </div>
            <div>
              <dt>Employees</dt>
              <dd>{company?.employees ? company.employees.toLocaleString('en-US') : '—'}</dd>
            </div>
            <div>
              <dt>Headquarters</dt>
              <dd>{[company?.city, company?.country].filter(Boolean).join(', ') || '—'}</dd>
            </div>
            <div>
              <dt>Website</dt>
              <dd>
                {company?.website ? (
                  <a href={company.website} target="_blank" rel="noreferrer">
                    {company.website.replace(/^https?:\/\//, '')}
                  </a>
                ) : (
                  '—'
                )}
              </dd>
            </div>
            <div>
              <dt>{range.toUpperCase()} change</dt>
              <dd className={stats && stats.rangePercent >= 0 ? 'up' : 'down'}>
                {stats ? `${stats.rangePercent >= 0 ? '+' : ''}${stats.rangePercent.toFixed(2)}%` : '—'}
              </dd>
            </div>
          </dl>
          {company?.summary ? (
            <details className="summary">
              <summary>Business summary</summary>
              <p>{company.summary}</p>
            </details>
          ) : null}
        </section>
      </main>
    </div>
  );
}
