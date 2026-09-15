import { useEffect, useMemo, useState } from 'react';
import Chart from './Chart.jsx';
import { companyFor, historyFor, searchSymbols, sliceHistory } from './api.js';

const RANGES = ['1y', '5y', 'max'];

export default function App() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [symbol, setSymbol] = useState('AAPL');
  const [range, setRange] = useState('5y');
  const [history, setHistory] = useState(null);
  const [company, setCompany] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    const controller = new AbortController();
    const timer = setTimeout(
      () => {
        searchSymbols(query, controller.signal).then(setResults, () => {});
      },
      query ? 150 : 0,
    );
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [query]);

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
          <span>market explorer</span>
        </div>
        <input
          className="search"
          type="search"
          value={query}
          autoFocus
          placeholder="Ticker or company"
          spellCheck={false}
          onChange={(event) => setQuery(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter' && results[0]) setSymbol(results[0].symbol);
          }}
        />
        <ul className="results">
          {results.map((result) => (
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
