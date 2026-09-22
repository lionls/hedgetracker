// Thin API layer. Everything it fetches is immutable for the session (daily
// bars and profiles are end-of-day data), so responses are cached as promises:
// a second request for the same symbol reuses the first one, in flight or not.
const API = '/api';
const cache = new Map();

function cached(key, load) {
  let entry = cache.get(key);
  if (!entry) {
    entry = load().catch((error) => {
      cache.delete(key);
      throw error;
    });
    cache.set(key, entry);
  }
  return entry;
}

async function getJSON(path) {
  const response = await fetch(API + path);
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}));
    throw new Error(detail.error || `${path} failed with ${response.status}`);
  }
  return response.json();
}

export function searchSymbols(query, signal, stocksOnly) {
  const filter = stocksOnly ? '&stocks=1' : '';
  return fetch(`${API}/search?q=${encodeURIComponent(query)}&limit=30${filter}`, { signal })
    .then((response) => (response.ok ? response.json() : Promise.reject(new Error('search failed'))))
    .then((payload) => payload.results);
}

// stocksList loads the browse list of the stocks page once: every operating
// company the dataset knows, alphabetical. It is that page's whole universe, so
// browsing, the sector filter and the search all share one request.
export function stocksList() {
  return cached('stocks', () => getJSON('/stocks').then((payload) => payload.stocks));
}

// fundamentalsFor resolves null for symbols the dataset filed no statements
// for — every fund — which is an answer rather than an error.
export function fundamentalsFor(symbol) {
  return cached(`fundamentals:${symbol}`, () =>
    getJSON(`/fundamentals/${encodeURIComponent(symbol)}`).catch(() => null),
  );
}

export function companyFor(symbol) {
  return cached(`company:${symbol}`, () => getJSON(`/company/${encodeURIComponent(symbol)}`));
}

// historyFor loads the whole history once. Reading one symbol's Parquet row
// groups dominates the request, while a longer range from the same row group is
// free, so the ranges are sliced on the client instead of refetched.
export function historyFor(symbol) {
  return cached(`bars:${symbol}`, () => getJSON(`/bars/${encodeURIComponent(symbol)}?range=max`).then(toHistory));
}

function toHistory(payload) {
  const { dates, open, high, low, close, volume } = payload;
  const candles = new Array(dates.length);
  const bars = new Array(dates.length);
  for (let i = 0; i < dates.length; i += 1) {
    const rising = close[i] >= open[i];
    candles[i] = { time: dates[i], open: open[i], high: high[i], low: low[i], close: close[i] };
    bars[i] = {
      time: dates[i],
      value: volume[i],
      color: rising ? 'rgba(38, 166, 154, 0.5)' : 'rgba(239, 83, 80, 0.5)',
    };
  }
  return { symbol: payload.symbol, candles, volume: bars, last: dates.length - 1 };
}

const YEARS = { '1y': 1, '5y': 5 };

// sliceHistory returns the trailing window for a range, reusing the same candle
// objects, so switching ranges allocates nothing but two array heads.
export function sliceHistory(history, range) {
  if (!history) return { candles: [], volume: [] };
  const years = YEARS[range];
  if (!years) return { candles: history.candles, volume: history.volume };
  const limit = new Date();
  limit.setUTCFullYear(limit.getUTCFullYear() - years);
  const from = limit.toISOString().slice(0, 10);

  let low = 0;
  let high = history.candles.length;
  while (low < high) {
    const mid = (low + high) >> 1;
    if (history.candles[mid].time < from) low = mid + 1;
    else high = mid;
  }
  return { candles: history.candles.slice(low), volume: history.volume.slice(low) };
}

export function formatChange(change) {
  const sign = change >= 0 ? '+' : '';
  return `${sign}${change.toFixed(2)}`;
}

// The 13F dashboard. None of it is promise-cached the way bars and profiles
// are: POST /13f/refresh rewrites these tables in place, so the same query has
// a different answer afterwards, and every query is a millisecond read of a
// local Parquet file. A rebuild is also the only thing that makes these
// requests fail (503 while the tables are unbuilt, building or stale), which is
// why every caller keeps its own error state instead of a shared cache.
function query(params) {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    const text = value === null || value === undefined ? '' : String(value).trim();
    if (text) search.set(key, text);
  }
  return search.toString();
}

// thirteenStatus is the lake's health: state, what it last built, how long it
// took, and how much is in it.
export function thirteenStatus() {
  return getJSON('/13f/status');
}

// thirteenFunds lists the funds in the lake, largest first. The search runs
// server-side: a name or a CIK, or nothing for the whole list.
export function thirteenFunds(search = '', limit = 500) {
  return getJSON(`/13f/funds?${query({ q: search, limit })}`);
}

// thirteenHoldings answers the filer's newest quarter when period is empty, and
// the response's period is the one it settled on. An unknown filer or a quarter
// that filer did not file is a 404 with the quarters list in the message.
export function thirteenHoldings(cik, period, limit = 500) {
  return getJSON(`/13f/holdings?${query({ cik, period, limit })}`);
}

// thirteenSignals reads the conviction view: filter by fund, ticker, action,
// signal class or quarter, all optional. Order is fixed server-side, by the
// size of the estimated flow.
export function thirteenSignals(params) {
  return getJSON(`/13f/signals?${query(params)}`);
}

export function thirteenVWAP(ticker) {
  return getJSON(`/13f/vwap?${query({ ticker })}`);
}

// thirteenRefresh answers 202 when it started a build and 409 when one is
// already running. Both bodies are the status, and both are states the
// dashboard renders rather than errors, so neither is thrown: the status it
// returns is what switches the page into its polling mode.
export async function thirteenRefresh() {
  const response = await fetch(`${API}/13f/refresh`, { method: 'POST' });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok && response.status !== 409) {
    throw new Error(payload.error || `the rebuild request failed with ${response.status}`);
  }
  return payload;
}

