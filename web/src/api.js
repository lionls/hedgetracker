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
