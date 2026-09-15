import { useEffect, useMemo, useState } from 'react';
import { fundamentalsFor } from './api.js';

// Statements are quoted in whole units of the listing currency and the
// magnitudes are what matter, so $47.94B is more readable than 47941000000.
const money = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 });
const plain = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 });

function formatValue(value, unit) {
  if (value === null || value === undefined) return '—';
  if (unit === 'perShare') return plain.format(value);
  if (unit === 'percent') return `${plain.format(value)}%`;
  return `$${money.format(value)}`;
}

// ratios adds the two figures the statements imply but do not state: how much
// of revenue reaches net income, and how the top line moved on the year before.
function ratios(rows, periods) {
  const net = rows.find((row) => row.key === 'net_income');
  const revenue = rows.find((row) => row.key === 'total_revenue');
  if (!net || !revenue) return [];
  const margin = new Array(periods).fill(null);
  const growth = new Array(periods).fill(null);
  for (let i = 0; i < periods; i += 1) {
    const income = net.values[i];
    const sales = revenue.values[i];
    const earlier = revenue.values[i + 1];
    if (income !== null && sales) margin[i] = (income / sales) * 100;
    if (sales !== null && earlier) growth[i] = ((sales - earlier) / Math.abs(earlier)) * 100;
  }
  return [
    { key: 'net_margin', label: 'Net margin', unit: 'percent', values: margin },
    { key: 'revenue_growth', label: 'Revenue growth', unit: 'percent', values: growth },
  ];
}

export default function Fundamentals({ symbol }) {
  // undefined while loading, null when the dataset has no statements for the
  // symbol, which is what every fund looks like.
  const [fundamentals, setFundamentals] = useState(undefined);

  useEffect(() => {
    let active = true;
    setFundamentals(undefined);
    fundamentalsFor(symbol).then(
      (payload) => active && setFundamentals(payload),
      () => active && setFundamentals(null),
    );
    return () => {
      active = false;
    };
  }, [symbol]);

  const groups = useMemo(() => {
    if (!fundamentals) return null;
    const periods = fundamentals.periods.length;
    return fundamentals.groups.map((group) =>
      group.name === 'Income statement'
        ? { ...group, rows: [...group.rows, ...ratios(group.rows, periods)] }
        : group,
    );
  }, [fundamentals]);

  if (fundamentals === undefined) {
    return (
      <section className="fundamentals">
        <h2>Fundamentals</h2>
        <p className="muted">loading statements…</p>
      </section>
    );
  }
  if (fundamentals === null) {
    return (
      <section className="fundamentals">
        <h2>Fundamentals</h2>
        <p className="muted">The dataset holds no financial statements for {symbol}.</p>
      </section>
    );
  }

  const shares = fundamentals.shares || 0;
  const trailing = fundamentals.trailingEps || 0;
  // The close arrives with the statements, so market cap and P/E do not wait on
  // the chart's bars and cannot show the previous symbol's price.
  const close = fundamentals.close || 0;
  const figures = [
    ['Market cap', shares && close ? `$${money.format(shares * close)}` : '—'],
    ['Shares outstanding', shares ? money.format(shares) : '—'],
    ['Trailing EPS', trailing ? plain.format(trailing) : '—'],
    ['P/E', trailing > 0 && close ? plain.format(close / trailing) : '—'],
  ];

  return (
    <section className="fundamentals">
      <header>
        <h2>Fundamentals</h2>
        <dl className="figures">
          {figures.map(([label, value]) => (
            <div key={label}>
              <dt>{label}</dt>
              <dd>{value}</dd>
            </div>
          ))}
        </dl>
      </header>
      {groups.map((group) => (
        <table className="statements" key={group.name}>
          <caption>{group.name}</caption>
          <thead>
            <tr>
              <th scope="col">Metric</th>
              {fundamentals.periods.map((period) => (
                <th key={period} scope="col" title={`Fiscal year ending ${period}`}>
                  {period.slice(0, 4)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {group.rows.map((row) => (
              <tr key={row.key}>
                <th scope="row">{row.label}</th>
                {row.values.map((value, index) => (
                  <td key={fundamentals.periods[index]}>{formatValue(value, row.unit)}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      ))}
      <p className="muted note">
        Annual figures as filed with the SEC, newest first. Market cap and P/E use the latest close and the
        latest trailing EPS.
      </p>
    </section>
  );
}
