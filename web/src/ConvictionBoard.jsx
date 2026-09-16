import { useEffect, useState } from 'react';
import SignalTable from './SignalTable.jsx';
import { thirteenSignals } from './api.js';
import { period as quarter } from './numbers.js';

// The six classes conviction_scores emits. The endpoint validates the same set
// and rejects anything else, so this list is the UI's copy of the view's CASE.
const CLASSES = [
  'HIGH_CONVICTION_BUY',
  'STANDARD_BUY',
  'CONVICTION_DUMP',
  'PASSIVE_REBALANCE',
  'MAINTAINED',
  'ROUTINE_ADJUSTMENT',
];

function label(name) {
  return name.replaceAll('_', ' ').toLowerCase();
}

// The market board: one signal class across every fund in the lake, largest
// estimated flow first. It is the question the fund drill-down cannot answer —
// who is buying this hard, whoever they are. With no quarter chosen the
// endpoint picks the lake's newest, and says which one it used.
export default function ConvictionBoard({ quarters, version, onTicker }) {
  const [signal, setSignal] = useState('HIGH_CONVICTION_BUY');
  const [reportPeriod, setReportPeriod] = useState('');
  const [payload, setPayload] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    setError('');
    thirteenSignals({ signal, period: reportPeriod, limit: 100 }).then(
      (result) => active && setPayload(result),
      (failure) => active && setError(failure.message),
    );
    return () => {
      active = false;
    };
  }, [signal, reportPeriod, version]);

  const rows = payload?.signals || [];

  return (
    <section className="section">
      <header>
        <h2>Conviction board</h2>
        <span className="muted">
          {payload ? `${rows.length} of ${payload.total} rows · ${quarter(payload.period)}` : 'reading…'}
        </span>
        <div className="filters">
          <select
            className="control"
            value={signal}
            onChange={(event) => setSignal(event.target.value)}
          >
            {CLASSES.map((name) => (
              <option key={name} value={name}>
                {label(name)}
              </option>
            ))}
          </select>
          <select
            className="control"
            value={reportPeriod}
            onChange={(event) => setReportPeriod(event.target.value)}
          >
            <option value="">newest quarter</option>
            {quarters.map((day) => (
              <option key={day} value={day}>
                {quarter(day)}
              </option>
            ))}
          </select>
        </div>
      </header>
      {error ? <p className="error">{error}</p> : null}
      {payload && rows.length === 0 && !error ? (
        <p className="muted">
          no {label(signal)} rows in {quarter(payload.period)}
        </p>
      ) : null}
      {rows.length > 0 ? <SignalTable rows={rows} onTicker={onTicker} /> : null}
    </section>
  );
}
