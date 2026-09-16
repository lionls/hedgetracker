import { useEffect, useState } from 'react';
import SignalTable from './SignalTable.jsx';
import { thirteenSignals } from './api.js';
import { count, period } from './numbers.js';

// One fund's quarter: every position it opened, added to, trimmed, exited or
// held. conviction_scores orders those rows by the size of the estimated flow,
// which is the order the question "what did this fund do" wants answered in.
export default function FundMovements({ cik, reportPeriod, version, onTicker }) {
  const [payload, setPayload] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!reportPeriod) return undefined;
    let active = true;
    setPayload(null);
    setError('');
    thirteenSignals({ cik, period: reportPeriod, limit: 500 }).then(
      (result) => active && setPayload(result),
      (failure) => active && setError(failure.message),
    );
    return () => {
      active = false;
    };
  }, [cik, reportPeriod, version]);

  const rows = payload?.signals || [];

  return (
    <section className="section">
      <header>
        <h2>Quarter moves</h2>
        <span className="muted">
          {payload
            ? `${count(payload.total)} changed positions · ${period(payload.period)}`
            : 'reading the flows…'}
        </span>
      </header>
      {error ? <p className="error">{error}</p> : null}
      {payload && rows.length === 0 ? (
        <p className="muted">
          no changes filed for {period(payload.period)} — this is the fund&apos;s first filing in the
          lake, and a flow needs two
        </p>
      ) : null}
      {rows.length > 0 ? <SignalTable rows={rows} fund onTicker={onTicker} /> : null}
    </section>
  );
}
