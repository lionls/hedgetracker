// Shared number formatting. The dashboard writes money, share counts and
// percentages in four tables at once, and they have to agree: $150.18B means
// the same thing in the fund picker and in a flow row. Deltas carry their sign
// in the text because the tables colour the cell rather than spell out the
// direction, and a compact "$36.14M" is what a column of six-figure values
// needs to stay readable.
const money = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 });
const plain = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 });
const precise = new Intl.NumberFormat('en-US', { maximumFractionDigits: 3 });

function signed(value, format) {
  if (value === null || value === undefined) return '—';
  return `${value < 0 ? '−' : '+'}${format.format(Math.abs(value))}`;
}

export function dollars(value) {
  if (value === null || value === undefined) return '—';
  return `$${money.format(value)}`;
}

// compact writes a magnitude that is not money — shares outstanding is the one
// figure on the dashboard that reads better as 14.61B than as 14,608,963,000.
export function compact(value) {
  if (value === null || value === undefined) return '—';
  return money.format(value);
}

export function signedDollars(value) {
  if (value === null || value === undefined) return '—';
  return `${value < 0 ? '−' : '+'}$${money.format(Math.abs(value))}`;
}

// Shares are whole in almost every filing but not in all of them, so this keeps
// two decimals rather than rounding a fractional holding away.
export function count(value) {
  if (value === null || value === undefined) return '—';
  return plain.format(value);
}

export function signedCount(value) {
  return signed(value, plain);
}

// plural writes a count with its noun: a lake that holds one filing otherwise
// says "1 filings".
export function plural(value, noun) {
  return `${count(value)} ${noun}${value === 1 ? '' : 's'}`;
}

export function percent(value) {
  if (value === null || value === undefined) return '—';
  return `${plain.format(value)}%`;
}

// precisePercent is percent for a share of a whole that can sit far below one:
// the tracked funds of the ownership panel against a company whose share count
// runs to the billions is 0.002%, which the two-digit formatter would render as
// the untruth "0%". Above one percent the two agree.
export function precisePercent(value) {
  if (value === null || value === undefined) return '—';
  return `${(value < 1 ? precise : plain).format(value)}%`;
}

export function signedPercent(value) {
  if (value === null || value === undefined) return '—';
  return `${signed(value, plain)}%`;
}

// points is a change in a percentage: a weight that went from 1.2% to 1.9% moved
// 0.7 percentage points, which is not the same thing as 0.7%.
export function signedPoints(value) {
  if (value === null || value === undefined) return '—';
  return `${signed(value, plain)}pp`;
}

const QUARTERS = ['Q1', 'Q2', 'Q3', 'Q4'];

// period turns the report date the API speaks (2024-06-30) into the quarter a
// dashboard selects on (2024 Q2). Report periods are always quarter ends.
export function period(day) {
  if (!day) return '';
  const [year, month] = day.split('-');
  const quarter = QUARTERS[Math.floor((Number(month) - 1) / 3)];
  return `${year} ${quarter}`;
}

// stamp renders the RFC 3339 build time as UTC wall clock, which is what the
// server logs and what the reader compares it against.
export function stamp(value) {
  if (!value) return '';
  return `${value.slice(0, 16).replace('T', ' ')} UTC`;
}
