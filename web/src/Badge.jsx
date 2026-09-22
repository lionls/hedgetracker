// A badge is what keeps a label from reading as another numeric column in the
// tables that are otherwise all figures: the 13F page's moves and this page's
// positions both mark the five actions, so the tones live here rather than in
// either table. A trim and a dump are the same direction of travel and share a
// colour, and a held position is bookkeeping, not a signal.
export const ACTION_TONE = { NEW: 'up', ADDED: 'up', TRIMMED: 'down', EXITED: 'down', HELD: 'flat' };

export function Badge({ tone, title, children }) {
  return (
    <span className={`badge ${tone}`} title={title}>
      {children}
    </span>
  );
}
