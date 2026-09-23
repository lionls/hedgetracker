import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App.jsx';
import Consensus from './Consensus.jsx';
import Flow from './Flow.jsx';
import Funds from './Funds.jsx';
import Leaderboard from './Leaderboard.jsx';
import Overlap from './Overlap.jsx';
import ThirteenF from './ThirteenF.jsx';
import './styles.css';

// No router: the server serves index.html for any extension-less path, so the
// page is picked from the URL once and the nav links reload the document. The
// order below is the only thing that matters here — each path is matched by
// prefix, and the fallback is the explorer.
const path = window.location.pathname;
const page = path.startsWith('/funds')
  ? 'funds'
  : path.startsWith('/flow')
    ? 'flow'
    : path.startsWith('/13f')
      ? '13f'
      : path.startsWith('/consensus')
        ? 'consensus'
        : path.startsWith('/leaderboard')
          ? 'leaderboard'
          : path.startsWith('/overlap')
            ? 'overlap'
            : path.startsWith('/stocks')
              ? 'stocks'
              : 'explorer';

createRoot(document.getElementById('root')).render(
  <StrictMode>
    {page === 'funds' ? (
      <Funds />
    ) : page === 'flow' ? (
      <Flow />
    ) : page === '13f' ? (
      <ThirteenF />
    ) : page === 'consensus' ? (
      <Consensus />
    ) : page === 'leaderboard' ? (
      <Leaderboard />
    ) : page === 'overlap' ? (
      <Overlap />
    ) : (
      <App page={page} />
    )}
  </StrictMode>,
);
