import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App.jsx';
import Funds from './Funds.jsx';
import ThirteenF from './ThirteenF.jsx';
import './styles.css';

// No router: the server serves index.html for any extension-less path, so the
// page is picked from the URL once and the nav links reload the document.
const path = window.location.pathname;
const page = path.startsWith('/funds')
  ? 'funds'
  : path.startsWith('/13f')
    ? '13f'
    : path.startsWith('/stocks')
      ? 'stocks'
      : 'explorer';

createRoot(document.getElementById('root')).render(
  <StrictMode>
    {page === 'funds' ? <Funds /> : page === '13f' ? <ThirteenF /> : <App page={page} />}
  </StrictMode>,
);
