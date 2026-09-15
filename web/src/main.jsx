import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App.jsx';
import './styles.css';

// No router: the server serves index.html for any extension-less path, so the
// page is picked from the URL once and the nav links reload the document.
const page = window.location.pathname.startsWith('/stocks') ? 'stocks' : 'explorer';

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App page={page} />
  </StrictMode>,
);
