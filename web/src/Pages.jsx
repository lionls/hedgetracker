// The nav every page carries. There is no router: the server answers any
// extension-less path with index.html, so a link is a document load and each
// page reads the path it was served on.
const PAGES = [
  { page: 'explorer', path: '/', label: 'Explorer' },
  { page: 'stocks', path: '/stocks', label: 'Stocks' },
  { page: '13f', path: '/13f', label: '13F' },
];

export default function Pages({ page }) {
  return (
    <nav className="pages">
      {PAGES.map((entry) => (
        <a
          key={entry.path}
          className={entry.page === page ? 'page active' : 'page'}
          href={entry.path}
        >
          {entry.label}
        </a>
      ))}
    </nav>
  );
}
