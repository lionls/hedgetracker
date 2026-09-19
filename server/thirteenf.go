package main

// The 13F side of the explorer: it reads the holdings lake the hedgetracker
// extractor writes, turns it into the four views in thirteenf.sql, and serves
// them as JSON for the dashboard. See thirteenf_api.go for the endpoints.
//
// Two sources, wired differently on purpose:
//
//   - Holdings are local Parquet, {lake}/13f_holdings/year=YYYY/quarter=Q/{CIK}.parquet.
//     They are the extractor's output and are read from disk, not over HTTP.
//   - Prices are the same defeatbeta dataset the rest of the server reads, and
//     they are optional. The conviction signals come from the flows alone, so a
//     deployment with no price source still gets NEW/ADDED/TRIMMED/EXITED and
//     every signal class; only the estimated capital flow and the quarterly
//     price bands come out null.
//
// The views are materialised to Parquet once instead of being queried live. One
// conviction query joins the whole lake to a full scan of the price table and
// takes tens of seconds, which no dashboard request can wait for. The build runs
// in the background at startup, /api/13f/status reports it, and the data
// endpoints answer 503 until the tables are there.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

//go:embed thirteenf.sql
var thirteenfViews string

// thirteenViewsHash identifies the SQL a build used, so a redeploy that changes
// the views rebuilds the tables instead of serving columns that no longer exist.
func thirteenViewsHash() string {
	sum := sha256.Sum256([]byte(thirteenfViews))
	return hex.EncodeToString(sum[:8])
}

// Names of the materialised tables and of the manifest that points at them.
const (
	// positions_base is the fixed-width half of the positions stage: the lake's
	// reporting lines collapsed to one row per filer, period and CUSIP, with
	// the issuer, class and ticker names joined in by the positions view. Both
	// halves are built one lake partition at a time.
	thirteenPositionsBase = "positions_base"
	thirteenPositions     = "positions"
	thirteenHoldings      = "holdings_normalized"
	thirteenVWAP          = "market_quarterly_vwap"
	thirteenFlows         = "fund_quarterly_flows"
	thirteenConviction    = "conviction_scores"
	thirteenManifest      = "manifest.json"
	thirteenTablesDir     = "tables"
)

// thirteenTables is what a build must produce, in dependency order. positions
// is not served to the dashboard: it is the collapse of the lake's reporting
// lines to one row per filer, period and CUSIP plus the security's names, and
// materialising it first is what keeps every later stage off the raw lake.
var thirteenTables = []string{
	thirteenPositionsBase, thirteenPositions,
	thirteenHoldings, thirteenVWAP, thirteenFlows, thirteenConviction,
}

// thirteenChunked are the tables that must not read the whole lake in one
// statement. The DuckDB the driver bundles has no out-of-core hash
// aggregation, so a grouped aggregate dies as soon as its state outgrows the
// memory limit, and both of these group by filer, period and CUSIP, or join
// those rows to a per-CUSIP name collapse. That state grows with the lake:
// measured on a 484k-position lake the collapse fails at 61MiB of a 64MB limit
// even with fixed-width payloads, and on a 2.8M-position lake with real CUSIP
// cardinality the name collapse alone exhausts a 256MB limit.
//
// No such group spans a period and the lake is partitioned by period, so
// building these tables one lake partition at a time bounds their state at one
// quarter instead of the whole lake. The stages above them read them back from
// Parquet and group by fund and period, which is a bounded number of groups.
var thirteenChunked = map[string]bool{
	thirteenPositionsBase: true,
	thirteenPositions:     true,
}

// thirteenActions are the position changes the flows view reports.
var thirteenActions = []string{"NEW", "ADDED", "TRIMMED", "EXITED", "HELD"}

// thirteenSignals are the conviction classes: the flow actions refined by weight
// thresholds, as specified with the views.
var thirteenSignals = []string{
	"HIGH_CONVICTION_BUY", "STANDARD_BUY", "PASSIVE_REBALANCE",
	"CONVICTION_DUMP", "MAINTAINED", "ROUTINE_ADJUSTMENT",
}

// manifest describes the materialised build the server is serving.
type manifest struct {
	BuiltAt     time.Time `json:"builtAt"`
	BuildSecs   float64   `json:"buildSeconds"`
	Dir         string    `json:"dir"`
	Views       string    `json:"views"` // hash of the SQL that built these tables
	LakeDir     string    `json:"lakeDir"`
	LakeFiles   int       `json:"lakeFiles"`
	PriceSource string    `json:"priceSource,omitempty"`
	SplitSource string    `json:"splitSource,omitempty"`
	Filings     int64     `json:"filings"`
	Funds       int64     `json:"funds"`
	Positions   int64     `json:"positions"`
	Flows       int64     `json:"flows"`
	Signals     int64     `json:"signals"`
	Quarters    []string  `json:"quarters"`
}

// thirteenState is the payload of /api/13f/status.
type thirteenState struct {
	State        string   `json:"state"`
	LakeDir      string   `json:"lakeDir,omitempty"`
	CacheDir     string   `json:"cacheDir,omitempty"`
	BuiltAt      string   `json:"builtAt,omitempty"`
	BuildSeconds float64  `json:"buildSeconds,omitempty"`
	LakeFiles    int      `json:"lakeFiles,omitempty"`
	Filings      int64    `json:"filings,omitempty"`
	Funds        int64    `json:"funds,omitempty"`
	Positions    int64    `json:"positions,omitempty"`
	Flows        int64    `json:"flows,omitempty"`
	Signals      int64    `json:"signals,omitempty"`
	Quarters     []string `json:"quarters,omitempty"`
	PriceSource  string   `json:"priceSource,omitempty"`
	SplitSource  string   `json:"splitSource,omitempty"`
	Error        string   `json:"error,omitempty"`
	LastError    string   `json:"lastError,omitempty"`
}

// ThirteenF owns the lake configuration, the materialised tables, and the state
// of the build that produces them.
type ThirteenF struct {
	db       *sql.DB
	lakeDir  string
	cacheDir string
	prices   string // quoted read_parquet argument, empty when there are no prices
	splits   string
	maxAge   time.Duration

	mu       sync.RWMutex
	current  *manifest
	building bool
	failure  string
	lakeMis  string // why the lake cannot be read at all, if it cannot
	lakeFile int
}

// NewThirteenF prepares the 13F side. The lake and the cache are inspected now;
// nothing is built until Warm or Rebuild runs. A lake that is missing or
// unusable is not fatal: the explorer keeps serving prices and company data and
// reports the problem through /api/13f/status.
func NewThirteenF(db *sql.DB, lakeDir, cacheDir, prices, splits string, maxAge time.Duration) *ThirteenF {
	t := &ThirteenF{db: db, lakeDir: lakeDir, cacheDir: cacheDir, prices: prices, splits: splits, maxAge: maxAge}

	info, err := os.Stat(lakeDir)
	switch {
	case err != nil:
		t.lakeMis = fmt.Sprintf("no holdings lake at %s: mount it read-only or pass -13f-lake", lakeDir)
	case !info.IsDir():
		t.lakeMis = fmt.Sprintf("%s is not a directory", lakeDir)
	default:
		files, _ := filepath.Glob(filepath.Join(lakeDir, "*", "*", "*.parquet"))
		t.lakeFile = len(files)
		if len(files) == 0 {
			t.lakeMis = fmt.Sprintf("no holdings files under %s: expected {lake}/13f_holdings/year=YYYY/quarter=Q/{CIK}.parquet", lakeDir)
		}
	}

	if stored, err := t.loadManifest(); err == nil {
		t.current = stored
	}
	return t
}

// loadManifest reads the last successful build, if there is one.
func (t *ThirteenF) loadManifest() (*manifest, error) {
	payload, err := os.ReadFile(filepath.Join(t.cacheDir, thirteenManifest))
	if err != nil {
		return nil, err
	}
	var stored manifest
	if err := json.Unmarshal(payload, &stored); err != nil {
		return nil, err
	}
	if stored.Dir == "" {
		return nil, errors.New("manifest without a build directory")
	}
	return &stored, nil
}

// Warm builds the tables in the background when the cache is missing or older
// than the configured age, the way WarmPriced fills the price index: startup
// must not wait for a scan of the price table.
func (t *ThirteenF) Warm(ctx context.Context) {
	if t.lakeMis != "" {
		log.Printf("13f: not building: %s", t.lakeMis)
		return
	}
	if t.fresh() {
		t.mu.RLock()
		stored := t.current
		t.mu.RUnlock()
		log.Printf("13f: reusing tables built at %s", stored.BuiltAt.Format(time.RFC3339))
		return
	}
	go t.Build(ctx)
}

// fresh reports whether the cached build can be reused as it is.
func (t *ThirteenF) fresh() bool {
	t.mu.RLock()
	stored := t.current
	t.mu.RUnlock()
	if stored == nil || stored.LakeDir != t.lakeDir {
		return false
	}
	// Tables built by other SQL are not this server's tables: a redeploy that
	// changes thirteenf.sql must rebuild, whatever the age says.
	if stored.Views != thirteenViewsHash() {
		return false
	}
	// A maximum age of zero means "reuse the tables until the lake changes".
	if t.maxAge > 0 && time.Since(stored.BuiltAt) > t.maxAge {
		return false
	}
	for _, table := range thirteenTables {
		path := t.path(stored, table)
		if thirteenChunked[table] {
			// A chunked table is a directory of one file per lake partition.
			path = filepath.Join(t.cacheDir, stored.Dir, table)
		}
		if _, err := os.Stat(path); err != nil {
			return false
		}
	}
	return true
}

// Rebuild forces a build, for the refresh endpoint. It reports whether this
// call started one; a build already running makes it a no-op.
func (t *ThirteenF) Rebuild(ctx context.Context) bool {
	if t.lakeMis != "" {
		return false
	}
	t.mu.Lock()
	if t.building {
		t.mu.Unlock()
		return false
	}
	t.building = true
	t.mu.Unlock()
	go t.Build(ctx)
	return true
}

// Build materialises the four views. It is safe to call concurrently: only one
// build runs, and a failure leaves the previous tables in place.
func (t *ThirteenF) Build(ctx context.Context) {
	if t.lakeMis != "" {
		return
	}

	t.mu.Lock()
	if t.building {
		t.mu.Unlock()
		return
	}
	t.building = true
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		t.building = false
		t.mu.Unlock()
	}()

	start := time.Now()
	stored, err := t.materialise(ctx)
	if err != nil {
		t.mu.Lock()
		t.failure = err.Error()
		t.mu.Unlock()
		log.Printf("13f: build failed after %s: %v", time.Since(start).Round(time.Millisecond), err)
		return
	}

	t.mu.Lock()
	t.current, t.failure = stored, ""
	t.mu.Unlock()
	log.Printf("13f: %d funds, %d positions, %d filings, %d flows in %s",
		stored.Funds, stored.Positions, stored.Filings, stored.Flows, time.Since(start).Round(time.Millisecond))
}

// materialise runs the views and writes them to a fresh build directory. The
// directory only becomes visible with the manifest, so a half-written build is
// never served and an interrupted one leaves the previous tables untouched.
func (t *ThirteenF) materialise(ctx context.Context) (*manifest, error) {
	start := time.Now()
	dir := filepath.Join(thirteenTablesDir, time.Now().UTC().Format("20060102T150405"))
	target := filepath.Join(t.cacheDir, dir)
	if err := os.MkdirAll(target, 0o755); err != nil {
		return nil, fmt.Errorf("create build dir: %w", err)
	}

	// A view belongs to a connection, so the whole build runs on one reserved
	// connection instead of through the pool.
	conn, err := t.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("reserve duckdb connection: %w", err)
	}
	defer conn.Close()

	if err := t.createRawViews(ctx, conn); err != nil {
		return nil, err
	}

	for _, statement := range splitStatements(thirteenfViews) {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return nil, fmt.Errorf("create view %s: %w", viewName(statement), err)
		}
	}

	for _, table := range thirteenTables {
		path := filepath.Join(target, table+".parquet")
		if thirteenChunked[table] {
			// One file per lake partition instead of one for the whole lake.
			path = filepath.Join(target, table, "*.parquet")
			if err := t.materialisePartitions(ctx, conn, target, table); err != nil {
				return nil, err
			}
		} else {
			query := fmt.Sprintf("COPY (SELECT * FROM %s) TO %s (FORMAT PARQUET)", table, sqlString(path))
			if _, err := conn.ExecContext(ctx, query); err != nil {
				return nil, fmt.Errorf("materialise %s: %w", table, err)
			}
		}

		// The views below read this table, so point it at what was just
		// written instead of leaving them to re-derive it. Re-deriving means
		// re-scanning every lake partition and re-running the aggregate once
		// per dependent view, which is what made a real lake exceed its memory
		// limit; the Parquet read streams instead.
		point := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet(%s)",
			table, sqlString(filepath.ToSlash(path)))
		if _, err := conn.ExecContext(ctx, point); err != nil {
			return nil, fmt.Errorf("read back %s: %w", table, err)
		}
	}

	stored := &manifest{
		BuiltAt:     time.Now().UTC(),
		BuildSecs:   time.Since(start).Seconds(),
		Dir:         dir,
		Views:       thirteenViewsHash(),
		LakeDir:     t.lakeDir,
		LakeFiles:   t.lakeFile,
		PriceSource: t.prices,
		SplitSource: t.splits,
	}
	if err := t.count(ctx, conn, stored); err != nil {
		return nil, err
	}

	// The manifest is the switch: written last, renamed into place, and the only
	// thing that makes a build visible.
	payload, err := json.MarshalIndent(stored, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode manifest: %w", err)
	}
	path := filepath.Join(t.cacheDir, thirteenManifest)
	if err := os.WriteFile(path+".tmp", append(payload, '\n'), 0o644); err != nil {
		return nil, fmt.Errorf("write manifest: %w", err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return nil, fmt.Errorf("publish manifest: %w", err)
	}

	t.prune(dir)
	return stored, nil
}

// materialisePartitions writes one Parquet file per lake partition. A chunked
// table collapses filer, period and CUSIP rows or joins them to the security
// names, and no such row spans a period, so running the statement once per
// partition bounds its state by that partition's rows. What it reads is pointed
// at that partition for the duration of its COPY: raw_holdings is the lake
// itself, and every chunked table built before it is its own file for that
// partition. The names are therefore collapsed per partition, so a CUSIP whose
// filers spell it differently can be named from a different spelling in each
// period.
func (t *ThirteenF) materialisePartitions(ctx context.Context, conn *sql.Conn, target, table string) error {
	partitions, err := filepath.Glob(filepath.Join(t.lakeDir, "*", "*"))
	if err != nil {
		return fmt.Errorf("list lake partitions: %w", err)
	}
	if len(partitions) == 0 {
		return fmt.Errorf("no lake partitions under %s", t.lakeDir)
	}
	dir := filepath.Join(target, table)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}

	namer := strings.NewReplacer("year=", "", "quarter=", "", string(os.PathSeparator), "-")
	for _, partition := range partitions {
		rel, err := filepath.Rel(t.lakeDir, partition)
		if err != nil {
			return fmt.Errorf("name lake partition %s: %w", partition, err)
		}
		name := namer.Replace(rel)

		type source struct{ view, path string }
		sources := []source{{"raw_holdings", filepath.Join(partition, "*.parquet")}}
		for _, built := range thirteenTables {
			if built == table {
				break
			}
			if thirteenChunked[built] {
				sources = append(sources, source{built, filepath.Join(target, built, name+".parquet")})
			}
		}
		for _, read := range sources {
			view := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet(%s)",
				read.view, sqlString(filepath.ToSlash(read.path)))
			if _, err := conn.ExecContext(ctx, view); err != nil {
				return fmt.Errorf("read %s for partition %s: %w", read.view, rel, err)
			}
		}

		out := filepath.Join(dir, name+".parquet")
		query := fmt.Sprintf("COPY (SELECT * FROM %s) TO %s (FORMAT PARQUET)", table, sqlString(out))
		if _, err := conn.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("materialise %s for %s: %w", table, rel, err)
		}
	}

	// Every later stage reads the whole lake, so put the sources back.
	for _, built := range thirteenTables {
		if !thirteenChunked[built] {
			continue
		}
		builtDir := filepath.Join(target, built)
		if _, err := os.Stat(builtDir); err != nil {
			continue
		}
		view := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet(%s)",
			built, sqlString(filepath.ToSlash(filepath.Join(builtDir, "*.parquet"))))
		if _, err := conn.ExecContext(ctx, view); err != nil {
			return fmt.Errorf("read back %s: %w", built, err)
		}
	}
	return t.createRawViews(ctx, conn)
}

// count fills in what the build produced, for the manifest and the status
// endpoint.
func (t *ThirteenF) count(ctx context.Context, conn *sql.Conn, stored *manifest) error {
	query := fmt.Sprintf(`SELECT (SELECT count(*) FROM (SELECT DISTINCT cik, report_period FROM %[1]s)),
		(SELECT count(DISTINCT cik) FROM %[1]s), (SELECT count(*) FROM %[1]s),
		(SELECT count(*) FROM %[2]s), (SELECT count(*) FROM %[3]s)`,
		thirteenHoldings, thirteenFlows, thirteenConviction)
	if err := conn.QueryRowContext(ctx, query).Scan(
		&stored.Filings, &stored.Funds, &stored.Positions, &stored.Flows, &stored.Signals); err != nil {
		return fmt.Errorf("count materialised rows: %w", err)
	}

	rows, err := conn.QueryContext(ctx, "SELECT DISTINCT report_period FROM "+thirteenHoldings+" ORDER BY report_period")
	if err != nil {
		return fmt.Errorf("list report periods: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var period time.Time
		if err := rows.Scan(&period); err != nil {
			return fmt.Errorf("scan report period: %w", err)
		}
		stored.Quarters = append(stored.Quarters, period.Format("2006-01-02"))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read report periods: %w", err)
	}
	return nil
}

// createRawViews builds the three views the SQL file expects. Their sources are
// configuration, so they cannot live in the SQL file itself.
func (t *ThirteenF) createRawViews(ctx context.Context, conn *sql.Conn) error {
	lake := sqlString(filepath.ToSlash(filepath.Join(t.lakeDir, "**", "*.parquet")))

	// The lake carries the ticker column derived from the CUSIP map; without it
	// every position would be labelled with its CUSIP.
	schema := fmt.Sprintf("SELECT count(*) FROM (DESCRIBE SELECT * FROM read_parquet(%s)) WHERE column_name = 'ticker'", lake)
	var hasTicker int
	if err := conn.QueryRowContext(ctx, schema).Scan(&hasTicker); err != nil {
		return fmt.Errorf("read lake schema: %w", err)
	}
	if hasTicker == 0 {
		return fmt.Errorf("the lake at %s has no ticker column: run `hedgetracker conform --base-dir %s` to add it offline",
			t.lakeDir, filepath.Dir(t.lakeDir))
	}

	views := []struct{ name, query string }{
		{"raw_holdings", "SELECT * FROM read_parquet(" + lake + ")"},
		{"raw_prices", t.priceSource()},
		{"raw_splits", t.splitSource()},
	}
	for _, view := range views {
		statement := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", view.name, view.query)
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create view %s: %w", view.name, err)
		}
	}
	return nil
}

// priceSource reads the daily bars, restricted to the symbols and the quarters
// the lake covers: the price table is far larger than any 13F analysis needs.
// Without a price source the view is empty but keeps its columns, so the SQL
// file runs unchanged.
func (t *ThirteenF) priceSource() string {
	if t.prices == "" {
		return "SELECT NULL::VARCHAR AS symbol, NULL::VARCHAR AS report_date, NULL::DOUBLE AS close," +
			" NULL::DOUBLE AS volume, NULL::DOUBLE AS high, NULL::DOUBLE AS low WHERE false"
	}
	return fmt.Sprintf(`SELECT * FROM read_parquet(%s)
		WHERE symbol IN (SELECT DISTINCT ticker FROM raw_holdings WHERE ticker IS NOT NULL AND ticker <> '')
		  AND CAST(report_date AS DATE) BETWEEN (SELECT min(CAST(report_period AS DATE)) FROM raw_holdings)
		                                    AND (SELECT max(CAST(report_period AS DATE)) FROM raw_holdings)`, t.prices)
}

// splitSource reads the split events that put share counts of different report
// periods on the same basis.
func (t *ThirteenF) splitSource() string {
	if t.splits == "" {
		return "SELECT NULL::VARCHAR AS symbol, NULL::VARCHAR AS report_date, NULL::VARCHAR AS split_factor WHERE false"
	}
	return fmt.Sprintf(`SELECT * FROM read_parquet(%s)
		WHERE symbol IN (SELECT DISTINCT ticker FROM raw_holdings WHERE ticker IS NOT NULL AND ticker <> '')`, t.splits)
}

// prune deletes the build directories that are no longer referenced. Failures
// are logged: a stale directory costs disk, not correctness.
func (t *ThirteenF) prune(keep string) {
	entries, err := os.ReadDir(filepath.Join(t.cacheDir, thirteenTablesDir))
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == filepath.Base(keep) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(t.cacheDir, thirteenTablesDir, entry.Name())); err != nil {
			log.Printf("13f: could not remove old build %s: %v", entry.Name(), err)
			continue
		}
		log.Printf("13f: removed old build %s", entry.Name())
	}
}

// path is where one materialised table of a build lives.
func (t *ThirteenF) path(stored *manifest, table string) string {
	return filepath.Join(t.cacheDir, stored.Dir, table+".parquet")
}

// Status reports the state of the 13F side for /api/13f/status and the health
// endpoint.
func (t *ThirteenF) Status() thirteenState {
	t.mu.RLock()
	stored, building, failure, lakeMis := t.current, t.building, t.failure, t.lakeMis
	t.mu.RUnlock()

	state := thirteenState{State: "unbuilt", LakeDir: t.lakeDir, CacheDir: t.cacheDir, LakeFiles: t.lakeFile}
	switch {
	case stored != nil && stored.Views == thirteenViewsHash():
		state.State = "ready"
		state.BuiltAt = stored.BuiltAt.Format(time.RFC3339)
		state.BuildSeconds = stored.BuildSecs
		state.LakeFiles = stored.LakeFiles
		state.Filings, state.Funds, state.Positions = stored.Filings, stored.Funds, stored.Positions
		state.Flows, state.Signals, state.Quarters = stored.Flows, stored.Signals, stored.Quarters
		state.PriceSource, state.SplitSource = stored.PriceSource, stored.SplitSource
	case stored != nil:
		// On disk, but built by another version of the views: not ours to serve.
		// If the lake is also unusable, that is the reason a rebuild cannot fix it.
		state.State, state.BuiltAt = "stale", stored.BuiltAt.Format(time.RFC3339)
		if lakeMis != "" {
			state.Error = lakeMis
		}
	case lakeMis != "":
		state.State, state.Error = "unconfigured", lakeMis
	case failure != "":
		state.State, state.Error = "failed", failure
	}
	if failure != "" {
		state.LastError = failure
	}
	if building {
		state.State = "building"
	}
	return state
}

// tablePath resolves a materialised table for a request, or writes the 503 that
// tells the dashboard whether the build is running, failed, or unconfigured.
func (t *ThirteenF) tablePath(w http.ResponseWriter, name string) (string, bool) {
	t.mu.RLock()
	stored := t.current
	t.mu.RUnlock()
	// Tables built by another version of the views are not served: their columns
	// are not necessarily the columns this binary scans. A build in flight does
	// not stop the current tables from serving; a different view hash does.
	if stored != nil && stored.Views == thirteenViewsHash() {
		return t.path(stored, name), true
	}

	state := t.Status()
	reason := state.Error
	if reason == "" {
		switch state.State {
		case "building":
			reason = "the 13F tables are being built"
		case "stale":
			reason = "the cached 13F tables were built by another version of the views"
		default:
			reason = "the 13F tables have not been built"
		}
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": reason, "status": state})
	return "", false
}

// sqlString quotes a path or URL for use as a SQL string literal.
func sqlString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// splitStatements cuts the view definitions into single statements, dropping
// comment lines so a semicolon inside prose cannot split one.
func splitStatements(script string) []string {
	kept := make([]string, 0, 8)
	for _, line := range strings.Split(script, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		kept = append(kept, line)
	}
	statements := make([]string, 0, 8)
	for _, statement := range strings.Split(strings.Join(kept, "\n"), ";") {
		if trimmed := strings.TrimSpace(statement); trimmed != "" {
			statements = append(statements, trimmed)
		}
	}
	return statements
}

// viewName names a statement for an error message.
func viewName(statement string) string {
	fields := strings.Fields(statement)
	for i, field := range fields {
		if strings.EqualFold(field, "VIEW") && i+1 < len(fields) {
			return fields[i+1]
		}
	}
	return "unknown"
}
