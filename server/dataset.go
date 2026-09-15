package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// datasetBase is the published location of the yfinance mirror. Table paths
// below it are not stable: the tables used to sit in `data/` and now sit in
// `data/US/`, so the directory is probed once at startup instead of hard-coded.
const datasetBase = "https://huggingface.co/datasets/defeatbeta/yahoo-finance-data/resolve/main"

// tablePrefixes are probed in order; the first that answers is used.
var tablePrefixes = []string{"data/US/", "data/"}

// Entry is one row of the ticker index backing the search box.
type Entry struct {
	Symbol string `json:"symbol"`
	Name   string `json:"name"`
	Sector string `json:"sector,omitempty"`
	Priced bool   `json:"priced"`
}

// Profile is the company information shown next to the chart.
type Profile struct {
	Symbol    string `json:"symbol"`
	Name      string `json:"name"`
	Sector    string `json:"sector,omitempty"`
	Industry  string `json:"industry,omitempty"`
	Employees int64  `json:"employees,omitempty"`
	Website   string `json:"website,omitempty"`
	City      string `json:"city,omitempty"`
	Country   string `json:"country,omitempty"`
	Summary   string `json:"summary,omitempty"`
}

// Bars is a chart series in column form: one array per field instead of one
// object per bar keeps the JSON (and its gzip) roughly three times smaller.
type Bars struct {
	Symbol string    `json:"symbol"`
	Range  string    `json:"range"`
	Dates  []string  `json:"dates"`
	Open   []float64 `json:"open"`
	High   []float64 `json:"high"`
	Low    []float64 `json:"low"`
	Close  []float64 `json:"close"`
	Volume []int64   `json:"volume"`
}

// Dataset holds the boot-time ticker index and the per-request caches. The
// Parquet files stay on Hugging Face: every cold read is an HTTPS range read,
// which costs a couple of seconds, so decoded results are cached in memory.
type Dataset struct {
	db     *sql.DB
	client *http.Client
	prefix string

	mu       sync.RWMutex
	names    map[string]string
	profiles map[string]Profile
	priced   map[string]bool
	index    []Entry

	barsMu    sync.Mutex
	bars      map[string]*Bars
	barsOrder []string
}

const barsCacheSize = 256

// NewDataset prepares the client used for the plain-HTTP parts of the dataset
// (the ticker file is small JSON, DuckDB would only add overhead).
func NewDataset(db *sql.DB, proxy *url.URL) *Dataset {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if proxy != nil {
		transport.Proxy = http.ProxyURL(proxy)
	}
	return &Dataset{
		db:       db,
		client:   &http.Client{Timeout: 120 * time.Second, Transport: transport},
		names:    map[string]string{},
		profiles: map[string]Profile{},
		priced:   map[string]bool{},
		bars:     map[string]*Bars{},
	}
}

// Prefix is the dataset subdirectory holding the tables, e.g. `data/US/`.
func (d *Dataset) Prefix() string { return d.prefix }

// file is the full URL of one dataset file.
func (d *Dataset) file(name string) string { return datasetBase + "/" + d.prefix + name }

// table is the URL of one Parquet table, quoted for use inside a query.
func (d *Dataset) table(name string) string { return "'" + d.file(name+".parquet") + "'" }

// Warm resolves the table directory and loads everything the search box and the
// company panel need, so a first request never waits for them.
func (d *Dataset) Warm(ctx context.Context) error {
	prefix, err := d.probePrefix(ctx)
	if err != nil {
		return err
	}
	d.prefix = prefix
	log.Printf("dataset: tables under %s", prefix)

	names, err := d.fetchNames(ctx, d.file("company_tickers.json"))
	if err != nil {
		return err
	}
	profiles, err := d.fetchProfiles(ctx)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.names, d.profiles = names, profiles
	d.rebuildIndexLocked()
	d.mu.Unlock()
	log.Printf("dataset: %d names, %d profiles, index %d symbols", len(names), len(profiles), len(d.index))
	return nil
}

// WarmPriced fills in which symbols actually have price history. Knowing that
// takes a scan of the whole price table (~37 s), so it runs after the server is
// already answering, and the index stays usable without it.
func (d *Dataset) WarmPriced(ctx context.Context) {
	start := time.Now()
	rows, err := d.db.QueryContext(ctx, fmt.Sprintf("SELECT DISTINCT symbol FROM %s", d.table("stock_prices")))
	if err != nil {
		log.Printf("dataset: priced symbols unavailable: %v", err)
		return
	}
	defer rows.Close()
	priced := make(map[string]bool, 1<<14)
	for rows.Next() {
		var symbol string
		if err := rows.Scan(&symbol); err != nil {
			log.Printf("dataset: priced symbols unavailable: %v", err)
			return
		}
		priced[symbol] = true
	}
	if err := rows.Err(); err != nil {
		log.Printf("dataset: priced symbols unavailable: %v", err)
		return
	}
	d.mu.Lock()
	d.priced = priced
	d.rebuildIndexLocked()
	d.mu.Unlock()
	log.Printf("dataset: %d priced symbols in %s", len(priced), time.Since(start).Round(time.Millisecond))
}

// probePrefix finds the directory the tables live in with a cheap probe of one
// Parquet file per candidate, so a moved table directory only costs one request.
func (d *Dataset) probePrefix(ctx context.Context) (string, error) {
	var last error
	for _, prefix := range tablePrefixes {
		target := datasetBase + "/" + prefix + "stock_prices.parquet"
		ok, err := d.reachable(ctx, target)
		if err == nil && ok {
			return prefix, nil
		}
		if err != nil {
			last = err
		} else {
			last = fmt.Errorf("%s: not found", target)
		}
	}
	return "", fmt.Errorf("no dataset directory responded: %w", last)
}

// reachable asks for a single byte of a remote file, which is enough to tell a
// redirect to the object apart from a 404.
func (d *Dataset) reachable(ctx context.Context, target string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := d.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return true, nil
	}
	return false, nil
}

// fetchNames reads the SEC-format ticker file: a JSON object keyed by index,
// each value carrying the ticker and the registrant's title.
func (d *Dataset) fetchNames(ctx context.Context, target string) (map[string]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", target, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: HTTP %d", target, resp.StatusCode)
	}
	var raw map[string]struct {
		Ticker string `json:"ticker"`
		Title  string `json:"title"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode ticker file: %w", err)
	}
	names := make(map[string]string, len(raw))
	for _, item := range raw {
		if item.Ticker == "" || item.Title == "" {
			continue
		}
		names[strings.ToUpper(item.Ticker)] = strings.TrimSpace(item.Title)
	}
	return names, nil
}

// fetchProfiles loads the dataset's own company table (2.5 MB, ~11k rows) and
// keeps it in memory; company lookups then never touch the network.
func (d *Dataset) fetchProfiles(ctx context.Context) (map[string]Profile, error) {
	query := fmt.Sprintf(`SELECT symbol, sector, industry, web_site, full_time_employees,
		long_business_summary, city, country FROM %s`, d.table("stock_profile"))
	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query stock_profile: %w", err)
	}
	defer rows.Close()

	profiles := make(map[string]Profile, 1<<14)
	for rows.Next() {
		var symbol string
		var sector, industry, website, summary, city, country sql.NullString
		var employees sql.NullInt64
		if err := rows.Scan(&symbol, &sector, &industry, &website, &employees, &summary, &city, &country); err != nil {
			return nil, fmt.Errorf("scan stock_profile: %w", err)
		}
		profiles[strings.ToUpper(symbol)] = Profile{
			Symbol:    strings.ToUpper(symbol),
			Sector:    sector.String,
			Industry:  industry.String,
			Employees: employees.Int64,
			Website:   website.String,
			City:      city.String,
			Country:   country.String,
			Summary:   summary.String,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read stock_profile: %w", err)
	}
	return profiles, nil
}

// rebuildIndexLocked merges both symbol sources into the searchable index. The
// dataset's profile table misses symbols that have prices (funds and the like)
// while the SEC file misses symbols the dataset never profiled, so the union
// covers more than either alone. Callers hold d.mu.
func (d *Dataset) rebuildIndexLocked() {
	index := make([]Entry, 0, len(d.profiles)+len(d.names))
	seen := make(map[string]bool, cap(index))
	for symbol, profile := range d.profiles {
		index = append(index, Entry{
			Symbol: symbol,
			Name:   nameOr(d.names[symbol], symbol),
			Sector: profile.Sector,
			Priced: d.priced[symbol],
		})
		seen[symbol] = true
	}
	for symbol, name := range d.names {
		if seen[symbol] {
			continue
		}
		index = append(index, Entry{Symbol: symbol, Name: name, Priced: d.priced[symbol]})
	}
	sort.Slice(index, func(i, j int) bool { return index[i].Symbol < index[j].Symbol })
	d.index = index
}

func nameOr(name, fallback string) string {
	if name == "" {
		return fallback
	}
	return name
}

// Search ranks symbol matches above name matches, because a search box for a
// chart is almost always fed a ticker.
func (d *Dataset) Search(query string, limit int) []Entry {
	d.mu.RLock()
	index := d.index
	d.mu.RUnlock()

	upper := strings.ToUpper(strings.TrimSpace(query))
	matches := make([]scored, 0, 64)
	for _, entry := range index {
		rank, ok := rankEntry(entry, upper)
		if !ok {
			continue
		}
		matches = append(matches, scored{entry: entry, rank: rank})
	}
	sort.Slice(matches, func(i, j int) bool {
		if upper == "" {
			// Browsing rather than searching: a predictable alphabetical list.
			if matches[i].entry.Priced != matches[j].entry.Priced {
				return matches[i].entry.Priced
			}
			return matches[i].entry.Symbol < matches[j].entry.Symbol
		}
		return lessRanked(matches[i], matches[j])
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	results := make([]Entry, len(matches))
	for i, match := range matches {
		results[i] = match.entry
	}
	return results
}

type scored struct {
	entry Entry
	rank  int
}

// rankEntry scores a candidate against an upper-cased query. Symbol matches
// outrank name matches, and a match at the start of a name word outranks one in
// the middle of a word, which is what separates "Apple Inc." from "Pineapple".
func rankEntry(entry Entry, upper string) (int, bool) {
	switch {
	case entry.Symbol == upper:
		return 0, true
	case strings.HasPrefix(entry.Symbol, upper):
		return 1, true
	case strings.Contains(entry.Symbol, upper):
		return 2, true
	}
	name := strings.ToUpper(entry.Name)
	if index := strings.Index(name, upper); index >= 0 {
		if index == 0 || name[index-1] == ' ' {
			return 2, true
		}
		return 3, true
	}
	return 0, false
}

// lessRanked orders matches by rank, then by the signals that stand in for how
// well known a company is: symbols with price history first, then the shorter
// — usually the more prominent — company name.
func lessRanked(a, b scored) bool {
	if a.rank != b.rank {
		return a.rank < b.rank
	}
	if a.entry.Priced != b.entry.Priced {
		return a.entry.Priced
	}
	if len(a.entry.Name) != len(b.entry.Name) {
		return len(a.entry.Name) < len(b.entry.Name)
	}
	if len(a.entry.Symbol) != len(b.entry.Symbol) {
		return len(a.entry.Symbol) < len(b.entry.Symbol)
	}
	return a.entry.Symbol < b.entry.Symbol
}

// Company returns the stored profile, with the SEC name filled in when the
// dataset has no profile for the symbol.
func (d *Dataset) Company(symbol string) (Profile, bool) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	d.mu.RLock()
	defer d.mu.RUnlock()
	profile, ok := d.profiles[symbol]
	name, named := d.names[symbol]
	if !ok {
		if !named {
			return Profile{}, false
		}
		profile = Profile{Symbol: symbol}
	}
	profile.Name = nameOr(name, symbol)
	return profile, true
}

// Bars returns the daily candles for one symbol. The first call for a symbol
// pays for the HTTPS range reads; later calls are served from memory.
func (d *Dataset) Bars(ctx context.Context, symbol, rng string) (*Bars, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	from, ok := rangeStart(rng)
	if !ok {
		return nil, fmt.Errorf("unknown range %q", rng)
	}
	key := symbol + "|" + rng
	if bars, ok := d.cachedBars(key); ok {
		return bars, nil
	}

	query := fmt.Sprintf(`SELECT report_date, CAST(open AS DOUBLE), CAST(high AS DOUBLE),
		CAST(low AS DOUBLE), CAST(close AS DOUBLE), CAST(volume AS BIGINT)
		FROM %s WHERE symbol = ? AND report_date >= ? ORDER BY report_date`, d.table("stock_prices"))
	rows, err := d.db.QueryContext(ctx, query, symbol, from)
	if err != nil {
		return nil, fmt.Errorf("query stock_prices: %w", err)
	}
	defer rows.Close()

	bars := &Bars{
		Symbol: symbol,
		Range:  rng,
		Dates:  []string{},
		Open:   []float64{},
		High:   []float64{},
		Low:    []float64{},
		Close:  []float64{},
		Volume: []int64{},
	}
	for rows.Next() {
		var date string
		var open, high, low, close float64
		var volume sql.NullInt64
		if err := rows.Scan(&date, &open, &high, &low, &close, &volume); err != nil {
			return nil, fmt.Errorf("scan stock_prices: %w", err)
		}
		bars.Dates = append(bars.Dates, date)
		bars.Open = append(bars.Open, open)
		bars.High = append(bars.High, high)
		bars.Low = append(bars.Low, low)
		bars.Close = append(bars.Close, close)
		bars.Volume = append(bars.Volume, volume.Int64)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read stock_prices: %w", err)
	}
	d.storeBars(key, bars)
	return bars, nil
}

// rangeStart maps a UI range to the earliest report date to read.
func rangeStart(rng string) (string, bool) {
	now := time.Now().UTC()
	switch rng {
	case "1y":
		return now.AddDate(-1, 0, 0).Format("2006-01-02"), true
	case "5y":
		return now.AddDate(-5, 0, 0).Format("2006-01-02"), true
	case "max":
		return "1900-01-01", true
	}
	return "", false
}

func (d *Dataset) cachedBars(key string) (*Bars, bool) {
	d.barsMu.Lock()
	defer d.barsMu.Unlock()
	bars, ok := d.bars[key]
	return bars, ok
}

func (d *Dataset) storeBars(key string, bars *Bars) {
	d.barsMu.Lock()
	defer d.barsMu.Unlock()
	if _, exists := d.bars[key]; !exists {
		d.barsOrder = append(d.barsOrder, key)
	}
	d.bars[key] = bars
	for len(d.barsOrder) > barsCacheSize {
		delete(d.bars, d.barsOrder[0])
		d.barsOrder = d.barsOrder[1:]
	}
}
