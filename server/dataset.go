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

// Stock is one row of the stocks page's browse list: a symbol, the name it is
// listed under, and the sector the list is grouped by.
type Stock struct {
	Symbol string `json:"symbol"`
	Name   string `json:"name"`
	Sector string `json:"sector,omitempty"`
}

// MetricRow is one line of the fundamentals panel: the same metric for every
// fiscal period in the payload, in the same order. Values are pointers so a
// hole in the dataset renders as a gap instead of a zero.
type MetricRow struct {
	Key    string     `json:"key"`
	Label  string     `json:"label"`
	Unit   string     `json:"unit,omitempty"`
	Values []*float64 `json:"values"`
}

// MetricGroup bundles rows under the statement they come from, so the panel
// renders in statement order without sorting anything itself.
type MetricGroup struct {
	Name string      `json:"name"`
	Rows []MetricRow `json:"rows"`
}

// Fundamentals is the fundamentals panel for one company: a curated metric set
// per fiscal year, newest first, plus the market figures the header derives
// market cap and P/E from. They travel together because the panel needs all of
// them at once, and none of them should wait on the chart's own bars.
type Fundamentals struct {
	Symbol   string        `json:"symbol"`
	Periods  []string      `json:"periods"`
	Groups   []MetricGroup `json:"groups"`
	Shares   int64         `json:"shares,omitempty"`
	Trailing float64       `json:"trailingEps,omitempty"`
	Close    float64       `json:"close,omitempty"`
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
	stocks   []Stock
	stockSet map[string]bool

	barsMu    sync.Mutex
	bars      map[string]*Bars
	barsOrder []string

	fundMu    sync.Mutex
	fund      map[string]*Fundamentals
	fundOrder []string
}

const (
	barsCacheSize = 256
	fundCacheSize = 256
)

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
		fund:     map[string]*Fundamentals{},
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
	stocks := make([]Stock, 0, len(d.profiles))
	stockSet := make(map[string]bool, len(d.profiles))
	for symbol, profile := range d.profiles {
		name := nameOr(d.names[symbol], symbol)
		index = append(index, Entry{
			Symbol: symbol,
			Name:   name,
			Sector: profile.Sector,
			Priced: d.priced[symbol],
		})
		seen[symbol] = true
		if isStock(symbol, profile.Sector, profile.Industry) {
			stocks = append(stocks, Stock{Symbol: symbol, Name: name, Sector: profile.Sector})
			stockSet[symbol] = true
		}
	}
	for symbol, name := range d.names {
		if seen[symbol] {
			continue
		}
		index = append(index, Entry{Symbol: symbol, Name: name, Priced: d.priced[symbol]})
	}
	sort.Slice(index, func(i, j int) bool { return index[i].Symbol < index[j].Symbol })
	sort.Slice(stocks, func(i, j int) bool { return stocks[i].Symbol < stocks[j].Symbol })
	d.index, d.stocks, d.stockSet = index, stocks, stockSet
}

// isStock reports whether a profile describes an operating company. yfinance
// leaves the company fields empty for funds and trusts, and the shells it does
// describe — SPAC units and their warrants — all sit in "Shell Companies". The
// dash forms left over are share classes (BRK-B), which belong on the page, and
// preferred and when-issued lines, which do not.
func isStock(symbol, sector, industry string) bool {
	if sector == "" || industry == "Shell Companies" {
		return false
	}
	dash := strings.LastIndex(symbol, "-")
	if dash < 0 {
		return true
	}
	if tail := symbol[dash+1:]; len(tail) == 2 && tail[0] == 'P' {
		return false
	}
	switch symbol[dash+1:] {
	case "CL", "RT", "UN", "WI", "WS", "WT":
		return false
	}
	return true
}

func nameOr(name, fallback string) string {
	if name == "" {
		return fallback
	}
	return name
}

// Search ranks symbol matches above name matches, because a search box for a
// chart is almost always fed a ticker. With stocksOnly the candidates are the
// operating companies the stocks page lists.
func (d *Dataset) Search(query string, limit int, stocksOnly bool) []Entry {
	d.mu.RLock()
	index, stockSet := d.index, d.stockSet
	d.mu.RUnlock()

	upper := strings.ToUpper(strings.TrimSpace(query))
	matches := make([]scored, 0, 64)
	for _, entry := range index {
		if stocksOnly && !stockSet[entry.Symbol] {
			continue
		}
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

// Stocks returns the stocks page's browse list, alphabetical. It is rebuilt
// only when the index is, so callers must treat the slice as read-only.
func (d *Dataset) Stocks() []Stock {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.stocks
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

// fundamentalsSpec is the metric set the panel shows, in render order. The keys
// are the dataset's own yfinance item names: the panel only ever sees the
// labels, so a rename upstream breaks one line here and nothing else.
var fundamentalsSpec = []struct {
	group string
	key   string
	label string
	unit  string
}{
	{"Income statement", "total_revenue", "Revenue", "usd"},
	{"Income statement", "gross_profit", "Gross profit", "usd"},
	{"Income statement", "operating_income", "Operating income", "usd"},
	{"Income statement", "net_income", "Net income", "usd"},
	{"Income statement", "diluted_eps", "Diluted EPS", "perShare"},
	{"Balance sheet", "total_assets", "Total assets", "usd"},
	{"Balance sheet", "total_liabilities_net_minority_interest", "Total liabilities", "usd"},
	{"Balance sheet", "stockholders_equity", "Shareholders' equity", "usd"},
	{"Balance sheet", "total_debt", "Total debt", "usd"},
	{"Balance sheet", "cash_and_cash_equivalents", "Cash and equivalents", "usd"},
	{"Cash flow", "operating_cash_flow", "Operating cash flow", "usd"},
	{"Cash flow", "free_cash_flow", "Free cash flow", "usd"},
	{"Cash flow", "capital_expenditure", "Capital expenditure", "usd"},
}

// fundYears is how many fiscal years the panel shows.
const fundYears = 5

// Fundamentals returns the curated metric set per fiscal year, newest first.
// The second result is false for symbols the dataset has no annual statements
// for, which is every fund. The first call for a symbol pays for the HTTPS
// range reads; later calls are served from memory, like Bars.
func (d *Dataset) Fundamentals(ctx context.Context, symbol string) (*Fundamentals, bool, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if cached, ok := d.cachedFundamentals(symbol); ok {
		return cached, true, nil
	}

	items := make([]string, len(fundamentalsSpec))
	for i, metric := range fundamentalsSpec {
		items[i] = metric.key
	}
	query := fmt.Sprintf(`SELECT report_date, item_name, CAST(item_value AS DOUBLE) FROM %s
		WHERE symbol = ? AND period_type = 'annual' AND item_name IN (%s)
		ORDER BY report_date DESC`,
		d.table("stock_statement"), placeholders(len(items)))
	args := make([]any, 0, len(items)+1)
	args = append(args, symbol)
	for _, item := range items {
		args = append(args, item)
	}
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("query stock_statement: %w", err)
	}
	defer rows.Close()

	// periods stays newest first, which is the order the rows arrive in.
	periods := make([]string, 0, fundYears)
	statements := map[string]map[string]float64{}
	for rows.Next() {
		var date, item string
		var value sql.NullFloat64
		if err := rows.Scan(&date, &item, &value); err != nil {
			return nil, false, fmt.Errorf("scan stock_statement: %w", err)
		}
		if !value.Valid {
			continue
		}
		period, ok := statements[date]
		if !ok {
			if len(periods) == fundYears {
				continue
			}
			period = make(map[string]float64, len(fundamentalsSpec))
			statements[date] = period
			periods = append(periods, date)
		}
		period[item] = value.Float64
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("read stock_statement: %w", err)
	}
	if len(periods) == 0 {
		return nil, false, nil
	}

	byGroup := map[string][]MetricRow{}
	for _, metric := range fundamentalsSpec {
		row := MetricRow{Key: metric.key, Label: metric.label, Unit: metric.unit, Values: make([]*float64, len(periods))}
		for i, period := range periods {
			if value, ok := statements[period][metric.key]; ok {
				value := value
				row.Values[i] = &value
			}
		}
		byGroup[metric.group] = append(byGroup[metric.group], row)
	}

	fundamentals := &Fundamentals{Symbol: symbol, Periods: periods}
	for _, metric := range fundamentalsSpec {
		if group := byGroup[metric.group]; group != nil {
			fundamentals.Groups = append(fundamentals.Groups, MetricGroup{Name: metric.group, Rows: group})
			delete(byGroup, metric.group)
		}
	}
	shares, trailing, close, ok := d.marketFigures(ctx, symbol)
	fundamentals.Shares, fundamentals.Trailing, fundamentals.Close = shares, trailing, close

	// A failed figures read is served but not cached: the statements are worth
	// having either way, while caching the zeroes would pin dashes on the
	// symbol for the life of the process when a retry would fill them in.
	if ok {
		d.storeFundamentals(symbol, fundamentals)
	}
	return fundamentals, true, nil
}

// marketFigures reads the three figures the panel header turns into market cap
// and P/E: the latest share count, the latest trailing EPS and the latest
// close. None is guaranteed to exist — the panel prints a dash for what the
// dataset does not hold — but the close rides along here so those figures never
// depend on whether the chart loaded its own bars. The last result is false
// when the reads failed, which is what keeps the empty figures out of the cache.
func (d *Dataset) marketFigures(ctx context.Context, symbol string) (int64, float64, float64, bool) {
	query := fmt.Sprintf(`SELECT 'shares', CAST(arg_max(shares_outstanding, report_date) AS DOUBLE) FROM %s WHERE symbol = ?
		UNION ALL SELECT 'eps', CAST(arg_max(tailing_eps, report_date) AS DOUBLE) FROM %s WHERE symbol = ?
		UNION ALL SELECT 'close', CAST(arg_max(close, report_date) AS DOUBLE) FROM %s WHERE symbol = ?`,
		d.table("stock_shares_outstanding"), d.table("stock_tailing_eps"), d.table("stock_prices"))
	rows, err := d.db.QueryContext(ctx, query, symbol, symbol, symbol)
	if err != nil {
		log.Printf("dataset: market figures for %s unavailable: %v", symbol, err)
		return 0, 0, 0, false
	}
	defer rows.Close()

	var shares int64
	var trailing, close float64
	for rows.Next() {
		var kind string
		var value sql.NullFloat64
		if err := rows.Scan(&kind, &value); err != nil {
			log.Printf("dataset: market figures for %s unavailable: %v", symbol, err)
			return 0, 0, 0, false
		}
		if !value.Valid {
			continue
		}
		switch kind {
		case "shares":
			shares = int64(value.Float64)
		case "eps":
			trailing = value.Float64
		default:
			close = value.Float64
		}
	}
	if err := rows.Err(); err != nil {
		log.Printf("dataset: market figures for %s unavailable: %v", symbol, err)
		return 0, 0, 0, false
	}
	return shares, trailing, close, true
}

// SharesOutstanding is the company's own share count at or before a date, read
// from the market dataset because a 13F reports none: the ownership panel divides
// the shares the tracked funds hold by it. The table carries shares outstanding,
// not float, so the caller has to name the denominator it is dividing by, and the
// count is the one reported for that date rather than restated for a later split.
// The second return is false when the symbol has no count by then, which is a
// hole a panel states rather than a zero.
func (d *Dataset) SharesOutstanding(ctx context.Context, symbol, asOf string) (int64, bool) {
	query := fmt.Sprintf(`SELECT CAST(arg_max(shares_outstanding, CAST(report_date AS DATE)) AS DOUBLE)
		FROM %s WHERE symbol = ? AND CAST(report_date AS DATE) <= CAST(? AS DATE)`,
		d.table("stock_shares_outstanding"))
	var shares sql.NullFloat64
	if err := d.db.QueryRowContext(ctx, query, symbol, asOf).Scan(&shares); err != nil {
		log.Printf("dataset: shares outstanding for %s: %v", symbol, err)
		return 0, false
	}
	if !shares.Valid || shares.Float64 <= 0 {
		return 0, false
	}
	return int64(shares.Float64), true
}

// SharesOutstandingFor is SharesOutstanding for a whole list of companies in one
// query, which is what a lake-wide panel needs: the crowded radar ranks a hundred
// tickers and cannot pay a query each. The result is keyed by the symbol with its
// share-class separator removed, which is how the two sources are reconciled —
// the bundled CUSIP map writes BRKB where the market dataset writes BRK-B — and a
// symbol the dataset has no count for by the date is absent from the map rather
// than zero.
func (d *Dataset) SharesOutstandingFor(ctx context.Context, symbols []string, asOf string) (map[string]int64, error) {
	counts := map[string]int64{}
	if len(symbols) == 0 {
		return counts, nil
	}
	args := make([]any, 0, len(symbols)+1)
	keys := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		key := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(symbol), "-", ""))
		if key == "" {
			continue
		}
		keys = append(keys, key)
		args = append(args, key)
	}
	if len(keys) == 0 {
		return counts, nil
	}
	args = append(args, asOf)
	query := fmt.Sprintf(`SELECT upper(replace(symbol, '-', '')) AS key,
			CAST(arg_max(shares_outstanding, CAST(report_date AS DATE)) AS DOUBLE)
		FROM %s
		WHERE upper(replace(symbol, '-', '')) IN (%s) AND CAST(report_date AS DATE) <= CAST(? AS DATE)
		GROUP BY 1`, d.table("stock_shares_outstanding"), placeholders(len(keys)))
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var shares sql.NullFloat64
		if err := rows.Scan(&key, &shares); err != nil {
			return nil, err
		}
		if shares.Valid && shares.Float64 > 0 {
			counts[key] = int64(shares.Float64)
		}
	}
	return counts, rows.Err()
}

// placeholders returns the "?,?,?" of an IN list of n values; n is always at
// least one because the specs it is built from are never empty.
func placeholders(n int) string {
	return strings.Repeat(",?", n)[1:]
}

func (d *Dataset) cachedFundamentals(symbol string) (*Fundamentals, bool) {
	d.fundMu.Lock()
	defer d.fundMu.Unlock()
	fundamentals, ok := d.fund[symbol]
	return fundamentals, ok
}

func (d *Dataset) storeFundamentals(symbol string, fundamentals *Fundamentals) {
	d.fundMu.Lock()
	defer d.fundMu.Unlock()
	if _, exists := d.fund[symbol]; !exists {
		d.fundOrder = append(d.fundOrder, symbol)
	}
	d.fund[symbol] = fundamentals
	for len(d.fundOrder) > fundCacheSize {
		delete(d.fund, d.fundOrder[0])
		d.fundOrder = d.fundOrder[1:]
	}
}
