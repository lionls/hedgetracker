package main

// The 13F dashboard endpoints. Each one maps onto one of the views in
// thirteenf.sql, served from the materialised Parquet tables that thirteenf.go
// builds:
//
//	/api/13f/status     what the build produced, and whether it is still running
//	/api/13f/funds      the fund picker: one row per filer, newest portfolio
//	/api/13f/holdings   holdings_normalized for one filer and quarter
//	/api/13f/flows      fund_quarterly_flows, the period-over-period changes
//	/api/13f/signals    conviction_scores, across funds, one ticker, or one fund
//	/api/13f/vwap       market_quarterly_vwap for one ticker
//	/api/13f/refresh    rebuild the tables from the lake
//
// Rows are objects rather than the column arrays the price bars use: these
// tables are small (a fund-quarter is hundreds of rows) and a dashboard wants
// named fields. Every list endpoint carries a total, so a client can page.

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// HoldingRow is one position of one filer in one quarter.
type HoldingRow struct {
	Cusip         string  `json:"cusip"`
	Ticker        string  `json:"ticker"`
	Issuer        string  `json:"issuer"`
	ClassTitle    string  `json:"classTitle,omitempty"`
	Shares        float64 `json:"shares"`
	ValueUSD      float64 `json:"valueUsd"`
	WeightPct     float64 `json:"weightPct"`
	ReportedLines int64   `json:"reportedLines"`
}

// FlowRow is one position over two consecutive filings of a fund. The deltas come
// from the view; deltaShares is split-adjusted, see thirteenf.sql.
type FlowRow struct {
	Cik             string   `json:"cik"`
	Period          string   `json:"period"`
	PrevPeriod      string   `json:"prevPeriod,omitempty"`
	QuartersBetween int64    `json:"quartersBetween"`
	Cusip           string   `json:"cusip"`
	Ticker          string   `json:"ticker"`
	Issuer          string   `json:"issuer"`
	Action          string   `json:"action"`
	Shares          float64  `json:"shares"`
	ValueUSD        float64  `json:"valueUsd"`
	WeightPct       float64  `json:"weightPct"`
	PrevShares      float64  `json:"prevShares"`
	PrevValueUSD    float64  `json:"prevValueUsd"`
	PrevWeightPct   float64  `json:"prevWeightPct"`
	DeltaShares     float64  `json:"deltaShares"`
	DeltaSharesPct  *float64 `json:"deltaSharesPct"`
	DeltaValueUSD   float64  `json:"deltaValueUsd"`
	DeltaWeightPct  float64  `json:"deltaWeightPct"`
	SplitFactor     float64  `json:"splitFactor"`
	SplitAdjusted   bool     `json:"splitAdjusted"`
}

// SignalRow is a flow with its market context and its conviction class.
type SignalRow struct {
	FlowRow
	Signal         string   `json:"signal"`
	QuarterlyVWAP  *float64 `json:"quarterlyVwap"`
	QuarterlyLow   *float64 `json:"quarterlyLow"`
	QuarterlyHigh  *float64 `json:"quarterlyHigh"`
	EstCapitalFlow *float64 `json:"estCapitalFlow"`
}

// FundRow is one filer in the picker.
type FundRow struct {
	Cik             string  `json:"cik"`
	Quarters        int64   `json:"quarters"`
	FirstPeriod     string  `json:"firstPeriod"`
	LatestPeriod    string  `json:"latestPeriod"`
	LatestValueUSD  float64 `json:"latestValueUsd"`
	LatestPositions int64   `json:"latestPositions"`
}

// VWAPRow is one quarter of price history for one ticker.
type VWAPRow struct {
	Symbol         string  `json:"symbol"`
	Year           int64   `json:"year"`
	Quarter        int64   `json:"quarter"`
	TradingDays    int64   `json:"tradingDays"`
	FirstTradeDate string  `json:"firstTradeDate"`
	LastTradeDate  string  `json:"lastTradeDate"`
	TotalVolume    float64 `json:"totalVolume"`
	VWAP           float64 `json:"vwap"`
	Low            float64 `json:"low"`
	High           float64 `json:"high"`
}

var (
	cikPattern     = regexp.MustCompile(`^[0-9]{1,10}$`)
	datePattern    = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
	quarterPattern = regexp.MustCompile(`^([0-9]{4})-?[Qq]([1-4])$`)
)

// quarterEnds maps a calendar quarter to the day a 13F report period ends on:
// report periods are always calendar quarter ends.
var quarterEnds = [...]string{"03-31", "06-30", "09-30", "12-31"}

func (a *API) thirteenfStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.thirteenF.Status())
}

// thirteenfRefresh rebuilds the tables. It answers 202 when the build started and
// 409 when one is already running.
func (a *API) thirteenfRefresh(w http.ResponseWriter, r *http.Request) {
	status := http.StatusConflict
	if a.thirteenF.Rebuild(r.Context()) {
		status = http.StatusAccepted
	}
	writeJSON(w, status, a.thirteenF.Status())
}

// thirteenfFunds is the fund picker: every filer in the lake with its newest
// portfolio, largest first. The lake stores no filer name, only the CIK.
func (a *API) thirteenfFunds(w http.ResponseWriter, r *http.Request) {
	path, ok := a.thirteenF.tablePath(w, thirteenHoldings)
	if !ok {
		return
	}
	limit := limitParam(r, 500, 5000)
	query := fmt.Sprintf(`WITH latest AS (
			SELECT cik, max(report_period) AS period, min(report_period) AS first_period,
			       count(DISTINCT report_period) AS quarters
			FROM %[1]s GROUP BY cik)
		SELECT l.cik, l.quarters, l.first_period, l.period, h.portfolio_value_total, count(*)
		FROM latest l JOIN %[1]s h ON h.cik = l.cik AND h.report_period = l.period
		GROUP BY 1, 2, 3, 4, 5 ORDER BY 5 DESC LIMIT %[2]d`, thirteenFrom(path), limit)

	funds := []FundRow{}
	if err := a.thirteenRows(r.Context(), query, nil, func(rs *sql.Rows) error {
		var row FundRow
		var first, latest time.Time
		if err := rs.Scan(&row.Cik, &row.Quarters, &first, &latest, &row.LatestValueUSD, &row.LatestPositions); err != nil {
			return err
		}
		row.FirstPeriod, row.LatestPeriod = formatPeriod(first), formatPeriod(latest)
		funds = append(funds, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "funds", err)
		return
	}

	var total int64
	if err := a.thirteenCount(r.Context(), fmt.Sprintf("SELECT count(DISTINCT cik) FROM %s", thirteenFrom(path)), nil, &total); err != nil {
		a.thirteenFailed(w, "funds", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"funds": funds, "total": total, "limit": limit})
}

// thirteenfHoldings is holdings_normalized for one filer: one row per CUSIP with
// its portfolio weight, the quarter's total value, and the quarters that filer
// is in the lake for.
func (a *API) thirteenfHoldings(w http.ResponseWriter, r *http.Request) {
	path, ok := a.thirteenF.tablePath(w, thirteenHoldings)
	if !ok {
		return
	}
	cik, ok := cikParam(w, r)
	if !ok {
		return
	}
	limit := limitParam(r, 500, 5000)
	from := thirteenFrom(path)

	quarters, err := a.thirteenQuarters(r.Context(), from, cik)
	if err != nil {
		a.thirteenFailed(w, "holdings", err)
		return
	}
	if len(quarters) == 0 {
		writeError(w, http.StatusNotFound, "no holdings for that filer; see /api/13f/funds")
		return
	}
	period, ok := thirteenPeriod(w, r, quarters)
	if !ok {
		return
	}
	if !oneOf(quarters, period) {
		writeError(w, http.StatusNotFound, "no holdings for that filer in that period; see the quarters list")
		return
	}
	args := []any{cik, period}

	query := fmt.Sprintf(`SELECT cusip, ticker, issuer, class_title, shares, value_usd,
			portfolio_weight_pct, reported_lines
		FROM %s WHERE cik = ? AND report_period = ? ORDER BY value_usd DESC LIMIT %d`, from, limit)
	holdings := []HoldingRow{}
	if err := a.thirteenRows(r.Context(), query, args, func(rs *sql.Rows) error {
		var row HoldingRow
		if err := rs.Scan(&row.Cusip, &row.Ticker, &row.Issuer, &row.ClassTitle,
			&row.Shares, &row.ValueUSD, &row.WeightPct, &row.ReportedLines); err != nil {
			return err
		}
		holdings = append(holdings, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "holdings", err)
		return
	}

	var total int64
	if err := a.thirteenCount(r.Context(),
		fmt.Sprintf("SELECT count(*) FROM %s WHERE cik = ? AND report_period = ?", from), args, &total); err != nil {
		a.thirteenFailed(w, "holdings", err)
		return
	}
	var value float64
	if err := a.thirteenCount(r.Context(),
		fmt.Sprintf("SELECT max(portfolio_value_total) FROM %s WHERE cik = ? AND report_period = ?", from), args, &value); err != nil {
		a.thirteenFailed(w, "holdings", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"cik":               cik,
		"period":            period,
		"quarters":          quarters,
		"portfolioValueUsd": value,
		"total":             total,
		"limit":             limit,
		"holdings":          holdings,
	})
}

// thirteenfFlows is fund_quarterly_flows for one filer: every position it opened,
// added to, trimmed, exited or held, newest quarter first.
func (a *API) thirteenfFlows(w http.ResponseWriter, r *http.Request) {
	path, ok := a.thirteenF.tablePath(w, thirteenFlows)
	if !ok {
		return
	}
	cik, ok := cikParam(w, r)
	if !ok {
		return
	}
	actions, ok := choiceParam(w, "action", thirteenActions, r.URL.Query().Get("action"))
	if !ok {
		return
	}
	limit := limitParam(r, 200, 2000)
	from := thirteenFrom(path)

	where := []string{"cik = ?"}
	args := []any{cik}
	period := ""
	if raw := strings.TrimSpace(r.URL.Query().Get("period")); raw != "" && !strings.EqualFold(raw, "latest") {
		parsed, ok := normalisePeriod(raw)
		if !ok {
			writeError(w, http.StatusBadRequest, "period must be YYYY-MM-DD or YYYYQn")
			return
		}
		period = parsed
		where = append(where, "report_period = ?")
		args = append(args, period)
	}
	if len(actions) > 0 {
		where = append(where, "action IN ("+placeholders(len(actions))+")")
		for _, action := range actions {
			args = append(args, action)
		}
	}
	clause := strings.Join(where, " AND ")

	query := fmt.Sprintf(`SELECT cik, report_period, prev_period, quarters_between, cusip, ticker, issuer, action,
			shares, value_usd, portfolio_weight_pct, prev_shares, prev_value_usd, prev_portfolio_weight_pct,
			delta_shares, delta_shares_pct, delta_value_usd, delta_weight_pct, split_factor, split_adjusted
		FROM %s WHERE %s ORDER BY report_period DESC, abs(delta_value_usd) DESC LIMIT %d`, from, clause, limit)
	flows := []FlowRow{}
	if err := a.thirteenRows(r.Context(), query, args, func(rs *sql.Rows) error {
		row, err := scanFlow(rs)
		if err != nil {
			return err
		}
		flows = append(flows, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "flows", err)
		return
	}

	var total int64
	if err := a.thirteenCount(r.Context(),
		fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", from, clause), args, &total); err != nil {
		a.thirteenFailed(w, "flows", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"cik": cik, "period": period, "actions": actions, "total": total, "limit": limit, "flows": flows,
	})
}

// thirteenfSignals is conviction_scores. Filer, ticker, quarter, action and signal
// narrow it; with no filter it is the whole lake's newest quarter, largest
// estimated flow first, which is what a dashboard opens on.
func (a *API) thirteenfSignals(w http.ResponseWriter, r *http.Request) {
	path, ok := a.thirteenF.tablePath(w, thirteenConviction)
	if !ok {
		return
	}
	actions, ok := choiceParam(w, "action", thirteenActions, r.URL.Query().Get("action"))
	if !ok {
		return
	}
	signals, ok := choiceParam(w, "signal", thirteenSignals, r.URL.Query().Get("signal"))
	if !ok {
		return
	}
	limit := limitParam(r, 200, 2000)
	from := thirteenFrom(path)

	where := []string{"1 = 1"}
	args := []any{}
	cik := ""
	if raw := strings.TrimSpace(r.URL.Query().Get("cik")); raw != "" {
		cik, ok = normaliseCik(raw)
		if !ok {
			writeError(w, http.StatusBadRequest, "cik must be 1-10 digits")
			return
		}
		where = append(where, "cik = ?")
		args = append(args, cik)
	}
	ticker := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("ticker")))
	if ticker != "" {
		if !validSymbol(ticker) {
			writeError(w, http.StatusBadRequest, "invalid ticker")
			return
		}
		where = append(where, "ticker = ?")
		args = append(args, ticker)
	}
	if len(actions) > 0 {
		where = append(where, "action IN ("+placeholders(len(actions))+")")
		for _, action := range actions {
			args = append(args, action)
		}
	}
	if len(signals) > 0 {
		where = append(where, "signal IN ("+placeholders(len(signals))+")")
		for _, signal := range signals {
			args = append(args, signal)
		}
	}

	// The default quarter is the newest one in the lake: the dashboard's opening
	// view is "what conviction moved this quarter". A lake with a single filing
	// has no flows and therefore no newest quarter, so the query stays unfiltered
	// and answers empty rather than failing on a null date.
	var period string
	if raw := strings.TrimSpace(r.URL.Query().Get("period")); raw != "" && !strings.EqualFold(raw, "latest") {
		period, ok = normalisePeriod(raw)
		if !ok {
			writeError(w, http.StatusBadRequest, "period must be YYYY-MM-DD or YYYYQn")
			return
		}
	} else {
		var latest sql.NullTime
		if err := a.thirteenCount(r.Context(),
			fmt.Sprintf("SELECT max(report_period) FROM %s", from), nil, &latest); err != nil {
			a.thirteenFailed(w, "signals", err)
			return
		}
		if latest.Valid {
			period = formatPeriod(latest.Time)
		}
	}
	if period != "" {
		where = append(where, "report_period = ?")
		args = append(args, period)
	}
	clause := strings.Join(where, " AND ")

	query := fmt.Sprintf(`SELECT cik, report_period, prev_period, quarters_between, cusip, ticker, issuer, action,
			shares, value_usd, portfolio_weight_pct, prev_shares, prev_value_usd, prev_portfolio_weight_pct,
			delta_shares, delta_shares_pct, delta_value_usd, delta_weight_pct, split_factor, split_adjusted,
			signal, quarterly_vwap, quarterly_low, quarterly_high, est_capital_flow
		FROM %s WHERE %s
		ORDER BY abs(est_capital_flow) DESC NULLS LAST, abs(delta_weight_pct) DESC LIMIT %d`, from, clause, limit)
	rows := []SignalRow{}
	if err := a.thirteenRows(r.Context(), query, args, func(rs *sql.Rows) error {
		row := SignalRow{}
		flow, err := scanFlow(rs, &row.Signal, &row.QuarterlyVWAP, &row.QuarterlyLow,
			&row.QuarterlyHigh, &row.EstCapitalFlow)
		if err != nil {
			return err
		}
		row.FlowRow = flow
		rows = append(rows, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "signals", err)
		return
	}

	var total int64
	if err := a.thirteenCount(r.Context(),
		fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", from, clause), args, &total); err != nil {
		a.thirteenFailed(w, "signals", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"period": period, "cik": cik, "ticker": ticker,
		"actions": actions, "signalClasses": signals,
		"total": total, "limit": limit, "signals": rows,
	})
}

// thirteenfVWAP is market_quarterly_vwap for one ticker.
func (a *API) thirteenfVWAP(w http.ResponseWriter, r *http.Request) {
	path, ok := a.thirteenF.tablePath(w, thirteenVWAP)
	if !ok {
		return
	}
	ticker := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("ticker")))
	if ticker == "" || !validSymbol(ticker) {
		writeError(w, http.StatusBadRequest, "ticker is required")
		return
	}
	limit := limitParam(r, 40, 400)

	query := fmt.Sprintf(`SELECT symbol, period_year, period_quarter, trading_days, first_trade_date,
			last_trade_date, total_volume, quarterly_vwap, quarterly_low, quarterly_high
		FROM %s WHERE symbol = ? ORDER BY period_year, period_quarter LIMIT %d`, thirteenFrom(path), limit)
	quarters := []VWAPRow{}
	if err := a.thirteenRows(r.Context(), query, []any{ticker}, func(rs *sql.Rows) error {
		var row VWAPRow
		var first, last sql.NullTime
		if err := rs.Scan(&row.Symbol, &row.Year, &row.Quarter, &row.TradingDays, &first, &last,
			&row.TotalVolume, &row.VWAP, &row.Low, &row.High); err != nil {
			return err
		}
		if first.Valid {
			row.FirstTradeDate = formatPeriod(first.Time)
		}
		if last.Valid {
			row.LastTradeDate = formatPeriod(last.Time)
		}
		quarters = append(quarters, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "vwap", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ticker": ticker, "quarters": quarters})
}

// thirteenRows runs a query over the materialised tables and hands every row to
// scan.
func (a *API) thirteenRows(ctx context.Context, query string, args []any, scan func(*sql.Rows) error) error {
	rows, err := a.thirteenF.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// thirteenCount runs a single-value query into an int64, a float64 or a time.
func (a *API) thirteenCount(ctx context.Context, query string, args []any, into any) error {
	return a.thirteenF.db.QueryRowContext(ctx, query, args...).Scan(into)
}

// scanFlow reads the columns every flow and signal row shares, in view order.
// extra carries whatever a caller selected after them (the signal view adds
// five), because a Rows is scanned once: the destinations must cover the whole
// result in a single call.
func scanFlow(rs *sql.Rows, extra ...any) (FlowRow, error) {
	var row FlowRow
	var period time.Time
	var previous sql.NullTime
	dest := append([]any{&row.Cik, &period, &previous, &row.QuartersBetween, &row.Cusip, &row.Ticker,
		&row.Issuer, &row.Action, &row.Shares, &row.ValueUSD, &row.WeightPct, &row.PrevShares,
		&row.PrevValueUSD, &row.PrevWeightPct, &row.DeltaShares, &row.DeltaSharesPct,
		&row.DeltaValueUSD, &row.DeltaWeightPct, &row.SplitFactor, &row.SplitAdjusted}, extra...)
	err := rs.Scan(dest...)
	if err != nil {
		return row, err
	}
	row.Period = formatPeriod(period)
	if previous.Valid {
		row.PrevPeriod = formatPeriod(previous.Time)
	}
	return row, nil
}

// thirteenQuarters lists the report periods one filer is in the lake for, oldest
// first, so a client can offer a quarter selector.
func (a *API) thirteenQuarters(ctx context.Context, from, cik string) ([]string, error) {
	query := fmt.Sprintf("SELECT DISTINCT report_period FROM %s WHERE cik = ? ORDER BY report_period", from)
	quarters := []string{}
	err := a.thirteenRows(ctx, query, []any{cik}, func(rs *sql.Rows) error {
		var period time.Time
		if err := rs.Scan(&period); err != nil {
			return err
		}
		quarters = append(quarters, formatPeriod(period))
		return nil
	})
	return quarters, err
}

// thirteenPeriod resolves the quarter a request asks for: the named one, or the
// filer's newest.
func thirteenPeriod(w http.ResponseWriter, r *http.Request, quarters []string) (string, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get("period"))
	if raw == "" || strings.EqualFold(raw, "latest") {
		return quarters[len(quarters)-1], true
	}
	period, ok := normalisePeriod(raw)
	if !ok {
		writeError(w, http.StatusBadRequest, "period must be YYYY-MM-DD or YYYYQn")
		return "", false
	}
	return period, true
}

// thirteenFailed logs the cause of a failed query and reports it without
// exposing the query layer to the client.
func (a *API) thirteenFailed(w http.ResponseWriter, what string, err error) {
	log.Printf("13f %s: %v", what, err)
	writeError(w, http.StatusInternalServerError, "could not read the 13F tables")
}

// thirteenFrom is the read_parquet expression of one materialised table.
func thirteenFrom(path string) string { return "read_parquet(" + sqlString(path) + ")" }

// normaliseCik zero-pads a filer CIK the way the lake stores it.
func normaliseCik(raw string) (string, bool) {
	if !cikPattern.MatchString(raw) {
		return "", false
	}
	return fmt.Sprintf("%010s", raw), true
}

// cikParam reads and normalises the filer of a request.
func cikParam(w http.ResponseWriter, r *http.Request) (string, bool) {
	cik, ok := normaliseCik(strings.TrimSpace(r.URL.Query().Get("cik")))
	if !ok {
		writeError(w, http.StatusBadRequest, "cik is required and must be 1-10 digits")
		return "", false
	}
	return cik, true
}

// normalisePeriod accepts the report date itself or a quarter label, which is
// what a dashboard selects on.
func normalisePeriod(raw string) (string, bool) {
	if datePattern.MatchString(raw) {
		if _, err := time.Parse("2006-01-02", raw); err != nil {
			return "", false
		}
		return raw, true
	}
	match := quarterPattern.FindStringSubmatch(raw)
	if match == nil {
		return "", false
	}
	quarter, _ := strconv.Atoi(match[2])
	return match[1] + "-" + quarterEnds[quarter-1], true
}

// choiceParam validates a comma-separated list against the values a view emits.
// An empty value selects nothing, which means "no filter".
func choiceParam(w http.ResponseWriter, name string, allowed []string, raw string) ([]string, bool) {
	choices := []string{}
	if strings.TrimSpace(raw) == "" {
		return choices, true
	}
	for _, choice := range strings.Split(raw, ",") {
		choice = strings.ToUpper(strings.TrimSpace(choice))
		if choice == "" {
			continue
		}
		if !oneOf(allowed, choice) {
			writeError(w, http.StatusBadRequest, "unknown "+name+" "+choice+"; one of "+strings.Join(allowed, ", "))
			return nil, false
		}
		choices = append(choices, choice)
	}
	return choices, true
}

// limitParam reads a row limit, bounded so one request cannot pull a whole lake.
func limitParam(r *http.Request, fallback, maximum int) int {
	raw := r.URL.Query().Get("limit")
	parsed, err := strconv.Atoi(raw)
	if raw == "" || err != nil || parsed <= 0 {
		return fallback
	}
	return min(parsed, maximum)
}

func oneOf(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

// formatPeriod renders a report period the way the SQL file and the API do.
func formatPeriod(period time.Time) string { return period.Format("2006-01-02") }
