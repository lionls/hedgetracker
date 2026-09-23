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
//	/api/13f/fund       one fund's quarter series, its marked positions, its book
//	/api/13f/flow       one fund's book over two filings, as the diagram's bands
//	/api/13f/owners     the funds holding one ticker: the stocks page's ownership panel
//	/api/13f/consensus  the lake's quarter: openings, exits, net flows, crowded positions
//	/api/13f/leaderboard every filer with its concentration, turnover and estimated marks
//	/api/13f/compare    two funds side by side: shared ground and contrarian positions
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
	"sort"
	"strconv"
	"strings"
	"time"
)

// HoldingRow is one position of one filer in one quarter. shares, valueUsd and
// weightPct are null for a filing that stated neither a value nor a share count
// — the lake keeps a 13F's optional elements null rather than zeroing them — and
// issuer is empty for a filing that named nothing: a reader shows the ticker.
type HoldingRow struct {
	Cusip         string   `json:"cusip"`
	Ticker        string   `json:"ticker"`
	Issuer        string   `json:"issuer"`
	ClassTitle    string   `json:"classTitle,omitempty"`
	Shares        *float64 `json:"shares"`
	ValueUSD      *float64 `json:"valueUsd"`
	WeightPct     *float64 `json:"weightPct"`
	ReportedLines int64    `json:"reportedLines"`
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
	// The fund's name, empty on a lake the extractor has not named.
	FilerName string `json:"filerName"`
}

// FundRow is one filer in the picker.
type FundRow struct {
	Cik string `json:"cik"`
	// The fund's name as EDGAR states it, empty on a lake the extractor has not
	// named; the picker falls back to the CIK.
	FilerName string `json:"filerName"`
	Quarters  int64  `json:"quarters"`
	// The newest filing's portfolio; null when that filing reported no values,
	// which the picker shows as a dash rather than as an empty book.
	FirstPeriod     string   `json:"firstPeriod"`
	LatestPeriod    string   `json:"latestPeriod"`
	LatestValueUSD  *float64 `json:"latestValueUsd"`
	LatestPositions int64    `json:"latestPositions"`
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

// FundSeriesRow is one quarter of a fund's ledger: what its filing was worth,
// what it moved, and what the marks did to it. The P&L columns are null both for
// a filer's first filing in the lake — there is no previous filing to mark
// against — and for a quarter whose positions the price dataset does not cover.
// portfolioValueUsd is null for a filing that reported no values at all, which
// is a filing the lake holds without a portfolio to state.
type FundSeriesRow struct {
	Period             string   `json:"period"`
	ReportYear         int64    `json:"reportYear"`
	ReportQuarter      int64    `json:"reportQuarter"`
	PrevPeriod         string   `json:"prevPeriod,omitempty"`
	QuartersBetween    int64    `json:"quartersBetween"`
	Positions          int64    `json:"positions"`
	PortfolioValueUSD  *float64 `json:"portfolioValueUsd"`
	MovedPositions     int64    `json:"movedPositions"`
	PositionsWithPnl   int64    `json:"positionsWithPnl"`
	PnlUSD             *float64 `json:"pnlUsd"`
	CumulativePnlUSD   *float64 `json:"cumulativePnlUsd"`
	CoveredValueUSD    *float64 `json:"coveredValueUsd"`
	CoveragePct        *float64 `json:"coveragePct"`
	NewPositions       int64    `json:"newPositions"`
	AddedPositions     int64    `json:"addedPositions"`
	TrimmedPositions   int64    `json:"trimmedPositions"`
	ExitedPositions    int64    `json:"exitedPositions"`
	HeldPositions      int64    `json:"heldPositions"`
	PurchasedUSD       *float64 `json:"purchasedUsd"`
	SoldUSD            *float64 `json:"soldUsd"`
	PurchasedPositions int64    `json:"purchasedPositions"`
	SoldPositions      int64    `json:"soldPositions"`
}

// FundPositionRow is one position of one filing with the P&L the marks imply.
// priced is false — and the P&L null — when the ticker has no VWAP for one of
// the two quarters, which is a hole in the estimate rather than a flat quarter.
// CumulativePnlUsd is the same marks summed per CUSIP over the fund's filings
// through this quarter — the series row's cumulativePnlUsd asked of one position.
// It is null when none of the position's quarters could be marked, and it is this
// quarter's own mark, 0, for a name the fund first held in this quarter. The sum
// is per CUSIP, not per holding: a name the fund sold and later bought back
// carries the earlier spell too.
type FundPositionRow struct {
	Cusip            string   `json:"cusip"`
	Ticker           string   `json:"ticker"`
	Issuer           string   `json:"issuer"`
	Action           string   `json:"action"`
	Shares           float64  `json:"shares"`
	ValueUSD         float64  `json:"valueUsd"`
	WeightPct        float64  `json:"weightPct"`
	PrevShares       float64  `json:"prevShares"`
	DeltaShares      float64  `json:"deltaShares"`
	DeltaValueUSD    float64  `json:"deltaValueUsd"`
	QuartersBetween  int64    `json:"quartersBetween"`
	SplitFactor      float64  `json:"splitFactor"`
	SplitAdjusted    bool     `json:"splitAdjusted"`
	PrevVWAP         *float64 `json:"prevVwap"`
	VWAP             *float64 `json:"vwap"`
	PnlUSD           *float64 `json:"pnlUsd"`
	PnlPct           *float64 `json:"pnlPct"`
	CumulativePnlUSD *float64 `json:"cumulativePnlUsd"`
	Priced           bool     `json:"priced"`
}

// holdingSlices is how many of a fund's positions the holdings panel is handed by
// name. A donut names about a dozen slices before its legend stops fitting beside
// it, and a book of 794 positions is not a chart: everything past the cut is one
// tail slice, whose size and count are in the same response, so the panel never
// draws a book it did not receive.
const holdingSlices = 12

// FundHoldingRow is one slice of a fund's reported book: a position of the filing
// by value, with the weight the filing implies. It is the quarter the page is on,
// which is what "current holdings" means for a 13F — the series is the charts.
type FundHoldingRow struct {
	Cusip     string   `json:"cusip"`
	Ticker    string   `json:"ticker"`
	Issuer    string   `json:"issuer"`
	ValueUSD  *float64 `json:"valueUsd"`
	WeightPct *float64 `json:"weightPct"`
}

// FundHoldings is the quarter's book as the holdings panel needs it: the largest
// positions by reported value, and the whole book's count and value, so the panel
// can draw what this list does not name as one tail slice. It reads
// holdings_normalized rather than the movement rows, because a position the fund
// exited is a row of position_quarter_pnl with nothing left in it and a book does
// not hold those — which is also why positions and the series row's `positions`
// are the same number. valueUsd is null for a book whose filing reported no
// values: an unstated portfolio is not a portfolio worth nothing.
type FundHoldings struct {
	Largest   []FundHoldingRow `json:"largest"`
	Positions int64            `json:"positions"`
	ValueUSD  *float64         `json:"valueUsd"`
}

// flowSlices is how many positions each side of the flow diagram names: the
// largest by reported value at the previous filing and the largest at this one.
// It is a union and not one ranking, because the position that was the fourth
// largest last quarter and is the fortieth now is the trim the diagram exists to
// show. Everything outside the union is one aggregate band per side.
const flowSlices = 12

// FlowBand is one position over the two filings the diagram draws: what it was
// worth on each side, and the trade the flow view measured between them. The two
// values are as reported, so each side's bands add up to that filing's book;
// estFlowUsd is what keeps the difference between them from being read as a trade
// when it was the market. It is null when the ticker has no daily bars, which is
// the one case the split below cannot be made — the page falls back to the action,
// which comes from the shares and not from a price.
type FlowBand struct {
	Cusip         string   `json:"cusip"`
	Ticker        string   `json:"ticker"`
	Issuer        string   `json:"issuer"`
	Action        string   `json:"action"`
	PrevValueUSD  float64  `json:"prevValueUsd"`
	ValueUSD      float64  `json:"valueUsd"`
	PrevWeightPct float64  `json:"prevWeightPct"`
	WeightPct     float64  `json:"weightPct"`
	EstFlowUSD    *float64 `json:"estFlowUsd"`
	SplitAdjusted bool     `json:"splitAdjusted"`
}

// FlowBook is one side of the diagram: a filing's own count and value, read from
// the book view rather than summed over the bands, so the ends of the picture are
// the same numbers the funds page states.
type FlowBook struct {
	Positions int64   `json:"positions"`
	ValueUSD  float64 `json:"valueUsd"`
}

// FlowOthers is every position the diagram does not name, both sides at once, with
// the trades those rows measured and how many of them have no price bars. The page
// draws it as one band with the same carried/bought/sold/market split as a named
// position, so the arithmetic of the tail is the arithmetic of the picture.
type FlowOthers struct {
	PrevPositions int64    `json:"prevPositions"`
	Positions     int64    `json:"positions"`
	PrevValueUSD  float64  `json:"prevValueUsd"`
	ValueUSD      float64  `json:"valueUsd"`
	EstFlowUSD    *float64 `json:"estFlowUsd"`
	Unpriced      int64    `json:"unpriced"`
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

// thirteenfFunds is the fund picker: every filer in the lake with the portfolio
// of its newest filing, largest first. `q` narrows it to the funds whose name or
// CIK contains it, which is how a fund is found by either. A lake the extractor
// has not named answers with the CIK in place of the name.
func (a *API) thirteenfFunds(w http.ResponseWriter, r *http.Request) {
	path, ok := a.thirteenF.tablePath(w, thirteenFunds)
	if !ok {
		return
	}
	limit := limitParam(r, 500, 5000)
	from := thirteenFrom(path)
	// A CIK is stored zero-padded and typed unpadded, so the digits are matched
	// anywhere in it rather than at the start.
	where := "1 = 1"
	args := []any{}
	if search := strings.TrimSpace(r.URL.Query().Get("q")); search != "" {
		where = "cik LIKE '%' || ? || '%' OR filer_name ILIKE '%' || ? || '%'"
		args = append(args, search, search)
	}
	query := fmt.Sprintf(`SELECT cik, filer_name, quarters, first_period, latest_period, latest_value_usd,
			latest_positions
		FROM %s WHERE %s ORDER BY latest_value_usd DESC, cik LIMIT %d`, from, where, limit)

	funds := []FundRow{}
	if err := a.thirteenRows(r.Context(), query, args, func(rs *sql.Rows) error {
		var row FundRow
		var filerName sql.NullString
		var first, latest time.Time
		if err := rs.Scan(&row.Cik, &filerName, &row.Quarters, &first, &latest,
			&row.LatestValueUSD, &row.LatestPositions); err != nil {
			return err
		}
		row.FilerName = filerName.String
		row.FirstPeriod, row.LatestPeriod = formatPeriod(first), formatPeriod(latest)
		funds = append(funds, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "funds", err)
		return
	}

	var total int64
	if err := a.thirteenCount(r.Context(),
		fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", from, where), args, &total); err != nil {
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

	query := fmt.Sprintf(`SELECT cusip, ticker, COALESCE(issuer, ''), COALESCE(class_title, ''),
			shares, value_usd, portfolio_weight_pct, reported_lines
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
	// The portfolio total of the filing, null when it stated no values at all;
	// the panel prints the dash rather than a zero the filing never filed.
	var value *float64
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
			signal, quarterly_vwap, quarterly_low, quarterly_high, est_capital_flow, filer_name
		FROM %s WHERE %s
		ORDER BY abs(est_capital_flow) DESC NULLS LAST, abs(delta_weight_pct) DESC LIMIT %d`, from, clause, limit)
	rows := []SignalRow{}
	if err := a.thirteenRows(r.Context(), query, args, func(rs *sql.Rows) error {
		row := SignalRow{}
		var filerName sql.NullString
		flow, err := scanFlow(rs, &row.Signal, &row.QuarterlyVWAP, &row.QuarterlyLow,
			&row.QuarterlyHigh, &row.EstCapitalFlow, &filerName)
		if err != nil {
			return err
		}
		row.FlowRow = flow
		row.FilerName = filerName.String
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

// thirteenfVWAP is market_quarterly_vwap for one ticker, newest quarter first:
// the panel is read against the quarter the dashboard has open, and the limit
// has to cut the oldest quarters off a long history rather than the newest.
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
		FROM %s WHERE symbol = ? ORDER BY period_year DESC, period_quarter DESC LIMIT %d`, thirteenFrom(path), limit)
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

// thirteenSorts are the rankings the fund endpoint offers, the first one being
// its default. A null P&L never outranks a number in either direction: a
// position the price dataset does not cover is a hole in the estimate, not the
// best or the worst performer.
var thirteenSorts = []struct{ name, order string }{
	{"value", "value_usd DESC, cusip"},
	{"weight", "portfolio_weight_pct DESC, value_usd DESC"},
	{"gain", "pnl_usd DESC NULLS LAST, value_usd DESC"},
	{"loss", "pnl_usd ASC NULLS LAST, value_usd DESC"},
}

// thirteenfFund is one fund's page in one request: the quarter series from
// fund_quarterly_performance, the positions of one quarter from
// position_quarter_pnl with the P&L the quarterly VWAPs imply — each of those
// rows carrying the same marks summed per CUSIP through that quarter, so it can
// say what the position has done, not only what it did this quarter — and the
// quarter's book from holdings_normalized, largest first, for the holdings panel.
// The series is every filing the fund is in the lake for — the charts are the
// page — while the positions answer for the quarter the request names, newest by
// default. A filer whose first filing is the requested one has an empty positions
// list and a series that still states what the filing was worth.
func (a *API) thirteenfFund(w http.ResponseWriter, r *http.Request) {
	path, ok := a.thirteenF.tablePath(w, thirteenPositionPnl)
	if !ok {
		return
	}
	performance, ok := a.thirteenF.tablePath(w, thirteenPerformance)
	if !ok {
		return
	}
	names, ok := a.thirteenF.tablePath(w, thirteenFilerNames)
	if !ok {
		return
	}
	book, ok := a.thirteenF.tablePath(w, thirteenHoldings)
	if !ok {
		return
	}
	bookFrom := thirteenFrom(book)
	cik, ok := cikParam(w, r)
	if !ok {
		return
	}
	sort, order, ok := sortOrder(w, r.URL.Query().Get("sort"))
	if !ok {
		return
	}
	limit := limitParam(r, 100, 2000)
	from, performanceFrom := thirteenFrom(path), thirteenFrom(performance)

	// performance has one row per filing, so it is also where the fund's quarters
	// come from: a reader can select any quarter it filed, not only the ones it
	// has a previous filing to mark against.
	quarters, err := a.thirteenQuarters(r.Context(), performanceFrom, cik)
	if err != nil {
		a.thirteenFailed(w, "fund", err)
		return
	}
	if len(quarters) == 0 {
		writeError(w, http.StatusNotFound, "no filings for that filer; see /api/13f/funds")
		return
	}
	period, ok := thirteenPeriod(w, r, quarters)
	if !ok {
		return
	}
	if !oneOf(quarters, period) {
		writeError(w, http.StatusNotFound, "no filing for that filer in that period; see the quarters list")
		return
	}

	seriesQuery := fmt.Sprintf(`SELECT report_period, report_year, report_quarter, prev_period,
			quarters_between, positions, portfolio_value_total, moved_positions, positions_with_pnl,
			pnl_usd, covered_value_usd, coverage_pct, new_positions, added_positions, trimmed_positions,
			exited_positions, held_positions, purchased_usd, sold_usd, purchased_positions,
			sold_positions, cumulative_pnl_usd
		FROM %s WHERE cik = ? ORDER BY report_period`, performanceFrom)
	series := []FundSeriesRow{}
	if err := a.thirteenRows(r.Context(), seriesQuery, []any{cik}, func(rs *sql.Rows) error {
		var row FundSeriesRow
		var report, previous sql.NullTime
		if err := rs.Scan(&report, &row.ReportYear, &row.ReportQuarter, &previous, &row.QuartersBetween,
			&row.Positions, &row.PortfolioValueUSD, &row.MovedPositions, &row.PositionsWithPnl,
			&row.PnlUSD, &row.CoveredValueUSD, &row.CoveragePct, &row.NewPositions,
			&row.AddedPositions, &row.TrimmedPositions, &row.ExitedPositions, &row.HeldPositions,
			&row.PurchasedUSD, &row.SoldUSD, &row.PurchasedPositions, &row.SoldPositions,
			&row.CumulativePnlUSD); err != nil {
			return err
		}
		row.Period = formatPeriod(report.Time)
		if previous.Valid {
			row.PrevPeriod = formatPeriod(previous.Time)
		}
		series = append(series, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "fund", err)
		return
	}

	// The quarter's marks, and beside them the same marks summed per CUSIP over
	// the fund's filings through this quarter — the fund's own cumulativePnlUsd
	// asked per position. A grouped aggregate and not a window, because the group
	// is this one fund's positions: a window over every fund's marks is the state
	// the build's memory limit rules out. The subquery renames its key so the
	// ranking below can go on naming bare columns.
	positionsQuery := fmt.Sprintf(`SELECT p.cusip, p.ticker, COALESCE(p.issuer, ''), p.action,
			p.shares, p.value_usd,
			p.portfolio_weight_pct, p.prev_shares, p.delta_shares, p.delta_value_usd, p.quarters_between,
			p.split_factor, p.split_adjusted, p.prev_vwap, p.cur_vwap, p.pnl_usd, p.pnl_pct, p.priced,
			cumulative.cumulative_pnl_usd
		FROM %s p
		LEFT JOIN (
			SELECT cusip AS position_cusip, SUM(pnl_usd) AS cumulative_pnl_usd
			FROM %s WHERE cik = ? AND report_period <= ?
			GROUP BY cusip
		) cumulative ON cumulative.position_cusip = p.cusip
		WHERE p.cik = ? AND p.report_period = ?
		ORDER BY %s LIMIT %d`, from, from, order, limit)
	positions := []FundPositionRow{}
	if err := a.thirteenRows(r.Context(), positionsQuery, []any{cik, period, cik, period}, func(rs *sql.Rows) error {
		var row FundPositionRow
		if err := rs.Scan(&row.Cusip, &row.Ticker, &row.Issuer, &row.Action, &row.Shares, &row.ValueUSD,
			&row.WeightPct, &row.PrevShares, &row.DeltaShares, &row.DeltaValueUSD, &row.QuartersBetween,
			&row.SplitFactor, &row.SplitAdjusted, &row.PrevVWAP, &row.VWAP, &row.PnlUSD, &row.PnlPct,
			&row.Priced, &row.CumulativePnlUSD); err != nil {
			return err
		}
		positions = append(positions, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "fund", err)
		return
	}

	var total int64
	if err := a.thirteenCount(r.Context(),
		fmt.Sprintf("SELECT count(*) FROM %s WHERE cik = ? AND report_period = ?", from),
		[]any{cik, period}, &total); err != nil {
		a.thirteenFailed(w, "fund", err)
		return
	}

	// The book the holdings panel draws: the filing's own positions, largest
	// first, with the whole book's count and value beside them. The panel needs
	// the book rather than this response's `positions`, which are the movement
	// rows — an exited position is a row of those with nothing left in it — and
	// it needs them whole rather than as the page of rows the table asked for,
	// because a ranking is not a composition.
	largestQuery := fmt.Sprintf(`SELECT cusip, ticker, COALESCE(issuer, ''), value_usd,
			portfolio_weight_pct
		FROM %s WHERE cik = ? AND report_period = ? ORDER BY value_usd DESC, cusip LIMIT %d`,
		bookFrom, holdingSlices)
	holdings := FundHoldings{Largest: []FundHoldingRow{}}
	if err := a.thirteenRows(r.Context(), largestQuery, []any{cik, period}, func(rs *sql.Rows) error {
		var row FundHoldingRow
		if err := rs.Scan(&row.Cusip, &row.Ticker, &row.Issuer, &row.ValueUSD, &row.WeightPct); err != nil {
			return err
		}
		holdings.Largest = append(holdings.Largest, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "fund", err)
		return
	}
	if err := a.thirteenCount(r.Context(),
		fmt.Sprintf("SELECT count(*) FROM %s WHERE cik = ? AND report_period = ?", bookFrom),
		[]any{cik, period}, &holdings.Positions); err != nil {
		a.thirteenFailed(w, "fund", err)
		return
	}
	// Summed without a COALESCE: a book whose filing stated no values has no
	// total, which the panel shows as a dash rather than as a book worth nothing.
	if err := a.thirteenCount(r.Context(),
		fmt.Sprintf("SELECT SUM(value_usd) FROM %s WHERE cik = ? AND report_period = ?", bookFrom),
		[]any{cik, period}, &holdings.ValueUSD); err != nil {
		a.thirteenFailed(w, "fund", err)
		return
	}
	// An aggregate rather than a row: a lake the extractor has not named has no
	// row for the fund at all, which leaves the name empty rather than failing.
	var filerName string
	if err := a.thirteenCount(r.Context(),
		fmt.Sprintf("SELECT COALESCE(max(filer_name), '') FROM %s WHERE cik = ?",
			thirteenFrom(names)), []any{cik}, &filerName); err != nil {
		a.thirteenFailed(w, "fund", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"cik": cik, "filerName": filerName, "period": period, "sort": sort,
		"quarters": quarters, "series": series, "total": total, "limit": limit,
		"positions": positions, "holdings": holdings,
	})
}

// thirteenfFlow is one fund's book over two filings, shaped for the flow diagram:
// the ends' own counts and values, the positions that moved either book, and one
// aggregate for the rest. The left side is the previous filing in the lake and the
// right side the requested quarter — the same pair position_quarter_pnl marks, so
// the diagram and the P&L panel answer for the same two filings — and the quarter
// is any the fund filed, so a reader can walk the whole series one transition at a
// time. A filer whose first filing is the requested one has no previous book: the
// response says so with a null `previous` and no bands, rather than drawing a book
// against a zero.
func (a *API) thirteenfFlow(w http.ResponseWriter, r *http.Request) {
	scores, ok := a.thirteenF.tablePath(w, thirteenConviction)
	if !ok {
		return
	}
	performance, ok := a.thirteenF.tablePath(w, thirteenPerformance)
	if !ok {
		return
	}
	names, ok := a.thirteenF.tablePath(w, thirteenFilerNames)
	if !ok {
		return
	}
	book, ok := a.thirteenF.tablePath(w, thirteenHoldings)
	if !ok {
		return
	}
	bookFrom, scoresFrom := thirteenFrom(book), thirteenFrom(scores)
	cik, ok := cikParam(w, r)
	if !ok {
		return
	}
	quarters, err := a.thirteenQuarters(r.Context(), thirteenFrom(performance), cik)
	if err != nil {
		a.thirteenFailed(w, "flow", err)
		return
	}
	if len(quarters) == 0 {
		writeError(w, http.StatusNotFound, "no filings for that filer; see /api/13f/funds")
		return
	}
	period, ok := thirteenPeriod(w, r, quarters)
	if !ok {
		return
	}
	if !oneOf(quarters, period) {
		writeError(w, http.StatusNotFound, "no filing for that filer in that period; see the quarters list")
		return
	}
	prevPeriod := ""
	for i, quarter := range quarters {
		if quarter == period && i > 0 {
			prevPeriod = quarters[i-1]
		}
	}

	bookOf := func(quarter string) (FlowBook, error) {
		var total FlowBook
		err := a.thirteenF.db.QueryRowContext(r.Context(),
			fmt.Sprintf(`SELECT count(*), COALESCE(SUM(value_usd), 0) FROM %s
				WHERE cik = ? AND report_period = ?`, bookFrom),
			cik, quarter).Scan(&total.Positions, &total.ValueUSD)
		return total, err
	}
	current, err := bookOf(period)
	if err != nil {
		a.thirteenFailed(w, "flow", err)
		return
	}
	var previous *FlowBook
	if prevPeriod != "" {
		left, err := bookOf(prevPeriod)
		if err != nil {
			a.thirteenFailed(w, "flow", err)
			return
		}
		previous = &left
	}

	// Every position of either book, with both values as filed. The view already
	// excludes a fund's first filing, so an empty result is one with no previous
	// book rather than a quarter that traded nothing.
	bandsQuery := fmt.Sprintf(`SELECT cusip, ticker, issuer, action, value_usd, prev_value_usd,
			portfolio_weight_pct, prev_portfolio_weight_pct, est_capital_flow, split_adjusted
		FROM %s WHERE cik = ? AND report_period = ?`, scoresFrom)
	type flowRow struct {
		FlowBand
		est sql.NullFloat64
	}
	rows := []flowRow{}
	if err := a.thirteenRows(r.Context(), bandsQuery, []any{cik, period}, func(rs *sql.Rows) error {
		var row flowRow
		if err := rs.Scan(&row.Cusip, &row.Ticker, &row.Issuer, &row.Action, &row.ValueUSD,
			&row.PrevValueUSD, &row.WeightPct, &row.PrevWeightPct, &row.est, &row.SplitAdjusted); err != nil {
			return err
		}
		if row.est.Valid {
			flow := row.est.Float64
			row.EstFlowUSD = &flow
		}
		rows = append(rows, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "flow", err)
		return
	}

	// The named set: the largest rows of each side, by the value that side reports.
	// A position that grew into the book and one that was sold out of it are both
	// named, which is the whole point of taking a union rather than one ranking.
	named := make([]bool, len(rows))
	rank := func(value func(flowRow) float64) {
		order := make([]int, len(rows))
		for i := range order {
			order[i] = i
		}
		sort.SliceStable(order, func(a, b int) bool {
			left, right := rows[order[a]], rows[order[b]]
			if value(left) != value(right) {
				return value(left) > value(right)
			}
			return left.Cusip < right.Cusip
		})
		for i, index := range order {
			if i >= flowSlices {
				break
			}
			named[index] = true
		}
	}
	rank(func(row flowRow) float64 { return row.ValueUSD })
	rank(func(row flowRow) float64 { return row.PrevValueUSD })

	positions := []FlowBand{}
	others := FlowOthers{}
	tailFlow := 0.0
	tailPriced := false
	for i, row := range rows {
		if named[i] {
			positions = append(positions, row.FlowBand)
			continue
		}
		others.PrevValueUSD += row.PrevValueUSD
		others.ValueUSD += row.ValueUSD
		if row.EstFlowUSD == nil {
			others.Unpriced++
		}
		if row.PrevValueUSD > 0 {
			others.PrevPositions++
		}
		if row.ValueUSD > 0 {
			others.Positions++
		}
		if row.EstFlowUSD != nil {
			tailFlow += *row.EstFlowUSD
			tailPriced = true
		}
	}
	if tailPriced {
		others.EstFlowUSD = &tailFlow
	}
	// Largest first by whichever side is bigger, so the rows of the response read
	// in the order a reader would point at them; the page ranks them again for its
	// own columns.
	sort.SliceStable(positions, func(a, b int) bool {
		left := max(positions[a].ValueUSD, positions[a].PrevValueUSD)
		right := max(positions[b].ValueUSD, positions[b].PrevValueUSD)
		if left != right {
			return left > right
		}
		return positions[a].Cusip < positions[b].Cusip
	})

	var filerName string
	if err := a.thirteenCount(r.Context(),
		fmt.Sprintf("SELECT COALESCE(max(filer_name), '') FROM %s WHERE cik = ?",
			thirteenFrom(names)), []any{cik}, &filerName); err != nil {
		a.thirteenFailed(w, "flow", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"cik": cik, "filerName": filerName, "period": period, "prevPeriod": prevPeriod,
		"quartersBetween": quartersApart(prevPeriod, period), "quarters": quarters,
		"slices": flowSlices, "previous": previous, "current": current,
		"positions": positions, "others": others,
	})
}

// ownerSlices is how many holders the roster names by default: the largest by
// portfolio weight, which is the one a reader is looking for when a lake holds
// hundreds of funds. The response carries the roster's true count, so a panel can
// say what the limit cut.
const ownerSlices = 100

// OwnerRow is one tracked fund's stake in one company as of one quarter: the
// position the filing reports, the move the fund's own previous filing makes of
// it, and the two figures a 13F does not file. A filing states no cost basis
// (thirteenf.sql deviation 11), so the basis is an estimate — the average VWAP of
// the quarters the fund bought this position at, over the window the lake covers
// and ignoring what it sold. estCostPerShare is that average, estCostUsd the same
// average applied to the shares held now — the basis of the position as it stands
// — and both are null for a fund that only held, or for a filing that stated no
// share count to apply the average to. `action` is empty, not HELD, when the
// fund's first filing in the lake is this quarter: there is no previous filing
// to compare against rather than no move. shares, valueUsd and weightPct are null
// for a filing that stated no share count or value, which is a filing the lake
// holds without the figures.
type OwnerRow struct {
	Cik             string   `json:"cik"`
	FilerName       string   `json:"filerName"`
	Shares          *float64 `json:"shares"`
	ValueUSD        *float64 `json:"valueUsd"`
	WeightPct       *float64 `json:"weightPct"`
	Action          string   `json:"action"`
	DeltaShares     float64  `json:"deltaShares"`
	DeltaWeightPct  float64  `json:"deltaWeightPct"`
	SplitAdjusted   bool     `json:"splitAdjusted"`
	BuyQuarters     int64    `json:"buyQuarters"`
	BoughtShares    float64  `json:"boughtShares"`
	EstCostPerShare *float64 `json:"estCostPerShare"`
	EstCostUSD      *float64 `json:"estCostUsd"`
}

// OwnerSummary is the ownership panel's header: what the tracked funds hold
// together, how much of the cohort's own book that is, and which way the quarter
// moved them. shares and valueUsd are the roster's holdings — the funds that
// still hold the name — while netShares is the quarter's flow across every fund
// in the lake, an exit included, so a name the whole cohort left is a negative
// flow with an empty roster. The three of them are null when no filing in the
// roster or the cohort stated the figures: a sum of filings that reported no
// values is not a zero. ownedPct divides the filed shares by the share count
// the market dataset reports for the quarter: that table carries shares
// outstanding, not float, so the panel names what it divides by. outstandingShares
// and ownedPct are absent when the dataset has no count for the symbol.
type OwnerSummary struct {
	Holders           int64    `json:"holders"`
	Shares            *float64 `json:"shares"`
	ValueUSD          *float64 `json:"valueUsd"`
	TrackedAumUSD     *float64 `json:"trackedAumUsd"`
	AumPct            *float64 `json:"aumPct"`
	OutstandingShares int64    `json:"outstandingShares,omitempty"`
	OwnedPct          *float64 `json:"ownedPct"`
	BoughtShares      float64  `json:"boughtShares"`
	SoldShares        float64  `json:"soldShares"`
	NetShares         float64  `json:"netShares"`
	BuyingFunds       int64    `json:"buyingFunds"`
	SellingFunds      int64    `json:"sellingFunds"`
	Exits             int64    `json:"exits"`
	// How many of the holders this roster serves have no estimated basis.
	UnpricedFunds int64 `json:"unpricedFunds"`
}

// thirteenfOwners answers the institutional side of the stocks page for one
// symbol: every tracked fund holding it in one quarter, ranked by how much of the
// fund's own book the position is, with the summary the panel's badges show. The
// quarter is the lake's newest — the stocks page has no quarter of its own, so
// the panel is a snapshot of what the filers last said — and the symbol is
// matched against the lake's ticker and, where the two sources spell a share
// class differently, its separator-stripped variant: the bundled CUSIP map writes
// BRKB where the market dataset writes BRK-B.
func (a *API) thirteenfOwners(w http.ResponseWriter, r *http.Request) {
	holdings, ok := a.thirteenF.tablePath(w, thirteenHoldings)
	if !ok {
		return
	}
	flows, ok := a.thirteenF.tablePath(w, thirteenFlows)
	if !ok {
		return
	}
	pnl, ok := a.thirteenF.tablePath(w, thirteenPositionPnl)
	if !ok {
		return
	}
	funds, ok := a.thirteenF.tablePath(w, thirteenFunds)
	if !ok {
		return
	}

	symbol := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("ticker")))
	if !validSymbol(symbol) {
		writeError(w, http.StatusBadRequest, "invalid ticker")
		return
	}
	limit := limitParam(r, ownerSlices, 1000)

	tickers := []any{symbol}
	if stripped := strings.ReplaceAll(symbol, "-", ""); stripped != symbol {
		tickers = append(tickers, stripped)
	}
	in := "IN (" + placeholders(len(tickers)) + ")"
	book, flowFrom, pnlFrom := thirteenFrom(holdings), thirteenFrom(flows), thirteenFrom(pnl)

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
			"SELECT max(report_period) FROM "+book, nil, &latest); err != nil {
			a.thirteenFailed(w, "owners", err)
			return
		}
		if latest.Valid {
			period = formatPeriod(latest.Time)
		}
	}
	// A lake with nothing in it has no quarter to answer for, and the panel says
	// so rather than failing on a null date.
	if period == "" {
		writeJSON(w, http.StatusOK, map[string]any{
			"symbol": symbol, "period": "", "limit": limit,
			"total": 0, "holders": []OwnerRow{}, "summary": OwnerSummary{},
		})
		return
	}

	// One argument list per side of the query below: the cost-basis aggregate
	// filters the same tickers through the same period the roster does.
	args := append(append([]any{}, tickers...), period)
	roster := fmt.Sprintf(`SELECT h.cik, COALESCE(n.filer_name, ''), h.shares, h.value_usd,
			h.portfolio_weight_pct, COALESCE(f.action, ''), COALESCE(f.delta_shares, 0),
			COALESCE(f.delta_weight_pct, 0), COALESCE(f.split_adjusted, false),
			COALESCE(p.buy_quarters, 0), COALESCE(p.bought_shares, 0), p.est_cost_per_share
		FROM %s h
		LEFT JOIN %s n ON n.cik = h.cik
		LEFT JOIN %s f
			ON f.cik = h.cik AND f.report_period = h.report_period AND f.cusip = h.cusip
		LEFT JOIN (
			SELECT cik, count(*) AS buy_quarters, SUM(delta_shares) AS bought_shares,
				ROUND(SUM(delta_shares * cur_vwap) / NULLIF(SUM(delta_shares), 0), 4)
					AS est_cost_per_share
			FROM %s
			WHERE ticker %s AND report_period <= ? AND action IN ('NEW', 'ADDED')
				AND cur_vwap IS NOT NULL
			GROUP BY cik
		) p ON p.cik = h.cik
		WHERE h.ticker %s AND h.report_period = ?
		ORDER BY h.portfolio_weight_pct DESC, h.value_usd DESC, h.cik
		LIMIT %d`,
		book, thirteenFrom(funds), flowFrom, pnlFrom, in, in, limit)
	rosterArgs := append(append([]any{}, args...), args...)

	holders := []OwnerRow{}
	err := a.thirteenRows(r.Context(), roster, rosterArgs, func(rs *sql.Rows) error {
		var row OwnerRow
		if err := rs.Scan(&row.Cik, &row.FilerName, &row.Shares, &row.ValueUSD, &row.WeightPct,
			&row.Action, &row.DeltaShares, &row.DeltaWeightPct, &row.SplitAdjusted,
			&row.BuyQuarters, &row.BoughtShares, &row.EstCostPerShare); err != nil {
			return err
		}
		if row.EstCostPerShare != nil && row.Shares != nil {
			basis := *row.EstCostPerShare * *row.Shares
			row.EstCostUSD = &basis
		}
		holders = append(holders, row)
		return nil
	})
	if err != nil {
		a.thirteenFailed(w, "owners", err)
		return
	}

	summary := OwnerSummary{}
	if err := a.thirteenRows(r.Context(), fmt.Sprintf(`SELECT count(*),
			SUM(h.shares), SUM(h.value_usd)
		FROM %s h WHERE h.ticker %s AND h.report_period = ?`, book, in), args,
		func(rs *sql.Rows) error {
			return rs.Scan(&summary.Holders, &summary.Shares, &summary.ValueUSD)
		}); err != nil {
		a.thirteenFailed(w, "owners", err)
		return
	}

	// The cohort's own book for the quarter: one row per filing, because the
	// positions of a filing each carry its portfolio total. Null when no filing
	// in the quarter stated a value, which is a cohort this panel cannot size.
	if err := a.thirteenCount(r.Context(), fmt.Sprintf(`SELECT SUM(value) FROM (
			SELECT DISTINCT cik, portfolio_value_total AS value FROM %s WHERE report_period = ?)`,
		book), []any{period}, &summary.TrackedAumUSD); err != nil {
		a.thirteenFailed(w, "owners", err)
		return
	}
	if summary.ValueUSD != nil && summary.TrackedAumUSD != nil && *summary.TrackedAumUSD > 0 {
		share := 100 * *summary.ValueUSD / *summary.TrackedAumUSD
		summary.AumPct = &share
	}

	// The quarter's flow across the whole cohort, which is not the roster's total:
	// a fund that left the name has a flow row with no position and belongs in the
	// net, and a fund that filed for the first time this quarter has neither.
	if err := a.thirteenRows(r.Context(), fmt.Sprintf(`SELECT
			count(*) FILTER (WHERE action IN ('NEW', 'ADDED')),
			count(*) FILTER (WHERE action IN ('TRIMMED', 'EXITED')),
			count(*) FILTER (WHERE action = 'EXITED'),
			COALESCE(SUM(delta_shares) FILTER (WHERE delta_shares > 0), 0),
			COALESCE(-SUM(delta_shares) FILTER (WHERE delta_shares < 0), 0),
			COALESCE(SUM(delta_shares), 0)
		FROM %s WHERE ticker %s AND report_period = ?`, flowFrom, in), args,
		func(rs *sql.Rows) error {
			return rs.Scan(&summary.BuyingFunds, &summary.SellingFunds, &summary.Exits,
				&summary.BoughtShares, &summary.SoldShares, &summary.NetShares)
		}); err != nil {
		a.thirteenFailed(w, "owners", err)
		return
	}

	for _, holder := range holders {
		if holder.EstCostUSD == nil {
			summary.UnpricedFunds++
		}
	}
	// The denominator comes from the market half of the app, which is the point of
	// this panel; a symbol the dataset has no count for keeps the badge empty
	// rather than dividing by a guess, and a roster that stated no share count
	// keeps it empty too.
	if outstanding, ok := a.dataset.SharesOutstanding(r.Context(), symbol, period); ok {
		summary.OutstandingShares = outstanding
		if summary.Shares != nil {
			owned := 100 * *summary.Shares / float64(outstanding)
			summary.OwnedPct = &owned
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"symbol": symbol, "period": period, "limit": limit,
		"total": summary.Holders, "summary": summary, "holders": holders,
	})
}

// ---------------------------------------------------------------------------
// The consensus board, the fund directory and the pairwise comparison read the
// lake as a whole rather than one filer. They are built from the tables the fund
// pages read and share their vocabulary: an action is one of thirteenActions, the
// dollar estimate of a move is est_capital_flow — the split-adjusted share change
// marked at the quarter's VWAP — and a weight is a filing's own
// portfolio_weight_pct. A 13F is long-only and carries no float: where an
// aggregate divides by a market figure the field names the denominator, and a
// share of the shares outstanding is not a share of a float.
// ---------------------------------------------------------------------------

// consensusSlices is how many names each list of the consensus board names by
// default; crowdSlices is how many of the quarter's largest positions the crowded
// radar ranks before it has a market share count to divide by; leaderboardRows is
// how many funds the directory lists at once; compareSlices is how many positions
// each list of a comparison names.
const (
	consensusSlices = 25
	crowdSlices     = 100
	leaderboardRows = 50
	compareSlices   = 50
)

// AccumulationRow is one company that opened positions in a quarter: how many
// filers opened one, what they opened, and the largest weight any of them gave
// it. boughtUsd is the shares opened marked at the quarter's VWAP and is null
// when the price dataset covers none of them; unpriced says how many openings
// that is.
type AccumulationRow struct {
	Ticker           string   `json:"ticker"`
	Issuer           string   `json:"issuer"`
	Funds            int64    `json:"funds"`
	Shares           float64  `json:"shares"`
	ValueUSD         float64  `json:"valueUsd"`
	BoughtUSD        *float64 `json:"boughtUsd"`
	Unpriced         int64    `json:"unpriced"`
	LargestWeightPct float64  `json:"largestWeightPct"`
}

// LiquidationRow is one company a set of filers left entirely in a quarter: how
// many filers exited, what they held in the previous filing, and what that was
// worth then. soldUsd is the position they left marked at the quarter's VWAP —
// the value that came out of the name over the quarter — and it is null when the
// price dataset covers none of the exits.
type LiquidationRow struct {
	Ticker       string   `json:"ticker"`
	Issuer       string   `json:"issuer"`
	Funds        int64    `json:"funds"`
	PrevShares   float64  `json:"prevShares"`
	PrevValueUSD float64  `json:"prevValueUsd"`
	SoldUSD      *float64 `json:"soldUsd"`
	Unpriced     int64    `json:"unpriced"`
}

// NetVolumeRow is one company's flow across every filer in the quarter: the net
// share change and what it is worth at the quarter's VWAP, positive for a net
// buy, with the counts of the funds on each side. netUsd is null when the price
// dataset covers none of the moves in the name.
type NetVolumeRow struct {
	Ticker    string   `json:"ticker"`
	Issuer    string   `json:"issuer"`
	Funds     int64    `json:"funds"`
	Buyers    int64    `json:"buyers"`
	Sellers   int64    `json:"sellers"`
	NetShares float64  `json:"netShares"`
	NetUSD    *float64 `json:"netUsd"`
	Unpriced  int64    `json:"unpriced"`
}

// CrowdedRow is one company the tracked funds own a large share of. shares is
// what the cohort filed this quarter; outstandingShares is the company's own
// count from the market dataset at that date and ownedPct is the filed shares as
// a percentage of it — a share of the shares outstanding, which is the only
// denominator either source carries, not a share of a float. Both the count and
// the share are absent when the dataset has no count for the symbol at that date,
// and the share counts only the tracked funds, so it is what this lake sees and
// not the company's whole register. shares and valueUsd are null for a company
// whose only filings in the quarter stated no share count or value.
type CrowdedRow struct {
	Ticker            string   `json:"ticker"`
	Issuer            string   `json:"issuer"`
	Funds             int64    `json:"funds"`
	Shares            *float64 `json:"shares"`
	ValueUSD          *float64 `json:"valueUsd"`
	OutstandingShares int64    `json:"outstandingShares,omitempty"`
	OwnedPct          *float64 `json:"ownedPct"`
}

// crowdedTickers is the market symbols a crowded radar asks the price dataset
// about: one per company, in the lake's own spelling.
func crowdedTickers(rows []CrowdedRow) []string {
	symbols := make([]string, 0, len(rows))
	for _, row := range rows {
		symbols = append(symbols, row.Ticker)
	}
	return symbols
}

// marketKey is a ticker as the market dataset keys a company: upper case and
// without the share-class separator, which is how the two sources are
// reconciled — the bundled CUSIP map writes BRKB where the dataset writes BRK-B.
func marketKey(ticker string) string {
	return strings.ReplaceAll(strings.ToUpper(strings.TrimSpace(ticker)), "-", "")
}

// lakeQuarters lists every report period in the lake, newest first, so a
// lake-wide panel can offer the quarter selector a fund page offers. It is not
// thirteenQuarters: that one is one filer's quarters, oldest first, which is the
// order a series is drawn in.
func (a *API) lakeQuarters(ctx context.Context, from string) ([]string, error) {
	quarters := []string{}
	err := a.thirteenRows(ctx, "SELECT DISTINCT report_period FROM "+from+" ORDER BY report_period DESC",
		nil, func(rs *sql.Rows) error {
			var period time.Time
			if err := rs.Scan(&period); err != nil {
				return err
			}
			quarters = append(quarters, formatPeriod(period))
			return nil
		})
	return quarters, err
}

// thirteenfConsensus answers the lake's quarter in four lists: the companies the
// most filers opened this quarter, the companies the most filers left, the
// largest net flows in either direction, and the companies the cohort owns the
// largest share of. The quarter is the lake's newest, because the board is a
// snapshot of what the filers last said, and the response carries every quarter
// the lake holds so a page can offer the same selector a fund page offers.
func (a *API) thirteenfConsensus(w http.ResponseWriter, r *http.Request) {
	holdings, ok := a.thirteenF.tablePath(w, thirteenHoldings)
	if !ok {
		return
	}
	signals, ok := a.thirteenF.tablePath(w, thirteenConviction)
	if !ok {
		return
	}
	limit := limitParam(r, consensusSlices, 200)

	book, signalFrom := thirteenFrom(holdings), thirteenFrom(signals)

	quarters, err := a.lakeQuarters(r.Context(), book)
	if err != nil {
		a.thirteenFailed(w, "consensus", err)
		return
	}

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
			"SELECT max(report_period) FROM "+book, nil, &latest); err != nil {
			a.thirteenFailed(w, "consensus", err)
			return
		}
		if latest.Valid {
			period = formatPeriod(latest.Time)
		}
	}
	// What the limit cut, per list, in one pass over the quarter's flows: the
	// lists cap at `limit` and a panel can say how many names there were.
	var accumulating, liquidated, moved, crowdedNames int64
	if err := a.thirteenRows(r.Context(), fmt.Sprintf(`SELECT
			count(DISTINCT ticker) FILTER (WHERE action = 'NEW'),
			count(DISTINCT ticker) FILTER (WHERE action = 'EXITED'),
			count(DISTINCT ticker)
		FROM %s WHERE report_period = ?`, signalFrom), []any{period},
		func(rs *sql.Rows) error {
			return rs.Scan(&accumulating, &liquidated, &moved)
		}); err != nil {
		a.thirteenFailed(w, "consensus", err)
		return
	}
	if err := a.thirteenCount(r.Context(), fmt.Sprintf(
		"SELECT count(DISTINCT ticker) FROM %s WHERE report_period = ?", book),
		[]any{period}, &crowdedNames); err != nil {
		a.thirteenFailed(w, "consensus", err)
		return
	}
	totals := map[string]int64{
		"accumulations": accumulating, "liquidations": liquidated,
		"netVolume": moved, "crowded": crowdedNames,
	}

	// A company initiated by the largest number of distinct funds: NEW is a
	// position the fund's previous filing did not have, so the count of openings
	// is the count of the funds that opened one.
	accumulations := []AccumulationRow{}
	if err := a.thirteenRows(r.Context(), fmt.Sprintf(`SELECT ticker, COALESCE(arg_max(issuer, value_usd), ''),
			count(DISTINCT cik), SUM(shares), SUM(value_usd), SUM(est_capital_flow),
			count(*) FILTER (WHERE est_capital_flow IS NULL), MAX(portfolio_weight_pct)
		FROM %s WHERE report_period = ? AND action = 'NEW'
		GROUP BY ticker
		ORDER BY count(DISTINCT cik) DESC, SUM(est_capital_flow) DESC NULLS LAST,
			SUM(value_usd) DESC, ticker
		LIMIT %d`, signalFrom, limit), []any{period}, func(rs *sql.Rows) error {
		var row AccumulationRow
		if err := rs.Scan(&row.Ticker, &row.Issuer, &row.Funds, &row.Shares, &row.ValueUSD,
			&row.BoughtUSD, &row.Unpriced, &row.LargestWeightPct); err != nil {
			return err
		}
		accumulations = append(accumulations, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "consensus", err)
		return
	}

	// A company the largest number of distinct funds left: the previous filing's
	// shares and value are what the exits gave up, and the sales are that position
	// marked at this quarter's VWAP.
	liquidations := []LiquidationRow{}
	if err := a.thirteenRows(r.Context(), fmt.Sprintf(`SELECT ticker, COALESCE(arg_max(issuer, prev_value_usd), ''),
			count(DISTINCT cik), SUM(prev_shares), SUM(prev_value_usd),
			-SUM(est_capital_flow), count(*) FILTER (WHERE est_capital_flow IS NULL)
		FROM %s WHERE report_period = ? AND action = 'EXITED'
		GROUP BY ticker
		ORDER BY count(DISTINCT cik) DESC, SUM(prev_value_usd) DESC NULLS LAST, ticker
		LIMIT %d`, signalFrom, limit), []any{period}, func(rs *sql.Rows) error {
		var row LiquidationRow
		if err := rs.Scan(&row.Ticker, &row.Issuer, &row.Funds, &row.PrevShares, &row.PrevValueUSD,
			&row.SoldUSD, &row.Unpriced); err != nil {
			return err
		}
		liquidations = append(liquidations, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "consensus", err)
		return
	}

	// The quarter's net flow per company, ranked by how much money moved either
	// way: the sign is in the row, so the top of this list is the quarter's largest
	// inflow and the bottom its largest outflow.
	netVolume := []NetVolumeRow{}
	if err := a.thirteenRows(r.Context(), fmt.Sprintf(`SELECT ticker, COALESCE(arg_max(issuer, value_usd), ''),
			count(DISTINCT cik),
			count(DISTINCT cik) FILTER (WHERE action IN ('NEW', 'ADDED')),
			count(DISTINCT cik) FILTER (WHERE action IN ('TRIMMED', 'EXITED')),
			SUM(delta_shares), SUM(est_capital_flow),
			count(*) FILTER (WHERE est_capital_flow IS NULL)
		FROM %s WHERE report_period = ?
		GROUP BY ticker
		ORDER BY abs(SUM(est_capital_flow)) DESC NULLS LAST, SUM(value_usd) DESC, ticker
		LIMIT %d`, signalFrom, limit), []any{period}, func(rs *sql.Rows) error {
		var row NetVolumeRow
		if err := rs.Scan(&row.Ticker, &row.Issuer, &row.Funds, &row.Buyers, &row.Sellers,
			&row.NetShares, &row.NetUSD, &row.Unpriced); err != nil {
			return err
		}
		netVolume = append(netVolume, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "consensus", err)
		return
	}

	// The crowded radar: the quarter's largest tracked positions first, because
	// the share of the company is only known once the market dataset has been
	// asked, and the ranking is by that share. A company the dataset cannot price
	// a count for keeps a null share and sorts last.
	crowded := []CrowdedRow{}
	if err := a.thirteenRows(r.Context(), fmt.Sprintf(`SELECT ticker, COALESCE(arg_max(issuer, value_usd), ''),
			count(DISTINCT cik), SUM(shares), SUM(value_usd)
		FROM %s WHERE report_period = ?
		GROUP BY ticker
		ORDER BY SUM(value_usd) DESC, ticker
		LIMIT %d`, book, crowdSlices), []any{period}, func(rs *sql.Rows) error {
		var row CrowdedRow
		if err := rs.Scan(&row.Ticker, &row.Issuer, &row.Funds, &row.Shares, &row.ValueUSD); err != nil {
			return err
		}
		crowded = append(crowded, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "consensus", err)
		return
	}
	counts, err := a.dataset.SharesOutstandingFor(r.Context(), crowdedTickers(crowded), period)
	if err != nil {
		// The radar's denominator comes from the market half of the app: a dataset
		// that cannot answer leaves the column empty, as the ownership panel does,
		// rather than failing the three lists that never left the lake.
		log.Printf("13f consensus: share counts unavailable: %v", err)
		counts = map[string]int64{}
	}
	for i := range crowded {
		count, found := counts[marketKey(crowded[i].Ticker)]
		if !found || crowded[i].Shares == nil {
			continue
		}
		crowded[i].OutstandingShares = count
		owned := 100 * *crowded[i].Shares / float64(count)
		crowded[i].OwnedPct = &owned
	}
	sort.SliceStable(crowded, func(i, j int) bool {
		left, right := crowded[i].OwnedPct, crowded[j].OwnedPct
		switch {
		case left == nil && right == nil:
			return crowded[i].ValueUSD != nil && crowded[j].ValueUSD != nil &&
				*crowded[i].ValueUSD > *crowded[j].ValueUSD
		case left == nil:
			return false
		case right == nil:
			return true
		default:
			return *left > *right
		}
	})
	crowded = crowded[:min(limit, len(crowded))]

	writeJSON(w, http.StatusOK, map[string]any{
		"period": period, "limit": limit, "quarters": quarters, "totals": totals,
		"newAccumulations": accumulations, "liquidations": liquidations,
		"netVolume": netVolume, "crowded": crowded,
	})
}

// leaderboardSorts are the directory's rankings, the first one being its
// default. A null never outranks a number in either direction: an estimate the
// price dataset could not make is a hole in the estimate, not a faultless fund.
var leaderboardSorts = []struct{ name, order string }{
	{"aum", "f.latest_value_usd DESC NULLS LAST, f.cik"},
	{"concentration", "t.top10_pct DESC NULLS LAST, f.latest_value_usd DESC"},
	{"turnover", "turnover_pct DESC NULLS LAST, f.latest_value_usd DESC"},
	{"quarter", "quarter_pnl_pct DESC NULLS LAST, f.latest_value_usd DESC"},
	{"year", "year_pnl_pct DESC NULLS LAST, f.latest_value_usd DESC"},
	{"threeyear", "three_year_pnl_pct DESC NULLS LAST, f.latest_value_usd DESC"},
	{"name", "f.filer_name ASC NULLS LAST, f.cik"},
	{"filings", "f.quarters DESC NULLS LAST, f.cik"},
}

// LeaderboardRow is one fund in the directory: the portfolio of its newest
// filing, how concentrated that book is, how much of it it turned over, and what
// the marks did — the quarter's, the last four filings', and the last twelve's.
// An estimate covers the positions the price dataset could mark, which is why
// coveragePct rides along: quarterPnlPct is the quarter's marks against the
// covered value rather than against the whole book, and the two aggregate
// percentages are what those quarters added to the newest filing's covered value.
// yearFilings and threeYearFilings count the filings in the window that could be
// marked at all, so a fund the lake holds three filings of does not read as a
// three-year record. A 13F files positions, never transactions, so turnoverPct is
// the estimated one: the quarter's purchases and sales at the quarter's VWAP,
// halved, over the filing's total. status is "current" when the fund filed the
// lake's newest quarter and "stale" otherwise, and quartersSinceLatest counts the
// gap between the two.
type LeaderboardRow struct {
	Cik          string `json:"cik"`
	FilerName    string `json:"filerName"`
	Quarters     int64  `json:"quarters"`
	FirstPeriod  string `json:"firstPeriod"`
	LatestPeriod string `json:"latestPeriod"`
	// The newest filing's portfolio; null for a filing that reported no values,
	// which is what the column shows as a dash. Every other figure here is an
	// estimate from a filing that can be marked, or a count.
	LatestValueUSD      *float64 `json:"latestValueUsd"`
	LatestPositions     int64    `json:"latestPositions"`
	Top10Pct            *float64 `json:"top10Pct"`
	TurnoverPct         *float64 `json:"turnoverPct"`
	QuarterPnlUSD       *float64 `json:"quarterPnlUsd"`
	QuarterPnlPct       *float64 `json:"quarterPnlPct"`
	YearPnlUSD          *float64 `json:"yearPnlUsd"`
	YearPnlPct          *float64 `json:"yearPnlPct"`
	YearFilings         int64    `json:"yearFilings"`
	ThreeYearPnlUSD     *float64 `json:"threeYearPnlUsd"`
	ThreeYearPnlPct     *float64 `json:"threeYearPnlPct"`
	ThreeYearFilings    int64    `json:"threeYearFilings"`
	CoveragePct         *float64 `json:"coveragePct"`
	Status              string   `json:"status"`
	QuartersSinceLatest int64    `json:"quartersSinceLatest"`
}

// thirteenfLeaderboard is the fund directory: every filer in the lake with the
// figures a screen on funds wants, ranked by one of leaderboardSorts. The
// estimates come from fund_quarterly_performance, whose pnl_usd is null on a
// filing with no previous one — the first filing in the lake has nothing to mark
// — so a window's totals are the quarters it could actually mark.
func (a *API) thirteenfLeaderboard(w http.ResponseWriter, r *http.Request) {
	funds, ok := a.thirteenF.tablePath(w, thirteenFunds)
	if !ok {
		return
	}
	performance, ok := a.thirteenF.tablePath(w, thirteenPerformance)
	if !ok {
		return
	}
	holdings, ok := a.thirteenF.tablePath(w, thirteenHoldings)
	if !ok {
		return
	}
	sort, order, ok := sortFrom(w, r.URL.Query().Get("sort"), leaderboardSorts)
	if !ok {
		return
	}
	limit := limitParam(r, leaderboardRows, 1000)

	book, performanceFrom := thirteenFrom(holdings), thirteenFrom(performance)
	fundFrom := thirteenFrom(funds)

	// The lake's newest filing is what "current" is measured against, and it is
	// the funds view's own latest_period rather than a date spelled here.
	var newest sql.NullTime
	if err := a.thirteenCount(r.Context(), "SELECT max(latest_period) FROM "+fundFrom, nil, &newest); err != nil {
		a.thirteenFailed(w, "leaderboard", err)
		return
	}
	lake := ""
	if newest.Valid {
		lake = formatPeriod(newest.Time)
	}

	// Two window questions the views do not answer: how much of a book its ten
	// largest positions are, and what the trailing four and twelve filings added.
	// The marks come from fund_quarterly_performance, one row per filing, and the
	// windows are the filings themselves — a fund that skips a quarter has fewer
	// rows in the window, which is what yearFilings and threeYearFilings report.
	query := fmt.Sprintf(`WITH top10 AS (
			SELECT cik, report_period, SUM(portfolio_weight_pct) AS top10_pct FROM (
				SELECT cik, report_period, portfolio_weight_pct,
					row_number() OVER (PARTITION BY cik, report_period
						ORDER BY value_usd DESC, cusip) AS rk
				FROM %s
			) WHERE rk <= 10
			GROUP BY cik, report_period
		),
		marks AS (
			SELECT cik, report_period, pnl_usd, purchased_usd, sold_usd, portfolio_value_total,
				covered_value_usd, coverage_pct,
				SUM(pnl_usd) OVER w4 AS year_pnl_usd,
				count(pnl_usd) OVER w4 AS year_filings,
				SUM(pnl_usd) OVER w12 AS three_year_pnl_usd,
				count(pnl_usd) OVER w12 AS three_year_filings
			FROM %s
			WINDOW
				w4 AS (PARTITION BY cik ORDER BY report_period
					ROWS BETWEEN 3 PRECEDING AND CURRENT ROW),
				w12 AS (PARTITION BY cik ORDER BY report_period
					ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
		)
		SELECT f.cik, COALESCE(f.filer_name, ''), f.quarters, f.first_period, f.latest_period,
			f.latest_value_usd, f.latest_positions, t.top10_pct,
			100.0 * (m.purchased_usd + m.sold_usd) / 2.0 / NULLIF(m.portfolio_value_total, 0)
				AS turnover_pct,
			m.pnl_usd, 100.0 * m.pnl_usd / NULLIF(m.covered_value_usd, 0) AS quarter_pnl_pct,
			m.year_pnl_usd, 100.0 * m.year_pnl_usd / NULLIF(m.covered_value_usd, 0) AS year_pnl_pct,
			COALESCE(m.year_filings, 0),
			m.three_year_pnl_usd,
			100.0 * m.three_year_pnl_usd / NULLIF(m.covered_value_usd, 0) AS three_year_pnl_pct,
			COALESCE(m.three_year_filings, 0), m.coverage_pct
		FROM %s f
		LEFT JOIN top10 t ON t.cik = f.cik AND t.report_period = f.latest_period
		LEFT JOIN marks m ON m.cik = f.cik AND m.report_period = f.latest_period
		ORDER BY %s
		LIMIT %d`, book, performanceFrom, fundFrom, order, limit)

	rows := []LeaderboardRow{}
	if err := a.thirteenRows(r.Context(), query, nil, func(rs *sql.Rows) error {
		var row LeaderboardRow
		var first, latest time.Time
		if err := rs.Scan(&row.Cik, &row.FilerName, &row.Quarters, &first, &latest,
			&row.LatestValueUSD, &row.LatestPositions, &row.Top10Pct, &row.TurnoverPct,
			&row.QuarterPnlUSD, &row.QuarterPnlPct, &row.YearPnlUSD, &row.YearPnlPct,
			&row.YearFilings, &row.ThreeYearPnlUSD, &row.ThreeYearPnlPct,
			&row.ThreeYearFilings, &row.CoveragePct); err != nil {
			return err
		}
		row.FirstPeriod = formatPeriod(first)
		row.LatestPeriod = formatPeriod(latest)
		row.QuartersSinceLatest = quartersApart(row.LatestPeriod, lake)
		row.Status = "stale"
		if row.LatestPeriod == lake {
			row.Status = "current"
		}
		rows = append(rows, row)
		return nil
	}); err != nil {
		a.thirteenFailed(w, "leaderboard", err)
		return
	}

	total := int64(0)
	if err := a.thirteenCount(r.Context(), "SELECT count(*) FROM "+fundFrom, nil, &total); err != nil {
		a.thirteenFailed(w, "leaderboard", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"sort": sort, "limit": limit, "total": total, "period": lake, "funds": rows,
	})
}

// CompareFund is one side of a comparison: the filer's newest filing in the lake
// and what it filed at the quarter being compared. inPeriod is false when the
// fund filed nothing that quarter — a hand-picked quarter can be one the other
// side skipped — so a page can tell an empty book from an unread one. valueUsd is
// null when the quarter's filing listed positions without values, which is a book
// the lake holds but cannot price.
type CompareFund struct {
	Cik          string   `json:"cik"`
	FilerName    string   `json:"filerName"`
	Quarters     int64    `json:"quarters"`
	LatestPeriod string   `json:"latestPeriod"`
	Positions    int64    `json:"positions"`
	ValueUSD     *float64 `json:"valueUsd"`
	InPeriod     bool     `json:"inPeriod"`
}

// CompareOverlap is the ground two books share: the positions in common, the
// Jaccard similarity of the two books, and the capital each side holds in common
// or alone. Positions are matched on CUSIP, which is the identity a filing uses;
// sharedPositions counts the intersection and unionPositions the union, so
// jaccardPct is the intersection over the union — the counts are what a filing
// always states, and they are why a pair whose values are missing still has an
// overlap. sharedValueUsd is the sum of the smaller value in each shared position
// where both sides stated one, so it is null when no shared position could be
// priced, and the two unique columns are each book minus that, null where the
// book's own total is unknown. A 13F is long-only, so what overlaps is long
// positions: the lake holds no short book to compare.
type CompareOverlap struct {
	SharedPositions int64    `json:"sharedPositions"`
	UnionPositions  int64    `json:"unionPositions"`
	JaccardPct      float64  `json:"jaccardPct"`
	SharedValueUSD  *float64 `json:"sharedValueUsd"`
	AOnlyValueUSD   *float64 `json:"aOnlyValueUsd"`
	BOnlyValueUSD   *float64 `json:"bOnlyValueUsd"`
	SharedPctOfA    *float64 `json:"sharedPctOfA"`
	SharedPctOfB    *float64 `json:"sharedPctOfB"`
}

// ComparePosition is one position of a shared or a contrarian list: what each
// side holds in the company, the weight it is of that side's own book, the move
// each side made this quarter, and the estimated basis of each side's position —
// the average VWAP of the quarters that fund bought the CUSIP at, the same
// estimate the ownership panel makes, and null for a fund that only held. buyer
// names the side that added in a contrarian row ("a" or "b") and is empty in a
// shared one. A position one side exited has no row in the quarter's book, so its
// shares, value and weight are zero there while the move stays in the record;
// they are null instead where that side has a row but stated no share count or
// value, which leaves the row with an action and nothing to size.
type ComparePosition struct {
	Cusip            string   `json:"cusip"`
	Ticker           string   `json:"ticker"`
	Issuer           string   `json:"issuer"`
	AShares          *float64 `json:"aShares"`
	BShares          *float64 `json:"bShares"`
	AValueUSD        *float64 `json:"aValueUsd"`
	BValueUSD        *float64 `json:"bValueUsd"`
	AWeightPct       *float64 `json:"aWeightPct"`
	BWeightPct       *float64 `json:"bWeightPct"`
	AAction          string   `json:"aAction"`
	BAction          string   `json:"bAction"`
	ADeltaShares     float64  `json:"aDeltaShares"`
	BDeltaShares     float64  `json:"bDeltaShares"`
	ADeltaWeightPct  float64  `json:"aDeltaWeightPct"`
	BDeltaWeightPct  float64  `json:"bDeltaWeightPct"`
	AEstCostPerShare *float64 `json:"aEstCostPerShare"`
	BEstCostPerShare *float64 `json:"bEstCostPerShare"`
	Buyer            string   `json:"buyer,omitempty"`
}

// thirteenfCompare answers one quarter of two funds side by side: what each filed,
// the ground the two books share, the positions in common with each side's weight
// and estimated basis, and the positions one side is buying while the other sells.
// The quarter defaults to the newest both funds filed, and the response carries
// every quarter they both filed so a page can offer a selector; a pair with no
// quarter in common answers with no period and empty lists.
//
// "Buying" and "selling" are the filing's own actions: an action is NEW, ADDED,
// TRIMMED, EXITED or HELD. There is no short side to compare, because a 13F
// reports long positions only.
func (a *API) thirteenfCompare(w http.ResponseWriter, r *http.Request) {
	holdings, ok := a.thirteenF.tablePath(w, thirteenHoldings)
	if !ok {
		return
	}
	flows, ok := a.thirteenF.tablePath(w, thirteenFlows)
	if !ok {
		return
	}
	pnl, ok := a.thirteenF.tablePath(w, thirteenPositionPnl)
	if !ok {
		return
	}
	funds, ok := a.thirteenF.tablePath(w, thirteenFunds)
	if !ok {
		return
	}
	aCik, okA := cikNamedParam(w, r, "a")
	if !okA {
		return
	}
	bCik, okB := cikNamedParam(w, r, "b")
	if !okB {
		return
	}
	limit := limitParam(r, compareSlices, 500)

	book, flowFrom, pnlFrom := thirteenFrom(holdings), thirteenFrom(flows), thirteenFrom(pnl)

	sides := map[string]*CompareFund{}
	if err := a.thirteenRows(r.Context(), fmt.Sprintf(
		"SELECT cik, COALESCE(filer_name, ''), quarters, latest_period FROM %s WHERE cik IN (?, ?)",
		thirteenFrom(funds)), []any{aCik, bCik}, func(rs *sql.Rows) error {
		var latest time.Time
		side := CompareFund{}
		if err := rs.Scan(&side.Cik, &side.FilerName, &side.Quarters, &latest); err != nil {
			return err
		}
		side.LatestPeriod = formatPeriod(latest)
		sides[side.Cik] = &side
		return nil
	}); err != nil {
		a.thirteenFailed(w, "compare", err)
		return
	}
	for _, cik := range []string{aCik, bCik} {
		if _, found := sides[cik]; !found {
			writeError(w, http.StatusNotFound, "no fund "+cik+" in the lake")
			return
		}
	}

	// The quarters both funds filed, newest first: the comparison is of one
	// quarter, and a quarter only one of them filed is not a comparison.
	quarters := []string{}
	if err := a.thirteenRows(r.Context(), fmt.Sprintf(`SELECT DISTINCT a.report_period
		FROM %s a JOIN %s b ON b.report_period = a.report_period
		WHERE a.cik = ? AND b.cik = ?
		ORDER BY a.report_period DESC`, book, book), []any{aCik, bCik},
		func(rs *sql.Rows) error {
			var period time.Time
			if err := rs.Scan(&period); err != nil {
				return err
			}
			quarters = append(quarters, formatPeriod(period))
			return nil
		}); err != nil {
		a.thirteenFailed(w, "compare", err)
		return
	}

	var period string
	if raw := strings.TrimSpace(r.URL.Query().Get("period")); raw != "" && !strings.EqualFold(raw, "latest") {
		period, ok = normalisePeriod(raw)
		if !ok {
			writeError(w, http.StatusBadRequest, "period must be YYYY-MM-DD or YYYYQn")
			return
		}
	} else if len(quarters) > 0 {
		period = quarters[0]
	}
	// A pair that shares no quarter has no quarter to compare, and the queries
	// below bind the period as a date — which an empty string is not.
	if period == "" {
		writeJSON(w, http.StatusOK, map[string]any{
			"period": "", "limit": limit, "quarters": quarters,
			"a": sides[aCik], "b": sides[bCik], "overlap": CompareOverlap{},
			"shared": []ComparePosition{}, "contrarian": []ComparePosition{}, "contrarianTotal": 0,
		})
		return
	}

	// What each side filed at the quarter, and the overlap of the two books: the
	// intersection and the union are counted on CUSIP, the identity a filing uses,
	// and the shared capital is the sum of the smaller value in each shared
	// position. The cost-basis aggregate is the average VWAP of the quarters a fund
	// bought a CUSIP at, matched per fund and CUSIP because the two sides have
	// their own basis in the same company.
	basis := fmt.Sprintf(`SELECT cik, cusip,
			ROUND(SUM(delta_shares * cur_vwap) / NULLIF(SUM(delta_shares), 0), 4)
				AS est_cost_per_share
		FROM %s
		WHERE cik IN (?, ?) AND report_period <= ? AND action IN ('NEW', 'ADDED')
			AND cur_vwap IS NOT NULL
		GROUP BY cik, cusip`, pnlFrom)

	totals := struct {
		APositions, BPositions, SharedPositions, UnionPositions int64
		SharedValue, AValue, BValue                             *float64
	}{}
	// The shared value of a position is the smaller of the two filings' values,
	// and it is unknown while either side's is: DuckDB's LEAST skips a null
	// instead of returning one, which would let one side's value stand in for a
	// part both funds hold.
	if err := a.thirteenRows(r.Context(), fmt.Sprintf(`WITH a AS (
			SELECT cusip, value_usd FROM %s WHERE cik = ? AND report_period = ?
		), b AS (
			SELECT cusip, value_usd FROM %s WHERE cik = ? AND report_period = ?
		), shared AS (
			SELECT a.cusip, CASE WHEN a.value_usd IS NULL OR b.value_usd IS NULL THEN NULL
				ELSE LEAST(a.value_usd, b.value_usd) END AS value
			FROM a JOIN b USING (cusip)
		)
		SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM b), (SELECT count(*) FROM shared),
			(SELECT count(*) FROM (SELECT cusip FROM a UNION SELECT cusip FROM b)),
			(SELECT SUM(value) FROM shared),
			(SELECT SUM(value_usd) FROM a),
			(SELECT SUM(value_usd) FROM b)`, book, book),
		[]any{aCik, period, bCik, period}, func(rs *sql.Rows) error {
			return rs.Scan(&totals.APositions, &totals.BPositions, &totals.SharedPositions,
				&totals.UnionPositions, &totals.SharedValue, &totals.AValue, &totals.BValue)
		}); err != nil {
		a.thirteenFailed(w, "compare", err)
		return
	}
	// A book minus what the two hold in common: the difference needs both
	// numbers, so a side of the comparison that filed no values has no
	// alone-value either.
	difference := func(whole, shared *float64) *float64 {
		if whole == nil || shared == nil {
			return nil
		}
		value := *whole - *shared
		return &value
	}
	overlap := CompareOverlap{
		SharedPositions: totals.SharedPositions,
		UnionPositions:  totals.UnionPositions,
		SharedValueUSD:  totals.SharedValue,
		AOnlyValueUSD:   difference(totals.AValue, totals.SharedValue),
		BOnlyValueUSD:   difference(totals.BValue, totals.SharedValue),
	}
	if totals.UnionPositions > 0 {
		overlap.JaccardPct = 100 * float64(totals.SharedPositions) / float64(totals.UnionPositions)
	}
	if totals.AValue != nil && totals.SharedValue != nil && *totals.AValue > 0 {
		share := 100 * *totals.SharedValue / *totals.AValue
		overlap.SharedPctOfA = &share
	}
	if totals.BValue != nil && totals.SharedValue != nil && *totals.BValue > 0 {
		share := 100 * *totals.SharedValue / *totals.BValue
		overlap.SharedPctOfB = &share
	}
	for _, side := range []*CompareFund{sides[aCik], sides[bCik]} {
		if side.Cik == aCik {
			side.Positions, side.ValueUSD = totals.APositions, totals.AValue
		} else {
			side.Positions, side.ValueUSD = totals.BPositions, totals.BValue
		}
		side.InPeriod = side.Positions > 0
	}

	// The positions in common, ranked by the weight the position carries for
	// whichever fund holds more of it: a name is high conviction if it is a large
	// part of either book.
	shared := []ComparePosition{}
	if err := a.thirteenRows(r.Context(), fmt.Sprintf(`SELECT a.cusip, a.ticker,
			COALESCE(a.issuer, fa.issuer, ''),
			a.shares, b.shares, a.value_usd, b.value_usd,
			a.portfolio_weight_pct, b.portfolio_weight_pct,
			COALESCE(fa.action, ''), COALESCE(fb.action, ''),
			COALESCE(fa.delta_shares, 0), COALESCE(fb.delta_shares, 0),
			COALESCE(fa.delta_weight_pct, 0), COALESCE(fb.delta_weight_pct, 0),
			pa.est_cost_per_share, pb.est_cost_per_share
		FROM %s a
		JOIN %s b ON b.cusip = a.cusip AND b.report_period = a.report_period
		LEFT JOIN %s fa ON fa.cik = a.cik AND fa.report_period = a.report_period AND fa.cusip = a.cusip
		LEFT JOIN %s fb ON fb.cik = b.cik AND fb.report_period = b.report_period AND fb.cusip = b.cusip
		LEFT JOIN (%s) pa ON pa.cik = a.cik AND pa.cusip = a.cusip
		LEFT JOIN (%s) pb ON pb.cik = b.cik AND pb.cusip = b.cusip
		WHERE a.cik = ? AND b.cik = ? AND a.report_period = ? AND b.report_period = ?
		ORDER BY GREATEST(a.portfolio_weight_pct, b.portfolio_weight_pct) DESC,
			a.value_usd + b.value_usd DESC, a.cusip
		LIMIT %d`, book, book, flowFrom, flowFrom, basis, basis, limit),
		[]any{aCik, bCik, period, aCik, bCik, period, aCik, bCik, period, period},
		func(rs *sql.Rows) error {
			row, err := scanCompare(rs)
			if err != nil {
				return err
			}
			shared = append(shared, row)
			return nil
		}); err != nil {
		a.thirteenFailed(w, "compare", err)
		return
	}

	// The contrarian pairs: one side opened or added to a position the other
	// trimmed or left. Ranked by how much of a book each side moved, so the pair
	// both funds feel strongest comes first.
	contrarian := []ComparePosition{}
	if err := a.thirteenRows(r.Context(), fmt.Sprintf(`SELECT fa.cusip, COALESCE(a.ticker, fa.ticker),
			COALESCE(a.issuer, fa.issuer, ''),
			CASE WHEN a.cusip IS NULL THEN 0 ELSE a.shares END,
			CASE WHEN b.cusip IS NULL THEN 0 ELSE b.shares END,
			CASE WHEN a.cusip IS NULL THEN 0 ELSE a.value_usd END,
			CASE WHEN b.cusip IS NULL THEN 0 ELSE b.value_usd END,
			CASE WHEN a.cusip IS NULL THEN 0 ELSE a.portfolio_weight_pct END,
			CASE WHEN b.cusip IS NULL THEN 0 ELSE b.portfolio_weight_pct END,
			fa.action, fb.action,
			COALESCE(fa.delta_shares, 0), COALESCE(fb.delta_shares, 0),
			COALESCE(fa.delta_weight_pct, 0), COALESCE(fb.delta_weight_pct, 0),
			pa.est_cost_per_share, pb.est_cost_per_share
		FROM %s fa
		JOIN %s fb ON fb.cusip = fa.cusip AND fb.report_period = fa.report_period
		LEFT JOIN %s a ON a.cik = fa.cik AND a.report_period = fa.report_period AND a.cusip = fa.cusip
		LEFT JOIN %s b ON b.cik = fb.cik AND b.report_period = fb.report_period AND b.cusip = fb.cusip
		LEFT JOIN (%s) pa ON pa.cik = fa.cik AND pa.cusip = fa.cusip
		LEFT JOIN (%s) pb ON pb.cik = fb.cik AND pb.cusip = fb.cusip
		WHERE fa.cik = ? AND fb.cik = ? AND fa.report_period = ?
			AND ((fa.action IN ('NEW', 'ADDED') AND fb.action IN ('TRIMMED', 'EXITED'))
				OR (fb.action IN ('NEW', 'ADDED') AND fa.action IN ('TRIMMED', 'EXITED')))
		ORDER BY abs(COALESCE(fa.delta_weight_pct, 0)) + abs(COALESCE(fb.delta_weight_pct, 0)) DESC,
			fa.cusip
		LIMIT %d`, flowFrom, flowFrom, book, book, basis, basis, limit),
		[]any{aCik, bCik, period, aCik, bCik, period, aCik, bCik, period, period},
		func(rs *sql.Rows) error {
			row, err := scanCompare(rs)
			if err != nil {
				return err
			}
			// The buyer is the side that added: A opened or added to what B left.
			row.Buyer = "a"
			if row.AAction == "TRIMMED" || row.AAction == "EXITED" {
				row.Buyer = "b"
			}
			contrarian = append(contrarian, row)
			return nil
		}); err != nil {
		a.thirteenFailed(w, "compare", err)
		return
	}

	contrarianTotal := int64(0)
	if err := a.thirteenCount(r.Context(), fmt.Sprintf(`SELECT count(*)
		FROM %s fa
		JOIN %s fb ON fb.cusip = fa.cusip AND fb.report_period = fa.report_period
		WHERE fa.cik = ? AND fb.cik = ? AND fa.report_period = ?
			AND ((fa.action IN ('NEW', 'ADDED') AND fb.action IN ('TRIMMED', 'EXITED'))
				OR (fb.action IN ('NEW', 'ADDED') AND fa.action IN ('TRIMMED', 'EXITED')))`,
		flowFrom, flowFrom), []any{aCik, bCik, period}, &contrarianTotal); err != nil {
		a.thirteenFailed(w, "compare", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"period": period, "limit": limit, "quarters": quarters,
		"a": sides[aCik], "b": sides[bCik], "overlap": overlap,
		"shared": shared, "contrarian": contrarian, "contrarianTotal": contrarianTotal,
	})
}

// scanCompare reads one row of the shared and contrarian lists, which select the
// same columns in the same order.
func scanCompare(rs *sql.Rows) (ComparePosition, error) {
	row := ComparePosition{}
	err := rs.Scan(&row.Cusip, &row.Ticker, &row.Issuer, &row.AShares, &row.BShares,
		&row.AValueUSD, &row.BValueUSD, &row.AWeightPct, &row.BWeightPct,
		&row.AAction, &row.BAction, &row.ADeltaShares, &row.BDeltaShares,
		&row.ADeltaWeightPct, &row.BDeltaWeightPct,
		&row.AEstCostPerShare, &row.BEstCostPerShare)
	return row, err
}

// sortOrder resolves one of the rankings a list endpoint offers, returning both
// the name it matched and the ORDER BY that answers it, so the response can echo
// which ranking produced the rows.
func sortOrder(w http.ResponseWriter, raw string) (string, string, bool) {
	return sortFrom(w, raw, thirteenSorts)
}

// sortFrom is sortOrder against the rankings of any list endpoint, so each one
// keeps its own names and the response can echo the one it matched.
func sortFrom(w http.ResponseWriter, raw string, sorts []struct{ name, order string }) (string, string, bool) {
	name := strings.ToLower(strings.TrimSpace(raw))
	if name == "" {
		name = sorts[0].name
	}
	for _, sort := range sorts {
		if sort.name == name {
			return sort.name, sort.order, true
		}
	}
	allowed := make([]string, 0, len(sorts))
	for _, sort := range sorts {
		allowed = append(allowed, sort.name)
	}
	writeError(w, http.StatusBadRequest, "unknown sort "+name+"; one of "+strings.Join(allowed, ", "))
	return "", "", false
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
	return cikNamedParam(w, r, "cik")
}

// cikNamedParam is cikParam for a query parameter that is not called cik, which
// is what a two-fund comparison needs for its two sides.
func cikNamedParam(w http.ResponseWriter, r *http.Request, name string) (string, bool) {
	cik, ok := normaliseCik(strings.TrimSpace(r.URL.Query().Get(name)))
	if !ok {
		writeError(w, http.StatusBadRequest, name+" is required and must be 1-10 digits")
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

// quartersApart counts the quarters between two report dates, positive when `to`
// is the later one and zero when there is no earlier date — the same arithmetic
// fund_quarterly_flows uses for its quarters_between, which is what marks a filer
// that skipped quarters and so carries a whole gap's mark in one filing.
func quartersApart(from, to string) int64 {
	if from == "" {
		return 0
	}
	start, startErr := time.Parse("2006-01-02", from)
	end, endErr := time.Parse("2006-01-02", to)
	if startErr != nil || endErr != nil {
		return 0
	}
	number := func(when time.Time) int64 { return int64(4*when.Year() + (int(when.Month())-1)/3 + 1) }
	return number(end) - number(start)
}
