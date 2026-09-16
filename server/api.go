package main

import (
	"compress/gzip"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// API serves the JSON endpoints and the built frontend.
type API struct {
	dataset   *Dataset
	thirteenF *ThirteenF
	webDir    string
}

// routes wires the endpoints. API responses are compressed; the frontend is
// served as-is because the assets are already small and content-addressed.
func (a *API) routes() http.Handler {
	api := http.NewServeMux()
	api.HandleFunc("GET /api/health", a.health)
	api.HandleFunc("GET /api/search", a.search)
	api.HandleFunc("GET /api/stocks", a.stocks)
	api.HandleFunc("GET /api/company/{symbol}", a.company)
	api.HandleFunc("GET /api/bars/{symbol}", a.bars)
	api.HandleFunc("GET /api/fundamentals/{symbol}", a.fundamentals)
	api.HandleFunc("GET /api/13f/status", a.thirteenfStatus)
	api.HandleFunc("GET /api/13f/funds", a.thirteenfFunds)
	api.HandleFunc("GET /api/13f/holdings", a.thirteenfHoldings)
	api.HandleFunc("GET /api/13f/flows", a.thirteenfFlows)
	api.HandleFunc("GET /api/13f/signals", a.thirteenfSignals)
	api.HandleFunc("GET /api/13f/vwap", a.thirteenfVWAP)
	api.HandleFunc("POST /api/13f/refresh", a.thirteenfRefresh)

	root := http.NewServeMux()
	root.Handle("/api/", gzipHandler(api))
	root.Handle("/", http.HandlerFunc(a.static))
	return logRequests(root)
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	a.dataset.mu.RLock()
	indexed, priced := len(a.dataset.index), len(a.dataset.priced)
	a.dataset.mu.RUnlock()
	// The 13F state is reported but never fails the check: the explorer serves
	// prices and company data without a holdings lake.
	thirteen := a.thirteenF.Status()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":           "ok",
		"tables":           a.dataset.Prefix(),
		"symbols":          indexed,
		"priced":           priced,
		"pricedDone":       priced > 0,
		"thirteenF":        thirteen.State,
		"thirteenFBuiltAt": thirteen.BuiltAt,
	})
}

func (a *API) search(w http.ResponseWriter, r *http.Request) {
	limit := 25
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = min(parsed, 200)
		}
	}
	query := r.URL.Query().Get("q")
	stocksOnly := r.URL.Query().Get("stocks") == "1"
	results := a.dataset.Search(query, limit, stocksOnly)
	writeJSON(w, http.StatusOK, map[string]any{"query": query, "results": results})
}

// stocks serves the browse list of the stocks page: every operating company in
// the dataset, alphabetical. It is built at warm time and served from memory.
func (a *API) stocks(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"stocks": a.dataset.Stocks()})
}

// fundamentals serves the statements the stocks page shows under the chart. A
// symbol the dataset never filed statements for — any fund — is a 404.
func (a *API) fundamentals(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	if !validSymbol(symbol) {
		writeError(w, http.StatusBadRequest, "invalid symbol")
		return
	}
	fundamentals, ok, err := a.dataset.Fundamentals(r.Context(), symbol)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "no fundamentals for symbol")
		return
	}
	writeJSON(w, http.StatusOK, fundamentals)
}

func (a *API) company(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	if !validSymbol(symbol) {
		writeError(w, http.StatusBadRequest, "invalid symbol")
		return
	}
	profile, ok := a.dataset.Company(symbol)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown symbol")
		return
	}
	writeJSON(w, http.StatusOK, profile)
}

func (a *API) bars(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	if !validSymbol(symbol) {
		writeError(w, http.StatusBadRequest, "invalid symbol")
		return
	}
	rng := r.URL.Query().Get("range")
	if rng == "" {
		rng = "5y"
	}
	if _, ok := rangeStart(rng); !ok {
		writeError(w, http.StatusBadRequest, "range must be one of 1y, 5y, max")
		return
	}

	start := time.Now()
	bars, err := a.dataset.Bars(r.Context(), symbol, rng)
	if err != nil {
		log.Printf("bars %s %s: %v", symbol, rng, err)
		writeError(w, http.StatusBadGateway, "could not read price history")
		return
	}
	log.Printf("bars %s %s: %d rows in %s", bars.Symbol, rng, len(bars.Dates), time.Since(start).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, bars)
}

// static serves the built frontend and falls back to index.html for client-side
// routes, so the Go binary can serve the whole app on its own.
func (a *API) static(w http.ResponseWriter, r *http.Request) {
	if a.webDir == "" {
		http.NotFound(w, r)
		return
	}
	clean := filepath.Clean("/" + r.URL.Path)
	if strings.Contains(clean, "..") {
		http.NotFound(w, r)
		return
	}
	target := filepath.Join(a.webDir, clean)
	if info, err := os.Stat(target); err == nil && !info.IsDir() {
		http.ServeFile(w, r, target)
		return
	}
	if filepath.Ext(clean) != "" {
		http.NotFound(w, r)
		return
	}
	index := filepath.Join(a.webDir, "index.html")
	if _, err := os.Stat(index); err != nil {
		writeError(w, http.StatusNotFound, "frontend not built; run `npm run build` in web/")
		return
	}
	http.ServeFile(w, r, index)
}

// validSymbol accepts the shapes exchanges use for tickers and rejects anything
// else before it reaches a query.
func validSymbol(symbol string) bool {
	if symbol == "" || len(symbol) > 16 {
		return false
	}
	for _, char := range symbol {
		switch {
		case char >= 'A' && char <= 'Z', char >= 'a' && char <= 'z':
		case char >= '0' && char <= '9':
		case char == '.', char == '-', char == '^', char == '=', char == '_':
		default:
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// gzipHandler compresses API responses. A price history is ~100 kB of JSON.
func gzipHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Add("Vary", "Accept-Encoding")
		w.Header().Set("Content-Encoding", "gzip")
		compressor := gzip.NewWriter(w)
		defer compressor.Close()
		next.ServeHTTP(gzipResponseWriter{ResponseWriter: w, writer: compressor}, r)
	})
}

type gzipResponseWriter struct {
	http.ResponseWriter
	writer *gzip.Writer
}

func (g gzipResponseWriter) Write(payload []byte) (int, error) { return g.writer.Write(payload) }

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			start := time.Now()
			recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(recorder, r)
			log.Printf("%s %s %d in %s", r.Method, r.URL.RequestURI(), recorder.status, time.Since(start).Round(time.Millisecond))
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(status int) {
	s.status = status
	s.ResponseWriter.WriteHeader(status)
}
