// Command marketdata serves the hedgetracker market explorer: a small JSON API
// over the defeatbeta/yahoo-finance-data dataset, read directly from Hugging
// Face by DuckDB, plus the built React frontend from web/dist.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	webDir := flag.String("web", "web/dist", "built frontend directory")
	tempDir := flag.String("temp-dir", filepath.Join(os.TempDir(), "hedgetracker-marketdata"), "DuckDB spill directory, kept off the repository")
	memory := flag.String("memory", "2GB", "DuckDB memory limit")
	threads := flag.Int("threads", 4, "DuckDB threads")
	health := flag.Bool("health", false, "ask a running server for /api/health, print it and exit")

	// The 13F dashboard side. The lake is the extractor's output, mounted
	// read-only; prices come from the same dataset unless they are overridden.
	lake := flag.String("13f-lake", filepath.Join("data", "lake", "13f_holdings"), "13F holdings lake written by the extractor")
	cache := flag.String("13f-cache", filepath.Join(*tempDir, "13f-dashboard"), "directory for the materialised 13F tables")
	prices := flag.String("13f-prices", "", "override the 13F price source (path or URL of a Parquet file); empty uses the dataset")
	splits := flag.String("13f-splits", "", "override the 13F split-event source (path or URL of a Parquet file); empty uses the dataset")
	offline := flag.Bool("13f-offline", false, "build the 13F tables without reading prices: signals keep working, estimated flows stay null")
	maxAge := flag.Duration("13f-max-age", 24*time.Hour, "rebuild the 13F tables when they are older than this; 0 reuses them until the lake changes")

	flag.Parse()

	log.SetPrefix("marketdata: ")
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)

	// The deployed image carries no shell tooling, so the container healthcheck
	// is the binary asking itself.
	if *health {
		os.Exit(checkHealth(*addr))
	}

	if err := os.MkdirAll(*tempDir, 0o755); err != nil {
		log.Fatalf("create temp dir: %v", err)
	}

	proxy := proxyFromEnv()
	if proxy != nil {
		log.Printf("using proxy %s", proxy.Host)
	}

	db, err := openDB(dbOptions{tempDir: *tempDir, memory: *memory, threads: *threads, proxy: proxy})
	if err != nil {
		log.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dataset := NewDataset(db, proxy)
	start := time.Now()
	if err := dataset.Warm(ctx); err != nil {
		log.Fatalf("load dataset index: %v", err)
	}
	log.Printf("index warm in %s", time.Since(start).Round(time.Millisecond))

	// Knowing which symbols have price history needs a full scan of the price
	// table, so it happens next to the live server rather than in front of it.
	go dataset.WarmPriced(ctx)

	// The 13F tables are materialised in the background for the same reason: one
	// conviction query scans the whole price table and the lake at once.
	priceSource, splitSource := "", ""
	if !*offline {
		priceSource, splitSource = dataset.table("stock_prices"), dataset.table("stock_split_events")
		if *prices != "" {
			priceSource = sqlString(*prices)
		}
		if *splits != "" {
			splitSource = sqlString(*splits)
		}
	}
	thirteenF := NewThirteenF(db, *lake, *cache, priceSource, splitSource, *maxAge)
	thirteenF.Warm(ctx)

	if _, err := os.Stat(filepath.Join(*webDir, "index.html")); err != nil {
		log.Printf("no built frontend in %s (run `npm run build` in web/); serving the API only", *webDir)
	}

	api := &API{dataset: dataset, thirteenF: thirteenF, webDir: *webDir}
	server := &http.Server{
		Addr:              *addr,
		Handler:           api.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      10 * time.Minute,
	}

	go func() {
		<-ctx.Done()
		log.Printf("shutting down")
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
	}()

	log.Printf("listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve: %v", err)
	}
}

// checkHealth asks a running server for /api/health and returns the exit status
// for the caller. It is what the container healthcheck runs: the deployed image
// installs no HTTP client of its own.
func checkHealth(addr string) int {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.Printf("health: unusable address %q: %v", addr, err)
		return 1
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/api/health")
	if err != nil {
		log.Printf("health: %v", err)
		return 1
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("health: %v", err)
		return 1
	}
	message := strings.TrimSpace(string(body))
	if resp.StatusCode != http.StatusOK {
		log.Printf("health: %s: %s", resp.Status, message)
		return 1
	}
	log.Printf("health: %s", message)
	return 0
}
