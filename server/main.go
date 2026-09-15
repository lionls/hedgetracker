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

	if _, err := os.Stat(filepath.Join(*webDir, "index.html")); err != nil {
		log.Printf("no built frontend in %s (run `npm run build` in web/); serving the API only", *webDir)
	}

	api := &API{dataset: dataset, webDir: *webDir}
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
