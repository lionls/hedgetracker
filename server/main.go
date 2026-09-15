// Command marketdata serves the hedgetracker market explorer: a small JSON API
// over the defeatbeta/yahoo-finance-data dataset, read directly from Hugging
// Face by DuckDB, plus the built React frontend from web/dist.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	webDir := flag.String("web", "web/dist", "built frontend directory")
	tempDir := flag.String("temp-dir", filepath.Join(os.TempDir(), "hedgetracker-marketdata"), "DuckDB spill directory, kept off the repository")
	flag.Parse()

	log.SetPrefix("marketdata: ")
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)

	if err := os.MkdirAll(*tempDir, 0o755); err != nil {
		log.Fatalf("create temp dir: %v", err)
	}

	proxy := proxyFromEnv()
	if proxy != nil {
		log.Printf("using proxy %s", proxy.Host)
	}

	db, err := openDB(*tempDir, proxy)
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
