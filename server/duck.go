package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"os"
	"strings"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// proxyEnvVars are the variables DuckDB reads on its own, in priority order.
var proxyEnvVars = []string{"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"}

// proxyFromEnv returns the ambient proxy as a URL and removes the variables
// DuckDB would otherwise read.
//
// DuckDB reads proxy environment variables itself but cannot parse a proxy URL
// that carries credentials; a value it cannot parse fails every remote read.
// The proxy is therefore taken from the environment once, then applied both as
// DuckDB configuration and to the Go HTTP client.
func proxyFromEnv() *url.URL {
	raw := ""
	for _, name := range proxyEnvVars {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			raw = value
			break
		}
	}
	for _, name := range proxyEnvVars {
		os.Unsetenv(name)
	}
	if raw == "" {
		return nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return nil
	}
	return parsed
}

// openDB returns a pooled DuckDB handle that can read the dataset over HTTPS.
func openDB(tempDir string, proxy *url.URL) (*sql.DB, error) {
	params := url.Values{}
	params.Set("threads", "4")
	params.Set("memory_limit", "2GB")
	params.Set("temp_directory", tempDir)
	params.Set("preserve_insertion_order", "false")
	// Remote Parquet is re-read per query, so keep the footer and schema caches on:
	// they turn a repeat query on an already-seen file into a metadata-free read.
	params.Set("enable_http_metadata_cache", "true")
	params.Set("enable_object_cache", "true")
	if proxy != nil {
		host := proxy.Host
		if proxy.Port() == "" {
			host = proxy.Hostname() + ":80"
		}
		params.Set("http_proxy", host)
		if proxy.User != nil {
			params.Set("http_proxy_username", proxy.User.Username())
			if password, ok := proxy.User.Password(); ok {
				params.Set("http_proxy_password", password)
			}
		}
	}

	connector, err := duckdb.NewConnector("?"+params.Encode(), func(exec driver.ExecerContext) error {
		return loadExtensions(exec)
	})
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	return db, nil
}

// loadExtensions makes httpfs available on a fresh connection. The extension is
// compiled into the bindings on some builds and downloaded on first use on
// others, so a failed LOAD falls back to INSTALL.
func loadExtensions(exec driver.ExecerContext) error {
	ctx := context.Background()
	if _, err := exec.ExecContext(ctx, "LOAD httpfs", nil); err == nil {
		return nil
	}
	if _, err := exec.ExecContext(ctx, "INSTALL httpfs", nil); err != nil {
		return fmt.Errorf("install httpfs: %w", err)
	}
	if _, err := exec.ExecContext(ctx, "LOAD httpfs", nil); err != nil {
		return fmt.Errorf("load httpfs: %w", err)
	}
	return nil
}
