package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigAppliesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.json")
	body := `{"upstream_base_url":"https://relay.example/v1","api_key":"sk-test"}`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}

	if cfg.Listen != "127.0.0.1:39493" {
		t.Fatalf("Listen = %q", cfg.Listen)
	}
	if cfg.LocalBasePath != "/v1" {
		t.Fatalf("LocalBasePath = %q", cfg.LocalBasePath)
	}
	if cfg.AuthHeader != "Authorization" {
		t.Fatalf("AuthHeader = %q", cfg.AuthHeader)
	}
	if cfg.AuthScheme != "Bearer" {
		t.Fatalf("AuthScheme = %q", cfg.AuthScheme)
	}
	if cfg.WebSocketMode != "bridge" {
		t.Fatalf("WebSocketMode = %q", cfg.WebSocketMode)
	}
	if cfg.WebSocketCompression == nil || !*cfg.WebSocketCompression {
		t.Fatalf("WebSocketCompression default = %#v", cfg.WebSocketCompression)
	}
	if cfg.TimeoutSeconds != 600 {
		t.Fatalf("TimeoutSeconds = %d", cfg.TimeoutSeconds)
	}
}

func TestLoadConfigRejectsMissingRequiredValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.json")
	if err := os.WriteFile(path, []byte(`{"api_key":"sk-test"}`), 0600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected missing upstream_base_url error")
	}
}

func TestConfigPathCandidatesPreferExecutableDirThenWorkingDir(t *testing.T) {
	candidates := ConfigPathCandidates("/tmp/proxy/codex-ws-proxy", "/work")
	if len(candidates) != 2 {
		t.Fatalf("candidate count = %d", len(candidates))
	}
	if candidates[0] != "/tmp/proxy/a.json" {
		t.Fatalf("first candidate = %q", candidates[0])
	}
	if candidates[1] != "/work/a.json" {
		t.Fatalf("second candidate = %q", candidates[1])
	}
}

func TestUpstreamURLForMapsLocalBasePath(t *testing.T) {
	cfg := Config{
		UpstreamBaseURL: "https://relay.example/v1/",
		LocalBasePath:   "/v1",
		APIKey:          "sk-test",
	}
	cfg.ApplyDefaults()

	got, err := cfg.UpstreamURLFor("/v1/responses?debug=1")
	if err != nil {
		t.Fatalf("UpstreamURLFor returned error: %v", err)
	}
	want := "https://relay.example/v1/responses?debug=1"
	if got != want {
		t.Fatalf("url = %q, want %q", got, want)
	}
}
