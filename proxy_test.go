package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPProxyForwardsSanitizedRequest(t *testing.T) {
	var sawPath string
	var sawAuth string
	var sawCookie string
	var sawBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawPath = r.URL.String()
		sawAuth = r.Header.Get("Authorization")
		sawCookie = r.Header.Get("Cookie")
		body, _ := io.ReadAll(r.Body)
		sawBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Set-Cookie", "leak=1")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	cfg := Config{UpstreamBaseURL: upstream.URL + "/v1", LocalBasePath: "/v1", APIKey: "relay-key"}
	cfg.ApplyDefaults()
	proxy := NewProxy(cfg, upstream.Client())

	req := httptest.NewRequest(http.MethodPost, "http://local/v1/responses?x=1", strings.NewReader(`{"input":[]}`))
	req.Header.Set("Authorization", "Bearer chatgpt")
	req.Header.Set("Cookie", "session=secret")
	rr := httptest.NewRecorder()

	proxy.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d", rr.Code)
	}
	if sawPath != "/v1/responses?x=1" {
		t.Fatalf("upstream path = %q", sawPath)
	}
	if sawAuth != "Bearer relay-key" {
		t.Fatalf("upstream auth = %q", sawAuth)
	}
	if sawCookie != "" {
		t.Fatalf("cookie leaked: %q", sawCookie)
	}
	if sawBody != `{"input":[]}` {
		t.Fatalf("body = %q", sawBody)
	}
	if rr.Header().Get("Set-Cookie") != "" {
		t.Fatalf("response Set-Cookie leaked")
	}
	if rr.Body.String() != `{"ok":true}` {
		t.Fatalf("response body = %q", rr.Body.String())
	}
}

func TestHTTPProxyRejectsOutsideBasePath(t *testing.T) {
	cfg := Config{UpstreamBaseURL: "https://relay.example/v1", LocalBasePath: "/v1", APIKey: "relay-key"}
	cfg.ApplyDefaults()
	proxy := NewProxy(cfg, &http.Client{Timeout: time.Second})

	req := httptest.NewRequest(http.MethodGet, "http://local/not-v1/models", nil)
	rr := httptest.NewRecorder()

	proxy.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rr.Code)
	}
}
