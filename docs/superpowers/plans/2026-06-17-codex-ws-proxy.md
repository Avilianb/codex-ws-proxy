# Codex WebSocket Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a lightweight local Go proxy that forwards OpenAI-compatible HTTP requests and bridges Codex `/v1/responses` WebSocket `response.create` frames to upstream HTTPS Responses API SSE without leaking ChatGPT/Codex credentials.

**Architecture:** The proxy is a small Go binary with focused files for config, header sanitization, HTTP forwarding, WebSocket/SSE bridging, and per-WebSocket state reconstruction. Normal HTTP requests stream through to the configured upstream, while `/v1/responses` WebSocket frames are converted into self-contained HTTP Responses API requests and upstream SSE `data:` JSON is streamed back as WebSocket text messages.

**Tech Stack:** Go 1.26.4, standard library `net/http`, `httptest`, `encoding/json`, plus `github.com/coder/websocket v1.8.15`.

## Global Constraints

- Read `a.json` from the executable directory when possible, falling back to current working directory during `go run`.
- Default listen address: `127.0.0.1:39493`.
- Default local base path: `/v1`.
- Default auth header: `Authorization`.
- Default auth scheme: `Bearer`.
- Default websocket mode: `bridge`.
- Default websocket compression: `true`.
- Default timeout: `600` seconds.
- Default request logging: `false`.
- Startup must fail if `api_key` or `upstream_base_url` is empty.
- Strip ChatGPT/Codex/OpenAI/OAuth-sensitive request headers before upstream forwarding.
- Inject configured upstream auth after stripping incoming credentials.
- Do not log `api_key`, incoming authorization headers, cookies, request bodies, or response bodies.
- WebSocket request bodies with outer `"type":"response.create"` must not forward `type`, `generate`, or `previous_response_id` to HTTP upstream.
- WebSocket prewarm frames with `generate:false` must not call upstream.
- Upstream SSE must be streamed incrementally to WebSocket text frames, not buffered as a whole.
- State reconstruction is per WebSocket connection and in memory only.

---

## File Structure

- Create `go.mod`: module declaration and `github.com/coder/websocket` dependency.
- Create `config.go`: `Config`, `LoadConfig`, `ApplyDefaults`, `Validate`, `ConfigPathCandidates`, `UpstreamURLFor`.
- Create `sanitize.go`: `SanitizeHeaders`, `SanitizeResponseHeaders`, `IsSensitiveRequestHeader`, `IsHopByHopHeader`, `BuildAuthValue`.
- Create `proxy.go`: `Proxy`, `NewProxy`, `ServeHTTP`, `handleHTTP`, `isWebSocketUpgrade`.
- Create `ws_bridge.go`: `handleResponsesWebSocket`, `handleWSMessage`, `bridgeToUpstream`, `readSSEData`, `writeLocalPrewarm`.
- Create `state.go`: `BridgeState`, `BuildHTTPBody`, `UpdateFromSSEData`, `appendDedupe`, `itemKey`.
- Create `main.go`: process entrypoint, config loading, server start.
- Create `a.example.json`: safe example config with replacement key string.
- Create `README.md`: setup, Codex config example, behavior notes.
- Tests live beside files as `config_test.go`, `sanitize_test.go`, `proxy_test.go`, `ws_bridge_test.go`, `state_test.go`.

---

### Task 1: Go Module and Config Loading

**Files:**
- Create: `go.mod`
- Create: `config.go`
- Test: `config_test.go`

**Interfaces:**
- Produces: `type Config struct { Listen string; UpstreamBaseURL string; LocalBasePath string; APIKey string; AuthHeader string; AuthScheme string; WebSocketMode string; WebSocketCompression *bool; TimeoutSeconds int; LogRequests bool }`
- Produces: `func LoadConfig(path string) (Config, error)`
- Produces: `func (c *Config) ApplyDefaults()`
- Produces: `func (c Config) Validate() error`
- Produces: `func (c Config) UpstreamURLFor(localPath string) (string, error)`
- Later tasks consume `Config` and `UpstreamURLFor`.

- [ ] **Step 1: Create module file**

Create `go.mod`:

```go
module codex-ws-proxy

go 1.26

require github.com/coder/websocket v1.8.15
```

- [ ] **Step 2: Write failing config tests**

Create `config_test.go`:

```go
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
```

- [ ] **Step 3: Run config tests to verify failure**

Run: `go test ./...`

Expected: FAIL with errors such as `undefined: LoadConfig` and `undefined: Config`.

- [ ] **Step 4: Implement config**

Create `config.go`:

```go
package main

import (
    "encoding/json"
    "errors"
    "fmt"
    "net/url"
    "os"
    "path"
    "strings"
)

type Config struct {
    Listen               string `json:"listen"`
    UpstreamBaseURL      string `json:"upstream_base_url"`
    LocalBasePath        string `json:"local_base_path"`
    APIKey               string `json:"api_key"`
    AuthHeader           string `json:"auth_header"`
    AuthScheme           string `json:"auth_scheme"`
    WebSocketMode        string `json:"websocket_mode"`
    WebSocketCompression *bool  `json:"websocket_compression"`
    TimeoutSeconds       int    `json:"timeout_seconds"`
    LogRequests          bool   `json:"log_requests"`
}

func LoadConfig(configPath string) (Config, error) {
    data, err := os.ReadFile(configPath)
    if err != nil {
        return Config{}, fmt.Errorf("read config %s: %w", configPath, err)
    }
    var cfg Config
    if err := json.Unmarshal(data, &cfg); err != nil {
        return Config{}, fmt.Errorf("parse config %s: %w", configPath, err)
    }
    cfg.ApplyDefaults()
    if err := cfg.Validate(); err != nil {
        return Config{}, err
    }
    return cfg, nil
}

func (c *Config) ApplyDefaults() {
    if c.Listen == "" {
        c.Listen = "127.0.0.1:39493"
    }
    if c.LocalBasePath == "" {
        c.LocalBasePath = "/v1"
    }
    if !strings.HasPrefix(c.LocalBasePath, "/") {
        c.LocalBasePath = "/" + c.LocalBasePath
    }
    c.LocalBasePath = strings.TrimRight(c.LocalBasePath, "/")
    if c.LocalBasePath == "" {
        c.LocalBasePath = "/v1"
    }
    if c.AuthHeader == "" {
        c.AuthHeader = "Authorization"
    }
    if c.AuthScheme == "" {
        c.AuthScheme = "Bearer"
    }
    if c.WebSocketMode == "" {
        c.WebSocketMode = "bridge"
    }
    if c.TimeoutSeconds == 0 {
        c.TimeoutSeconds = 600
    }
    if c.WebSocketCompression == nil {
        enabled := true
        c.WebSocketCompression = &enabled
    }
    c.UpstreamBaseURL = strings.TrimRight(c.UpstreamBaseURL, "/")
}

func (c Config) Validate() error {
    if strings.TrimSpace(c.UpstreamBaseURL) == "" {
        return errors.New("upstream_base_url is required")
    }
    if _, err := url.ParseRequestURI(c.UpstreamBaseURL); err != nil {
        return fmt.Errorf("upstream_base_url is invalid: %w", err)
    }
    if strings.TrimSpace(c.APIKey) == "" {
        return errors.New("api_key is required")
    }
    if c.WebSocketMode != "bridge" {
        return fmt.Errorf("unsupported websocket_mode %q", c.WebSocketMode)
    }
    if c.TimeoutSeconds < 1 {
        return errors.New("timeout_seconds must be positive")
    }
    return nil
}

func (c Config) UpstreamURLFor(localURL string) (string, error) {
    parsedLocal, err := url.ParseRequestURI(localURL)
    if err != nil {
        return "", fmt.Errorf("parse local url path: %w", err)
    }
    if parsedLocal.Path != c.LocalBasePath && !strings.HasPrefix(parsedLocal.Path, c.LocalBasePath+"/") {
        return "", fmt.Errorf("path %q is outside local_base_path %q", parsedLocal.Path, c.LocalBasePath)
    }
    suffix := strings.TrimPrefix(parsedLocal.Path, c.LocalBasePath)
    if suffix == "" {
        suffix = "/"
    }

    upstream, err := url.Parse(c.UpstreamBaseURL)
    if err != nil {
        return "", fmt.Errorf("parse upstream url: %w", err)
    }
    upstream.Path = path.Join(upstream.Path, suffix)
    if strings.HasSuffix(suffix, "/") && !strings.HasSuffix(upstream.Path, "/") {
        upstream.Path += "/"
    }
    upstream.RawQuery = parsedLocal.RawQuery
    return upstream.String(), nil
}
```

- [ ] **Step 5: Run config tests to verify pass**

Run: `go test ./...`

Expected: PASS.

- [ ] **Step 6: Commit config task**

```bash
git add go.mod config.go config_test.go
git commit -m "Add proxy config loading"
```

---

### Task 2: Header Sanitization

**Files:**
- Create: `sanitize.go`
- Test: `sanitize_test.go`

**Interfaces:**
- Consumes: `Config` from Task 1.
- Produces: `func SanitizeHeaders(in http.Header, cfg Config) http.Header`
- Produces: `func SanitizeResponseHeaders(in http.Header) http.Header`
- Produces: `func IsSensitiveRequestHeader(name string) bool`
- Produces: `func IsHopByHopHeader(name string) bool`
- Produces: `func BuildAuthValue(cfg Config) string`
- Later HTTP and WebSocket tasks call `SanitizeHeaders` for upstream requests.

- [ ] **Step 1: Write failing sanitizer tests**

Create `sanitize_test.go`:

```go
package main

import (
    "net/http"
    "testing"
)

func TestSanitizeHeadersStripsSensitiveAndInjectsAuth(t *testing.T) {
    cfg := Config{APIKey: "relay-key", AuthHeader: "Authorization", AuthScheme: "Bearer"}
    cfg.ApplyDefaults()
    in := http.Header{}
    in.Set("Authorization", "Bearer chatgpt")
    in.Set("Cookie", "session=secret")
    in.Set("OpenAI-Beta", "responses_websockets=2026-02-06")
    in.Set("X-OpenAI-Client-User-Agent", "secret")
    in.Set("X-Stainless-Auth", "secret")
    in.Set("X-CodeX-Turn-State", "state")
    in.Set("Session-ID", "session")
    in.Set("Connection", "upgrade")
    in.Set("Content-Type", "application/json")

    got := SanitizeHeaders(in, cfg)

    if got.Get("Authorization") != "Bearer relay-key" {
        t.Fatalf("Authorization = %q", got.Get("Authorization"))
    }
    stripped := []string{"Cookie", "OpenAI-Beta", "X-OpenAI-Client-User-Agent", "X-Stainless-Auth", "X-CodeX-Turn-State", "Session-ID", "Connection"}
    for _, name := range stripped {
        if got.Get(name) != "" {
            t.Fatalf("%s leaked as %q", name, got.Get(name))
        }
    }
    if got.Get("Content-Type") != "application/json" {
        t.Fatalf("Content-Type = %q", got.Get("Content-Type"))
    }
}

func TestSanitizeResponseHeadersStripsSetCookieAndHopByHop(t *testing.T) {
    in := http.Header{}
    in.Set("Set-Cookie", "session=secret")
    in.Set("Transfer-Encoding", "chunked")
    in.Set("Content-Type", "text/event-stream")

    got := SanitizeResponseHeaders(in)

    if got.Get("Set-Cookie") != "" {
        t.Fatalf("Set-Cookie leaked")
    }
    if got.Get("Transfer-Encoding") != "" {
        t.Fatalf("Transfer-Encoding leaked")
    }
    if got.Get("Content-Type") != "text/event-stream" {
        t.Fatalf("Content-Type = %q", got.Get("Content-Type"))
    }
}
```

- [ ] **Step 2: Run sanitizer tests to verify failure**

Run: `go test ./...`

Expected: FAIL with `undefined: SanitizeHeaders`.

- [ ] **Step 3: Implement sanitizer**

Create `sanitize.go`:

```go
package main

import (
    "net/http"
    "strings"
)

func SanitizeHeaders(in http.Header, cfg Config) http.Header {
    out := http.Header{}
    for name, values := range in {
        if IsSensitiveRequestHeader(name) || IsHopByHopHeader(name) {
            continue
        }
        for _, value := range values {
            out.Add(name, value)
        }
    }
    out.Set(cfg.AuthHeader, BuildAuthValue(cfg))
    return out
}

func SanitizeResponseHeaders(in http.Header) http.Header {
    out := http.Header{}
    for name, values := range in {
        lower := strings.ToLower(name)
        if lower == "set-cookie" || IsHopByHopHeader(name) {
            continue
        }
        for _, value := range values {
            out.Add(name, value)
        }
    }
    return out
}

func IsSensitiveRequestHeader(name string) bool {
    lower := strings.ToLower(strings.TrimSpace(name))
    exact := map[string]bool{
        "authorization":        true,
        "cookie":               true,
        "set-cookie":           true,
        "proxy-authorization":  true,
        "x-stainless-auth":     true,
        "session-id":           true,
        "thread-id":            true,
        "x-client-request-id":  true,
    }
    if exact[lower] {
        return true
    }
    prefixes := []string{"openai-", "x-openai-", "x-stainless-", "x-oai-", "x-codex-"}
    for _, prefix := range prefixes {
        if strings.HasPrefix(lower, prefix) {
            return true
        }
    }
    return false
}

func IsHopByHopHeader(name string) bool {
    switch strings.ToLower(strings.TrimSpace(name)) {
    case "connection", "keep-alive", "proxy-authenticate", "te", "trailer", "transfer-encoding", "upgrade":
        return true
    default:
        return false
    }
}

func BuildAuthValue(cfg Config) string {
    if strings.TrimSpace(cfg.AuthScheme) == "" {
        return cfg.APIKey
    }
    return strings.TrimSpace(cfg.AuthScheme) + " " + cfg.APIKey
}
```

- [ ] **Step 4: Run sanitizer tests to verify pass**

Run: `go test ./...`

Expected: PASS.

- [ ] **Step 5: Commit sanitizer task**

```bash
git add sanitize.go sanitize_test.go
git commit -m "Add upstream header sanitization"
```

---

### Task 3: HTTP Reverse Proxy

**Files:**
- Create: `proxy.go`
- Test: `proxy_test.go`

**Interfaces:**
- Consumes: `Config.UpstreamURLFor` from Task 1.
- Consumes: `SanitizeHeaders` and `SanitizeResponseHeaders` from Task 2.
- Produces: `type Proxy struct { cfg Config; client *http.Client }`
- Produces: `func NewProxy(cfg Config, client *http.Client) *Proxy`
- Produces: `func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request)`
- Produces: `func (p *Proxy) handleHTTP(w http.ResponseWriter, r *http.Request)`
- Later WebSocket task extends `ServeHTTP` to route upgrades before HTTP forwarding.

- [ ] **Step 1: Write failing HTTP proxy tests**

Create `proxy_test.go`:

```go
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
```

- [ ] **Step 2: Run proxy tests to verify failure**

Run: `go test ./...`

Expected: FAIL with `undefined: NewProxy`.

- [ ] **Step 3: Implement HTTP proxy**

Create `proxy.go`:

```go
package main

import (
    "context"
    "fmt"
    "io"
    "log"
    "net/http"
    "strings"
    "time"
)

type Proxy struct {
    cfg    Config
    client *http.Client
}

func NewProxy(cfg Config, client *http.Client) *Proxy {
    if client == nil {
        client = &http.Client{Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second}
    }
    return &Proxy{cfg: cfg, client: client}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if r.URL.Path != p.cfg.LocalBasePath && !strings.HasPrefix(r.URL.Path, p.cfg.LocalBasePath+"/") {
        http.NotFound(w, r)
        return
    }
    p.handleHTTP(w, r)
}

func (p *Proxy) handleHTTP(w http.ResponseWriter, r *http.Request) {
    start := time.Now()
    upstreamURL, err := p.cfg.UpstreamURLFor(r.URL.RequestURI())
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }

    ctx := r.Context()
    if _, ok := ctx.Deadline(); !ok && p.cfg.TimeoutSeconds > 0 {
        var cancel context.CancelFunc
        ctx, cancel = context.WithTimeout(ctx, time.Duration(p.cfg.TimeoutSeconds)*time.Second)
        defer cancel()
    }

    req, err := http.NewRequestWithContext(ctx, r.Method, upstreamURL, r.Body)
    if err != nil {
        http.Error(w, fmt.Sprintf("build upstream request: %v", err), http.StatusBadGateway)
        return
    }
    req.Header = SanitizeHeaders(r.Header, p.cfg)

    resp, err := p.client.Do(req)
    if err != nil {
        http.Error(w, fmt.Sprintf("upstream request failed: %v", err), http.StatusBadGateway)
        return
    }
    defer resp.Body.Close()

    for name, values := range SanitizeResponseHeaders(resp.Header) {
        for _, value := range values {
            w.Header().Add(name, value)
        }
    }
    w.WriteHeader(resp.StatusCode)
    written, copyErr := io.Copy(w, resp.Body)
    if p.cfg.LogRequests {
        log.Printf("%s %s -> %s status=%d bytes=%d duration=%s", r.Method, r.URL.RequestURI(), upstreamURL, resp.StatusCode, written, time.Since(start))
    }
    if copyErr != nil {
        log.Printf("copy upstream response failed: %v", copyErr)
    }
}
```

- [ ] **Step 4: Run HTTP proxy tests to verify pass**

Run: `go test ./...`

Expected: PASS.

- [ ] **Step 5: Commit HTTP proxy task**

```bash
git add proxy.go proxy_test.go
git commit -m "Add streaming HTTP reverse proxy"
```

---

### Task 4: WebSocket Prewarm and Request Conversion

**Files:**
- Modify: `proxy.go`
- Create: `ws_bridge.go`
- Test: `ws_bridge_test.go`

**Interfaces:**
- Consumes: `Proxy`, `Config`, `SanitizeHeaders`.
- Produces: `func isWebSocketUpgrade(r *http.Request) bool`
- Produces: `func (p *Proxy) handleResponsesWebSocket(w http.ResponseWriter, r *http.Request)`
- Produces: `func (p *Proxy) handleWSMessage(ctx context.Context, conn *websocket.Conn, state *BridgeState, data []byte) error`
- Task 5 adds `BridgeState`; for this task create minimal `type BridgeState struct{ prewarmCount int }` in `state.go` if Task 5 has not been completed yet.

- [ ] **Step 1: Write failing WebSocket prewarm test**

Create `ws_bridge_test.go`:

```go
package main

import (
    "context"
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "testing"
    "time"

    "github.com/coder/websocket"
)

func TestWebSocketPrewarmDoesNotHitUpstream(t *testing.T) {
    upstreamHits := 0
    upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        upstreamHits++
        w.WriteHeader(http.StatusInternalServerError)
    }))
    defer upstream.Close()

    cfg := Config{UpstreamBaseURL: upstream.URL + "/v1", LocalBasePath: "/v1", APIKey: "relay-key"}
    cfg.ApplyDefaults()
    proxy := NewProxy(cfg, upstream.Client())
    server := httptest.NewServer(proxy)
    defer server.Close()

    wsURL := "ws" + server.URL[len("http"):] + "/v1/responses"
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()
    conn, _, err := websocket.Dial(ctx, wsURL, nil)
    if err != nil {
        t.Fatalf("dial websocket: %v", err)
    }
    defer conn.Close(websocket.StatusNormalClosure, "done")

    prewarm := map[string]any{
        "type":     "response.create",
        "model":    "gpt-test",
        "input":    []any{},
        "tools":    []any{},
        "stream":   true,
        "generate": false,
    }
    payload, _ := json.Marshal(prewarm)
    if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
        t.Fatalf("write prewarm: %v", err)
    }

    _, first, err := conn.Read(ctx)
    if err != nil {
        t.Fatalf("read first event: %v", err)
    }
    _, second, err := conn.Read(ctx)
    if err != nil {
        t.Fatalf("read second event: %v", err)
    }

    if upstreamHits != 0 {
        t.Fatalf("upstream was hit %d times", upstreamHits)
    }
    if !json.Valid(first) || !json.Valid(second) {
        t.Fatalf("prewarm events were not JSON: %q %q", first, second)
    }
    var created map[string]any
    if err := json.Unmarshal(first, &created); err != nil {
        t.Fatal(err)
    }
    if created["type"] != "response.created" {
        t.Fatalf("first event type = %v", created["type"])
    }
}
```

- [ ] **Step 2: Run WebSocket prewarm test to verify failure**

Run: `go test ./...`

Expected: FAIL because `github.com/coder/websocket` is not downloaded and WebSocket handling is missing.

- [ ] **Step 3: Download WebSocket dependency**

Run: `go mod tidy`

Expected: `go.sum` is created and includes `github.com/coder/websocket`.

- [ ] **Step 4: Implement WebSocket routing and prewarm**

Modify `proxy.go` `ServeHTTP` and add `isWebSocketUpgrade`:

```go
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if r.URL.Path != p.cfg.LocalBasePath && !strings.HasPrefix(r.URL.Path, p.cfg.LocalBasePath+"/") {
        http.NotFound(w, r)
        return
    }
    if r.URL.Path == p.cfg.LocalBasePath+"/responses" && isWebSocketUpgrade(r) {
        p.handleResponsesWebSocket(w, r)
        return
    }
    p.handleHTTP(w, r)
}

func isWebSocketUpgrade(r *http.Request) bool {
    return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") && strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}
```

Create `state.go` with the temporary minimal state if it does not exist yet:

```go
package main

type BridgeState struct {
    prewarmCount int
}
```

Create `ws_bridge.go`:

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "time"

    "github.com/coder/websocket"
)

func (p *Proxy) handleResponsesWebSocket(w http.ResponseWriter, r *http.Request) {
    opts := &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled}
    if p.cfg.WebSocketCompression != nil && *p.cfg.WebSocketCompression {
        opts.CompressionMode = websocket.CompressionContextTakeover
    }
    conn, err := websocket.Accept(w, r, opts)
    if err != nil {
        return
    }
    defer conn.Close(websocket.StatusNormalClosure, "done")

    state := &BridgeState{}
    ctx := r.Context()
    for {
        typ, data, err := conn.Read(ctx)
        if err != nil {
            return
        }
        if typ != websocket.MessageText {
            _ = conn.Close(websocket.StatusUnsupportedData, "text frames only")
            return
        }
        if err := p.handleWSMessage(ctx, conn, state, data); err != nil {
            _ = conn.Close(websocket.StatusPolicyViolation, err.Error())
            return
        }
    }
}

func (p *Proxy) handleWSMessage(ctx context.Context, conn *websocket.Conn, state *BridgeState, data []byte) error {
    var msg map[string]any
    if err := json.Unmarshal(data, &msg); err != nil {
        return fmt.Errorf("invalid JSON frame")
    }
    if msg["type"] != "response.create" {
        return fmt.Errorf("unsupported websocket message type")
    }
    if generate, ok := msg["generate"].(bool); ok && !generate {
        return writeLocalPrewarm(ctx, conn, state)
    }
    return fmt.Errorf("upstream bridge is not implemented yet")
}

func writeLocalPrewarm(ctx context.Context, conn *websocket.Conn, state *BridgeState) error {
    state.prewarmCount++
    id := fmt.Sprintf("local-prewarm-%d", state.prewarmCount)
    events := []map[string]any{
        {"type": "response.created", "response": map[string]any{"id": id}},
        {"type": "response.completed", "response": map[string]any{"id": id, "output": []any{}, "usage": nil}},
    }
    for _, event := range events {
        payload, err := json.Marshal(event)
        if err != nil {
            return err
        }
        writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
        err = conn.Write(writeCtx, websocket.MessageText, payload)
        cancel()
        if err != nil {
            return err
        }
    }
    return nil
}
```

- [ ] **Step 5: Run WebSocket prewarm test to verify pass**

Run: `go test ./...`

Expected: PASS.

- [ ] **Step 6: Commit prewarm task**

```bash
git add go.mod go.sum proxy.go ws_bridge.go ws_bridge_test.go state.go
git commit -m "Add websocket prewarm handling"
```

---

### Task 5: SSE Bridge for Real WebSocket Requests

**Files:**
- Modify: `ws_bridge.go`
- Modify: `state.go`
- Test: `ws_bridge_test.go`

**Interfaces:**
- Consumes: `Config.UpstreamURLFor`, `SanitizeHeaders`, `BridgeState`.
- Produces: `func (p *Proxy) bridgeToUpstream(ctx context.Context, conn *websocket.Conn, state *BridgeState, msg map[string]any, incoming http.Header) error`
- Produces: `func readSSEData(r io.Reader, onData func([]byte) error) error`
- Produces: `func (s *BridgeState) BuildHTTPBody(msg map[string]any) ([]byte, error)` initially only deletes `type`, `generate`, `previous_response_id` and forces `stream:true`.

- [ ] **Step 1: Add failing real bridge test**

Append to `ws_bridge_test.go`:

```go
func TestWebSocketBridgePostsHTTPAndStreamsSSEData(t *testing.T) {
    upstreamBodies := make(chan map[string]any, 1)
    upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if r.URL.Path != "/v1/responses" {
            t.Errorf("path = %s", r.URL.Path)
        }
        if r.Header.Get("Accept") != "text/event-stream" {
            t.Errorf("Accept = %q", r.Header.Get("Accept"))
        }
        if r.Header.Get("Authorization") != "Bearer relay-key" {
            t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
        }
        var body map[string]any
        if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
            t.Errorf("decode body: %v", err)
        }
        upstreamBodies <- body
        w.Header().Set("Content-Type", "text/event-stream")
        flusher := w.(http.Flusher)
        _, _ = w.Write([]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\"}}\n\n"))
        flusher.Flush()
        _, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[]}}\n\n"))
        flusher.Flush()
    }))
    defer upstream.Close()

    cfg := Config{UpstreamBaseURL: upstream.URL + "/v1", LocalBasePath: "/v1", APIKey: "relay-key"}
    cfg.ApplyDefaults()
    proxy := NewProxy(cfg, upstream.Client())
    server := httptest.NewServer(proxy)
    defer server.Close()

    wsURL := "ws" + server.URL[len("http"):] + "/v1/responses"
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()
    conn, _, err := websocket.Dial(ctx, wsURL, nil)
    if err != nil {
        t.Fatalf("dial websocket: %v", err)
    }
    defer conn.Close(websocket.StatusNormalClosure, "done")

    frame := map[string]any{
        "type":                 "response.create",
        "model":                "gpt-test",
        "input":                []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "hi"}}}},
        "tools":                []any{},
        "stream":               true,
        "generate":             true,
        "previous_response_id": "resp-old",
    }
    payload, _ := json.Marshal(frame)
    if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
        t.Fatalf("write frame: %v", err)
    }

    _, first, err := conn.Read(ctx)
    if err != nil {
        t.Fatalf("read first SSE event: %v", err)
    }
    _, second, err := conn.Read(ctx)
    if err != nil {
        t.Fatalf("read second SSE event: %v", err)
    }
    if string(first) == string(second) {
        t.Fatalf("expected distinct events")
    }

    body := <-upstreamBodies
    if _, ok := body["type"]; ok {
        t.Fatalf("type leaked to upstream body: %#v", body)
    }
    if _, ok := body["generate"]; ok {
        t.Fatalf("generate leaked to upstream body: %#v", body)
    }
    if _, ok := body["previous_response_id"]; ok {
        t.Fatalf("previous_response_id leaked to upstream body: %#v", body)
    }
    if body["stream"] != true {
        t.Fatalf("stream = %#v", body["stream"])
    }
}
```

- [ ] **Step 2: Run real bridge test to verify failure**

Run: `go test ./...`

Expected: FAIL with `upstream bridge is not implemented yet`.

- [ ] **Step 3: Implement basic HTTP body conversion and SSE streaming**

Replace `state.go` with:

```go
package main

import "encoding/json"

type BridgeState struct {
    prewarmCount int
}

func (s *BridgeState) BuildHTTPBody(msg map[string]any) ([]byte, error) {
    body := make(map[string]any, len(msg))
    for key, value := range msg {
        switch key {
        case "type", "generate", "previous_response_id":
            continue
        default:
            body[key] = value
        }
    }
    if _, ok := body["stream"]; !ok {
        body["stream"] = true
    }
    return json.Marshal(body)
}

func (s *BridgeState) UpdateFromSSEData(data []byte) {}
```

Modify `ws_bridge.go` imports to include:

```go
import (
    "bufio"
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "strings"
    "time"

    "github.com/coder/websocket"
)
```

Replace the real-request branch in `handleWSMessage`:

```go
    return p.bridgeToUpstream(ctx, conn, state, msg, http.Header{})
```

Add functions to `ws_bridge.go`:

```go
func (p *Proxy) bridgeToUpstream(ctx context.Context, conn *websocket.Conn, state *BridgeState, msg map[string]any, incoming http.Header) error {
    body, err := state.BuildHTTPBody(msg)
    if err != nil {
        return fmt.Errorf("build HTTP body: %w", err)
    }
    upstreamURL, err := p.cfg.UpstreamURLFor(p.cfg.LocalBasePath + "/responses")
    if err != nil {
        return err
    }
    req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL, bytes.NewReader(body))
    if err != nil {
        return err
    }
    req.Header = SanitizeHeaders(incoming, p.cfg)
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("Accept", "text/event-stream")

    resp, err := p.client.Do(req)
    if err != nil {
        return sendWSError(ctx, conn, "upstream request failed", err.Error())
    }
    defer resp.Body.Close()
    if resp.StatusCode < 200 || resp.StatusCode >= 300 {
        detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
        return sendWSError(ctx, conn, fmt.Sprintf("upstream returned %d", resp.StatusCode), string(detail))
    }

    return readSSEData(resp.Body, func(data []byte) error {
        trimmed := strings.TrimSpace(string(data))
        if trimmed == "" || trimmed == "[DONE]" {
            return nil
        }
        state.UpdateFromSSEData(data)
        writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
        err := conn.Write(writeCtx, websocket.MessageText, data)
        cancel()
        return err
    })
}

func readSSEData(r io.Reader, onData func([]byte) error) error {
    scanner := bufio.NewScanner(r)
    scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
    var dataLines [][]byte
    flush := func() error {
        if len(dataLines) == 0 {
            return nil
        }
        data := bytes.Join(dataLines, []byte("\n"))
        dataLines = nil
        return onData(data)
    }
    for scanner.Scan() {
        line := scanner.Bytes()
        if len(line) == 0 {
            if err := flush(); err != nil {
                return err
            }
            continue
        }
        if bytes.HasPrefix(line, []byte("data:")) {
            value := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
            copied := append([]byte(nil), value...)
            dataLines = append(dataLines, copied)
        }
    }
    if err := scanner.Err(); err != nil {
        return err
    }
    return flush()
}

func sendWSError(ctx context.Context, conn *websocket.Conn, message, detail string) error {
    payload, _ := json.Marshal(map[string]any{
        "type": "response.failed",
        "error": map[string]any{"message": message, "detail": detail},
    })
    writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
    err := conn.Write(writeCtx, websocket.MessageText, payload)
    cancel()
    return err
}
```

- [ ] **Step 4: Run real bridge test to verify pass**

Run: `go test ./...`

Expected: PASS.

- [ ] **Step 5: Commit SSE bridge task**

```bash
git add ws_bridge.go state.go ws_bridge_test.go
git commit -m "Bridge websocket responses to upstream SSE"
```

---

### Task 6: Per-Connection State Reconstruction

**Files:**
- Modify: `state.go`
- Test: `state_test.go`

**Interfaces:**
- Consumes: `BridgeState.BuildHTTPBody` from Task 5.
- Produces: `type BridgeState struct { prewarmCount int; fullInput []any; responseContexts map[string][]any; outputItemsByCallID map[string]map[string]any; lastResponseID string }`
- Produces: `func NewBridgeState() *BridgeState`
- Produces: `func (s *BridgeState) BuildHTTPBody(msg map[string]any) ([]byte, error)` with context reconstruction.
- Produces: `func (s *BridgeState) UpdateFromSSEData(data []byte)` to record response ids and output items.
- WebSocket handler must instantiate `NewBridgeState()` instead of `&BridgeState{}`.

- [ ] **Step 1: Write failing state reconstruction tests**

Create `state_test.go`:

```go
package main

import (
    "encoding/json"
    "testing"
)

func decodeBody(t *testing.T, body []byte) map[string]any {
    t.Helper()
    var out map[string]any
    if err := json.Unmarshal(body, &out); err != nil {
        t.Fatalf("decode body: %v", err)
    }
    return out
}

func TestBridgeStateReconstructsFunctionCallOutputContext(t *testing.T) {
    state := NewBridgeState()
    first := map[string]any{
        "type":  "response.create",
        "model": "gpt-test",
        "input": []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "run"}}}},
        "tools": []any{},
    }
    body, err := state.BuildHTTPBody(first)
    if err != nil {
        t.Fatal(err)
    }
    _ = decodeBody(t, body)

    state.UpdateFromSSEData([]byte(`{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"shell","arguments":"{}"}}`))
    state.UpdateFromSSEData([]byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"function_call","call_id":"call-1","name":"shell","arguments":"{}"}]}}`))

    second := map[string]any{
        "type":                 "response.create",
        "model":                "gpt-test",
        "previous_response_id": "resp-1",
        "input":                []any{map[string]any{"type": "function_call_output", "call_id": "call-1", "output": "ok"}},
        "tools":                []any{},
    }
    body, err = state.BuildHTTPBody(second)
    if err != nil {
        t.Fatal(err)
    }
    decoded := decodeBody(t, body)
    input := decoded["input"].([]any)

    var sawCall bool
    var sawOutput bool
    for _, raw := range input {
        item := raw.(map[string]any)
        if item["type"] == "function_call" && item["call_id"] == "call-1" {
            sawCall = true
        }
        if item["type"] == "function_call_output" && item["call_id"] == "call-1" {
            sawOutput = true
        }
    }
    if !sawCall || !sawOutput {
        t.Fatalf("reconstructed input missing call=%v output=%v: %#v", sawCall, sawOutput, input)
    }
    if _, ok := decoded["previous_response_id"]; ok {
        t.Fatalf("previous_response_id leaked: %#v", decoded)
    }
}
```

- [ ] **Step 2: Run state tests to verify failure**

Run: `go test ./...`

Expected: FAIL with `undefined: NewBridgeState` or missing reconstructed call item.

- [ ] **Step 3: Implement state reconstruction**

Replace `state.go` with:

```go
package main

import (
    "encoding/json"
    "sort"
)

type BridgeState struct {
    prewarmCount        int
    fullInput           []any
    responseContexts    map[string][]any
    outputItemsByCallID map[string]map[string]any
    lastResponseID      string
}

func NewBridgeState() *BridgeState {
    return &BridgeState{
        responseContexts:    map[string][]any{},
        outputItemsByCallID: map[string]map[string]any{},
    }
}

func (s *BridgeState) BuildHTTPBody(msg map[string]any) ([]byte, error) {
    incoming := asArray(msg["input"])
    previousID, _ := msg["previous_response_id"].(string)

    reconstructed := cloneItems(incoming)
    if previousID != "" {
        if ctx, ok := s.responseContexts[previousID]; ok {
            reconstructed = appendDedupe(cloneItems(ctx), incoming...)
        } else if len(s.fullInput) > 0 {
            reconstructed = appendDedupe(cloneItems(s.fullInput), incoming...)
        }
    }
    reconstructed = s.ensureFunctionCalls(reconstructed)

    body := make(map[string]any, len(msg))
    for key, value := range msg {
        switch key {
        case "type", "generate", "previous_response_id":
            continue
        case "input":
            body[key] = reconstructed
        default:
            body[key] = value
        }
    }
    if _, ok := body["input"]; !ok {
        body["input"] = reconstructed
    }
    if _, ok := body["stream"]; !ok {
        body["stream"] = true
    }
    s.fullInput = appendDedupe(s.fullInput, reconstructed...)
    return json.Marshal(body)
}

func (s *BridgeState) UpdateFromSSEData(data []byte) {
    var event map[string]any
    if err := json.Unmarshal(data, &event); err != nil {
        return
    }
    if response, ok := event["response"].(map[string]any); ok {
        if id, ok := response["id"].(string); ok && id != "" {
            s.lastResponseID = id
        }
        if output := asArray(response["output"]); len(output) > 0 {
            s.recordOutputItems(output)
            if s.lastResponseID != "" {
                s.responseContexts[s.lastResponseID] = appendDedupe(cloneItems(s.fullInput), output...)
            }
        }
    }
    if item, ok := event["item"].(map[string]any); ok {
        s.recordOutputItems([]any{item})
        if s.lastResponseID != "" {
            s.responseContexts[s.lastResponseID] = appendDedupe(cloneItems(s.fullInput), item)
        }
    }
}

func (s *BridgeState) recordOutputItems(items []any) {
    for _, raw := range items {
        item, ok := raw.(map[string]any)
        if !ok {
            continue
        }
        callID, _ := item["call_id"].(string)
        typ, _ := item["type"].(string)
        if callID != "" && (typ == "function_call" || typ == "custom_tool_call") {
            s.outputItemsByCallID[callID] = cloneMap(item)
        }
    }
}

func (s *BridgeState) ensureFunctionCalls(items []any) []any {
    out := make([]any, 0, len(items)+2)
    presentCalls := map[string]bool{}
    for _, raw := range items {
        if item, ok := raw.(map[string]any); ok {
            if typ, _ := item["type"].(string); typ == "function_call" || typ == "custom_tool_call" {
                if callID, _ := item["call_id"].(string); callID != "" {
                    presentCalls[callID] = true
                }
            }
        }
    }
    for _, raw := range items {
        if item, ok := raw.(map[string]any); ok {
            typ, _ := item["type"].(string)
            callID, _ := item["call_id"].(string)
            if (typ == "function_call_output" || typ == "custom_tool_call_output") && callID != "" && !presentCalls[callID] {
                if callItem, ok := s.outputItemsByCallID[callID]; ok {
                    out = append(out, cloneMap(callItem))
                    presentCalls[callID] = true
                }
            }
        }
        out = append(out, raw)
    }
    return out
}

func asArray(value any) []any {
    if value == nil {
        return nil
    }
    items, ok := value.([]any)
    if !ok {
        return nil
    }
    return items
}

func appendDedupe(base []any, additions ...any) []any {
    out := cloneItems(base)
    seen := map[string]bool{}
    for _, item := range out {
        seen[itemKey(item)] = true
    }
    for _, item := range additions {
        key := itemKey(item)
        if seen[key] {
            continue
        }
        seen[key] = true
        out = append(out, cloneValue(item))
    }
    return out
}

func itemKey(item any) string {
    normalized := normalizeValue(item)
    data, err := json.Marshal(normalized)
    if err != nil {
        return ""
    }
    return string(data)
}

func normalizeValue(value any) any {
    switch v := value.(type) {
    case map[string]any:
        keys := make([]string, 0, len(v))
        for key := range v {
            keys = append(keys, key)
        }
        sort.Strings(keys)
        out := make(map[string]any, len(v))
        for _, key := range keys {
            out[key] = normalizeValue(v[key])
        }
        return out
    case []any:
        out := make([]any, len(v))
        for i, item := range v {
            out[i] = normalizeValue(item)
        }
        return out
    default:
        return v
    }
}

func cloneItems(items []any) []any {
    if len(items) == 0 {
        return nil
    }
    out := make([]any, 0, len(items))
    for _, item := range items {
        out = append(out, cloneValue(item))
    }
    return out
}

func cloneMap(in map[string]any) map[string]any {
    out := make(map[string]any, len(in))
    for key, value := range in {
        out[key] = cloneValue(value)
    }
    return out
}

func cloneValue(value any) any {
    switch v := value.(type) {
    case map[string]any:
        return cloneMap(v)
    case []any:
        return cloneItems(v)
    default:
        return v
    }
}
```

Modify `ws_bridge.go` state initialization:

```go
    state := NewBridgeState()
```

- [ ] **Step 4: Run state tests to verify pass**

Run: `go test ./...`

Expected: PASS.

- [ ] **Step 5: Commit state reconstruction task**

```bash
git add state.go state_test.go ws_bridge.go
git commit -m "Reconstruct websocket response context"
```

---

### Task 7: Main Entrypoint, Example Config, and README

**Files:**
- Create: `main.go`
- Create: `a.example.json`
- Create: `README.md`
- Test: update existing tests only if package build requires it.

**Interfaces:**
- Consumes: `LoadConfig`, `NewProxy`, `Config`.
- Produces: executable behavior `go run .` loads `a.json`, listens on configured address, and serves proxy.
- Produces: `func findConfigPath() string`.

- [ ] **Step 1: Write startup helper test**

Append to `config_test.go`:

```go
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
```

- [ ] **Step 2: Run startup helper test to verify failure**

Run: `go test ./...`

Expected: FAIL with `undefined: ConfigPathCandidates`.

- [ ] **Step 3: Add config path helper**

Append to `config.go`:

```go
func ConfigPathCandidates(executablePath, workingDir string) []string {
    candidates := []string{}
    if executablePath != "" {
        candidates = append(candidates, path.Join(path.Dir(executablePath), "a.json"))
    }
    if workingDir != "" {
        cwdPath := path.Join(workingDir, "a.json")
        if len(candidates) == 0 || candidates[0] != cwdPath {
            candidates = append(candidates, cwdPath)
        }
    }
    return candidates
}
```

- [ ] **Step 4: Create main entrypoint**

Create `main.go`:

```go
package main

import (
    "errors"
    "fmt"
    "log"
    "net/http"
    "os"
    "path/filepath"
    "time"
)

func main() {
    configPath, err := findConfigPath()
    if err != nil {
        log.Fatal(err)
    }
    cfg, err := LoadConfig(configPath)
    if err != nil {
        log.Fatal(err)
    }

    client := &http.Client{Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second}
    proxy := NewProxy(cfg, client)
    server := &http.Server{
        Addr:              cfg.Listen,
        Handler:           proxy,
        ReadHeaderTimeout: 15 * time.Second,
    }

    log.Printf("codex-ws-proxy listening on http://%s%s", cfg.Listen, cfg.LocalBasePath)
    if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
        log.Fatal(err)
    }
}

func findConfigPath() (string, error) {
    exe, _ := os.Executable()
    cwd, _ := os.Getwd()
    candidates := []string{}
    for _, candidate := range ConfigPathCandidates(filepath.ToSlash(exe), filepath.ToSlash(cwd)) {
        candidates = append(candidates, filepath.FromSlash(candidate))
    }
    for _, candidate := range candidates {
        if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
            return candidate, nil
        }
    }
    return "", fmt.Errorf("a.json not found; checked %v", candidates)
}
```

- [ ] **Step 5: Create example config and README**

Create `a.example.json`:

```json
{
  "listen": "127.0.0.1:39493",
  "upstream_base_url": "https://example-relay.com/v1",
  "local_base_path": "/v1",
  "api_key": "replace-with-api-key",
  "auth_header": "Authorization",
  "auth_scheme": "Bearer",
  "websocket_mode": "bridge",
  "websocket_compression": true,
  "timeout_seconds": 600,
  "log_requests": true
}
```

Create `README.md`:

```markdown
# Codex WebSocket-to-HTTPS Proxy

A lightweight local proxy for Codex model requests. Codex can keep its ChatGPT login and remote-control behavior while model requests are billed through an OpenAI-compatible upstream relay.

## Build

```bash
go mod tidy
go build -o codex-ws-proxy .
```

## Configure

Place `a.json` next to the binary, or in the working directory when running with `go run .`.

```json
{
  "listen": "127.0.0.1:39493",
  "upstream_base_url": "https://example-relay.com/v1",
  "local_base_path": "/v1",
  "api_key": "replace-with-api-key",
  "auth_header": "Authorization",
  "auth_scheme": "Bearer",
  "websocket_mode": "bridge",
  "websocket_compression": true,
  "timeout_seconds": 600,
  "log_requests": false
}
```

Point Codex at the proxy:

```toml
openai_base_url = "http://127.0.0.1:39493/v1"
```

## Behavior

- Normal HTTP requests under `/v1` are forwarded to the upstream base URL.
- WebSocket requests to `/v1/responses` are accepted locally and converted to HTTP `POST /v1/responses` with `Accept: text/event-stream`.
- Incoming Codex, ChatGPT, OAuth, cookie, and OpenAI-specific headers are stripped before forwarding.
- Upstream auth is injected from `a.json`.
- Codex WebSocket-only fields `type`, `generate`, and `previous_response_id` are not sent to HTTP upstream.
- Prewarm frames with `generate:false` are answered locally.
- Per-connection in-memory state reconstructs tool-call context for stateless upstream relays.

## Verify

```bash
go test ./...
```
```

- [ ] **Step 6: Run all tests and build**

Run: `go test ./...`

Expected: PASS.

Run: `go build -o codex-ws-proxy .`

Expected: binary is created.

- [ ] **Step 7: Commit entrypoint and docs task**

```bash
git add main.go config.go config_test.go a.example.json README.md go.mod go.sum
git commit -m "Add proxy entrypoint and usage docs"
```

---

### Task 8: Final Verification Pass

**Files:**
- Modify only if tests reveal a defect in prior files.

**Interfaces:**
- Consumes the full executable from Tasks 1-7.
- Produces verified local artifact `codex-ws-proxy`.

- [ ] **Step 1: Run full test suite**

Run: `go test ./...`

Expected: PASS.

- [ ] **Step 2: Build the binary**

Run: `go build -o codex-ws-proxy .`

Expected: command exits successfully and `codex-ws-proxy` exists.

- [ ] **Step 3: Inspect git status**

Run: `git status --short`

Expected: no uncommitted implementation files. Existing unrelated files such as `.mcp.json` or `clawd/` may remain untracked if they were present before this work.

- [ ] **Step 4: Report verification result**

If Step 1 and Step 2 passed without code changes, do not create an empty commit. If either step failed, fix the specific failing file named by the compiler or test output, rerun `go test ./...` and `go build -o codex-ws-proxy .`, then commit the concrete files changed by that fix with message `Fix proxy verification issues`.

---

## Self-Review Notes

- Spec coverage: Tasks cover config, header sanitization, HTTP forwarding, WebSocket prewarm, WebSocket-to-SSE bridge, state reconstruction, docs, tests, and final build verification.
- Red-flag scan: This plan avoids incomplete markers and undefined later-step interface names. Each code-changing step includes exact code to write.
- Type consistency: `Config`, `Proxy`, `BridgeState`, `SanitizeHeaders`, `BuildHTTPBody`, and `UpdateFromSSEData` names are consistent across tasks.
- Scope: The plan implements one local proxy binary only; no TLS server, persistent storage, public deployment, or upstream WebSocket mode is included.
