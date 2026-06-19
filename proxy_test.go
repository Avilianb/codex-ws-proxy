package main

import (
	"encoding/json"
	"errors"
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

func TestHTTPProxySanitizesResponsesBody(t *testing.T) {
	upstreamBodies := make(chan map[string]any, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		upstreamBodies <- body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	cfg := Config{UpstreamBaseURL: upstream.URL + "/v1", LocalBasePath: "/v1", APIKey: "relay-key"}
	cfg.ApplyDefaults()
	proxy := NewProxy(cfg, upstream.Client())

	body := `{"input":[{"type":"reasoning","id":"rs-1","summary":null},{"type":"reasoning","id":"rs-2","summary":"bad"},{"type":"reasoning","id":"rs-3","summary":[{"type":"summary_text","text":"kept"}]},{"type":"reasoning","id":"rs-4"},{"type":"message","id":"msg-1","summary":"bad"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "http://local/v1/responses", strings.NewReader(body))
	rr := httptest.NewRecorder()

	proxy.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	got := <-upstreamBodies
	input := got["input"].([]any)
	for _, raw := range input {
		item := raw.(map[string]any)
		switch item["id"] {
		case "rs-1", "rs-2", "rs-4":
			summary, ok := item["summary"].([]any)
			if !ok || len(summary) != 0 {
				t.Fatalf("invalid reasoning summary was not normalized: %#v", input)
			}
		case "rs-3":
			if _, ok := item["summary"]; !ok {
				t.Fatalf("array summary was removed: %#v", input)
			}
		case "msg-1":
			if _, ok := item["summary"]; ok {
				t.Fatalf("non-reasoning invalid summary was preserved: %#v", input)
			}
		}
	}
}

func TestHTTPProxyPassesThroughNonJSONResponsesBody(t *testing.T) {
	upstreamBodies := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		upstreamBodies <- string(body)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	cfg := Config{UpstreamBaseURL: upstream.URL + "/v1", LocalBasePath: "/v1", APIKey: "relay-key"}
	cfg.ApplyDefaults()
	proxy := NewProxy(cfg, upstream.Client())

	req := httptest.NewRequest(http.MethodPost, "http://local/v1/responses", strings.NewReader(`(not json)`))
	rr := httptest.NewRecorder()

	proxy.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if got := <-upstreamBodies; got != `(not json)` {
		t.Fatalf("body = %q", got)
	}
}

func TestHTTPProxyRetriesTransientResponsesNetworkError(t *testing.T) {
	rt := &flakyRoundTripper{}
	cfg := Config{UpstreamBaseURL: "https://relay.example/v1", LocalBasePath: "/v1", APIKey: "relay-key"}
	cfg.ApplyDefaults()
	proxy := NewProxy(cfg, &http.Client{Transport: rt})

	req := httptest.NewRequest(http.MethodPost, "http://local/v1/responses", strings.NewReader(`{"input":[]}`))
	rr := httptest.NewRecorder()

	proxy.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if rt.attempts != 2 {
		t.Fatalf("attempts = %d", rt.attempts)
	}
	if rt.body != `{"input":[]}` {
		t.Fatalf("retried body = %q", rt.body)
	}
}

type flakyRoundTripper struct {
	attempts int
	body     string
}

func (f *flakyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	f.attempts++
	if f.attempts == 1 {
		return nil, errors.New(`dial tcp 151.240.13.116:443: connectex: A connection attempt failed`)
	}
	body, _ := io.ReadAll(req.Body)
	f.body = string(body)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
		Request:    req,
	}, nil
}

func TestHTTPProxyAdaptsOpenAIModelsResponseForCodex(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gpt-test","object":"model","owned_by":"relay"}]}`))
	}))
	defer upstream.Close()

	cfg := Config{UpstreamBaseURL: upstream.URL + "/v1", LocalBasePath: "/v1", APIKey: "relay-key"}
	cfg.ApplyDefaults()
	proxy := NewProxy(cfg, upstream.Client())

	req := httptest.NewRequest(http.MethodGet, "http://local/v1/models", nil)
	rr := httptest.NewRecorder()

	proxy.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"models"`) {
		t.Fatalf("models wrapper missing: %s", body)
	}
	if !strings.Contains(body, `"slug":"gpt-test"`) {
		t.Fatalf("slug missing: %s", body)
	}
	if !strings.Contains(body, `"base_instructions"`) {
		t.Fatalf("base instructions missing: %s", body)
	}
	if !strings.Contains(body, `"model_messages":{"instructions_template"`) {
		t.Fatalf("model messages missing: %s", body)
	}
	for _, field := range []string{
		`"additional_speed_tiers":["fast"]`,
		`"service_tiers":[{"description":"Faster responses through the upstream priority tier.","id":"priority","name":"Fast"}]`,
		`"supports_reasoning_summaries":true`,
		`"default_reasoning_summary":"none"`,
		`"support_verbosity":true`,
		`"default_verbosity":"low"`,
		`"apply_patch_tool_type":"freeform"`,
		`"web_search_tool_type":"text_and_image"`,
		`"truncation_policy":{"limit":10000,"mode":"tokens"}`,
		`"supports_parallel_tool_calls":true`,
		`"supports_image_detail_original":true`,
		`"context_window":272000`,
		`"max_context_window":272000`,
		`"effective_context_window_percent":95`,
		`"experimental_supported_tools":[]`,
		`"input_modalities":["text","image"]`,
		`"supports_search_tool":true`,
	} {
		if !strings.Contains(body, field) {
			t.Fatalf("field %s missing: %s", field, body)
		}
	}
	if strings.Contains(body, `"data"`) {
		t.Fatalf("upstream data wrapper leaked: %s", body)
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
