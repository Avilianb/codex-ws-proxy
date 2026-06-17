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
