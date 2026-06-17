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
	if r.URL.Path == p.cfg.LocalBasePath+"/responses" && isWebSocketUpgrade(r) {
		p.handleResponsesWebSocket(w, r)
		return
	}
	p.handleHTTP(w, r)
}

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") && strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
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
