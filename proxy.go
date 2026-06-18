package main

import (
	"context"
	"encoding/json"
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
	if r.Method == http.MethodGet && r.URL.Path == p.cfg.LocalBasePath+"/models" && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
		if readErr != nil {
			http.Error(w, fmt.Sprintf("read upstream models response: %v", readErr), http.StatusBadGateway)
			return
		}
		if adapted, ok := adaptModelsResponse(data); ok {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Del("Content-Length")
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write(adapted)
			if p.cfg.LogRequests {
				log.Printf("%s %s -> %s status=%d bytes=%d duration=%s", r.Method, r.URL.RequestURI(), upstreamURL, resp.StatusCode, len(adapted), time.Since(start))
			}
			return
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(data)
		if p.cfg.LogRequests {
			log.Printf("%s %s -> %s status=%d bytes=%d duration=%s", r.Method, r.URL.RequestURI(), upstreamURL, resp.StatusCode, len(data), time.Since(start))
		}
		return
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

func adaptModelsResponse(data []byte) ([]byte, bool) {
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, false
	}
	if _, ok := body["models"]; ok {
		return data, false
	}
	upstreamModels, ok := body["data"].([]any)
	if !ok {
		return nil, false
	}
	models := make([]any, 0, len(upstreamModels))
	for i, raw := range upstreamModels {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		id, _ := item["id"].(string)
		if id == "" {
			continue
		}
		model := cloneMap(item)
		model["slug"] = id
		if _, ok := model["display_name"]; !ok {
			model["display_name"] = id
		}
		if _, ok := model["description"]; !ok {
			model["description"] = "Model provided by the configured upstream relay."
		}
		if _, ok := model["default_reasoning_level"]; !ok {
			model["default_reasoning_level"] = "medium"
		}
		if _, ok := model["supported_reasoning_levels"]; !ok {
			model["supported_reasoning_levels"] = []any{
				map[string]any{"effort": "low", "description": "Fast responses with lighter reasoning"},
				map[string]any{"effort": "medium", "description": "Balances speed and reasoning depth"},
				map[string]any{"effort": "high", "description": "Greater reasoning depth for complex work"},
				map[string]any{"effort": "xhigh", "description": "Extra high reasoning depth"},
			}
		}
		if _, ok := model["shell_type"]; !ok {
			model["shell_type"] = "shell_command"
		}
		if _, ok := model["visibility"]; !ok {
			model["visibility"] = "list"
		}
		if _, ok := model["supported_in_api"]; !ok {
			model["supported_in_api"] = true
		}
		if _, ok := model["priority"]; !ok {
			model["priority"] = i
		}
		if _, ok := model["additional_speed_tiers"]; !ok {
			model["additional_speed_tiers"] = []any{"fast"}
		}
		if _, ok := model["service_tiers"]; !ok {
			model["service_tiers"] = []any{
				map[string]any{
					"id":          "priority",
					"name":        "Fast",
					"description": "Faster responses through the upstream priority tier.",
				},
			}
		}
		if _, ok := model["availability_nux"]; !ok {
			model["availability_nux"] = nil
		}
		if _, ok := model["upgrade"]; !ok {
			model["upgrade"] = nil
		}
		if _, ok := model["base_instructions"]; !ok {
			model["base_instructions"] = codexBaseInstructions()
		}
		if _, ok := model["model_messages"]; !ok {
			model["model_messages"] = codexModelMessages(model["base_instructions"])
		}
		if _, ok := model["supports_reasoning_summaries"]; !ok {
			model["supports_reasoning_summaries"] = true
		}
		if _, ok := model["default_reasoning_summary"]; !ok {
			model["default_reasoning_summary"] = "none"
		}
		if _, ok := model["support_verbosity"]; !ok {
			model["support_verbosity"] = true
		}
		if _, ok := model["default_verbosity"]; !ok {
			model["default_verbosity"] = "low"
		}
		if _, ok := model["apply_patch_tool_type"]; !ok {
			model["apply_patch_tool_type"] = "freeform"
		}
		if _, ok := model["web_search_tool_type"]; !ok {
			model["web_search_tool_type"] = "text_and_image"
		}
		if _, ok := model["truncation_policy"]; !ok {
			model["truncation_policy"] = map[string]any{"mode": "tokens", "limit": 10000}
		}
		if _, ok := model["supports_parallel_tool_calls"]; !ok {
			model["supports_parallel_tool_calls"] = true
		}
		if _, ok := model["supports_image_detail_original"]; !ok {
			model["supports_image_detail_original"] = true
		}
		if _, ok := model["context_window"]; !ok {
			model["context_window"] = 272000
		}
		if _, ok := model["max_context_window"]; !ok {
			model["max_context_window"] = 272000
		}
		if _, ok := model["effective_context_window_percent"]; !ok {
			model["effective_context_window_percent"] = 95
		}
		if _, ok := model["experimental_supported_tools"]; !ok {
			model["experimental_supported_tools"] = []any{}
		}
		if _, ok := model["input_modalities"]; !ok {
			model["input_modalities"] = []any{"text", "image"}
		}
		if _, ok := model["supports_search_tool"]; !ok {
			model["supports_search_tool"] = true
		}
		models = append(models, model)
	}
	if len(models) == 0 {
		return nil, false
	}
	out, err := json.Marshal(map[string]any{"models": models})
	if err != nil {
		return nil, false
	}
	return out, true
}

func codexBaseInstructions() string {
	return "You are Codex, a coding agent based on GPT-5. You and the user share one workspace, and your job is to collaborate with them until their goal is genuinely handled."
}

func codexModelMessages(base any) map[string]any {
	baseInstructions, _ := base.(string)
	if baseInstructions == "" {
		baseInstructions = codexBaseInstructions()
	}
	return map[string]any{
		"instructions_template": baseInstructions + "\n\n{{ personality }}",
		"instructions_variables": map[string]any{
			"personality_default":   "",
			"personality_pragmatic": "# Personality\n\nYou are a pragmatic, effective software engineer. You communicate efficiently, focus on the task, and keep the user clearly informed about actionable next steps.",
			"personality_friendly":  "# Personality\n\nYou are a warm, curious collaborator. You stay clear, helpful, and proactive while keeping the work moving.",
		},
	}
}
