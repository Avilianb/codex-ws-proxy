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
		"authorization":       true,
		"cookie":              true,
		"set-cookie":          true,
		"proxy-authorization": true,
		"x-stainless-auth":    true,
		"session-id":          true,
		"thread-id":           true,
		"x-client-request-id": true,
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
