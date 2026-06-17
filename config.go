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
