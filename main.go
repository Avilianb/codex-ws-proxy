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
