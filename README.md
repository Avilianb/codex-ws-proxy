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
