# Codex WebSocket-to-HTTPS Proxy Design

Date: 2026-06-17

## Goal

Build a very small local proxy that lets Codex keep its ChatGPT login and remote-control behavior while routing model billing requests through an OpenAI-compatible upstream relay.

Codex will set `openai_base_url` to the local proxy, for example `http://127.0.0.1:39493/v1`. The proxy reads `a.json` from its executable/current directory, strips Codex/ChatGPT/OpenAI login headers before forwarding, injects the third-party API key, and bridges Codex Responses WebSocket traffic to upstream HTTPS Responses API SSE.

## Source Observations

The official `openai/codex` repository was cloned for protocol inspection. Relevant observations:

- `codex-rs/core/src/client.rs` documents that `ModelClientSession` caches a Responses WebSocket connection, the last request, and last response. It says WebSocket prewarm is a v2-only `response.create` with `generate=false` and that later requests can reuse `previous_response_id`.
- `codex-rs/codex-api/src/common.rs` defines `ResponsesApiRequest` for HTTP. It does not contain `previous_response_id` or `generate`.
- The same file defines `ResponseCreateWsRequest`, which adds `previous_response_id` and `generate`, and `ResponsesWsRequest`, serialized as a JSON object with outer field `"type": "response.create"`.
- `codex-rs/core/tests/suite/agent_websocket.rs` asserts real WebSocket request bodies have `type: response.create`, prewarm has `generate: false`, and v2 follow-up requests can set `previous_response_id` while only sending incremental `input` items such as `function_call_output`.
- `codex-rs/responses-api-proxy` is an official HTTP-only Responses API proxy. It is useful as a reference for forwarding `/v1/responses`, but it does not implement the WebSocket-to-HTTPS bridge required here.

## Recommended Approach

Use a Go single-binary project. Keep the proxy small and dependency-light:

- Standard library for HTTP server, HTTP client, config loading, JSON, and SSE parsing.
- One lightweight WebSocket dependency, preferably `github.com/coder/websocket`, for local WebSocket accept/read/write and optional compression.

The proxy will default to a state-rebuilding WebSocket bridge, not a minimal field transform. This is necessary because third-party OpenAI-compatible relays often do not preserve OpenAI Responses state for `previous_response_id`; blindly forwarding incremental frames can trigger upstream errors such as `No tool call found for function call output`.

## Project Layout

- `main.go`: process startup, `a.json` loading, server creation, graceful log output.
- `config.go`: config structure, defaults, validation, listen parsing, upstream URL joining.
- `proxy.go`: normal HTTP reverse proxy for `local_base_path/*`.
- `ws_bridge.go`: WebSocket endpoint for `local_base_path + /responses`; converts WS text frames into HTTP SSE requests and streams SSE data back to WebSocket text messages.
- `sanitize.go`: sensitive header stripping, hop-by-hop header filtering, upstream auth injection.
- `state.go`: per-WebSocket connection state and context reconstruction rules.
- `*_test.go`: focused tests for config, header filtering, HTTP forwarding, WebSocket/SSE bridging, prewarm, and tool-call state reconstruction.

## Configuration

The proxy reads `a.json` from the same directory as the binary when possible, falling back to the current working directory for `go run` development.

Supported config:

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

Defaults:

- `listen`: `127.0.0.1:39493`
- `local_base_path`: `/v1`
- `auth_header`: `Authorization`
- `auth_scheme`: `Bearer`
- `websocket_mode`: `bridge`
- `websocket_compression`: `true`
- `timeout_seconds`: `600`
- `log_requests`: `false`

Startup fails if `upstream_base_url` or `api_key` is empty.

## HTTP Forwarding

For non-WebSocket requests under `local_base_path`, the proxy:

1. Maps `local_base_path + suffix` to `upstream_base_url + suffix`.
2. Removes sensitive and hop-by-hop headers.
3. Injects configured auth, normally `Authorization: Bearer <api_key>`.
4. Streams the request body to upstream.
5. Streams upstream status, safe response headers, and response body back to the client.

This path is intentionally generic so `/v1/models`, `/v1/responses`, and other OpenAI-compatible endpoints can pass through where the upstream supports them.

## Header Sanitization

The proxy must never forward ChatGPT login state or Codex OAuth material to the third-party relay.

Remove request headers when the lower-case header name is any of:

- `authorization`
- `cookie`
- `set-cookie`
- `proxy-authorization`
- `x-stainless-auth`
- `session-id`
- `thread-id`
- `x-client-request-id`

Remove request headers matching these prefixes:

- `openai-`
- `x-openai-`
- `x-stainless-`
- `x-oai-`
- `x-codex-`

Also remove hop-by-hop headers: `connection`, `keep-alive`, `proxy-authenticate`, `te`, `trailer`, `transfer-encoding`, and `upgrade`.

Then inject only the configured upstream auth header. If `auth_scheme` is empty, the value is the raw `api_key`; otherwise it is `<auth_scheme> <api_key>`.

Response forwarding also removes hop-by-hop headers and `set-cookie`.

## WebSocket Bridge

Only `local_base_path + /responses` gets WebSocket bridge handling.

For each WebSocket text frame:

1. Parse JSON.
2. Require `type == "response.create"`; otherwise close with protocol error.
3. If `generate == false`, treat it as Codex prewarm and do not call upstream. Return minimal local response events over WebSocket:
   - `{"type":"response.created","response":{"id":"local-prewarm-N"}}`
   - `{"type":"response.completed","response":{"id":"local-prewarm-N","output":[],"usage":null}}`
4. For real requests, build an HTTP Responses API body by deleting WebSocket-only fields:
   - delete outer `type`
   - delete `generate`
   - delete `previous_response_id` after using it for local context lookup
5. Ensure `stream: true` unless the incoming body explicitly sets it to another JSON value. The default bridge path is optimized for Codex streaming.
6. POST to `upstream_base_url + /responses` with `Accept: text/event-stream` and sanitized headers.
7. Parse upstream SSE incrementally. For each complete SSE event, send each `data:` JSON payload as one WebSocket text message. Do not buffer the whole response.
8. Ignore `[DONE]` or treat it as end-of-stream for that request.

The WebSocket stays open after one request so Codex can send follow-up frames on the same connection.

## State Reconstruction

Each WebSocket connection owns an in-memory bridge state. It is discarded when the connection closes.

State tracks:

- `full_input`: best-known full conversation input for the connection.
- `response_contexts`: map from response id to a snapshot of `full_input` plus assistant output items known after that response.
- `last_response_id`: response id from the latest completed upstream response.
- `output_items_by_call_id`: latest `function_call` or custom call items keyed by `call_id`.
- `seen_input_keys`: optional de-duplication keys to avoid adding identical items twice.

For a real request:

1. If there is no `previous_response_id`, treat incoming `input` as full enough for upstream and set/replace `full_input` with it.
2. If `previous_response_id` exists and maps to a saved context, start from that saved context and append the incoming incremental `input`.
3. If no context exists for `previous_response_id`, fall back to current `full_input` and append incoming `input`.
4. Before sending to upstream, make sure any `function_call_output` item has a matching prior call item in the request body. If the matching `function_call` is known in `output_items_by_call_id` but is not already present in the reconstructed body, insert it immediately before the output item.
5. Remove `previous_response_id` from the outgoing HTTP body so a stateless relay sees a self-contained request.

SSE events update state as they stream:

- If an event has `response.id`, record it as the current response id.
- If an event carries a completed output item (`response.output_item.done`, `response.output_item.added`, or compatible shapes), record the item.
- If a terminal `response.completed` event includes `response.output`, merge those output items into the context snapshot for that response id.

This reconstruction is best-effort because relay compatibility varies, but it directly addresses the known Codex incremental WebSocket behavior.

## Error Handling

Startup errors are fatal with a concise message.

HTTP path:

- Upstream status and body are returned as-is after response header sanitization.
- Network errors return `502 Bad Gateway`.
- Unsupported paths return `404 Not Found`.

WebSocket path:

- Invalid JSON or unsupported frame type closes with a protocol/policy error.
- Upstream HTTP non-2xx sends a JSON error text frame with a `response.failed`-style shape and keeps the WebSocket usable when possible.
- Upstream network/SSE errors send one error text frame and stop the current request.
- Client close cancels the active upstream request through request context cancellation.

## Logging

When `log_requests` is true, log only method, local path, upstream URL, status, byte counts, and timing. Never log `api_key`, incoming auth headers, cookies, request bodies, or response bodies.

## Testing Plan

Use Go tests with local `httptest` servers and WebSocket clients.

Required tests:

1. Config loads `a.json`, applies defaults, and rejects missing `api_key` or `upstream_base_url`.
2. Header sanitizer removes ChatGPT/Codex/OpenAI/OAuth-sensitive headers and injects upstream auth.
3. HTTP proxy maps `/v1/models` and `/v1/responses` correctly and streams response bodies.
4. WebSocket bridge rejects non-`response.create` frames.
5. Prewarm `generate:false` returns local created/completed events and does not hit upstream.
6. Real WebSocket request deletes `type`, `generate`, and `previous_response_id` before upstream HTTP POST.
7. SSE `data:` lines are forwarded as separate WebSocket text messages without waiting for the full response.
8. Function-call reconstruction sends a request containing both matching `function_call` and `function_call_output` when Codex sends only incremental output after a prior tool call.
9. `go test ./...` passes.

## Non-Goals

- No remote deployment, public listen address, TLS termination, or multi-user auth.
- No persistent conversation database; state is per WebSocket connection and in memory.
- No attempt to support upstream WebSocket, because the target relays do not support it.
- No request/response body logging by default.
- No guarantee that every third-party relay supports every Responses API field Codex sends; the proxy preserves fields unless they are WebSocket-only or sensitive.
