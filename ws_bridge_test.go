package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
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
	conn := dialTestWS(t, ctx, wsURL)
	defer conn.Close()

	prewarm := map[string]any{
		"type":     "response.create",
		"model":    "gpt-test",
		"input":    []any{},
		"tools":    []any{},
		"stream":   true,
		"generate": false,
	}
	payload, _ := json.Marshal(prewarm)
	writeTestWSText(t, conn, payload)

	first := readTestWSText(t, conn)
	second := readTestWSText(t, conn)

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
	conn := dialTestWS(t, ctx, wsURL)
	defer conn.Close()

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
	writeTestWSText(t, conn, payload)

	first := readTestWSText(t, conn)
	second := readTestWSText(t, conn)
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

func TestWebSocketBridgeAcceptsFragmentedTextMessage(t *testing.T) {
	upstreamBodies := make(chan map[string]any, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	conn := dialTestWS(t, ctx, wsURL)
	defer conn.Close()

	frame := map[string]any{
		"type":     "response.create",
		"model":    "gpt-test",
		"input":    []any{map[string]any{"type": "message", "role": "user", "content": "hi"}},
		"tools":    []any{},
		"stream":   true,
		"generate": true,
	}
	payload, _ := json.Marshal(frame)
	splitAt := len(payload) / 2
	writeTestWSFrame(t, conn, false, wsOpcodeText, payload[:splitAt])
	writeTestWSFrame(t, conn, true, wsOpcodeContinuation, payload[splitAt:])

	first := readTestWSText(t, conn)
	second := readTestWSText(t, conn)
	if !strings.Contains(string(first), "response.created") {
		t.Fatalf("first event = %q", first)
	}
	if !strings.Contains(string(second), "response.completed") {
		t.Fatalf("second event = %q", second)
	}

	body := <-upstreamBodies
	if body["model"] != "gpt-test" {
		t.Fatalf("model = %#v", body["model"])
	}
}

func TestWebSocketBridgeDoesNotUseHTTPClientTimeoutForSSEStream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\"}}\n\n"))
		flusher.Flush()
		time.Sleep(80 * time.Millisecond)
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[]}}\n\n"))
		flusher.Flush()
	}))
	defer upstream.Close()

	cfg := Config{UpstreamBaseURL: upstream.URL + "/v1", LocalBasePath: "/v1", APIKey: "relay-key", TimeoutSeconds: 5}
	cfg.ApplyDefaults()
	proxy := NewProxy(cfg, &http.Client{Timeout: 25 * time.Millisecond, Transport: upstream.Client().Transport})
	server := httptest.NewServer(proxy)
	defer server.Close()

	wsURL := "ws" + server.URL[len("http"):] + "/v1/responses"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn := dialTestWS(t, ctx, wsURL)
	defer conn.Close()

	frame := map[string]any{
		"type":     "response.create",
		"model":    "gpt-test",
		"input":    []any{},
		"tools":    []any{},
		"stream":   true,
		"generate": true,
	}
	payload, _ := json.Marshal(frame)
	writeTestWSText(t, conn, payload)

	first := readTestWSText(t, conn)
	second := readTestWSText(t, conn)

	if !strings.Contains(string(first), "response.created") {
		t.Fatalf("first event = %q", first)
	}
	if !strings.Contains(string(second), "response.completed") {
		t.Fatalf("second event = %q", second)
	}
}

func TestWebSocketCompressionNegotiationIsDeclined(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{name: "enabled in config", enabled: true},
		{name: "disabled in config", enabled: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
			defer upstream.Close()

			cfg := Config{UpstreamBaseURL: upstream.URL + "/v1", LocalBasePath: "/v1", APIKey: "relay-key", WebSocketCompression: &tc.enabled}
			cfg.ApplyDefaults()
			proxy := NewProxy(cfg, upstream.Client())
			server := httptest.NewServer(proxy)
			defer server.Close()

			wsURL := "ws" + server.URL[len("http"):] + "/v1/responses"
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			conn, resp := dialTestWSWithExtensions(t, ctx, wsURL, "permessage-deflate")
			defer conn.Close()

			gotExtension := strings.Contains(resp.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate")
			if gotExtension {
				t.Fatalf("compression extension was negotiated")
			}
		})
	}
}

func TestWebSocketCompressionRequestStillGetsPlainPrewarmFrame(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()

	enabled := true
	cfg := Config{UpstreamBaseURL: upstream.URL + "/v1", LocalBasePath: "/v1", APIKey: "relay-key", WebSocketCompression: &enabled}
	cfg.ApplyDefaults()
	proxy := NewProxy(cfg, upstream.Client())
	server := httptest.NewServer(proxy)
	defer server.Close()

	wsURL := "ws" + server.URL[len("http"):] + "/v1/responses"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, resp := dialTestWSWithExtensions(t, ctx, wsURL, "permessage-deflate")
	defer conn.Close()
	if strings.Contains(resp.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate") {
		t.Fatalf("compression was negotiated")
	}

	prewarm := map[string]any{
		"type":     "response.create",
		"model":    "gpt-test",
		"input":    []any{},
		"tools":    []any{},
		"stream":   true,
		"generate": false,
	}
	payload, _ := json.Marshal(prewarm)
	writeTestWSText(t, conn, payload)

	first, opcode, compressed := readTestWSFrameWithFlags(t, conn.r)
	if opcode != 0x1 {
		t.Fatalf("opcode = %d", opcode)
	}
	if compressed {
		t.Fatalf("server frame was compressed")
	}
	if !json.Valid(first) {
		t.Fatalf("frame is not JSON: %q", first)
	}
}

func TestWebSocketProtocolErrorSendsCloseFrame(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	conn := dialTestWS(t, ctx, wsURL)
	defer conn.Close()

	// Client-to-server frames must be masked. This deliberately invalid frame
	// exercises the server-side protocol-error path.
	if _, err := conn.Write([]byte{0x81, 0x02, 'o', 'k'}); err != nil {
		t.Fatalf("write invalid websocket frame: %v", err)
	}

	payload, opcode := readTestWSFrame(t, conn.r)
	if opcode != wsOpcodeClose {
		t.Fatalf("opcode = %d, want close; payload %q", opcode, payload)
	}
	if len(payload) < 2 || binary.BigEndian.Uint16(payload[:2]) != wsClosePolicyViolation {
		t.Fatalf("close payload = %v, want policy violation", payload)
	}
}

func readTestWSFrameWithFlags(t *testing.T, r *bufio.Reader) ([]byte, byte, bool) {
	t.Helper()
	head := make([]byte, 2)
	if _, err := io.ReadFull(r, head); err != nil {
		t.Fatalf("read websocket header: %v", err)
	}
	opcode := head[0] & 0x0f
	compressed := head[0]&0x40 != 0
	masked := head[1]&0x80 != 0
	length := uint64(head[1] & 0x7f)
	if length == 126 {
		buf := make([]byte, 2)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("read websocket length16: %v", err)
		}
		length = uint64(binary.BigEndian.Uint16(buf))
	} else if length == 127 {
		buf := make([]byte, 8)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("read websocket length64: %v", err)
		}
		length = binary.BigEndian.Uint64(buf)
	}
	var maskKey []byte
	if masked {
		maskKey = make([]byte, 4)
		if _, err := io.ReadFull(r, maskKey); err != nil {
			t.Fatalf("read websocket mask: %v", err)
		}
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		t.Fatalf("read websocket payload: %v", err)
	}
	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return payload, opcode, compressed
}

type testWSConn struct {
	net.Conn
	r *bufio.Reader
}

func dialTestWS(t *testing.T, ctx context.Context, rawURL string) *testWSConn {
	conn, _ := dialTestWSWithExtensions(t, ctx, rawURL, "")
	return conn
}

func dialTestWSWithExtensions(t *testing.T, ctx context.Context, rawURL, extensions string) (*testWSConn, *http.Response) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", u.Host)
	if err != nil {
		t.Fatalf("dial websocket tcp: %v", err)
	}
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)
	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	extHeader := ""
	if extensions != "" {
		extHeader = "Sec-WebSocket-Extensions: " + extensions + "\r\n"
	}
	_, err = fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n%s\r\n", path, u.Host, key, extHeader)
	if err != nil {
		conn.Close()
		t.Fatalf("write handshake: %v", err)
	}
	r := bufio.NewReader(conn)
	resp, err := http.ReadResponse(r, &http.Request{Method: http.MethodGet})
	if err != nil {
		conn.Close()
		t.Fatalf("read handshake: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		t.Fatalf("handshake status = %d", resp.StatusCode)
	}
	wantAccept := computeAcceptKey(key)
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != wantAccept {
		conn.Close()
		t.Fatalf("accept = %q, want %q", got, wantAccept)
	}
	return &testWSConn{Conn: conn, r: r}, resp
}

func writeTestWSText(t *testing.T, conn *testWSConn, payload []byte) {
	t.Helper()
	writeTestWSFrame(t, conn, true, wsOpcodeText, payload)
}

func writeTestWSFrame(t *testing.T, conn *testWSConn, fin bool, opcode byte, payload []byte) {
	t.Helper()
	firstByte := opcode
	if fin {
		firstByte |= 0x80
	}
	frame := []byte{firstByte}
	maskKey := []byte{1, 2, 3, 4}
	length := len(payload)
	switch {
	case length < 126:
		frame = append(frame, 0x80|byte(length))
	case length <= 65535:
		frame = append(frame, 0x80|126, byte(length>>8), byte(length))
	default:
		frame = append(frame, 0x80|127)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(length))
		frame = append(frame, b[:]...)
	}
	frame = append(frame, maskKey...)
	for i, b := range payload {
		frame = append(frame, b^maskKey[i%4])
	}
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write websocket frame: %v", err)
	}
}

func readTestWSText(t *testing.T, conn *testWSConn) []byte {
	t.Helper()
	for {
		payload, opcode := readTestWSFrame(t, conn.r)
		switch opcode {
		case 0x1:
			return payload
		case 0x8:
			t.Fatalf("websocket closed: %q", payload)
		case 0x9, 0xA:
			continue
		default:
			t.Fatalf("unexpected opcode %d", opcode)
		}
	}
}

func readTestWSFrame(t *testing.T, r *bufio.Reader) ([]byte, byte) {
	t.Helper()
	head := make([]byte, 2)
	if _, err := io.ReadFull(r, head); err != nil {
		t.Fatalf("read websocket header: %v", err)
	}
	opcode := head[0] & 0x0f
	masked := head[1]&0x80 != 0
	length := uint64(head[1] & 0x7f)
	if length == 126 {
		buf := make([]byte, 2)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("read websocket length16: %v", err)
		}
		length = uint64(binary.BigEndian.Uint16(buf))
	} else if length == 127 {
		buf := make([]byte, 8)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("read websocket length64: %v", err)
		}
		length = binary.BigEndian.Uint64(buf)
	}
	var maskKey []byte
	if masked {
		maskKey = make([]byte, 4)
		if _, err := io.ReadFull(r, maskKey); err != nil {
			t.Fatalf("read websocket mask: %v", err)
		}
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		t.Fatalf("read websocket payload: %v", err)
	}
	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return payload, opcode
}

func computeAcceptKey(key string) string {
	h := sha1.Sum([]byte(strings.TrimSpace(key) + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(h[:])
}
