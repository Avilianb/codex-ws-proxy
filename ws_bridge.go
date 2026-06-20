package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

type wsConn struct {
	conn        net.Conn
	r           *bufio.Reader
	fragOpcode  byte
	fragPayload []byte
}

func (p *Proxy) handleResponsesWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := acceptWS(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.close()

	state := NewBridgeState()
	ctx := r.Context()
	for {
		data, opcode, err := conn.readFrame()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				_ = conn.writeClose(wsClosePolicyViolation, err.Error())
			}
			return
		}
		switch opcode {
		case wsOpcodeText:
			if err := p.handleWSMessage(ctx, conn, state, data); err != nil {
				_ = conn.writeClose(wsClosePolicyViolation, err.Error())
				return
			}
		case wsOpcodeClose:
			_ = conn.writeClose(wsCloseNormalClosure, "")
			return
		case wsOpcodePing:
			_ = conn.writeFrame(wsOpcodePong, data)
		case wsOpcodePong:
			continue
		default:
			_ = conn.writeClose(wsCloseUnsupportedData, "text frames only")
			return
		}
	}
}

func (p *Proxy) handleWSMessage(ctx context.Context, conn *wsConn, state *BridgeState, data []byte) error {
	var msg map[string]any
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("invalid JSON frame")
	}
	if msg["type"] != "response.create" {
		return fmt.Errorf("unsupported websocket message type")
	}
	if generate, ok := msg["generate"].(bool); ok && !generate {
		return writeLocalPrewarm(ctx, conn, state)
	}
	return p.bridgeToUpstream(ctx, conn, state, msg, http.Header{})
}

func (p *Proxy) bridgeToUpstream(ctx context.Context, conn *wsConn, state *BridgeState, msg map[string]any, incoming http.Header) error {
	if _, ok := ctx.Deadline(); !ok && p.cfg.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(p.cfg.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	body, err := state.BuildHTTPBody(msg)
	if err != nil {
		return fmt.Errorf("build HTTP body: %w", err)
	}
	body, err = sanitizeResponsesJSONBody(body)
	if err != nil {
		return fmt.Errorf("sanitize HTTP body: %w", err)
	}
	upstreamURL, err := p.cfg.UpstreamURLFor(p.cfg.LocalBasePath + "/responses")
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header = SanitizeHeaders(incoming, p.cfg)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	client := p.client
	if client.Timeout != 0 {
		streamClient := *client
		streamClient.Timeout = 0
		client = &streamClient
	}
	resp, err := doUpstreamRequest(client, req)
	if err != nil {
		return sendWSError(ctx, conn, "upstream request failed", err.Error(), p.cfg.LogRequests)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return sendWSError(ctx, conn, fmt.Sprintf("upstream returned %d", resp.StatusCode), string(detail), p.cfg.LogRequests)
	}

	err = readSSEData(resp.Body, func(data []byte) error {
		trimmed := strings.TrimSpace(string(data))
		if trimmed == "" || trimmed == "[DONE]" {
			return nil
		}
		if p.cfg.LogRequests {
			logUpstreamFailureEvent(data)
		}
		state.UpdateFromSSEData(data)
		return conn.writeText(ctx, data)
	})
	if err != nil {
		return sendWSError(ctx, conn, "upstream stream failed", err.Error(), p.cfg.LogRequests)
	}
	return nil
}

func readSSEData(r io.Reader, onData func([]byte) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSSEEventBytes)
	var dataLines [][]byte
	flush := func() error {
		if len(dataLines) == 0 {
			return nil
		}
		data := bytes.Join(dataLines, []byte("\n"))
		dataLines = nil
		return onData(data)
	}
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			value := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
			copied := append([]byte(nil), value...)
			dataLines = append(dataLines, copied)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return flush()
}

func sendWSError(ctx context.Context, conn *wsConn, message, detail string, logDetail bool) error {
	if logDetail {
		log.Printf("websocket bridge sending response.failed: message=%q detail=%q", message, truncateForLog(detail, 2048))
	}
	payload, _ := json.Marshal(map[string]any{
		"type":  "response.failed",
		"error": map[string]any{"message": message, "detail": detail},
	})
	if _, ok := ctx.Deadline(); ok && ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}
	return conn.writeText(ctx, payload)
}

func logUpstreamFailureEvent(data []byte) {
	var event map[string]any
	if err := json.Unmarshal(data, &event); err != nil {
		return
	}
	typ, _ := event["type"].(string)
	if typ != "response.failed" && typ != "error" {
		return
	}
	log.Printf("upstream sent %s event: %s", typ, summarizeFailureEvent(event))
}

func summarizeFailureEvent(event map[string]any) string {
	if raw, ok := event["error"]; ok {
		if data, err := json.Marshal(raw); err == nil {
			return truncateForLog(string(data), 2048)
		}
	}
	if raw, ok := event["response"].(map[string]any); ok {
		if errValue, ok := raw["error"]; ok {
			if data, err := json.Marshal(errValue); err == nil {
				return truncateForLog(string(data), 2048)
			}
		}
	}
	data, err := json.Marshal(event)
	if err != nil {
		return "<unserializable>"
	}
	return truncateForLog(string(data), 2048)
}

func truncateForLog(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "...[truncated]"
}

func writeLocalPrewarm(ctx context.Context, conn *wsConn, state *BridgeState) error {
	state.prewarmCount++
	id := fmt.Sprintf("local-prewarm-%d", state.prewarmCount)
	events := []map[string]any{
		{"type": "response.created", "response": map[string]any{"id": id}},
		{"type": "response.completed", "response": map[string]any{"id": id, "output": []any{}, "usage": nil}},
	}
	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if err := conn.writeText(ctx, payload); err != nil {
			return err
		}
	}
	return nil
}

const (
	wsOpcodeContinuation   = 0x0
	wsOpcodeText           = 0x1
	wsOpcodeBinary         = 0x2
	wsOpcodeClose          = 0x8
	wsOpcodePing           = 0x9
	wsOpcodePong           = 0xA
	wsMagicGUID            = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	wsCloseNormalClosure   = 1000
	wsCloseUnsupportedData = 1003
	wsClosePolicyViolation = 1008
	maxWSMessageBytes      = 16 * 1024 * 1024
	maxSSEEventBytes       = 64 * 1024 * 1024
)

func acceptWS(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	if !isWebSocketUpgrade(r) {
		return nil, errors.New("not a websocket upgrade")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" || r.Header.Get("Sec-WebSocket-Version") != "13" {
		return nil, errors.New("invalid websocket handshake")
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("http hijacking not supported")
	}
	netConn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}
	accept := computeWebSocketAccept(key)
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n"
	response += "\r\n"
	if _, err := rw.WriteString(response); err != nil {
		netConn.Close()
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		netConn.Close()
		return nil, err
	}
	return &wsConn{conn: netConn, r: rw.Reader}, nil
}

func computeWebSocketAccept(key string) string {
	h := sha1.Sum([]byte(strings.TrimSpace(key) + wsMagicGUID))
	return base64.StdEncoding.EncodeToString(h[:])
}

func (c *wsConn) close() {
	_ = c.conn.Close()
}

func (c *wsConn) writeText(ctx context.Context, payload []byte) error {
	return c.writeFrameContext(ctx, wsOpcodeText, payload)
}

func (c *wsConn) writeClose(code uint16, reason string) error {
	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload[:2], code)
	copy(payload[2:], reason)
	return c.writeFrame(wsOpcodeClose, payload)
}

func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	return c.writeFrameContext(context.Background(), opcode, payload)
}

func (c *wsConn) writeFrameContext(ctx context.Context, opcode byte, payload []byte) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetWriteDeadline(deadline)
	} else {
		_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	}
	defer c.conn.SetWriteDeadline(time.Time{})

	firstByte := byte(0x80 | opcode)

	frame := []byte{firstByte}
	length := len(payload)
	switch {
	case length < 126:
		frame = append(frame, byte(length))
	case length <= 65535:
		frame = append(frame, 126, byte(length>>8), byte(length))
	default:
		frame = append(frame, 127)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(length))
		frame = append(frame, b[:]...)
	}
	frame = append(frame, payload...)
	_, err := c.conn.Write(frame)
	return err
}

func (c *wsConn) readFrame() ([]byte, byte, error) {
	for {
		payload, opcode, fin, rsv1, err := c.readRawFrame()
		if err != nil {
			return nil, 0, err
		}

		switch opcode {
		case wsOpcodeText, wsOpcodeBinary:
			if c.fragOpcode != 0 {
				return nil, 0, errors.New("new websocket message started before fragmented message completed")
			}
			if fin {
				if rsv1 {
					return nil, 0, errors.New("unexpected compressed websocket frame")
				}
				return payload, opcode, nil
			}
			if rsv1 {
				return nil, 0, errors.New("unexpected compressed websocket frame")
			}
			c.fragOpcode = opcode
			c.fragPayload = append(c.fragPayload[:0], payload...)
		case wsOpcodeContinuation:
			if c.fragOpcode == 0 {
				return nil, 0, errors.New("unexpected websocket continuation frame")
			}
			if rsv1 {
				return nil, 0, errors.New("unexpected compressed websocket continuation frame")
			}
			if len(c.fragPayload)+len(payload) > maxWSMessageBytes {
				c.resetFragment()
				return nil, 0, errors.New("websocket message too large")
			}
			c.fragPayload = append(c.fragPayload, payload...)
			if !fin {
				continue
			}
			payload = append([]byte(nil), c.fragPayload...)
			opcode = c.fragOpcode
			c.resetFragment()
			return payload, opcode, nil
		case wsOpcodeClose, wsOpcodePing, wsOpcodePong:
			if !fin {
				return nil, 0, errors.New("fragmented websocket control frame")
			}
			if rsv1 {
				return nil, 0, errors.New("compressed websocket control frame")
			}
			if len(payload) > 125 {
				return nil, 0, errors.New("websocket control frame too large")
			}
			return payload, opcode, nil
		default:
			return nil, 0, errors.New("unsupported websocket opcode")
		}
	}
}

func (c *wsConn) resetFragment() {
	c.fragOpcode = 0
	c.fragPayload = nil
}

func (c *wsConn) readRawFrame() ([]byte, byte, bool, bool, error) {
	head := make([]byte, 2)
	if _, err := io.ReadFull(c.r, head); err != nil {
		return nil, 0, false, false, err
	}
	fin := head[0]&0x80 != 0
	rsv1 := head[0]&0x40 != 0
	opcode := head[0] & 0x0f
	masked := head[1]&0x80 != 0
	if !masked {
		return nil, 0, false, false, errors.New("client websocket frames must be masked")
	}
	length := uint64(head[1] & 0x7f)
	if length == 126 {
		buf := make([]byte, 2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return nil, 0, false, false, err
		}
		length = uint64(binary.BigEndian.Uint16(buf))
	} else if length == 127 {
		buf := make([]byte, 8)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return nil, 0, false, false, err
		}
		length = binary.BigEndian.Uint64(buf)
	}
	if length > maxWSMessageBytes {
		return nil, 0, false, false, errors.New("websocket frame too large")
	}
	maskKey := make([]byte, 4)
	if _, err := io.ReadFull(c.r, maskKey); err != nil {
		return nil, 0, false, false, err
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.r, payload); err != nil {
		return nil, 0, false, false, err
	}
	for i := range payload {
		payload[i] ^= maskKey[i%4]
	}
	return payload, opcode, fin, rsv1, nil
}
