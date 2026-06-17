package main

import "encoding/json"

type BridgeState struct {
	prewarmCount int
}

func (s *BridgeState) BuildHTTPBody(msg map[string]any) ([]byte, error) {
	body := make(map[string]any, len(msg))
	for key, value := range msg {
		switch key {
		case "type", "generate", "previous_response_id":
			continue
		default:
			body[key] = value
		}
	}
	if _, ok := body["stream"]; !ok {
		body["stream"] = true
	}
	return json.Marshal(body)
}

func (s *BridgeState) UpdateFromSSEData(data []byte) {}
