package main

import (
	"encoding/json"
	"testing"
)

func decodeBody(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return out
}

func TestBridgeStateReplacesContextWhenNoPreviousResponseID(t *testing.T) {
	state := NewBridgeState()
	first := map[string]any{
		"type":  "response.create",
		"model": "gpt-test",
		"input": []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "old"}}}},
		"tools": []any{},
	}
	if _, err := state.BuildHTTPBody(first); err != nil {
		t.Fatal(err)
	}

	second := map[string]any{
		"type":  "response.create",
		"model": "gpt-test",
		"input": []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "new"}}}},
		"tools": []any{},
	}
	body, err := state.BuildHTTPBody(second)
	if err != nil {
		t.Fatal(err)
	}
	_ = decodeBody(t, body)
	if len(state.fullInput) != 1 {
		t.Fatalf("fullInput length = %d, want 1: %#v", len(state.fullInput), state.fullInput)
	}
}

func TestBridgeStateReconstructsFunctionCallOutputContext(t *testing.T) {
	state := NewBridgeState()
	first := map[string]any{
		"type":  "response.create",
		"model": "gpt-test",
		"input": []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "run"}}}},
		"tools": []any{},
	}
	body, err := state.BuildHTTPBody(first)
	if err != nil {
		t.Fatal(err)
	}
	_ = decodeBody(t, body)

	state.UpdateFromSSEData([]byte(`{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"shell","arguments":"{}"}}`))
	state.UpdateFromSSEData([]byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"function_call","call_id":"call-1","name":"shell","arguments":"{}"}]}}`))

	second := map[string]any{
		"type":                 "response.create",
		"model":                "gpt-test",
		"previous_response_id": "resp-1",
		"input":                []any{map[string]any{"type": "function_call_output", "call_id": "call-1", "output": "ok"}},
		"tools":                []any{},
	}
	body, err = state.BuildHTTPBody(second)
	if err != nil {
		t.Fatal(err)
	}
	decoded := decodeBody(t, body)
	input := decoded["input"].([]any)

	var sawCall bool
	var sawOutput bool
	for _, raw := range input {
		item := raw.(map[string]any)
		if item["type"] == "function_call" && item["call_id"] == "call-1" {
			sawCall = true
		}
		if item["type"] == "function_call_output" && item["call_id"] == "call-1" {
			sawOutput = true
		}
	}
	if !sawCall || !sawOutput {
		t.Fatalf("reconstructed input missing call=%v output=%v: %#v", sawCall, sawOutput, input)
	}
	if _, ok := decoded["previous_response_id"]; ok {
		t.Fatalf("previous_response_id leaked: %#v", decoded)
	}
}
