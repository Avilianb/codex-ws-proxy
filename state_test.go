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

func TestBridgeStateDoesNotDuplicateContextWhenIncomingAlreadyHasHistory(t *testing.T) {
	state := NewBridgeState()
	firstInput := map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "hello"}}}
	first := map[string]any{
		"type":  "response.create",
		"model": "gpt-test",
		"input": []any{firstInput},
		"tools": []any{},
	}
	if _, err := state.BuildHTTPBody(first); err != nil {
		t.Fatal(err)
	}
	state.UpdateFromSSEData([]byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"message","id":"msg-upstream","role":"assistant","content":[{"type":"output_text","text":"hello back"}]}]}}`))

	second := map[string]any{
		"type":                 "response.create",
		"model":                "gpt-test",
		"previous_response_id": "resp-1",
		"input": []any{
			firstInput,
			map[string]any{"type": "message", "id": "msg-client", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "hello back"}}},
			map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "next"}}},
		},
		"tools": []any{},
	}
	body, err := state.BuildHTTPBody(second)
	if err != nil {
		t.Fatal(err)
	}
	decoded := decodeBody(t, body)
	input := decoded["input"].([]any)

	var assistantCopies int
	for _, raw := range input {
		item := raw.(map[string]any)
		if item["role"] != "assistant" {
			continue
		}
		content := item["content"].([]any)
		part := content[0].(map[string]any)
		if part["text"] == "hello back" {
			assistantCopies++
		}
	}
	if assistantCopies != 1 {
		t.Fatalf("assistant context copies = %d, want 1: %#v", assistantCopies, input)
	}
}

func TestBridgeStateKeepsSavedContextWhenIncomingHasPartialHistory(t *testing.T) {
	state := NewBridgeState()
	firstInput := map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "original"}}}
	first := map[string]any{
		"type":  "response.create",
		"model": "gpt-test",
		"input": []any{firstInput},
		"tools": []any{},
	}
	if _, err := state.BuildHTTPBody(first); err != nil {
		t.Fatal(err)
	}
	state.UpdateFromSSEData([]byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"message","id":"msg-upstream","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}}`))

	second := map[string]any{
		"type":                 "response.create",
		"model":                "gpt-test",
		"previous_response_id": "resp-1",
		"input": []any{
			map[string]any{"type": "message", "id": "msg-client", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "answer"}}},
			map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "next"}}},
		},
		"tools": []any{},
	}
	body, err := state.BuildHTTPBody(second)
	if err != nil {
		t.Fatal(err)
	}
	decoded := decodeBody(t, body)
	input := decoded["input"].([]any)

	var sawOriginal bool
	var sawAnswer bool
	var sawNext bool
	for _, raw := range input {
		item := raw.(map[string]any)
		content, _ := item["content"].([]any)
		if len(content) == 0 {
			continue
		}
		part := content[0].(map[string]any)
		switch part["text"] {
		case "original":
			sawOriginal = true
		case "answer":
			sawAnswer = true
		case "next":
			sawNext = true
		}
	}
	if !sawOriginal || !sawAnswer || !sawNext {
		t.Fatalf("reconstructed input missing original=%v answer=%v next=%v: %#v", sawOriginal, sawAnswer, sawNext, input)
	}
}

func TestBridgeStatePreservesServiceTier(t *testing.T) {
	state := NewBridgeState()
	body, err := state.BuildHTTPBody(map[string]any{
		"type":         "response.create",
		"model":        "gpt-test",
		"service_tier": "priority",
		"input":        []any{map[string]any{"type": "message", "role": "user"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded := decodeBody(t, body)
	if decoded["service_tier"] != "priority" {
		t.Fatalf("service_tier = %#v", decoded["service_tier"])
	}
}

func TestBridgeStateDropsNullSummaryFromReconstructedContext(t *testing.T) {
	state := NewBridgeState()
	_, err := state.BuildHTTPBody(map[string]any{
		"type":  "response.create",
		"model": "gpt-test",
		"input": []any{map[string]any{"type": "message", "role": "user"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	state.UpdateFromSSEData([]byte(`{"type":"response.completed","response":{"id":"resp-1","output":[{"type":"reasoning","id":"rs-1","summary":null},{"type":"reasoning","id":"rs-2","summary":[{"type":"summary_text","text":"kept"}]}]}}`))
	body, err := state.BuildHTTPBody(map[string]any{
		"type":                 "response.create",
		"model":                "gpt-test",
		"previous_response_id": "resp-1",
		"input":                []any{map[string]any{"type": "message", "role": "user"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded := decodeBody(t, body)
	input := decoded["input"].([]any)

	var sawEmptySummary bool
	var sawNonNullSummary bool
	for _, raw := range input {
		item, ok := raw.(map[string]any)
		if !ok || item["type"] != "reasoning" {
			continue
		}
		if item["id"] == "rs-1" {
			summary, ok := item["summary"].([]any)
			sawEmptySummary = ok && len(summary) == 0
		}
		if item["id"] == "rs-2" {
			_, sawNonNullSummary = item["summary"]
		}
	}
	if !sawEmptySummary {
		t.Fatalf("null reasoning summary was not normalized to an empty array: %#v", input)
	}
	if !sawNonNullSummary {
		t.Fatalf("non-null summary was removed: %#v", input)
	}
}

func TestBridgeStateDropsInvalidSummaryFromIncomingInput(t *testing.T) {
	state := NewBridgeState()
	body, err := state.BuildHTTPBody(map[string]any{
		"type":  "response.create",
		"model": "gpt-test",
		"input": []any{
			map[string]any{"type": "reasoning", "id": "rs-1", "summary": nil},
			map[string]any{"type": "reasoning", "id": "rs-2", "summary": "bad"},
			map[string]any{"type": "reasoning", "id": "rs-3", "summary": []any{map[string]any{"type": "summary_text", "text": "kept"}}},
			map[string]any{"type": "reasoning", "id": "rs-4"},
			map[string]any{"type": "message", "id": "msg-1", "summary": "bad"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded := decodeBody(t, body)
	input := decoded["input"].([]any)

	for _, raw := range input {
		item := raw.(map[string]any)
		switch item["id"] {
		case "rs-1", "rs-2", "rs-4":
			summary, ok := item["summary"].([]any)
			if !ok || len(summary) != 0 {
				t.Fatalf("invalid reasoning summary was not normalized: %#v", input)
			}
		case "rs-3":
			if _, ok := item["summary"]; !ok {
				t.Fatalf("array summary was removed: %#v", input)
			}
		case "msg-1":
			if _, ok := item["summary"]; ok {
				t.Fatalf("non-reasoning invalid summary was preserved: %#v", input)
			}
		}
	}
}
