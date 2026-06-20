package main

import (
	"encoding/json"
	"sort"
)

type BridgeState struct {
	prewarmCount        int
	fullInput           []any
	responseContexts    map[string][]any
	outputItemsByCallID map[string]map[string]any
	lastResponseID      string
}

func NewBridgeState() *BridgeState {
	return &BridgeState{
		responseContexts:    map[string][]any{},
		outputItemsByCallID: map[string]map[string]any{},
	}
}

func (s *BridgeState) BuildHTTPBody(msg map[string]any) ([]byte, error) {
	incoming := asArray(msg["input"])
	previousID, _ := msg["previous_response_id"].(string)

	reconstructed := cloneItems(incoming)
	incomingAlreadyHasContext := false
	if previousID != "" {
		if ctx, ok := s.responseContexts[previousID]; ok {
			if incomingCoversContext(ctx, incoming) {
				incomingAlreadyHasContext = true
			} else {
				reconstructed = appendContextDedupe(cloneItems(ctx), incoming...)
			}
		} else if len(s.fullInput) > 0 {
			if incomingCoversContext(s.fullInput, incoming) {
				incomingAlreadyHasContext = true
			} else {
				reconstructed = appendContextDedupe(cloneItems(s.fullInput), incoming...)
			}
		}
	}
	reconstructed = s.ensureFunctionCalls(reconstructed)

	body := make(map[string]any, len(msg))
	for key, value := range msg {
		switch key {
		case "type", "generate", "previous_response_id":
			continue
		case "input":
			body[key] = reconstructed
		default:
			body[key] = value
		}
	}
	if _, ok := body["input"]; !ok {
		body["input"] = reconstructed
	}
	if _, ok := body["stream"]; !ok {
		body["stream"] = true
	}
	if previousID == "" || incomingAlreadyHasContext {
		s.fullInput = cloneItems(reconstructed)
	} else {
		s.fullInput = appendDedupe(s.fullInput, reconstructed...)
	}
	sanitizeResponsesBody(body)
	return json.Marshal(body)
}

func incomingCoversContext(contextItems, incoming []any) bool {
	if len(contextItems) == 0 || len(incoming) == 0 {
		return false
	}
	incomingKeys := map[string]bool{}
	for _, item := range incoming {
		key := semanticItemKey(item)
		if key != "" {
			incomingKeys[key] = true
		}
	}
	checked := 0
	for _, item := range contextItems {
		key := semanticItemKey(item)
		if key == "" {
			continue
		}
		checked++
		if !incomingKeys[key] {
			return false
		}
	}
	return checked > 0
}

func isOutputContextItem(item any) bool {
	m, ok := item.(map[string]any)
	if !ok {
		return false
	}
	if role, _ := m["role"].(string); role == "assistant" {
		return true
	}
	switch typ, _ := m["type"].(string); typ {
	case "reasoning", "function_call", "custom_tool_call":
		return true
	default:
		return false
	}
}

func semanticItemKey(item any) string {
	normalized := stripVolatileFields(normalizeValue(item))
	data, err := json.Marshal(normalized)
	if err != nil {
		return ""
	}
	return string(data)
}

func stripVolatileFields(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, child := range v {
			switch key {
			case "id", "status":
				continue
			default:
				out[key] = stripVolatileFields(child)
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = stripVolatileFields(item)
		}
		return out
	default:
		return v
	}
}

func (s *BridgeState) UpdateFromSSEData(data []byte) {
	var event map[string]any
	if err := json.Unmarshal(data, &event); err != nil {
		return
	}
	if response, ok := event["response"].(map[string]any); ok {
		if id, ok := response["id"].(string); ok && id != "" {
			s.lastResponseID = id
		}
		if output := asArray(response["output"]); len(output) > 0 {
			s.recordOutputItems(output)
			if s.lastResponseID != "" {
				s.responseContexts[s.lastResponseID] = appendDedupe(cloneItems(s.fullInput), output...)
			}
		}
	}
	if item, ok := event["item"].(map[string]any); ok {
		s.recordOutputItems([]any{item})
		if s.lastResponseID != "" {
			s.responseContexts[s.lastResponseID] = appendDedupe(cloneItems(s.fullInput), item)
		}
	}
}

func (s *BridgeState) recordOutputItems(items []any) {
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		callID, _ := item["call_id"].(string)
		typ, _ := item["type"].(string)
		if callID != "" && (typ == "function_call" || typ == "custom_tool_call") {
			s.outputItemsByCallID[callID] = cloneMap(item)
		}
	}
}

func (s *BridgeState) ensureFunctionCalls(items []any) []any {
	out := make([]any, 0, len(items)+2)
	presentCalls := map[string]bool{}
	for _, raw := range items {
		if item, ok := raw.(map[string]any); ok {
			if typ, _ := item["type"].(string); typ == "function_call" || typ == "custom_tool_call" {
				if callID, _ := item["call_id"].(string); callID != "" {
					presentCalls[callID] = true
				}
			}
		}
	}
	for _, raw := range items {
		if item, ok := raw.(map[string]any); ok {
			typ, _ := item["type"].(string)
			callID, _ := item["call_id"].(string)
			if (typ == "function_call_output" || typ == "custom_tool_call_output") && callID != "" && !presentCalls[callID] {
				if callItem, ok := s.outputItemsByCallID[callID]; ok {
					out = append(out, cloneMap(callItem))
					presentCalls[callID] = true
				}
			}
		}
		out = append(out, raw)
	}
	return out
}

func asArray(value any) []any {
	if value == nil {
		return nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	return items
}

func appendContextDedupe(base []any, additions ...any) []any {
	out := cloneItems(base)
	seen := map[string]bool{}
	for _, item := range out {
		seen[contextMergeKey(item)] = true
	}
	for _, item := range additions {
		key := contextMergeKey(item)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, cloneValue(item))
	}
	return out
}

func contextMergeKey(item any) string {
	if isOutputContextItem(item) {
		return "semantic:" + semanticItemKey(item)
	}
	return "strict:" + itemKey(item)
}

func appendDedupe(base []any, additions ...any) []any {
	out := cloneItems(base)
	seen := map[string]bool{}
	for _, item := range out {
		seen[itemKey(item)] = true
	}
	for _, item := range additions {
		key := itemKey(item)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, cloneValue(item))
	}
	return out
}

func itemKey(item any) string {
	normalized := normalizeValue(item)
	data, err := json.Marshal(normalized)
	if err != nil {
		return ""
	}
	return string(data)
}

func normalizeValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make(map[string]any, len(v))
		for _, key := range keys {
			out[key] = normalizeValue(v[key])
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = normalizeValue(item)
		}
		return out
	default:
		return v
	}
}

func cloneItems(items []any) []any {
	if len(items) == 0 {
		return nil
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, cloneValue(item))
	}
	return out
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneMap(v)
	case []any:
		return cloneItems(v)
	default:
		return v
	}
}

func sanitizeResponsesBody(value any) {
	switch v := value.(type) {
	case map[string]any:
		itemType, _ := v["type"].(string)
		if itemType == "reasoning" {
			if _, ok := v["summary"].([]any); !ok {
				v["summary"] = []any{}
			}
		}
		for key, child := range v {
			if key == "summary" {
				if itemType == "reasoning" {
					continue
				}
				if _, ok := child.([]any); !ok {
					delete(v, key)
					continue
				}
			}
			sanitizeResponsesBody(child)
		}
	case []any:
		for _, child := range v {
			sanitizeResponsesBody(child)
		}
	}
}

func sanitizeResponsesJSONBody(data []byte) ([]byte, error) {
	var body any
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, err
	}
	sanitizeResponsesBody(body)
	return json.Marshal(body)
}
