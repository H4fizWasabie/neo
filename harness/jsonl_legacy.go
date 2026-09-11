package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type legacyNode struct {
	Type     string
	ID       string
	ParentID *string
	Raw      map[string]json.RawMessage
	Time     int64
}

func isLegacyV3(data []byte) bool {
	lineEnd := bytes.IndexByte(data, '\n')
	if lineEnd < 0 {
		lineEnd = len(data)
	}
	var header map[string]json.RawMessage
	if json.Unmarshal(data[:lineEnd], &header) != nil {
		return false
	}
	var kind string
	var version int
	_ = json.Unmarshal(header["type"], &kind)
	_ = json.Unmarshal(header["version"], &version)
	return kind == "session" && version == 3
}

func openLegacyJSONLStorage(path string, data []byte, options JSONLStorageOptions) (*JSONLStorage, SessionMetadata, error) {
	lines := bytes.Split(data, []byte{'\n'})
	if len(lines) == 0 || len(lines[0]) == 0 {
		return nil, SessionMetadata{}, fmt.Errorf("legacy session has no header")
	}
	var header map[string]json.RawMessage
	if err := json.Unmarshal(lines[0], &header); err != nil {
		return nil, SessionMetadata{}, err
	}
	id := rawString(header, "id")
	if id == "" {
		return nil, SessionMetadata{}, fmt.Errorf("legacy session has no id")
	}
	createdAt, err := legacyTimestamp(header["timestamp"])
	if err != nil {
		return nil, SessionMetadata{}, err
	}
	nodes := make([]legacyNode, 0, len(lines)-1)
	byID := make(map[string]legacyNode)
	name := ""
	nameSet := false
	labels := make([]struct{ target, value string }, 0)
	var aggregate Usage
	for _, line := range lines[1:] {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(line, &raw); err != nil {
			return nil, SessionMetadata{}, fmt.Errorf("decode legacy line: %w", err)
		}
		node := legacyNode{Type: rawString(raw, "type"), ID: rawString(raw, "id"), Raw: raw}
		if node.ID == "" || node.Type == "" {
			return nil, SessionMetadata{}, fmt.Errorf("legacy node is missing type or id")
		}
		if parent, ok := raw["parentId"]; ok && string(parent) != "null" {
			value := rawString(raw, "parentId")
			node.ParentID = &value
		}
		node.Time, err = legacyTimestamp(raw["timestamp"])
		if err != nil {
			return nil, SessionMetadata{}, err
		}
		nodes = append(nodes, node)
		byID[node.ID] = node
		if node.Type == "session_info" {
			if value := rawString(raw, "name"); value != "" {
				name, nameSet = value, true
			}
		}
		if node.Type == "label" {
			target := rawString(raw, "targetId")
			if target == "" {
				target = dereferencedParent(node.ParentID)
			}
			labels = append(labels, struct{ target, value string }{target, rawString(raw, "label")})
		}
		if usage, ok := raw["usage"]; ok {
			u := legacyUsage(usage)
			aggregate.Input += u.Input
			aggregate.Output += u.Output
			aggregate.CacheRead += u.CacheRead
			aggregate.CacheWrite += u.CacheWrite
			aggregate.Reasoning += u.Reasoning
			aggregate.Total += u.Total
			if u.Cost != nil {
				if aggregate.Cost == nil {
					aggregate.Cost = &Cost{}
				}
				aggregate.Cost.Input += u.Cost.Input
				aggregate.Cost.Output += u.Cost.Output
				aggregate.Cost.CacheRead += u.Cost.CacheRead
				aggregate.Cost.CacheWrite += u.Cost.CacheWrite
				aggregate.Cost.Total += u.Cost.Total
			}
		}
	}
	generator := NewUUIDv7Generator()
	idMap := make(map[string]string)
	kept := make(map[string]bool)
	for _, node := range nodes {
		if legacyRetainedType(node.Type) {
			idMap[node.ID] = generator.Next(node.Time)
			kept[node.ID] = true
		}
	}
	records := make([]jsonlRecord, 0, len(nodes)+3)
	for _, node := range nodes {
		if !kept[node.ID] {
			continue
		}
		parent := nearestRetained(node.ParentID, byID, kept, idMap)
		entry, err := legacyEntry(node, parent, idMap, byID, kept)
		if err != nil {
			return nil, SessionMetadata{}, err
		}
		records = append(records, recordForEntryWithSequence(entry, int64(len(records)+1)))
	}
	mainLeaf := ""
	for i := len(nodes) - 1; i >= 0; i-- {
		if id := nearestRetained(&nodes[i].ID, byID, kept, idMap); id != nil {
			mainLeaf = *id
			break
		}
	}
	if nameSet {
		records = append(records, jsonlRecord{Kind: string(WriteRegister), Seq: int64(len(records) + 1), Op: RegisterSet, Namespace: RegisterFactName, Key: "", Value: name})
	}
	for _, label := range labels {
		if target := idMap[label.target]; target != "" && label.value != "" {
			records = append(records, jsonlRecord{Kind: string(WriteRegister), Seq: int64(len(records) + 1), Op: RegisterSet, Namespace: RegisterFactLabel, Key: target, Value: label.value})
		}
	}
	if mainLeaf != "" {
		records = append(records, jsonlRecord{Kind: string(WriteRegister), Seq: int64(len(records) + 1), Op: RegisterSet, Namespace: RegisterLaneLeaf, Key: "main", Value: &mainLeaf})
	} else {
		records = append(records, jsonlRecord{Kind: string(WriteRegister), Seq: int64(len(records) + 1), Op: RegisterSet, Namespace: RegisterLaneLeaf, Key: "main", Value: (*string)(nil)})
	}
	replayStorage := &JSONLStorage{memory: NewMemoryStorage(MemoryStorageOptions{Codec: options.Codec, Now: options.Now}), path: path, legacy: true, legacyAggregate: &aggregate, header: jsonlHeader{V: jsonlVersion, Kind: "header", ID: id, StorageVersion: CurrentStorageVersion, CreatedAt: createdAt, CWD: rawString(header, "cwd"), LegacyParentSessionPath: rawString(header, "parentSession")}}
	if err := replayStorage.replay(records); err != nil {
		return nil, SessionMetadata{}, err
	}
	replayStorage.memory.data.mu.Lock()
	replayStorage.memory.data.stats = SessionStats{}
	for _, entry := range replayStorage.memory.data.entries {
		if entry.Type == EntryMessage {
			replayStorage.memory.data.stats.MessageCount++
		}
	}
	for _, node := range nodes {
		if usage, ok := node.Raw["usage"]; ok {
			addUsage(&replayStorage.memory.data.stats, legacyUsage(usage))
		}
	}
	replayStorage.memory.data.mu.Unlock()
	replayStorage.file, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, SessionMetadata{}, err
	}
	metadata := SessionMetadata{ID: id, CreatedAt: createdAt, StorageVersion: CurrentStorageVersion, CWD: rawString(header, "cwd"), LegacyParentSessionPath: rawString(header, "parentSession")}
	return replayStorage, metadata, nil
}

func (s *JSONLStorage) normalizeLegacyLocked(ctx context.Context) error {
	if !s.legacy {
		return nil
	}
	if s.legacyAggregate != nil {
		row := UsageRow{ID: NewUUIDv7Generator().Next(), Usage: *s.legacyAggregate, Adjustment: true, Details: map[string]any{"source": "v3-import"}}
		if _, err := s.memory.Commit(ctx, Transaction{Writes: []Write{{Kind: WriteUsage, Usage: &UsageWrite{Row: row}}}}); err != nil {
			return err
		}
	}
	if err := s.compactLocked(ctx, nil); err != nil {
		return err
	}
	s.legacy, s.legacyAggregate = false, nil
	return nil
}

func legacyRetainedType(kind string) bool {
	return kind == "message" || kind == "compaction" || kind == "branch_summary" || kind == "custom_message" || kind == "custom"
}

func nearestRetained(parent *string, byID map[string]legacyNode, kept map[string]bool, idMap map[string]string) *string {
	for parent != nil {
		if kept[*parent] {
			value := idMap[*parent]
			return &value
		}
		node, ok := byID[*parent]
		if !ok {
			return nil
		}
		parent = node.ParentID
	}
	return nil
}

func legacyEntry(node legacyNode, parent *string, idMap map[string]string, byID map[string]legacyNode, kept map[string]bool) (Entry, error) {
	base := EntryBase{ID: idMap[node.ID], ParentID: parent, Timestamp: node.Time}
	switch node.Type {
	case "message":
		var message AgentMessage
		if err := json.Unmarshal(node.Raw["message"], &message); err != nil {
			return Entry{}, err
		}
		if message.Role == "toolResult" {
			message.Role = "tool"
		}
		message.StopReason = normalizeLegacyStopReason(message.StopReason)
		return Entry{EntryBase: EntryBase{ID: base.ID, ParentID: base.ParentID, Type: EntryMessage, Timestamp: base.Timestamp}, Message: &message, Terminate: rawBool(node.Raw, "terminate")}, nil
	case "custom_message":
		return Entry{EntryBase: EntryBase{ID: base.ID, ParentID: base.ParentID, Type: EntryCustom, Timestamp: base.Timestamp}, CustomType: "message", Data: rawJSON(node.Raw["message"])}, nil
	case "custom":
		return Entry{EntryBase: EntryBase{ID: base.ID, ParentID: base.ParentID, Type: EntryCustom, Timestamp: base.Timestamp}, CustomType: rawString(node.Raw, "customType"), Data: rawJSON(node.Raw["data"])}, nil
	case "compaction":
		var tail []AgentMessage
		first := rawString(node.Raw, "firstKeptEntryId")
		if first != "" {
			tail = legacyRetainedTail(first, node.ParentID, byID, kept)
		}
		var usage *Usage
		if value, ok := node.Raw["usage"]; ok {
			parsed := legacyUsage(value)
			usage = &parsed
		}
		return Entry{EntryBase: EntryBase{ID: base.ID, ParentID: base.ParentID, Type: EntryCompaction, Timestamp: base.Timestamp}, Summary: rawString(node.Raw, "summary"), RetainedTail: tail, TokensBefore: rawInt64(node.Raw, "tokensBefore"), Details: rawJSON(node.Raw["details"]), Usage: usage, FromHook: rawBool(node.Raw, "fromHook")}, nil
	case "branch_summary":
		from := idMap[rawString(node.Raw, "fromId")]
		return Entry{EntryBase: EntryBase{ID: base.ID, ParentID: base.ParentID, Type: EntryBranchSummary, Timestamp: base.Timestamp}, FromID: from, Summary: rawString(node.Raw, "summary"), Details: rawJSON(node.Raw["details"]), FromHook: rawBool(node.Raw, "fromHook")}, nil
	default:
		return Entry{}, fmt.Errorf("unsupported retained legacy type %q", node.Type)
	}
}

func legacyRetainedTail(first string, stop *string, byID map[string]legacyNode, kept map[string]bool) []AgentMessage {
	path := make([]legacyNode, 0)
	current := first
	for current != "" {
		node, ok := byID[current]
		if !ok {
			break
		}
		if kept[current] && node.Type == "message" {
			path = append(path, node)
		}
		if stop != nil && current == *stop {
			break
		}
		if node.ParentID == nil {
			break
		}
		current = *node.ParentID
	}
	result := make([]AgentMessage, 0, len(path))
	for i := len(path) - 1; i >= 0; i-- {
		var message AgentMessage
		if json.Unmarshal(path[i].Raw["message"], &message) == nil {
			message.StopReason = normalizeLegacyStopReason(message.StopReason)
			result = append(result, message)
		}
	}
	return result
}

func recordForEntryWithSequence(entry Entry, seq int64) jsonlRecord {
	record := recordForEntry(entry)
	record.Seq = seq
	return record
}
func rawString(raw map[string]json.RawMessage, key string) string {
	var value string
	_ = json.Unmarshal(raw[key], &value)
	return value
}
func rawBool(raw map[string]json.RawMessage, key string) bool {
	var value bool
	_ = json.Unmarshal(raw[key], &value)
	return value
}
func rawInt64(raw map[string]json.RawMessage, key string) int64 {
	var value int64
	_ = json.Unmarshal(raw[key], &value)
	return value
}
func rawJSON(value json.RawMessage) JSONValue {
	if len(value) == 0 {
		return nil
	}
	var result JSONValue
	if json.Unmarshal(value, &result) != nil {
		return nil
	}
	return result
}
func dereferencedParent(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func legacyTimestamp(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("legacy timestamp is missing")
	}
	var number int64
	if json.Unmarshal(raw, &number) == nil {
		return number, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return 0, err
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return 0, err
	}
	return parsed.UnixMilli(), nil
}

func legacyUsage(raw json.RawMessage) Usage {
	var value map[string]json.RawMessage
	_ = json.Unmarshal(raw, &value)
	result := Usage{Input: rawInt64(value, "input"), Output: rawInt64(value, "output"), CacheRead: rawInt64(value, "cacheRead"), CacheWrite: rawInt64(value, "cacheWrite"), Reasoning: rawInt64(value, "reasoning")}
	result.Total = rawInt64(value, "total")
	if result.Total == 0 {
		result.Total = rawInt64(value, "totalTokens")
	}
	if cost, ok := value["cost"]; ok {
		var c Cost
		var rawCost map[string]json.RawMessage
		_ = json.Unmarshal(cost, &rawCost)
		c.Input = rawFloat(rawCost, "input")
		c.Output = rawFloat(rawCost, "output")
		c.CacheRead = rawFloat(rawCost, "cacheRead")
		c.CacheWrite = rawFloat(rawCost, "cacheWrite")
		c.Total = rawFloat(rawCost, "total")
		result.Cost = &c
	}
	return result
}
func rawFloat(raw map[string]json.RawMessage, key string) float64 {
	var value float64
	_ = json.Unmarshal(raw[key], &value)
	return value
}
func normalizeLegacyStopReason(reason StopReason) StopReason {
	switch reason {
	case StopReason("toolUse"):
		return StopReasonToolUse
	case StopReason("maxTokens"):
		return StopReasonLength
	default:
		return reason
	}
}
