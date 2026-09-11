package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

type SessionCodec struct {
	customMessageSchemas map[string]MessageSchema
}

func NewSessionCodec(options SessionCodecOptions) SessionCodec {
	schemas := make(map[string]MessageSchema, len(options.CustomMessageSchemas))
	for role, schema := range options.CustomMessageSchemas {
		if schema != nil {
			schemas[role] = schema
		}
	}
	return SessionCodec{customMessageSchemas: schemas}
}

func (c SessionCodec) EncodeEntry(entry Entry) ([]byte, error) {
	if err := validateEntry(entry, nil, c); err != nil {
		return nil, err
	}
	return json.Marshal(entry)
}

func (c SessionCodec) DecodeEntry(data []byte) (Entry, error) {
	var entry Entry
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&entry); err != nil {
		return Entry{}, fmt.Errorf("decode entry: %w", err)
	}
	if err := validateEntry(entry, nil, c); err != nil {
		return Entry{}, err
	}
	return entry, nil
}

func (c SessionCodec) EncodeRegister(register Register) ([]byte, error) {
	if err := validateRegister(register.Namespace, register.Value); err != nil {
		return nil, err
	}
	return json.Marshal(register)
}

func (c SessionCodec) DecodeRegister(data []byte) (Register, error) {
	var raw struct {
		Namespace RegisterNamespace `json:"namespace"`
		Key       string            `json:"key"`
		Value     json.RawMessage   `json:"value"`
		Seq       int64             `json:"seq"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return Register{}, fmt.Errorf("decode register: %w", err)
	}
	value, err := decodeRegisterValue(raw.Namespace, raw.Value)
	if err != nil {
		return Register{}, err
	}
	if err := validateRegister(raw.Namespace, value); err != nil {
		return Register{}, err
	}
	return Register{Namespace: raw.Namespace, Key: raw.Key, Value: value, Seq: raw.Seq}, nil
}

func validateWrite(write Write, entries map[string]Entry, usage map[string]UsageRow, codec SessionCodec) error {
	switch write.Kind {
	case WriteEntry:
		if write.Entry == nil {
			return fmt.Errorf("entry write is missing entry")
		}
		entry := write.Entry.Entry
		if err := validateEntry(entry, entries, codec); err != nil {
			return err
		}
		if entry.Seq != 0 || entry.Timestamp != 0 {
			return fmt.Errorf("entry %q must not provide storage fields", entry.ID)
		}
		if _, exists := entries[entry.ID]; exists {
			return fmt.Errorf("duplicate immutable id %q", entry.ID)
		}
	case WriteUsage:
		if write.Usage == nil {
			return fmt.Errorf("usage write is missing row")
		}
		row := write.Usage.Row
		if !isUUIDv7(row.ID) || row.Seq != 0 {
			return fmt.Errorf("usage row %q must have a UUIDv7 id and no sequence", row.ID)
		}
		if _, exists := usage[row.ID]; exists {
			return fmt.Errorf("duplicate immutable id %q", row.ID)
		}
		if _, exists := entries[row.ID]; exists {
			return fmt.Errorf("usage id %q collides with entry", row.ID)
		}
		if row.EntryID != nil {
			if _, exists := entries[*row.EntryID]; !exists {
				return fmt.Errorf("usage row references missing entry %q", *row.EntryID)
			}
		}
		if err := jsonValid(row); err != nil {
			return err
		}
	case WriteRegister:
		if write.Register == nil {
			return fmt.Errorf("register write is missing register")
		}
		rw := write.Register
		if !validNamespace(rw.Namespace) {
			return fmt.Errorf("unknown register namespace %q", rw.Namespace)
		}
		if rw.Operation != RegisterSet && rw.Operation != RegisterDelete {
			return fmt.Errorf("unknown register operation %q", rw.Operation)
		}
		if rw.Operation == RegisterSet {
			if err := validateRegister(rw.Namespace, rw.Value); err != nil {
				return err
			}
			if rw.Namespace == RegisterLaneLeaf {
				leaf, ok := rw.Value.(*string)
				if !ok {
					return fmt.Errorf("lane leaf references missing entry")
				}
				if leaf != nil && (entries == nil || !hasEntry(entries, *leaf)) {
					return fmt.Errorf("lane leaf references missing entry")
				}
			}
		}
	default:
		return fmt.Errorf("unknown write kind %q", write.Kind)
	}
	return nil
}

func validateEntry(entry Entry, entries map[string]Entry, codec SessionCodec) error {
	if !isUUIDv7(entry.ID) {
		return fmt.Errorf("entry %q must have a UUIDv7 id", entry.ID)
	}
	if entry.ParentID != nil && entries != nil && !hasEntry(entries, *entry.ParentID) {
		return fmt.Errorf("entry %q references missing parent %q", entry.ID, *entry.ParentID)
	}
	if err := jsonValid(entry); err != nil {
		return err
	}
	switch entry.Type {
	case EntryMessage:
		if entry.Message == nil {
			return fmt.Errorf("message entry %q has no message", entry.ID)
		}
		if err := validateMessage(*entry.Message, codec); err != nil {
			return fmt.Errorf("message entry %q: %w", entry.ID, err)
		}
		if entry.CustomType != "" || entry.Summary != "" || entry.FromID != "" || entry.Data != nil {
			return fmt.Errorf("message entry %q has fields for another entry type", entry.ID)
		}
	case EntryCompaction:
		if entry.CustomType != "" || entry.Message != nil || entry.FromID != "" || entry.Summary == "" {
			return fmt.Errorf("invalid compaction entry %q", entry.ID)
		}
		for _, message := range entry.RetainedTail {
			if err := validateMessage(message, codec); err != nil {
				return fmt.Errorf("compaction entry %q: %w", entry.ID, err)
			}
		}
	case EntryBranchSummary:
		if entry.CustomType != "" || entry.Message != nil || entry.FromID == "" || entry.Summary == "" {
			return fmt.Errorf("invalid branch summary entry %q", entry.ID)
		}
	case EntryCustom:
		if entry.CustomType == "" || entry.Message != nil || entry.Summary != "" || entry.FromID != "" {
			return fmt.Errorf("invalid custom entry %q", entry.ID)
		}
	default:
		return fmt.Errorf("unknown entry type %q", entry.Type)
	}
	return nil
}

func validateMessage(message AgentMessage, codec SessionCodec) error {
	if message.Role == "" {
		return fmt.Errorf("message role is required")
	}
	if message.Role == "assistant" && message.StopReason == StopReason("pending") {
		return fmt.Errorf("pending assistant messages cannot be persisted")
	}
	if !builtinMessageRole(message.Role) {
		schema, ok := codec.customMessageSchemas[message.Role]
		if !ok {
			return fmt.Errorf("unknown custom role %q", message.Role)
		}
		if err := schema(message); err != nil {
			return fmt.Errorf("custom role %q: %w", message.Role, err)
		}
	}
	return jsonValid(message)
}

func builtinMessageRole(role string) bool {
	switch role {
	case "system", "developer", "user", "assistant", "tool", "function":
		return true
	default:
		return false
	}
}

func validateRegister(namespace RegisterNamespace, value RegisterValue) error {
	if err := jsonValid(value); err != nil {
		return err
	}
	valid := false
	switch namespace {
	case RegisterLaneLeaf:
		valid = value == nil || isStringPointer(value)
	case RegisterLaneConfig:
		_, valid = value.(LaneConfiguration)
	case RegisterLaneState:
		_, valid = value.(LaneState)
	case RegisterLaneLastResult:
		_, valid = value.(LaneLastResult)
	case RegisterOpMeta:
		_, valid = value.(Operation)
	case RegisterOpState:
		_, valid = value.(OperationState)
	case RegisterOpToolArgs:
		_, valid = value.(map[string]JSONValue)
	case RegisterOpPreparation:
		_, valid = value.(DurableStructuralPreparation)
	case RegisterPendingEntry:
		_, valid = value.(PendingEntry)
	case RegisterFactName, RegisterFactLabel:
		_, valid = value.(string)
	case RegisterFactCustom:
		valid = true
	default:
		return fmt.Errorf("unknown register namespace %q", namespace)
	}
	if !valid {
		return fmt.Errorf("invalid value for register %s", namespace)
	}
	if namespace == RegisterLaneConfig {
		config := value.(LaneConfiguration)
		if config.Model.Provider == "" || config.Model.ModelID == "" || config.ActiveToolNames == nil {
			return fmt.Errorf("lane configuration is not total")
		}
	}
	if namespace == RegisterLaneState {
		state := value.(LaneState)
		if state.PendingNextRun == nil {
			return fmt.Errorf("lane state is not total")
		}
	}
	return nil
}

func decodeRegisterValue(namespace RegisterNamespace, data []byte) (RegisterValue, error) {
	switch namespace {
	case RegisterLaneLeaf:
		if string(data) == "null" {
			return (*string)(nil), nil
		}
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return &value, nil
	case RegisterLaneConfig:
		var value LaneConfiguration
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return value, nil
	case RegisterLaneState:
		var value LaneState
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return value, nil
	case RegisterLaneLastResult:
		var value LaneLastResult
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return value, nil
	case RegisterOpMeta:
		var value Operation
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return value, nil
	case RegisterOpState:
		var value OperationState
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return value, nil
	case RegisterOpToolArgs:
		var value map[string]JSONValue
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return value, nil
	case RegisterOpPreparation:
		var value DurableStructuralPreparation
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return value, nil
	case RegisterPendingEntry:
		var value PendingEntry
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return value, nil
	case RegisterFactName, RegisterFactLabel:
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return value, nil
	case RegisterFactCustom:
		var value JSONValue
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unknown register namespace %q", namespace)
	}
}

func isStringPointer(value any) bool {
	if value == nil {
		return true
	}
	_, ok := value.(*string)
	return ok
}

func validNamespace(namespace RegisterNamespace) bool {
	switch namespace {
	case RegisterLaneLeaf, RegisterLaneConfig, RegisterLaneState, RegisterLaneLastResult,
		RegisterOpMeta, RegisterOpState, RegisterOpToolArgs, RegisterOpPreparation,
		RegisterPendingEntry, RegisterFactName, RegisterFactLabel, RegisterFactCustom:
		return true
	default:
		return false
	}
}

func hasEntry(entries map[string]Entry, id string) bool {
	_, ok := entries[id]
	return ok
}

func structureOf(entry Entry) EntryStructure {
	return EntryStructure{ID: entry.ID, ParentID: cloneStringPointer(entry.ParentID), Seq: entry.Seq, Timestamp: entry.Timestamp, Type: entry.Type, CustomType: entry.CustomType}
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func scanBranchLocked(entries map[string]Entry, query BranchScan) ([]Entry, error) {
	if query.Start == "" {
		return nil, fmt.Errorf("branch scan start is required")
	}
	path := make([]Entry, 0)
	seen := make(map[string]bool)
	current := query.Start
	for current != "" {
		if seen[current] {
			return nil, fmt.Errorf("cycle in entry parents at %q", current)
		}
		seen[current] = true
		entry, ok := entries[current]
		if !ok {
			return nil, fmt.Errorf("missing parent or start entry %q", current)
		}
		path = append(path, cloneEntry(entry))
		if (query.StopAtID != "" && entry.ID == query.StopAtID) || (query.StopAtType != nil && entry.Type == *query.StopAtType) {
			break
		}
		if entry.ParentID == nil {
			break
		}
		current = *entry.ParentID
	}
	if isOldestFirst(query.Order) {
		sort.Slice(path, func(i, j int) bool { return path[i].Seq < path[j].Seq })
	}
	result := make([]Entry, 0, len(path))
	for _, entry := range path {
		if query.Type != nil && entry.Type != *query.Type {
			continue
		}
		if query.CustomType != "" && entry.CustomType != query.CustomType {
			continue
		}
		if query.Cursor != nil && (isOldestFirst(query.Order) && entry.Seq <= query.Cursor.AfterSeq || !isOldestFirst(query.Order) && entry.Seq >= query.Cursor.AfterSeq) {
			continue
		}
		result = append(result, entry)
	}
	return limitEntries(result, query.Limit), nil
}

func sortEntries(entries []Entry, order EntryOrder) {
	sort.Slice(entries, func(i, j int) bool {
		if isOldestFirst(order) {
			return entries[i].Seq < entries[j].Seq
		}
		return entries[i].Seq > entries[j].Seq
	})
}

func isOldestFirst(order EntryOrder) bool {
	return order == OldestFirst || string(order) == "asc" || string(order) == "oldest"
}

func applyEntryCursor(entries []Entry, cursor *EntryCursor, order EntryOrder) []Entry {
	if cursor == nil {
		return entries
	}
	result := entries[:0]
	for _, entry := range entries {
		if isOldestFirst(order) && entry.Seq > cursor.AfterSeq || !isOldestFirst(order) && entry.Seq < cursor.AfterSeq {
			result = append(result, entry)
		}
	}
	return result
}

func limitEntries(entries []Entry, limit int) []Entry {
	if limit > 0 && len(entries) > limit {
		return entries[:limit]
	}
	return entries
}
