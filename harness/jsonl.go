package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const jsonlVersion = 4

type JSONLStorageOptions struct {
	Codec SessionCodecOptions
	Now   func() time.Time
}

type jsonlHeader struct {
	V                       int    `json:"v"`
	Kind                    string `json:"kind"`
	ID                      string `json:"id"`
	StorageVersion          int    `json:"storageVersion"`
	StoreGeneration         int64  `json:"storeGeneration"`
	CreatedAt               int64  `json:"createdAt"`
	CWD                     string `json:"cwd,omitempty"`
	ParentSessionID         string `json:"parentSessionId,omitempty"`
	LegacyParentSessionPath string `json:"legacyParentSessionPath,omitempty"`
}

type jsonlRecord struct {
	Kind         string            `json:"kind"`
	Seq          int64             `json:"seq,omitempty"`
	Timestamp    int64             `json:"timestamp,omitempty"`
	ID           string            `json:"id,omitempty"`
	ParentID     *string           `json:"parentId,omitempty"`
	Type         EntryType         `json:"type,omitempty"`
	CustomType   string            `json:"customType,omitempty"`
	Message      *AgentMessage     `json:"message,omitempty"`
	Terminate    bool              `json:"terminate,omitempty"`
	Summary      string            `json:"summary,omitempty"`
	RetainedTail []AgentMessage    `json:"retainedTail,omitempty"`
	TokensBefore int64             `json:"tokensBefore,omitempty"`
	Details      JSONValue         `json:"details,omitempty"`
	Usage        *Usage            `json:"usage,omitempty"`
	FromHook     bool              `json:"fromHook,omitempty"`
	FromID       string            `json:"fromId,omitempty"`
	Data         JSONValue         `json:"data,omitempty"`
	Op           RegisterOperation `json:"op,omitempty"`
	Namespace    RegisterNamespace `json:"namespace,omitempty"`
	Key          string            `json:"key,omitempty"`
	Value        JSONValue         `json:"value"`
	EntryID      *string           `json:"entryId,omitempty"`
	Adjustment   bool              `json:"adjustment,omitempty"`
}

type JSONLStorage struct {
	memory          *MemoryStorage
	path            string
	header          jsonlHeader
	fileMu          sync.Mutex
	file            *os.File
	legacy          bool
	legacyAggregate *Usage
}

func CreateJSONLStorage(path string, metadata SessionMetadata, options JSONLStorageOptions) (*JSONLStorage, error) {
	if metadata.StorageVersion == 0 {
		metadata.StorageVersion = CurrentStorageVersion
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	storage := &JSONLStorage{memory: NewMemoryStorage(MemoryStorageOptions{Codec: options.Codec, Now: options.Now}), path: path, file: file, header: jsonlHeader{V: jsonlVersion, Kind: "header", ID: metadata.ID, StorageVersion: metadata.StorageVersion, StoreGeneration: metadata.StoreGeneration, CreatedAt: metadata.CreatedAt, CWD: metadata.CWD, ParentSessionID: metadata.ParentSessionID, LegacyParentSessionPath: metadata.LegacyParentSessionPath}}
	if err := storage.writeHeader(); err != nil {
		file.Close()
		return nil, err
	}
	return storage, nil
}

func OpenJSONLStorage(path string, options JSONLStorageOptions) (*JSONLStorage, SessionMetadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, SessionMetadata{}, err
	}
	if len(data) == 0 {
		return nil, SessionMetadata{}, fmt.Errorf("empty JSONL session")
	}
	if isLegacyV3(data) {
		return openLegacyJSONLStorage(path, data, options)
	}
	lastNewline := bytes.LastIndexByte(data, '\n')
	if lastNewline < 0 {
		return nil, SessionMetadata{}, fmt.Errorf("session has no complete header")
	}
	if lastNewline != len(data)-1 {
		data = data[:lastNewline+1]
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return nil, SessionMetadata{}, err
		}
	}
	lines := bytes.Split(data, []byte{'\n'})
	if len(lines) < 2 {
		return nil, SessionMetadata{}, fmt.Errorf("session has no header")
	}
	var header jsonlHeader
	if err := json.Unmarshal(lines[0], &header); err != nil {
		return nil, SessionMetadata{}, fmt.Errorf("decode JSONL header: %w", err)
	}
	if header.Kind != "header" || header.V != jsonlVersion || header.ID == "" {
		return nil, SessionMetadata{}, fmt.Errorf("invalid JSONL header")
	}
	if header.StorageVersion > CurrentStorageVersion {
		return nil, SessionMetadata{}, fmt.Errorf("session storage version %d is newer than binary version %d", header.StorageVersion, CurrentStorageVersion)
	}
	storage := &JSONLStorage{memory: NewMemoryStorage(MemoryStorageOptions{Codec: options.Codec, Now: options.Now}), path: path, header: header}
	for lineNumber, line := range lines[1:] {
		if len(line) == 0 {
			continue
		}
		var records []jsonlRecord
		if line[0] == '[' {
			if err := json.Unmarshal(line, &records); err != nil {
				return nil, SessionMetadata{}, fmt.Errorf("decode JSONL line %d: %w", lineNumber+2, err)
			}
		} else {
			var record jsonlRecord
			if err := json.Unmarshal(line, &record); err != nil {
				return nil, SessionMetadata{}, fmt.Errorf("decode JSONL line %d: %w", lineNumber+2, err)
			}
			records = []jsonlRecord{record}
		}
		if err := storage.replay(records); err != nil {
			return nil, SessionMetadata{}, fmt.Errorf("replay JSONL line %d: %w", lineNumber+2, err)
		}
	}
	storage.file, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, SessionMetadata{}, err
	}
	metadata := SessionMetadata{ID: header.ID, CreatedAt: header.CreatedAt, StorageVersion: header.StorageVersion, StoreGeneration: header.StoreGeneration, CWD: header.CWD, ParentSessionID: header.ParentSessionID, LegacyParentSessionPath: header.LegacyParentSessionPath}
	return storage, metadata, nil
}

func (s *JSONLStorage) Commit(ctx context.Context, tx Transaction) (CommitResult, error) {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	if s.file == nil {
		return CommitResult{}, errStorageClosed
	}
	if s.legacy {
		if err := s.normalizeLegacyLocked(ctx); err != nil {
			return CommitResult{}, err
		}
	}
	var result CommitResult
	result, err := s.memory.commitWithPublish(ctx, tx, func(result CommitResult) error {
		records := make([]jsonlRecord, len(tx.Writes))
		for i, write := range tx.Writes {
			records[i] = recordForWrite(write, result.Seqs[i], result.Timestamp)
		}
		var payload []byte
		var marshalErr error
		if len(records) == 1 {
			payload, marshalErr = json.Marshal(records[0])
		} else {
			payload, marshalErr = json.Marshal(records)
		}
		if marshalErr != nil {
			return marshalErr
		}
		payload = append(payload, '\n')
		_, err := s.file.Write(payload)
		return err
	})
	if err != nil {
		return CommitResult{}, err
	}
	return result, nil
}

func (s *JSONLStorage) GetEntries(ctx context.Context, ids []string) (map[string]Entry, error) {
	return s.memory.GetEntries(ctx, ids)
}
func (s *JSONLStorage) GetRegister(ctx context.Context, namespace RegisterNamespace, key string) (*Register, error) {
	return s.memory.GetRegister(ctx, namespace, key)
}
func (s *JSONLStorage) ListRegisters(ctx context.Context, namespace RegisterNamespace, prefix string) ([]Register, error) {
	return s.memory.ListRegisters(ctx, namespace, prefix)
}
func (s *JSONLStorage) ScanBranch(ctx context.Context, query BranchScan) ([]Entry, error) {
	return s.memory.ScanBranch(ctx, query)
}
func (s *JSONLStorage) ScanBranchStructure(ctx context.Context, query BranchScan) ([]EntryStructure, error) {
	return s.memory.ScanBranchStructure(ctx, query)
}
func (s *JSONLStorage) ScanEntries(ctx context.Context, query EntryScan) ([]Entry, error) {
	return s.memory.ScanEntries(ctx, query)
}
func (s *JSONLStorage) ScanUsage(ctx context.Context, query UsageScan) ([]UsageRow, error) {
	return s.memory.ScanUsage(ctx, query)
}
func (s *JSONLStorage) GetStats(ctx context.Context) (SessionStats, error) {
	return s.memory.GetStats(ctx)
}

func (s *JSONLStorage) Close(ctx context.Context) error {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.memory.Close(ctx)
	closeErr := s.file.Close()
	s.file = nil
	if err != nil {
		return err
	}
	return closeErr
}

// Compact rewrites current logical state to header + live entries + live
// registers + usage. A nil keep predicate retains every record.
func (s *JSONLStorage) Compact(ctx context.Context, keep func(kind, id string) bool) error {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	return s.compactLocked(ctx, keep)
}

func (s *JSONLStorage) compactLocked(ctx context.Context, keep func(kind, id string) bool) error {
	if s.file == nil {
		return errStorageClosed
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	s.memory.data.mu.RLock()
	entries := cloneEntries(s.memory.data.entries)
	registers := cloneRegisters(s.memory.data.registers)
	usage := cloneUsage(s.memory.data.usage)
	s.memory.data.mu.RUnlock()
	records := make([]jsonlRecord, 0, len(entries)+len(registers)+len(usage))
	entryIDs := make([]string, 0, len(entries))
	for id := range entries {
		entryIDs = append(entryIDs, id)
	}
	sort.Slice(entryIDs, func(i, j int) bool { return entries[entryIDs[i]].Seq < entries[entryIDs[j]].Seq })
	for _, id := range entryIDs {
		if keep != nil && !keep("entry", id) {
			continue
		}
		records = append(records, recordForEntry(entries[id]))
	}
	usageIDs := make([]string, 0, len(usage))
	for id := range usage {
		usageIDs = append(usageIDs, id)
	}
	sort.Slice(usageIDs, func(i, j int) bool { return usage[usageIDs[i]].Seq < usage[usageIDs[j]].Seq })
	for _, id := range usageIDs {
		if keep != nil && !keep("usage", id) {
			continue
		}
		records = append(records, recordForUsage(usage[id]))
	}
	registerKeys := make([]string, 0, len(registers))
	for key := range registers {
		registerKeys = append(registerKeys, key)
	}
	sort.Strings(registerKeys)
	for _, key := range registerKeys {
		register := registers[key]
		if keep != nil && !keep("register", registerKey(register.Namespace, register.Key)) {
			continue
		}
		records = append(records, recordForRegister(register))
	}
	sort.SliceStable(records, func(i, j int) bool { return records[i].Seq < records[j].Seq })
	headerValue := s.header
	headerValue.StoreGeneration++
	temporary, err := os.CreateTemp(filepath.Dir(s.path), ".neo-jsonl-compact-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	writer := bufio.NewWriter(temporary)
	header, err := json.Marshal(headerValue)
	if err == nil {
		_, err = writer.Write(append(header, '\n'))
	}
	for _, record := range records {
		if err != nil {
			break
		}
		line, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			err = marshalErr
			break
		}
		_, err = writer.Write(append(line, '\n'))
	}
	if err == nil {
		err = writer.Flush()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := s.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, s.path); err != nil {
		return err
	}
	s.header = headerValue
	s.file, err = os.OpenFile(s.path, os.O_RDWR|os.O_APPEND, 0o600)
	return err
}

func (s *JSONLStorage) writeHeader() error {
	payload, err := json.Marshal(s.header)
	if err != nil {
		return err
	}
	_, err = s.file.Write(append(payload, '\n'))
	return err
}

func (s *JSONLStorage) replay(records []jsonlRecord) error {
	s.memory.data.mu.Lock()
	defer s.memory.data.mu.Unlock()
	entries := cloneEntries(s.memory.data.entries)
	registers := cloneRegisters(s.memory.data.registers)
	usage := cloneUsage(s.memory.data.usage)
	children := cloneChildren(s.memory.data.children)
	stats := s.memory.data.stats
	nextSeq := s.memory.data.nextSeq
	lastSeq := int64(0)
	for _, record := range records {
		if record.Seq <= lastSeq || record.Seq < nextSeq {
			return fmt.Errorf("sequence is not strictly increasing")
		}
		lastSeq = record.Seq
		if record.Timestamp <= 0 && record.Kind != "usage" && record.Kind != "register" {
			return fmt.Errorf("invalid timestamp")
		}
		switch record.Kind {
		case string(WriteEntry):
			entry := entryForRecord(record)
			if err := validateEntry(entry, entries, s.memory.codec); err != nil {
				return err
			}
			if _, ok := entries[entry.ID]; ok {
				return fmt.Errorf("duplicate entry id %q", entry.ID)
			}
			entries[entry.ID] = entry
			if entry.ParentID != nil {
				children[*entry.ParentID] = append(children[*entry.ParentID], entry.ID)
			}
			if entry.Type == EntryMessage {
				stats.MessageCount++
			}
		case string(WriteUsage):
			if record.ID == "" || !isUUIDv7(record.ID) {
				return fmt.Errorf("invalid usage id")
			}
			if record.Usage == nil {
				return fmt.Errorf("usage record has no usage")
			}
			if _, ok := usage[record.ID]; ok {
				return fmt.Errorf("duplicate usage id %q", record.ID)
			}
			if record.EntryID != nil {
				if _, ok := entries[*record.EntryID]; !ok {
					return fmt.Errorf("usage references missing entry")
				}
			}
			row := UsageRow{ID: record.ID, Seq: record.Seq, Usage: cloneValue(*record.Usage).(Usage), EntryID: cloneStringPointer(record.EntryID), Adjustment: record.Adjustment, Details: cloneValue(record.Details)}
			usage[row.ID] = row
			addUsage(&stats, row.Usage)
		case string(WriteRegister):
			if !validNamespace(record.Namespace) || (record.Op != RegisterSet && record.Op != RegisterDelete) {
				return fmt.Errorf("invalid register record")
			}
			if record.Op == RegisterSet {
				rawValue, err := json.Marshal(record.Value)
				if err != nil {
					return err
				}
				value, err := decodeRegisterValue(record.Namespace, rawValue)
				if err != nil {
					return err
				}
				if err := validateRegister(record.Namespace, value); err != nil {
					return err
				}
				if record.Namespace == RegisterLaneLeaf {
					leaf := value.(*string)
					if leaf != nil && !hasEntry(entries, *leaf) {
						return fmt.Errorf("lane leaf references missing entry")
					}
				}
				registers[registerKey(record.Namespace, record.Key)] = Register{Namespace: record.Namespace, Key: record.Key, Value: value, Seq: record.Seq}
			} else {
				delete(registers, registerKey(record.Namespace, record.Key))
			}
		default:
			return fmt.Errorf("unknown record kind %q", record.Kind)
		}
		if record.Seq >= nextSeq {
			nextSeq = record.Seq + 1
		}
	}
	s.memory.data.entries, s.memory.data.registers, s.memory.data.usage = entries, registers, usage
	s.memory.data.children, s.memory.data.stats, s.memory.data.nextSeq = children, stats, nextSeq
	return nil
}

func recordForWrite(write Write, seq, timestamp int64) jsonlRecord {
	switch write.Kind {
	case WriteEntry:
		entry := cloneEntry(write.Entry.Entry)
		entry.Seq, entry.Timestamp = seq, timestamp
		return recordForEntry(entry)
	case WriteUsage:
		row := cloneUsageRow(write.Usage.Row)
		row.Seq = seq
		return recordForUsage(row)
	default:
		rw := write.Register
		return jsonlRecord{Kind: string(WriteRegister), Seq: seq, Op: rw.Operation, Namespace: rw.Namespace, Key: rw.Key, Value: cloneValue(rw.Value)}
	}
}

func recordForEntry(entry Entry) jsonlRecord {
	return jsonlRecord{Kind: string(WriteEntry), Seq: entry.Seq, Timestamp: entry.Timestamp, ID: entry.ID, ParentID: cloneStringPointer(entry.ParentID), Type: entry.Type, CustomType: entry.CustomType, Message: cloneValue(entry.Message).(*AgentMessage), Terminate: entry.Terminate, Summary: entry.Summary, RetainedTail: cloneValue(entry.RetainedTail).([]AgentMessage), TokensBefore: entry.TokensBefore, Details: cloneValue(entry.Details), Usage: cloneValue(entry.Usage).(*Usage), FromHook: entry.FromHook, FromID: entry.FromID, Data: cloneValue(entry.Data)}
}
func recordForUsage(row UsageRow) jsonlRecord {
	return jsonlRecord{Kind: string(WriteUsage), Seq: row.Seq, ID: row.ID, Usage: &row.Usage, EntryID: cloneStringPointer(row.EntryID), Adjustment: row.Adjustment, Details: cloneValue(row.Details)}
}
func recordForRegister(register Register) jsonlRecord {
	return jsonlRecord{Kind: string(WriteRegister), Seq: register.Seq, Op: RegisterSet, Namespace: register.Namespace, Key: register.Key, Value: cloneValue(register.Value)}
}

func entryForRecord(record jsonlRecord) Entry {
	return Entry{EntryBase: EntryBase{ID: record.ID, ParentID: cloneStringPointer(record.ParentID), Seq: record.Seq, Timestamp: record.Timestamp, Type: record.Type}, CustomType: record.CustomType, Message: cloneValue(record.Message).(*AgentMessage), Terminate: record.Terminate, Summary: record.Summary, RetainedTail: cloneValue(record.RetainedTail).([]AgentMessage), TokensBefore: record.TokensBefore, Details: cloneValue(record.Details), Usage: cloneValue(record.Usage).(*Usage), FromHook: record.FromHook, FromID: record.FromID, Data: cloneValue(record.Data)}
}

type JSONLSessionRepo struct {
	dir     string
	options JSONLStorageOptions
	mu      sync.Mutex
}

func NewJSONLSessionRepo(dir string, options JSONLStorageOptions) *JSONLSessionRepo {
	return &JSONLSessionRepo{dir: dir, options: options}
}
func (r *JSONLSessionRepo) path(id string) string { return filepath.Join(r.dir, id+".jsonl") }

func (r *JSONLSessionRepo) Create(ctx context.Context, options SessionCreateOptions) (Session, error) {
	id := options.ID
	if id == "" {
		id = NewUUIDv7Generator().Next()
	}
	if !validSessionFileID(id) {
		return nil, sessionError(SessionInvalidPayload, fmt.Errorf("invalid session file id %q", id))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := os.Stat(r.path(id)); err == nil {
		return nil, sessionError(SessionAlreadyExists, fmt.Errorf("session already exists: %s", id))
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	metadata := SessionMetadata{ID: id, CreatedAt: time.Now().UnixMilli(), StorageVersion: CurrentStorageVersion, ParentSessionID: options.ParentSessionID}
	storage, err := CreateJSONLStorage(r.path(id), metadata, r.options)
	if err != nil {
		return nil, err
	}
	var leaf *string
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterLaneLeaf, "main", leaf), registerSet(RegisterLaneState, "main", LaneState{PendingNextRun: []string{}})}}); err != nil {
		storage.Close(ctx)
		return nil, err
	}
	return newSession(metadata, storage), nil
}

func (r *JSONLSessionRepo) Open(ctx context.Context, metadata SessionMetadata) (Session, error) {
	storage, stored, err := OpenJSONLStorage(r.path(metadata.ID), r.options)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, sessionError(SessionNotFound, err)
		}
		return nil, err
	}
	return newSession(stored, storage), nil
}

func (r *JSONLSessionRepo) List(ctx context.Context, _ JSONValue) ([]SessionMetadata, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	files, err := filepath.Glob(filepath.Join(r.dir, "*.jsonl"))
	if err != nil {
		return nil, err
	}
	result := make([]SessionMetadata, 0, len(files))
	for _, path := range files {
		storage, metadata, openErr := OpenJSONLStorage(path, r.options)
		if openErr != nil {
			return nil, openErr
		}
		_ = storage.Close(ctx)
		result = append(result, metadata)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt < result[j].CreatedAt })
	return result, nil
}

func (r *JSONLSessionRepo) Delete(ctx context.Context, metadata SessionMetadata) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := os.Remove(r.path(metadata.ID)); err != nil {
		if os.IsNotExist(err) {
			return sessionError(SessionNotFound, err)
		}
		return err
	}
	return nil
}

func (r *JSONLSessionRepo) Fork(ctx context.Context, source SessionMetadata, options ForkOptions, create SessionCreateOptions) (Session, error) {
	sourceSession, err := r.Open(ctx, source)
	if err != nil {
		return nil, err
	}
	defer sourceSession.Close(ctx)
	allEntries, err := sourceSession.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil {
		return nil, err
	}
	selected := make(map[string]bool, len(allEntries))
	if options.Scope == "tree" {
		for _, entry := range allEntries {
			selected[entry.ID] = true
		}
	} else {
		leaf, leafErr := sourceSession.GetLeafID(ctx)
		if leafErr != nil {
			return nil, leafErr
		}
		start := options.EntryID
		if start == "" && leaf != nil {
			start = *leaf
		}
		branch, scanErr := sourceSession.FindEntriesOnBranch(ctx, BranchScan{Start: start, Order: NewestFirst})
		if scanErr != nil {
			return nil, scanErr
		}
		for _, entry := range branch {
			selected[entry.ID] = true
		}
		if options.Position == "before" && options.EntryID != "" {
			delete(selected, options.EntryID)
		}
	}
	created, err := r.Create(ctx, create)
	if err != nil {
		return nil, err
	}
	for _, entry := range allEntries {
		if !selected[entry.ID] {
			continue
		}
		entry.Seq, entry.Timestamp = 0, 0
		if entry.ParentID != nil && !selected[*entry.ParentID] {
			entry.ParentID = nil
		}
		if _, err := created.Commit(ctx, Transaction{Writes: []Write{{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}}}}); err != nil {
			return nil, err
		}
	}
	writes := make([]Write, 0)
	copyRegister := func(namespace RegisterNamespace, key string, onlyIf func() bool) error {
		register, err := sourceSession.GetRegister(ctx, namespace, key)
		if err != nil {
			return err
		}
		if register != nil && (onlyIf == nil || onlyIf()) {
			writes = append(writes, registerSet(namespace, key, register.Value))
		}
		return nil
	}
	if err := copyRegister(RegisterFactName, "", nil); err != nil {
		return nil, err
	}
	customFacts, err := sourceSession.ListRegisters(ctx, RegisterFactCustom, "")
	if err != nil {
		return nil, err
	}
	for _, register := range customFacts {
		writes = append(writes, registerSet(register.Namespace, register.Key, register.Value))
	}
	labels, err := sourceSession.ListRegisters(ctx, RegisterFactLabel, "")
	if err != nil {
		return nil, err
	}
	for _, register := range labels {
		if selected[register.Key] {
			writes = append(writes, registerSet(register.Namespace, register.Key, register.Value))
		}
	}
	if options.Scope == "tree" {
		leaves, err := sourceSession.ListRegisters(ctx, RegisterLaneLeaf, "")
		if err != nil {
			return nil, err
		}
		for _, leaf := range leaves {
			if leaf.Key == "main" {
				continue
			}
			value, ok := leaf.Value.(*string)
			if !ok || value == nil || !selected[*value] {
				value = nil
			}
			writes = append(writes, registerSet(RegisterLaneLeaf, leaf.Key, value), registerSet(RegisterLaneState, leaf.Key, LaneState{PendingNextRun: []string{}}))
			config, configErr := sourceSession.GetRegister(ctx, RegisterLaneConfig, leaf.Key)
			if configErr != nil {
				return nil, configErr
			}
			if config != nil {
				writes = append(writes, registerSet(RegisterLaneConfig, leaf.Key, config.Value))
			}
		}
	}
	mainLeaf := (*string)(nil)
	for i := len(allEntries) - 1; i >= 0; i-- {
		if selected[allEntries[i].ID] {
			mainLeaf = stringPointer(allEntries[i].ID)
			break
		}
	}
	writes = append(writes, registerSet(RegisterLaneLeaf, "main", mainLeaf))
	mainConfig, err := sourceSession.GetRegister(ctx, RegisterLaneConfig, "main")
	if err != nil {
		return nil, err
	}
	if mainConfig != nil {
		writes = append(writes, registerSet(RegisterLaneConfig, "main", mainConfig.Value))
	}
	if len(writes) > 0 {
		if _, err := created.Commit(ctx, Transaction{Writes: writes}); err != nil {
			return nil, err
		}
	}
	return created, nil
}

func validSessionFileID(id string) bool {
	return id != "" && id != "." && id != ".." && !strings.ContainsAny(id, `/\\`)
}
