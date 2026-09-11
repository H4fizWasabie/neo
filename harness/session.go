package harness

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

const CurrentStorageVersion = 1

type MemorySessionRepo struct {
	mu       sync.RWMutex
	sessions map[string]*memoryData
	metadata map[string]SessionMetadata
	codec    SessionCodec
	now      func() time.Time
}

func NewMemorySessionRepo(options SessionCodecOptions) *MemorySessionRepo {
	return &MemorySessionRepo{
		sessions: make(map[string]*memoryData),
		metadata: make(map[string]SessionMetadata),
		codec:    NewSessionCodec(options),
		now:      time.Now,
	}
}

func (r *MemorySessionRepo) Create(ctx context.Context, options SessionCreateOptions) (Session, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	id := options.ID
	if id == "" {
		id = NewUUIDv7Generator().Next()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.sessions[id]; exists {
		return nil, sessionError(SessionAlreadyExists, fmt.Errorf("session already exists: %s", id))
	}
	metadata := SessionMetadata{ID: id, CreatedAt: r.now().UnixMilli(), StorageVersion: CurrentStorageVersion, ParentSessionID: options.ParentSessionID}
	data := newMemoryData()
	storage := newMemoryStorage(data, r.codec, r.now)
	var leaf *string
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{
		registerSet(RegisterLaneLeaf, "main", leaf),
		registerSet(RegisterLaneState, "main", LaneState{PendingNextRun: []string{}}),
	}}); err != nil {
		return nil, err
	}
	r.sessions[id] = data
	r.metadata[id] = metadata
	return newMemorySession(metadata, newMemoryStorage(data, r.codec, r.now)), nil
}

func (r *MemorySessionRepo) Open(ctx context.Context, metadata SessionMetadata) (Session, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	r.mu.RLock()
	data := r.sessions[metadata.ID]
	stored, ok := r.metadata[metadata.ID]
	r.mu.RUnlock()
	if !ok {
		return nil, sessionError(SessionNotFound, fmt.Errorf("session not found: %s", metadata.ID))
	}
	if stored.StorageVersion > CurrentStorageVersion {
		return nil, sessionError(SessionStorageFailure, fmt.Errorf("session storage version %d is newer than binary version %d", stored.StorageVersion, CurrentStorageVersion))
	}
	return newMemorySession(stored, newMemoryStorage(data, r.codec, r.now)), nil
}

func (r *MemorySessionRepo) List(ctx context.Context, _ JSONValue) ([]SessionMetadata, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]SessionMetadata, 0, len(r.metadata))
	for _, metadata := range r.metadata {
		result = append(result, metadata)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt < result[j].CreatedAt })
	return result, nil
}

func (r *MemorySessionRepo) Delete(ctx context.Context, metadata SessionMetadata) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sessions[metadata.ID]; !ok {
		return sessionError(SessionNotFound, fmt.Errorf("session not found: %s", metadata.ID))
	}
	delete(r.sessions, metadata.ID)
	delete(r.metadata, metadata.ID)
	return nil
}

func (r *MemorySessionRepo) Fork(ctx context.Context, source SessionMetadata, options ForkOptions, create SessionCreateOptions) (Session, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	sourceData, ok := r.sessions[source.ID]
	if !ok {
		return nil, sessionError(SessionNotFound, fmt.Errorf("session not found: %s", source.ID))
	}
	id := create.ID
	if id == "" {
		id = NewUUIDv7Generator().Next()
	}
	if _, exists := r.sessions[id]; exists {
		return nil, sessionError(SessionAlreadyExists, fmt.Errorf("session already exists: %s", id))
	}
	data, err := forkMemoryData(sourceData, options, r.codec)
	if err != nil {
		return nil, sessionError(SessionStorageFailure, err)
	}
	metadata := SessionMetadata{ID: id, CreatedAt: r.now().UnixMilli(), StorageVersion: CurrentStorageVersion, ParentSessionID: source.ID}
	r.sessions[id] = data
	r.metadata[id] = metadata
	return newMemorySession(metadata, newMemoryStorage(data, r.codec, r.now)), nil
}

func newMemoryData() *memoryData {
	return &memoryData{entries: make(map[string]Entry), registers: make(map[string]Register), usage: make(map[string]UsageRow), children: make(map[string][]string), nextSeq: 1}
}

type MemorySession struct {
	metadata SessionMetadata
	storage  Storage
	idgen    *UUIDv7Generator
	main     *memoryTree
}

func newMemorySession(metadata SessionMetadata, storage *MemoryStorage) *MemorySession {
	return newSession(metadata, storage)
}

func newSession(metadata SessionMetadata, storage Storage) *MemorySession {
	idgen := NewUUIDv7Generator()
	return &MemorySession{metadata: metadata, storage: storage, idgen: idgen, main: &memoryTree{session: nil, storage: storage, lane: "main", idgen: idgen}}
}

func (s *MemorySession) Metadata() SessionMetadata { return s.metadata }
func (s *MemorySession) IDGenerator() IDGenerator  { return s.idgen }
func (s *MemorySession) View(lane string) SessionTree {
	return &memoryTree{session: s, storage: s.storage, lane: lane, idgen: s.idgen}
}
func (s *MemorySession) Commit(ctx context.Context, tx Transaction) (CommitResult, error) {
	return s.storage.Commit(ctx, tx)
}
func (s *MemorySession) GetEntries(ctx context.Context, ids []string) (map[string]Entry, error) {
	return s.storage.GetEntries(ctx, ids)
}
func (s *MemorySession) GetRegister(ctx context.Context, namespace RegisterNamespace, key string) (*Register, error) {
	return s.storage.GetRegister(ctx, namespace, key)
}
func (s *MemorySession) ListRegisters(ctx context.Context, namespace RegisterNamespace, prefix string) ([]Register, error) {
	return s.storage.ListRegisters(ctx, namespace, prefix)
}
func (s *MemorySession) Close(ctx context.Context) error { return s.storage.Close(ctx) }

func (s *MemorySession) CreateLane(ctx context.Context, name string, at *string, config LaneConfiguration) error {
	if name == "" {
		return sessionError(SessionInvalidLane, fmt.Errorf("lane name is required"))
	}
	if existing, err := s.storage.GetRegister(ctx, RegisterLaneState, name); err != nil {
		return err
	} else if existing != nil {
		return sessionError(SessionInvalidLane, fmt.Errorf("lane already exists: %s", name))
	}
	if at != nil {
		entries, err := s.storage.GetEntries(ctx, []string{*at})
		if err != nil {
			return err
		}
		if _, ok := entries[*at]; !ok {
			return sessionError(SessionInvalidPayload, fmt.Errorf("lane anchor does not exist: %s", *at))
		}
	}
	_, err := s.storage.Commit(ctx, Transaction{Writes: []Write{
		registerSet(RegisterLaneConfig, name, config),
		registerSet(RegisterLaneLeaf, name, at),
		registerSet(RegisterLaneState, name, LaneState{PendingNextRun: []string{}}),
	}})
	return err
}

func (s *MemorySession) SetLaneConfiguration(ctx context.Context, name string, config LaneConfiguration) error {
	if register, err := s.storage.GetRegister(ctx, RegisterLaneState, name); err != nil {
		return err
	} else if register == nil {
		return sessionError(SessionInvalidLane, fmt.Errorf("lane does not exist: %s", name))
	}
	_, err := s.storage.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterLaneConfig, name, config)}})
	return err
}

func (s *MemorySession) LaneConfiguration(ctx context.Context, name string) (LaneConfiguration, error) {
	register, err := s.storage.GetRegister(ctx, RegisterLaneConfig, name)
	if err != nil {
		return LaneConfiguration{}, err
	}
	if register == nil {
		return LaneConfiguration{}, sessionError(SessionInvalidLane, fmt.Errorf("lane has no configuration: %s", name))
	}
	config, ok := register.Value.(LaneConfiguration)
	if !ok {
		return LaneConfiguration{}, sessionError(SessionInvalidLane, fmt.Errorf("lane has invalid configuration: %s", name))
	}
	return config, nil
}

func (s *MemorySession) GetLeafID(ctx context.Context) (*string, error) { return s.main.GetLeafID(ctx) }
func (s *MemorySession) GetEntry(ctx context.Context, id string) (*Entry, error) {
	return s.main.GetEntry(ctx, id)
}
func (s *MemorySession) GetStats(ctx context.Context) (SessionStats, error) {
	return s.main.GetStats(ctx)
}
func (s *MemorySession) GetName(ctx context.Context) (*string, error) { return s.main.GetName(ctx) }
func (s *MemorySession) SetName(ctx context.Context, name *string) error {
	return s.main.SetName(ctx, name)
}
func (s *MemorySession) GetLabel(ctx context.Context, id string) (*string, error) {
	return s.main.GetLabel(ctx, id)
}
func (s *MemorySession) SetLabel(ctx context.Context, id string, label *string) error {
	return s.main.SetLabel(ctx, id, label)
}
func (s *MemorySession) GetCustomFact(ctx context.Context, key string) (JSONValue, error) {
	return s.main.GetCustomFact(ctx, key)
}
func (s *MemorySession) SetCustomFact(ctx context.Context, key string, value JSONValue) error {
	return s.main.SetCustomFact(ctx, key, value)
}
func (s *MemorySession) DeleteCustomFact(ctx context.Context, key string) error {
	return s.main.DeleteCustomFact(ctx, key)
}
func (s *MemorySession) FindEntries(ctx context.Context, query EntryQuery) ([]Entry, error) {
	return s.main.FindEntries(ctx, query)
}
func (s *MemorySession) FindEntry(ctx context.Context, query EntryQuery) (*Entry, error) {
	return s.main.FindEntry(ctx, query)
}
func (s *MemorySession) FindEntriesOnBranch(ctx context.Context, query BranchScan) ([]Entry, error) {
	return s.main.FindEntriesOnBranch(ctx, query)
}
func (s *MemorySession) FindEntryOnBranch(ctx context.Context, query BranchScan) (*Entry, error) {
	return s.main.FindEntryOnBranch(ctx, query)
}
func (s *MemorySession) AppendMessage(ctx context.Context, message AgentMessage) (string, error) {
	return s.main.AppendMessage(ctx, message)
}
func (s *MemorySession) AppendCustomEntry(ctx context.Context, customType string, data JSONValue) (string, error) {
	return s.main.AppendCustomEntry(ctx, customType, data)
}

type memoryTree struct {
	session *MemorySession
	storage Storage
	lane    string
	idgen   IDGenerator
}

func (t *memoryTree) GetLeafID(ctx context.Context) (*string, error) {
	register, err := t.storage.GetRegister(ctx, RegisterLaneLeaf, t.lane)
	if err != nil {
		return nil, err
	}
	if register == nil {
		return nil, sessionError(SessionInvalidLane, fmt.Errorf("lane does not exist: %s", t.lane))
	}
	if register.Value == nil {
		return nil, nil
	}
	leaf, ok := register.Value.(*string)
	if !ok {
		return nil, sessionError(SessionInvalidLane, fmt.Errorf("lane %s has invalid leaf", t.lane))
	}
	return cloneStringPointer(leaf), nil
}

func (t *memoryTree) GetEntry(ctx context.Context, id string) (*Entry, error) {
	entries, err := t.storage.GetEntries(ctx, []string{id})
	if err != nil {
		return nil, err
	}
	entry, ok := entries[id]
	if !ok {
		return nil, nil
	}
	return &entry, nil
}

func (t *memoryTree) GetStats(ctx context.Context) (SessionStats, error) {
	return t.storage.GetStats(ctx)
}

func (t *memoryTree) GetName(ctx context.Context) (*string, error) {
	register, err := t.storage.GetRegister(ctx, RegisterFactName, "")
	if err != nil || register == nil {
		return nil, err
	}
	name, ok := register.Value.(string)
	if !ok {
		return nil, fmt.Errorf("name fact has invalid value")
	}
	return &name, nil
}

func (t *memoryTree) SetName(ctx context.Context, name *string) error {
	if name == nil {
		return t.commitFact(ctx, RegisterFactName, "", nil, false)
	}
	return t.commitFact(ctx, RegisterFactName, "", *name, true)
}

func (t *memoryTree) GetLabel(ctx context.Context, id string) (*string, error) {
	register, err := t.storage.GetRegister(ctx, RegisterFactLabel, id)
	if err != nil || register == nil {
		return nil, err
	}
	label, ok := register.Value.(string)
	if !ok {
		return nil, fmt.Errorf("label fact has invalid value")
	}
	return &label, nil
}

func (t *memoryTree) SetLabel(ctx context.Context, id string, label *string) error {
	entry, err := t.GetEntry(ctx, id)
	if err != nil {
		return err
	}
	if entry == nil {
		return sessionError(SessionInvalidPayload, fmt.Errorf("label target does not exist: %s", id))
	}
	if label == nil {
		return t.commitFact(ctx, RegisterFactLabel, id, nil, false)
	}
	return t.commitFact(ctx, RegisterFactLabel, id, *label, true)
}

func (t *memoryTree) GetCustomFact(ctx context.Context, key string) (JSONValue, error) {
	register, err := t.storage.GetRegister(ctx, RegisterFactCustom, key)
	if err != nil || register == nil {
		return nil, err
	}
	return cloneValue(register.Value), nil
}

func (t *memoryTree) SetCustomFact(ctx context.Context, key string, value JSONValue) error {
	return t.commitFact(ctx, RegisterFactCustom, key, value, true)
}

func (t *memoryTree) DeleteCustomFact(ctx context.Context, key string) error {
	return t.commitFact(ctx, RegisterFactCustom, key, nil, false)
}

func (t *memoryTree) commitFact(ctx context.Context, namespace RegisterNamespace, key string, value any, set bool) error {
	operation := RegisterDelete
	if set {
		operation = RegisterSet
	}
	_, err := t.storage.Commit(ctx, Transaction{Writes: []Write{{Kind: WriteRegister, Register: &RegisterWrite{Operation: operation, Namespace: namespace, Key: key, Value: value}}}})
	return err
}

func (t *memoryTree) FindEntries(ctx context.Context, query EntryQuery) ([]Entry, error) {
	return t.storage.ScanEntries(ctx, EntryScan{Type: query.Type, CustomType: query.CustomType, Order: query.Order, Limit: query.Limit, Cursor: query.Cursor})
}

func (t *memoryTree) FindEntry(ctx context.Context, query EntryQuery) (*Entry, error) {
	entries, err := t.FindEntries(ctx, query)
	if err != nil || len(entries) == 0 {
		return nil, err
	}
	return &entries[0], nil
}

func (t *memoryTree) FindEntriesOnBranch(ctx context.Context, query BranchScan) ([]Entry, error) {
	if query.Start == "" {
		leaf, err := t.GetLeafID(ctx)
		if err != nil {
			return nil, err
		}
		if leaf == nil {
			return []Entry{}, nil
		}
		query.Start = *leaf
	}
	return t.storage.ScanBranch(ctx, query)
}

func (t *memoryTree) FindEntryOnBranch(ctx context.Context, query BranchScan) (*Entry, error) {
	entries, err := t.FindEntriesOnBranch(ctx, query)
	if err != nil || len(entries) == 0 {
		return nil, err
	}
	return &entries[0], nil
}

func (t *memoryTree) AppendMessage(ctx context.Context, message AgentMessage) (string, error) {
	return t.append(ctx, Entry{EntryBase: EntryBase{ID: t.idgen.Next(), Type: EntryMessage}, Message: &message})
}

func (t *memoryTree) AppendCustomEntry(ctx context.Context, customType string, data JSONValue) (string, error) {
	return t.append(ctx, Entry{EntryBase: EntryBase{ID: t.idgen.Next(), Type: EntryCustom}, CustomType: customType, Data: data})
}

func (t *memoryTree) append(ctx context.Context, entry Entry) (string, error) {
	leaf, err := t.GetLeafID(ctx)
	if err != nil {
		return "", err
	}
	entry.ParentID = leaf
	if entry.Type == EntryCustom && entry.CustomType == "" {
		return "", sessionError(SessionInvalidPayload, fmt.Errorf("custom type is required"))
	}
	if _, err := t.storage.Commit(ctx, Transaction{Writes: []Write{
		{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}},
		{Kind: WriteRegister, Register: &RegisterWrite{Operation: RegisterSet, Namespace: RegisterLaneLeaf, Key: t.lane, Value: stringPointer(entry.ID)}},
	}}); err != nil {
		return "", err
	}
	return entry.ID, nil
}

func stringPointer(value string) *string { return &value }

func registerSet(namespace RegisterNamespace, key string, value any) Write {
	return Write{Kind: WriteRegister, Register: &RegisterWrite{Operation: RegisterSet, Namespace: namespace, Key: key, Value: value}}
}

func sessionError(code SessionErrorCode, cause error) error {
	return &SessionError{Code: code, Cause: cause, Msg: cause.Error()}
}

func forkMemoryData(source *memoryData, options ForkOptions, codec SessionCodec) (*memoryData, error) {
	source.mu.RLock()
	entries := cloneEntries(source.entries)
	registers := cloneRegisters(source.registers)
	source.mu.RUnlock()
	destination := newMemoryData()
	selected := make(map[string]bool)
	if options.Scope == "tree" {
		for id := range entries {
			selected[id] = true
		}
	} else {
		start := options.EntryID
		if start == "" {
			if leaf, ok := registers[registerKey(RegisterLaneLeaf, "main")]; ok && leaf.Value != nil {
				if leafID, ok := leaf.Value.(*string); ok && leafID != nil {
					start = *leafID
				}
			}
		}
		for start != "" {
			entry, ok := entries[start]
			if !ok {
				break
			}
			selected[start] = true
			if entry.ParentID == nil {
				break
			}
			start = *entry.ParentID
		}
		if options.Position == "before" && options.EntryID != "" {
			delete(selected, options.EntryID)
		}
	}
	ids := make([]string, 0, len(selected))
	for id := range selected {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return entries[ids[i]].Seq < entries[ids[j]].Seq })
	storage := newMemoryStorage(destination, codec, time.Now)
	writes := make([]Write, 0, len(ids)+8)
	for _, id := range ids {
		entry := entries[id]
		entry.Seq = 0
		entry.Timestamp = 0
		if entry.ParentID != nil && !selected[*entry.ParentID] {
			entry.ParentID = nil
		}
		writes = append(writes, Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}})
	}
	if name, ok := registers[registerKey(RegisterFactName, "")]; ok {
		writes = append(writes, registerSet(RegisterFactName, "", name.Value))
	}
	for _, register := range registers {
		if register.Namespace == RegisterFactCustom {
			writes = append(writes, registerSet(register.Namespace, register.Key, register.Value))
		}
		if register.Namespace == RegisterFactLabel && selected[register.Key] {
			writes = append(writes, registerSet(register.Namespace, register.Key, register.Value))
		}
	}
	if options.Scope == "tree" {
		lanes := make([]string, 0)
		for _, register := range registers {
			if register.Namespace == RegisterLaneLeaf {
				lanes = append(lanes, register.Key)
			}
		}
		sort.Strings(lanes)
		for _, lane := range lanes {
			var leaf *string
			if register, ok := registers[registerKey(RegisterLaneLeaf, lane)]; ok {
				leaf, _ = register.Value.(*string)
				if leaf != nil && !selected[*leaf] {
					leaf = nil
				}
			}
			writes = append(writes, registerSet(RegisterLaneLeaf, lane, leaf), registerSet(RegisterLaneState, lane, LaneState{PendingNextRun: []string{}}))
			if register, ok := registers[registerKey(RegisterLaneConfig, lane)]; ok {
				writes = append(writes, registerSet(RegisterLaneConfig, lane, register.Value))
			}
		}
	} else {
		var leaf *string
		if len(ids) != 0 {
			leaf = stringPointer(ids[len(ids)-1])
		}
		writes = append(writes, registerSet(RegisterLaneLeaf, "main", leaf), registerSet(RegisterLaneState, "main", LaneState{PendingNextRun: []string{}}))
		if register, ok := registers[registerKey(RegisterLaneConfig, "main")]; ok {
			writes = append(writes, registerSet(RegisterLaneConfig, "main", register.Value))
		}
	}
	if len(writes) > 0 {
		if _, err := storage.Commit(context.Background(), Transaction{Writes: writes}); err != nil {
			return nil, err
		}
	}
	return destination, nil
}
