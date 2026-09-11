package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"
)

var errStorageClosed = errors.New("storage is closed")

type MemoryStorageOptions struct {
	Codec SessionCodecOptions
	Now   func() time.Time
}

type memoryData struct {
	mu        sync.RWMutex
	entries   map[string]Entry
	registers map[string]Register
	usage     map[string]UsageRow
	children  map[string][]string
	nextSeq   int64
	stats     SessionStats
}

// MemoryStorage is the reference backend. It stages a complete map snapshot
// per commit; this is O(n) and intentional for the small reference backend.
// ponytail: replace snapshots with copy-on-write pages only after profiling.
type MemoryStorage struct {
	data   *memoryData
	codec  SessionCodec
	now    func() time.Time
	mu     sync.Mutex
	closed bool
}

func NewMemoryStorage(options MemoryStorageOptions) *MemoryStorage {
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &MemoryStorage{
		data: &memoryData{
			entries:   make(map[string]Entry),
			registers: make(map[string]Register),
			usage:     make(map[string]UsageRow),
			children:  make(map[string][]string),
			nextSeq:   1,
		},
		codec: NewSessionCodec(options.Codec),
		now:   now,
	}
}

func newMemoryStorage(data *memoryData, codec SessionCodec, now func() time.Time) *MemoryStorage {
	return &MemoryStorage{data: data, codec: codec, now: now}
}

func (s *MemoryStorage) admitted(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStorageClosed
	}
	return nil
}

func (s *MemoryStorage) Commit(ctx context.Context, tx Transaction) (CommitResult, error) {
	if err := s.admitted(ctx); err != nil {
		return CommitResult{}, err
	}
	s.data.mu.Lock()
	defer s.data.mu.Unlock()
	return s.commitLocked(tx)
}

func (s *MemoryStorage) commitLocked(tx Transaction) (CommitResult, error) {
	entries := cloneEntries(s.data.entries)
	registers := cloneRegisters(s.data.registers)
	usage := cloneUsage(s.data.usage)
	children := cloneChildren(s.data.children)
	stats := s.data.stats
	seq := s.data.nextSeq
	timestamp := s.now().UnixMilli()
	result := CommitResult{Timestamp: timestamp}

	for i, write := range tx.Writes {
		if err := validateWrite(write, entries, usage, s.codec); err != nil {
			return CommitResult{}, fmt.Errorf("write %d: %w", i, err)
		}
		if result.FirstSeq == 0 {
			result.FirstSeq = seq
		}
		result.Seqs = append(result.Seqs, seq)
		switch write.Kind {
		case WriteEntry:
			entry := cloneEntry(write.Entry.Entry)
			entry.Seq = seq
			entry.Timestamp = timestamp
			entries[entry.ID] = entry
			if entry.ParentID != nil {
				parent := *entry.ParentID
				children[parent] = append(children[parent], entry.ID)
			}
			if entry.Type == EntryMessage {
				stats.MessageCount++
			}
		case WriteUsage:
			row := cloneUsageRow(write.Usage.Row)
			row.Seq = seq
			usage[row.ID] = row
			addUsage(&stats, row.Usage)
		case WriteRegister:
			rw := write.Register
			key := registerKey(rw.Namespace, rw.Key)
			if rw.Operation == RegisterDelete {
				delete(registers, key)
			} else {
				registers[key] = Register{
					Namespace: rw.Namespace,
					Key:       rw.Key,
					Value:     cloneValue(rw.Value),
					Seq:       seq,
				}
			}
		}
		seq++
	}

	s.data.entries = entries
	s.data.registers = registers
	s.data.usage = usage
	s.data.children = children
	s.data.stats = stats
	s.data.nextSeq = seq
	return result, nil
}

func (s *MemoryStorage) GetEntries(ctx context.Context, ids []string) (map[string]Entry, error) {
	if err := s.admitted(ctx); err != nil {
		return nil, err
	}
	s.data.mu.RLock()
	defer s.data.mu.RUnlock()
	out := make(map[string]Entry, len(ids))
	for _, id := range ids {
		if entry, ok := s.data.entries[id]; ok {
			out[id] = cloneEntry(entry)
		}
	}
	return out, nil
}

func (s *MemoryStorage) GetRegister(ctx context.Context, namespace RegisterNamespace, key string) (*Register, error) {
	if err := s.admitted(ctx); err != nil {
		return nil, err
	}
	s.data.mu.RLock()
	defer s.data.mu.RUnlock()
	register, ok := s.data.registers[registerKey(namespace, key)]
	if !ok {
		return nil, nil
	}
	register.Value = cloneValue(register.Value)
	return &register, nil
}

func (s *MemoryStorage) ListRegisters(ctx context.Context, namespace RegisterNamespace, keyPrefix string) ([]Register, error) {
	if err := s.admitted(ctx); err != nil {
		return nil, err
	}
	s.data.mu.RLock()
	defer s.data.mu.RUnlock()
	result := make([]Register, 0)
	for _, register := range s.data.registers {
		if register.Namespace == namespace && hasPrefix(register.Key, keyPrefix) {
			register.Value = cloneValue(register.Value)
			result = append(result, register)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func (s *MemoryStorage) ScanBranch(ctx context.Context, query BranchScan) ([]Entry, error) {
	if err := s.admitted(ctx); err != nil {
		return nil, err
	}
	s.data.mu.RLock()
	defer s.data.mu.RUnlock()
	return scanBranchLocked(s.data.entries, query)
}

func (s *MemoryStorage) ScanBranchStructure(ctx context.Context, query BranchScan) ([]EntryStructure, error) {
	entries, err := s.ScanBranch(ctx, query)
	if err != nil {
		return nil, err
	}
	result := make([]EntryStructure, len(entries))
	for i, entry := range entries {
		result[i] = structureOf(entry)
	}
	return result, nil
}

func (s *MemoryStorage) ScanEntries(ctx context.Context, query EntryScan) ([]Entry, error) {
	if err := s.admitted(ctx); err != nil {
		return nil, err
	}
	s.data.mu.RLock()
	defer s.data.mu.RUnlock()
	result := make([]Entry, 0)
	for _, entry := range s.data.entries {
		if query.Type != nil && entry.Type != *query.Type {
			continue
		}
		if query.CustomType != "" && entry.CustomType != query.CustomType {
			continue
		}
		if query.FromSeq != 0 && entry.Seq < query.FromSeq || query.ToSeq != 0 && entry.Seq > query.ToSeq {
			continue
		}
		result = append(result, cloneEntry(entry))
	}
	sortEntries(result, query.Order)
	result = applyEntryCursor(result, query.Cursor, query.Order)
	return limitEntries(result, query.Limit), nil
}

func (s *MemoryStorage) ScanUsage(ctx context.Context, query UsageScan) ([]UsageRow, error) {
	if err := s.admitted(ctx); err != nil {
		return nil, err
	}
	s.data.mu.RLock()
	defer s.data.mu.RUnlock()
	result := make([]UsageRow, 0)
	for _, row := range s.data.usage {
		if query.FromSeq != 0 && row.Seq < query.FromSeq || query.ToSeq != 0 && row.Seq > query.ToSeq {
			continue
		}
		result = append(result, cloneUsageRow(row))
	}
	sort.Slice(result, func(i, j int) bool {
		if isOldestFirst(query.Order) {
			return result[i].Seq < result[j].Seq
		}
		return result[i].Seq > result[j].Seq
	})
	if query.Cursor != nil {
		filtered := result[:0]
		for _, row := range result {
			if isOldestFirst(query.Order) && row.Seq > query.Cursor.AfterSeq || !isOldestFirst(query.Order) && row.Seq < query.Cursor.AfterSeq {
				filtered = append(filtered, row)
			}
		}
		result = filtered
	}
	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}
	return result, nil
}

func (s *MemoryStorage) GetStats(ctx context.Context) (SessionStats, error) {
	if err := s.admitted(ctx); err != nil {
		return SessionStats{}, err
	}
	s.data.mu.RLock()
	defer s.data.mu.RUnlock()
	return s.data.stats, nil
}

func (s *MemoryStorage) Close(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func registerKey(namespace RegisterNamespace, key string) string {
	return string(namespace) + "\x00" + key
}

func hasPrefix(value, prefix string) bool {
	if prefix == "" {
		return true
	}
	return len(value) >= len(prefix) && value[:len(prefix)] == prefix
}

func cloneEntries(input map[string]Entry) map[string]Entry {
	output := make(map[string]Entry, len(input))
	for key, entry := range input {
		output[key] = cloneEntry(entry)
	}
	return output
}

func cloneRegisters(input map[string]Register) map[string]Register {
	output := make(map[string]Register, len(input))
	for key, register := range input {
		register.Value = cloneValue(register.Value)
		output[key] = register
	}
	return output
}

func cloneUsage(input map[string]UsageRow) map[string]UsageRow {
	output := make(map[string]UsageRow, len(input))
	for key, row := range input {
		output[key] = cloneUsageRow(row)
	}
	return output
}

func cloneChildren(input map[string][]string) map[string][]string {
	output := make(map[string][]string, len(input))
	for key, values := range input {
		output[key] = append([]string(nil), values...)
	}
	return output
}

func cloneEntry(entry Entry) Entry        { return cloneValue(entry).(Entry) }
func cloneUsageRow(row UsageRow) UsageRow { return cloneValue(row).(UsageRow) }

func cloneValue(value any) any {
	if value == nil {
		return nil
	}
	return cloneReflect(reflect.ValueOf(value)).Interface()
}

func cloneReflect(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.New(value.Type()).Elem()
		out.Set(cloneReflect(value.Elem()))
		return out
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.New(value.Type().Elem())
		out.Elem().Set(cloneReflect(value.Elem()))
		return out
	case reflect.Struct:
		out := reflect.New(value.Type()).Elem()
		for i := 0; i < value.NumField(); i++ {
			if out.Field(i).CanSet() {
				out.Field(i).Set(cloneReflect(value.Field(i)))
			}
		}
		return out
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			out.Index(i).Set(cloneReflect(value.Index(i)))
		}
		return out
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			out.SetMapIndex(cloneReflect(iter.Key()), cloneReflect(iter.Value()))
		}
		return out
	case reflect.Array:
		out := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			out.Index(i).Set(cloneReflect(value.Index(i)))
		}
		return out
	default:
		return value
	}
}

func addUsage(stats *SessionStats, usage Usage) {
	stats.CachedTokens += usage.CacheRead
	stats.UncachedTokens += usage.Input
	stats.TotalTokens += usage.Total
	if usage.Total == 0 {
		stats.TotalTokens += usage.Input + usage.Output
	}
	if usage.Cost != nil {
		stats.CostTotal += usage.Cost.Total
	}
}

func jsonValid(value any) error {
	if _, err := json.Marshal(value); err != nil {
		return fmt.Errorf("value is not JSON: %w", err)
	}
	return nil
}
