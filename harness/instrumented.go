package harness

import (
	"context"
	"sync"
)

type CommitObservation struct {
	Transaction Transaction
	Result      CommitResult
}

// InstrumentedStorage is a test decorator; it observes commit order without
// introducing a second durable write history.
type InstrumentedStorage struct {
	Inner   Storage
	mu      sync.Mutex
	commits []CommitObservation
}

func NewInstrumentedStorage(inner Storage) *InstrumentedStorage {
	return &InstrumentedStorage{Inner: inner}
}

func (s *InstrumentedStorage) Commit(ctx context.Context, tx Transaction) (CommitResult, error) {
	result, err := s.Inner.Commit(ctx, tx)
	if err != nil {
		return CommitResult{}, err
	}
	s.mu.Lock()
	s.commits = append(s.commits, CommitObservation{Transaction: cloneTransaction(tx), Result: result})
	s.mu.Unlock()
	return result, nil
}

func (s *InstrumentedStorage) Commits() []CommitObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]CommitObservation, len(s.commits))
	copy(result, s.commits)
	for i := range result {
		result[i].Transaction = cloneTransaction(result[i].Transaction)
		result[i].Result.Seqs = append([]int64(nil), result[i].Result.Seqs...)
	}
	return result
}

func (s *InstrumentedStorage) GetEntries(ctx context.Context, ids []string) (map[string]Entry, error) {
	return s.Inner.GetEntries(ctx, ids)
}
func (s *InstrumentedStorage) GetRegister(ctx context.Context, namespace RegisterNamespace, key string) (*Register, error) {
	return s.Inner.GetRegister(ctx, namespace, key)
}
func (s *InstrumentedStorage) ListRegisters(ctx context.Context, namespace RegisterNamespace, prefix string) ([]Register, error) {
	return s.Inner.ListRegisters(ctx, namespace, prefix)
}
func (s *InstrumentedStorage) ScanBranch(ctx context.Context, query BranchScan) ([]Entry, error) {
	return s.Inner.ScanBranch(ctx, query)
}
func (s *InstrumentedStorage) ScanBranchStructure(ctx context.Context, query BranchScan) ([]EntryStructure, error) {
	return s.Inner.ScanBranchStructure(ctx, query)
}
func (s *InstrumentedStorage) ScanEntries(ctx context.Context, query EntryScan) ([]Entry, error) {
	return s.Inner.ScanEntries(ctx, query)
}
func (s *InstrumentedStorage) ScanUsage(ctx context.Context, query UsageScan) ([]UsageRow, error) {
	return s.Inner.ScanUsage(ctx, query)
}
func (s *InstrumentedStorage) GetStats(ctx context.Context) (SessionStats, error) {
	return s.Inner.GetStats(ctx)
}
func (s *InstrumentedStorage) Close(ctx context.Context) error { return s.Inner.Close(ctx) }

func cloneTransaction(tx Transaction) Transaction { return cloneValue(tx).(Transaction) }
