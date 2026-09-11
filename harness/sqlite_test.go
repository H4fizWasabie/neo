package harness

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSQLiteRepositoryPersistsConformingSession(t *testing.T) {
	ctx := context.Background()
	repo := NewSQLiteSessionRepo(t.TempDir(), SQLiteStorageOptions{OwnerID: "owner-a"})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	entryID, err := session.AppendMessage(ctx, AgentMessage{Role: "user", Content: "sqlite"})
	if err != nil {
		t.Fatal(err)
	}
	metadata := session.Metadata()
	deleted, err := session.Commit(ctx, Transaction{Writes: []Write{{Kind: WriteRegister, Register: &RegisterWrite{Operation: RegisterDelete, Namespace: RegisterFactCustom, Key: "absent"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := repo.Open(ctx, metadata)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := reopened.GetEntry(ctx, entryID)
	if err != nil || entry == nil || entry.Message.Content != "sqlite" {
		t.Fatalf("SQLite reopen failed: %v %+v", err, entry)
	}
	secondID, err := reopened.AppendMessage(ctx, AgentMessage{Role: "assistant", Content: "still monotonic"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := reopened.GetEntry(ctx, secondID)
	if err != nil || second == nil || second.Seq <= deleted.Seqs[0] {
		t.Fatalf("SQLite reused a sequence after reopening: %v %+v after %d", err, second, deleted.Seqs[0])
	}
	if _, err := reopened.FindEntriesOnBranch(ctx, BranchScan{}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(ctx); err != nil {
		t.Fatal(err)
	}
	rewritten, err := repo.Rewrite(ctx, metadata, func(string, string) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if got, err := rewritten.FindEntries(ctx, EntryQuery{Order: OldestFirst}); err != nil || len(got) != 2 {
		t.Fatalf("rewrite lost entries: %v %+v", err, got)
	}
	if err := rewritten.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteWriterLeaseFencesSecondOwner(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "session.sqlite")
	metadata := SessionMetadata{ID: "session", CreatedAt: 1700000000000, StorageVersion: CurrentStorageVersion}
	first, err := CreateSQLiteStorage(path, metadata, SQLiteStorageOptions{OwnerID: "owner-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenSQLiteStorage(path, SQLiteStorageOptions{OwnerID: "owner-b"}); err == nil {
		t.Fatal("second writer acquired an active lease")
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	second, _, err := OpenSQLiteStorage(path, SQLiteStorageOptions{OwnerID: "owner-b"})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteStaleOwnerCannotReleaseReplacementLease(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "session.sqlite")
	now := time.UnixMilli(1700000000000)
	first, err := CreateSQLiteStorage(path, SessionMetadata{ID: "session", CreatedAt: now.UnixMilli(), StorageVersion: CurrentStorageVersion}, SQLiteStorageOptions{OwnerID: "owner-a", Now: func() time.Time { return now }, LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	clock := now.Add(2 * time.Second)
	second, _, err := OpenSQLiteStorage(path, SQLiteStorageOptions{OwnerID: "owner-b", Now: func() time.Time { return clock }, LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	id := NewUUIDv7Generator().Next(clock.UnixMilli())
	if _, err := second.Commit(ctx, Transaction{Writes: []Write{{Kind: WriteEntry, Entry: &EntryWrite{Entry: Entry{EntryBase: EntryBase{ID: id, Type: EntryCustom}, CustomType: "lease"}}}}}); err != nil {
		t.Fatalf("stale owner released replacement lease: %v", err)
	}
	if err := second.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteBranchSegmentsRepairAndPlan(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "session.sqlite")
	storage, err := CreateSQLiteStorage(path, SessionMetadata{ID: "session", CreatedAt: 1700000000000, StorageVersion: CurrentStorageVersion}, SQLiteStorageOptions{OwnerID: "owner-a"})
	if err != nil {
		t.Fatal(err)
	}
	session := newSession(SessionMetadata{ID: "session"}, storage)
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterLaneLeaf, "main", (*string)(nil)), registerSet(RegisterLaneState, "main", LaneState{PendingNextRun: []string{}})}}); err != nil {
		t.Fatal(err)
	}
	root, err := session.AppendMessage(ctx, AgentMessage{Role: "user", Content: "root"})
	if err != nil {
		t.Fatal(err)
	}
	child, err := session.AppendMessage(ctx, AgentMessage{Role: "assistant", Content: "child"})
	if err != nil {
		t.Fatal(err)
	}
	branch, err := session.AppendMessage(ctx, AgentMessage{Role: "user", Content: "branch"})
	if err != nil {
		t.Fatal(err)
	}
	_ = branch
	got, err := session.FindEntriesOnBranch(ctx, BranchScan{Start: child, Order: OldestFirst})
	if err != nil || len(got) != 2 || got[0].ID != root || got[1].ID != child {
		t.Fatalf("segmented branch query failed: %v %+v", err, got)
	}
	var plan []string
	rows, err := storage.db.Query(`EXPLAIN QUERY PLAN SELECT e.payload FROM branch_entries b CROSS JOIN entries e ON e.id = b.entry_id WHERE b.branch_id = ? AND b.entry_seq > ? AND b.entry_seq <= ? ORDER BY b.entry_seq DESC`, child, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, detail string
		var selectID, order int
		if err := rows.Scan(&selectID, &order, &id, &detail); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	rows.Close()
	planText := strings.Join(plan, "\n")
	if len(plan) == 0 || strings.Contains(planText, "USE TEMP B-TREE") || strings.Contains(planText, "SCAN entries") || !strings.Contains(planText, "ix_branch_seq") || !strings.Contains(planText, "SEARCH e USING PRIMARY KEY") {
		t.Fatalf("branch query plan regressed: %v", plan)
	}
	if _, err := storage.db.Exec(`DELETE FROM branch_entries; DELETE FROM branch_meta;`); err != nil {
		t.Fatal(err)
	}
	if err := storage.Repair(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := session.FindEntriesOnBranch(ctx, BranchScan{Start: child}); err != nil || len(got) != 2 {
		t.Fatalf("repair did not restore branch cache: %v %+v", err, got)
	}
	if err := storage.VacuumInto(ctx, filepath.Join(t.TempDir(), "snapshot.sqlite")); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteForkCopiesFactsConfigurationAndParent(t *testing.T) {
	ctx := context.Background()
	repo := NewSQLiteSessionRepo(t.TempDir(), SQLiteStorageOptions{OwnerID: "owner-a"})
	source, err := repo.Create(ctx, SessionCreateOptions{ID: "source"})
	if err != nil {
		t.Fatal(err)
	}
	root, err := source.AppendMessage(ctx, AgentMessage{Role: "user", Content: "root"})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.SetName(ctx, stringPointer("Neo")); err != nil {
		t.Fatal(err)
	}
	if err := source.SetCustomFact(ctx, "mode", map[string]any{"safe": true}); err != nil {
		t.Fatal(err)
	}
	config := LaneConfiguration{Model: Model{Provider: "provider", ModelID: "model"}, ThinkingLevel: ThinkingLow, ActiveToolNames: []string{}}
	if err := source.(*MemorySession).SetLaneConfiguration(ctx, "main", config); err != nil {
		t.Fatal(err)
	}
	forked, err := repo.Fork(ctx, source.Metadata(), ForkOptions{EntryID: root}, SessionCreateOptions{ID: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	if forked.Metadata().ParentSessionID != "source" {
		t.Fatalf("fork parent metadata missing: %+v", forked.Metadata())
	}
	if name, _ := forked.GetName(ctx); name == nil || *name != "Neo" {
		t.Fatalf("fork lost name: %v", name)
	}
	if fact, err := forked.GetCustomFact(ctx, "mode"); err != nil || fact == nil {
		t.Fatalf("fork lost custom fact: %v %#v", err, fact)
	}
	if got, err := forked.(*MemorySession).LaneConfiguration(ctx, "main"); err != nil || got.Model.ModelID != "model" {
		t.Fatalf("fork lost lane configuration: %v %+v", err, got)
	}
	stats, err := forked.GetStats(ctx)
	if err != nil || stats.MessageCount != 1 || stats.TotalTokens != 0 {
		t.Fatalf("fork stats incorrect: %v %+v", err, stats)
	}
	if listed, err := repo.List(ctx, nil); err != nil || len(listed) != 2 {
		t.Fatalf("listing open sessions failed: %v %+v", err, listed)
	}
	if _, err := source.AppendMessage(ctx, AgentMessage{Role: "assistant", Content: "still writable"}); err != nil {
		t.Fatalf("fork invalidated source lease: %v", err)
	}
	if err := source.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := forked.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
