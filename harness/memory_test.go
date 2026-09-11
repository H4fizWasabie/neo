package harness

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestMemoryCommitIsAtomicAndSequencesWrites(t *testing.T) {
	ctx := context.Background()
	generator := NewUUIDv7Generator()
	parent := generator.Next(1700000000000)
	child := generator.Next(1700000000001)
	storage := NewMemoryStorage(MemoryStorageOptions{})
	bad := Transaction{Writes: []Write{
		entryWrite(parent, nil, "root"),
		entryWrite(child, stringPointer("missing"), "child"),
	}}
	if _, err := storage.Commit(ctx, bad); err == nil {
		t.Fatal("invalid transaction committed")
	}
	if entries, _ := storage.GetEntries(ctx, []string{parent}); len(entries) != 0 {
		t.Fatal("failed transaction leaked its first entry")
	}
	result, err := storage.Commit(ctx, Transaction{Writes: []Write{entryWrite(parent, nil, "root"), entryWrite(child, stringPointer(parent), "child")}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Seqs, []int64{1, 2}) || result.FirstSeq != 1 {
		t.Fatalf("unexpected sequence allocation: %+v", result)
	}
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{entryWrite(parent, nil, "again")}}); err == nil {
		t.Fatal("duplicate immutable id was accepted")
	}
}

func TestMemoryRegistersStatsAndImmutableReads(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage(MemoryStorageOptions{})
	generator := NewUUIDv7Generator()
	id := generator.Next()
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{
		entryWrite(id, nil, "hello"),
		{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: generator.Next(), Usage: Usage{Input: 2, Output: 3, Total: 5, Cost: &Cost{Total: 1.25}}, EntryID: stringPointer(id)}}},
		registerSet(RegisterFactCustom, "nullable", nil),
	}}); err != nil {
		t.Fatal(err)
	}
	register, err := storage.GetRegister(ctx, RegisterFactCustom, "nullable")
	if err != nil || register == nil || register.Value != nil {
		t.Fatalf("JSON null register was not preserved: %#v %v", register, err)
	}
	entry, _ := storage.GetEntries(ctx, []string{id})
	entry[id] = Entry{}
	if got, _ := storage.GetEntries(ctx, []string{id}); got[id].Message == nil || got[id].Message.Content != "hello" {
		t.Fatal("returned entry mutated stored state")
	}
	stats, _ := storage.GetStats(ctx)
	if stats.MessageCount != 1 || stats.TotalTokens != 5 || stats.CostTotal != 1.25 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{{Kind: WriteRegister, Register: &RegisterWrite{Operation: RegisterDelete, Namespace: RegisterFactCustom, Key: "unset"}}}}); err != nil {
		t.Fatal(err)
	}
}

func TestMemorySessionTreeFactsBranchesContextAndFork(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	sessionValue, err := repo.Create(ctx, SessionCreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root, err := sessionValue.AppendMessage(ctx, AgentMessage{Role: "user", Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	assistant, err := sessionValue.AppendMessage(ctx, AgentMessage{Role: "assistant", Content: "done", StopReason: StopReasonStop})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessionValue.AppendCustomEntry(ctx, "state", map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	name := "Neo"
	if err := sessionValue.SetName(ctx, &name); err != nil {
		t.Fatal(err)
	}
	if err := sessionValue.SetCustomFact(ctx, "json-null", nil); err != nil {
		t.Fatal(err)
	}
	if err := sessionValue.DeleteCustomFact(ctx, "json-null"); err != nil {
		t.Fatal(err)
	}
	entries, err := sessionValue.FindEntriesOnBranch(ctx, BranchScan{Order: NewestFirst})
	if err != nil || len(entries) != 3 || entries[0].ID == root || entries[1].ID != assistant {
		t.Fatalf("unexpected branch: %v %+v", err, entries)
	}
	projected, err := ProjectContext(ctx, entries, nil)
	if err != nil || len(projected) != 2 || projected[0].Content != "hello" {
		t.Fatalf("unexpected context projection: %v %+v", err, projected)
	}
	view := sessionValue.View("other")
	if _, err := view.AppendMessage(ctx, AgentMessage{Role: "user", Content: "no"}); err == nil {
		t.Fatal("unknown lane accepted an append")
	}
	forked, err := repo.Fork(ctx, sessionValue.Metadata(), ForkOptions{}, SessionCreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	forkedEntries, _ := forked.FindEntries(ctx, EntryQuery{})
	if len(forkedEntries) != 3 {
		t.Fatalf("fork lost entries: %d", len(forkedEntries))
	}
	forkStats, _ := forked.GetStats(ctx)
	if forkStats.TotalTokens != 0 || forkStats.MessageCount != 2 {
		t.Fatalf("fork ledger/stats incorrect: %+v", forkStats)
	}
}

func TestMemoryLaneDivergenceQueriesStopsAndCustomProjection(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	sessionValue, err := repo.Create(ctx, SessionCreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root, err := sessionValue.AppendMessage(ctx, AgentMessage{Role: "user", Content: "root"})
	if err != nil {
		t.Fatal(err)
	}
	config := LaneConfiguration{Model: Model{Provider: "p", ModelID: "m"}, ThinkingLevel: ThinkingLow, ActiveToolNames: []string{}}
	concrete := sessionValue.(*MemorySession)
	if err := concrete.CreateLane(ctx, "branch", stringPointer(root), config); err != nil {
		t.Fatal(err)
	}
	if _, err := concrete.View("branch").AppendMessage(ctx, AgentMessage{Role: "user", Content: "branch"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessionValue.AppendMessage(ctx, AgentMessage{Role: "user", Content: "main"}); err != nil {
		t.Fatal(err)
	}
	mainEntries, _ := sessionValue.FindEntriesOnBranch(ctx, BranchScan{Order: NewestFirst, Limit: 1})
	branchEntries, _ := concrete.View("branch").FindEntriesOnBranch(ctx, BranchScan{Order: NewestFirst})
	if len(mainEntries) != 1 || mainEntries[0].Message.Content != "main" || len(branchEntries) != 2 || branchEntries[0].Message.Content != "branch" {
		t.Fatalf("lanes did not diverge: main=%+v branch=%+v", mainEntries, branchEntries)
	}
	stopped, err := concrete.View("branch").FindEntriesOnBranch(ctx, BranchScan{StopAtID: root, Order: NewestFirst})
	if err != nil || len(stopped) != 2 {
		t.Fatalf("inclusive stop failed: %v %+v", err, stopped)
	}
	cursor := &EntryCursor{AfterSeq: branchEntries[0].Seq}
	older, err := concrete.View("branch").FindEntriesOnBranch(ctx, BranchScan{Cursor: cursor, Order: NewestFirst})
	if err != nil || len(older) != 1 || older[0].Message.Content != "root" {
		t.Fatalf("exclusive cursor failed: %v %+v", err, older)
	}
	customID, err := concrete.View("branch").AppendCustomEntry(ctx, "projected", map[string]any{"value": 7})
	if err != nil {
		t.Fatal(err)
	}
	customEntries, _ := concrete.View("branch").FindEntriesOnBranch(ctx, BranchScan{Order: NewestFirst})
	projected, err := ProjectContext(ctx, customEntries, map[string]EntryProjector{
		"projected": func(context.Context, Entry) ([]AgentMessage, error) {
			return []AgentMessage{{Role: "system", Content: "projected"}}, nil
		},
	})
	if err != nil || len(projected) != 3 || projected[0].Content != "root" && projected[0].Content != "projected" {
		t.Fatalf("custom projection failed: %v %+v (custom=%s)", err, projected, customID)
	}
	treeFork, err := repo.Fork(ctx, sessionValue.Metadata(), ForkOptions{Scope: "tree"}, SessionCreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	treeForked := treeFork.(*MemorySession)
	if _, err := treeForked.LaneConfiguration(ctx, "branch"); err != nil {
		t.Fatalf("tree fork lost lane configuration: %v", err)
	}
	branchLeaf, err := treeForked.View("branch").GetLeafID(ctx)
	if err != nil || branchLeaf == nil {
		t.Fatalf("tree fork lost lane leaf: %v %v", err, branchLeaf)
	}
}

func TestMemoryCloseSealsInstanceAndRepoReopensState(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	sessionValue, err := repo.Create(ctx, SessionCreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessionValue.AppendMessage(ctx, AgentMessage{Role: "user", Content: "survives"}); err != nil {
		t.Fatal(err)
	}
	metadata := sessionValue.Metadata()
	if err := sessionValue.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := sessionValue.FindEntries(ctx, EntryQuery{}); err == nil {
		t.Fatal("closed session accepted a read")
	}
	reopened, err := repo.Open(ctx, metadata)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := reopened.FindEntries(ctx, EntryQuery{})
	if err != nil || len(entries) != 1 || entries[0].Message.Content != "survives" {
		t.Fatalf("reopen lost committed state: %v %+v", err, entries)
	}
}

func TestMemoryCodecRejectsUnknownRolesAndSupportsRegisteredRoles(t *testing.T) {
	ctx := context.Background()
	storage := NewMemoryStorage(MemoryStorageOptions{Codec: SessionCodecOptions{CustomMessageSchemas: map[string]MessageSchema{
		"notice": func(message AgentMessage) error {
			if message.Content != "ok" {
				return errors.New("content must be ok")
			}
			return nil
		},
	}}})
	generator := NewUUIDv7Generator()
	unknown := entryWrite(generator.Next(), nil, "x")
	unknown.Entry.Entry.Message.Role = "alien"
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{unknown}}); err == nil || !strings.Contains(err.Error(), "unknown custom role") {
		t.Fatalf("unknown role was accepted: %v", err)
	}
	known := entryWrite(generator.Next(), nil, "ok")
	known.Entry.Entry.Message.Role = "notice"
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{known}}); err != nil {
		t.Fatal(err)
	}
}

func TestUUIDv7FollowerPreservesTimestampPrefix(t *testing.T) {
	generator := NewUUIDv7Generator()
	leader := generator.Next(1700000000000)
	follower := generator.Next(1700000000000)
	if !isUUIDv7(leader) || !isUUIDv7(follower) || leader[:15] != follower[:15] {
		t.Fatalf("follower did not preserve UUIDv7 time prefix: %s %s", leader, follower)
	}
}

func entryWrite(id string, parent *string, content string) Write {
	return Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: Entry{EntryBase: EntryBase{ID: id, ParentID: parent, Type: EntryMessage}, Message: &AgentMessage{Role: "user", Content: content}}}}
}
