package harness

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestJSONLCommitReplayTornTailAndCompaction(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	generator := NewUUIDv7Generator()
	metadata := SessionMetadata{ID: generator.Next(), CreatedAt: 1700000000000, StorageVersion: CurrentStorageVersion}
	path := filepath.Join(directory, "session.jsonl")
	storage, err := CreateJSONLStorage(path, metadata, JSONLStorageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id := generator.Next()
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{
		entryWrite(id, nil, "persisted"),
		registerSet(RegisterFactCustom, "value", map[string]any{"n": 1}),
	}}); err != nil {
		t.Fatal(err)
	}
	if err := storage.Compact(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(mustReadFile(t, path), []byte(`{"kind":"entry"}`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := OpenJSONLStorage(path, JSONLStorageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := reopened.ScanEntries(ctx, EntryScan{})
	if err != nil || len(entries) != 1 || entries[0].Message.Content != "persisted" {
		t.Fatalf("replay after torn tail failed: %v %+v", err, entries)
	}
	if err := reopened.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestJSONLRejectsMalformedInteriorLine(t *testing.T) {
	directory := t.TempDir()
	generator := NewUUIDv7Generator()
	metadata := SessionMetadata{ID: generator.Next(), CreatedAt: 1700000000000, StorageVersion: CurrentStorageVersion}
	header := `{"v":4,"kind":"header","id":"` + metadata.ID + `","storageVersion":1,"createdAt":1700000000000}`
	path := filepath.Join(directory, "bad.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join([]string{header, "{bad", "{}", ""}, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenJSONLStorage(path, JSONLStorageOptions{}); err == nil {
		t.Fatal("malformed interior JSONL line was accepted")
	}
}

func TestJSONLRepositoryCreatesListsReopensAndForks(t *testing.T) {
	ctx := context.Background()
	repo := NewJSONLSessionRepo(t.TempDir(), JSONLStorageOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entryID, err := session.AppendMessage(ctx, AgentMessage{Role: "user", Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	metadata := session.Metadata()
	listed, err := repo.List(ctx, nil)
	if err != nil || len(listed) != 1 || listed[0].ID != metadata.ID {
		t.Fatalf("list failed: %v %+v", err, listed)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := repo.Open(ctx, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if entry, err := reopened.GetEntry(ctx, entryID); err != nil || entry == nil {
		t.Fatalf("reopen failed: %v %+v", err, entry)
	}
	fork, err := repo.Fork(ctx, metadata, ForkOptions{}, SessionCreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if entries, err := fork.FindEntries(ctx, EntryQuery{}); err != nil || len(entries) != 1 {
		t.Fatalf("fork failed: %v %+v", err, entries)
	}
}

func TestJSONLLegacyV3IsReadOnlyUntilFirstWriteAndThenMigrates(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "legacy.jsonl")
	legacy := strings.Join([]string{
		`{"type":"session","version":3,"id":"legacy","timestamp":"2026-01-01T00:00:00Z","cwd":"/workspace","parentSession":"/missing-parent.jsonl"}`,
		`{"type":"model_change","id":"model","parentId":null,"provider":"p","modelId":"m","timestamp":"2026-01-01T00:00:01Z"}`,
		`{"type":"message","id":"user","parentId":"model","message":{"role":"user","content":"hello"},"timestamp":"2026-01-01T00:00:02Z","usage":{"input":2,"output":3,"totalTokens":5,"cost":{"total":1.5}}}`,
		`{"type":"compaction","id":"compact","parentId":"user","summary":"summary","firstKeptEntryId":"user","tokensBefore":5,"timestamp":"2026-01-01T00:00:03Z"}`,
		`{"type":"message","id":"after","parentId":"compact","message":{"role":"assistant","content":"after","stopReason":"stop"},"timestamp":"2026-01-01T00:00:04Z"}`,
		`{"type":"session_info","id":"info","parentId":"after","name":"Legacy name","timestamp":"2026-01-01T00:00:05Z"}`,
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	before := mustReadFile(t, path)
	storage, metadata, err := OpenJSONLStorage(path, JSONLStorageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if metadata.LegacyParentSessionPath != "/missing-parent.jsonl" || string(mustReadFile(t, path)) != string(before) {
		t.Fatal("read-only legacy open changed the source file")
	}
	entries, err := storage.ScanEntries(ctx, EntryScan{Order: OldestFirst})
	if err != nil || len(entries) != 3 || !isUUIDv7(entries[0].ID) || entries[0].ParentID != nil {
		t.Fatalf("legacy normalization failed: %v %+v", err, entries)
	}
	name, err := storage.GetRegister(ctx, RegisterFactName, "")
	if err != nil || name == nil || name.Value != "Legacy name" {
		t.Fatalf("legacy fact normalization failed: %v %+v", err, name)
	}
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{entryWrite(NewUUIDv7Generator().Next(), nil, "new")}}); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(ctx); err != nil {
		t.Fatal(err)
	}
	converted := mustReadFile(t, path)
	if !strings.Contains(string(converted), `"v":4`) || !strings.Contains(string(converted), `"source":"v3-import"`) {
		t.Fatalf("first write did not publish format 4 conversion: %s", converted)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
