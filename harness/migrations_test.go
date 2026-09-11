package harness

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMemoryOpenMigratesVersionAndPreservesOpenOperation(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: &scriptedModels{}, Model: Model{Provider: "provider", ModelID: "model"}})
	if err != nil {
		t.Fatal(err)
	}
	defer harness.Close(ctx)
	opID := seedRunOperation(t, session, RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationNeedAssistant}}}, nil)
	repo.mu.Lock()
	metadata := repo.metadata[t.Name()]
	metadata.StorageVersion = 1
	repo.metadata[t.Name()] = metadata
	repo.mu.Unlock()
	opened, err := repo.Open(ctx, session.Metadata())
	if err != nil || opened.Metadata().StorageVersion != CurrentStorageVersion {
		t.Fatalf("memory migration = %v %+v", err, opened.Metadata())
	}
	restored, err := Restore(ctx, opened, "main")
	if err != nil || restored.Current == nil || restored.Current.Operation.OperationID != opID {
		t.Fatalf("open operation was not preserved = %v %+v", err, restored)
	}
	metadata.StorageVersion = CurrentStorageVersion + 1
	repo.mu.Lock()
	repo.metadata[t.Name()] = metadata
	repo.mu.Unlock()
	if _, err := repo.Open(ctx, session.Metadata()); err == nil {
		t.Fatal("newer memory session was accepted")
	}
}

func TestJSONLOpenMigratesAndCompactsOldVersion(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "session.jsonl")
	metadata := SessionMetadata{ID: "session", CreatedAt: 1, StorageVersion: 1}
	storage, err := CreateJSONLStorage(path, metadata, JSONLStorageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterLaneLeaf, "main", (*string)(nil))}}); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(ctx); err != nil {
		t.Fatal(err)
	}
	opened, migrated, err := OpenJSONLStorage(path, JSONLStorageOptions{})
	if err != nil || migrated.StorageVersion != CurrentStorageVersion {
		t.Fatalf("JSONL migration = %v %+v", err, migrated)
	}
	if err := opened.Close(ctx); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), `"storageVersion":2`) || strings.Count(string(data), "\n") != 2 {
		t.Fatalf("JSONL migration did not compact = %v %s", err, data)
	}
	newer := filepath.Join(directory, "newer.jsonl")
	if err := os.WriteFile(newer, []byte(`{"v":4,"kind":"header","id":"newer","storageVersion":3,"createdAt":1}`+"\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenJSONLStorage(newer, JSONLStorageOptions{}); err == nil {
		t.Fatal("newer JSONL session was accepted")
	}
}

func TestSQLiteOpenMigratesUnderLease(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "session.sqlite")
	storage, err := CreateSQLiteStorage(path, SessionMetadata{ID: "session", CreatedAt: 1, StorageVersion: CurrentStorageVersion}, SQLiteStorageOptions{OwnerID: "migration-test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE session SET storage_version = 1`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opened, metadata, err := OpenSQLiteStorage(path, SQLiteStorageOptions{OwnerID: "migration-test"})
	if err != nil || metadata.StorageVersion != CurrentStorageVersion {
		t.Fatalf("SQLite migration = %v %+v", err, metadata)
	}
	if err := opened.Close(ctx); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE session SET storage_version = 3`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if _, _, err := OpenSQLiteStorage(path, SQLiteStorageOptions{OwnerID: "migration-test"}); err == nil {
		t.Fatal("newer SQLite session was accepted")
	}
}
