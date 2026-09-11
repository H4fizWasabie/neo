package harness

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteSearchSyncsRanksAndReconciles(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	first, err := repo.Create(ctx, SessionCreateOptions{ID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	firstEntry, err := first.AppendMessage(ctx, AgentMessage{Role: "user", Content: "authentication migration"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := repo.Create(ctx, SessionCreateOptions{ID: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.AppendMessage(ctx, AgentMessage{Role: "user", Content: "unrelated note"}); err != nil {
		t.Fatal(err)
	}
	search, err := NewSQLiteSearchService(SQLiteSearchOptions{Repo: repo, Path: filepath.Join(t.TempDir(), "search.sqlite"), Debounce: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := search.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	hits, err := search.SearchSessions(ctx, SearchQuery{Text: "authentication"})
	if err != nil || len(hits) != 1 || hits[0].SessionID != "first" || hits[0].Top.EntryID != firstEntry {
		t.Fatalf("unexpected session search: %v %+v", err, hits)
	}
	entries, err := search.SearchEntries(ctx, SearchQuery{Text: "migration"})
	if err != nil || len(entries) != 1 || entries[0].EntryID != firstEntry {
		t.Fatalf("unexpected entry search: %v %+v", err, entries)
	}
	if err := repo.Delete(ctx, second.Metadata()); err != nil {
		t.Fatal(err)
	}
	if err := search.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if hits, err := search.SearchSessions(ctx, SearchQuery{Text: "unrelated"}); err != nil || len(hits) != 0 {
		t.Fatalf("deleted session remained indexed: %v %+v", err, hits)
	}
	if err := search.Remove(ctx, "first"); err != nil {
		t.Fatal(err)
	}
	if hits, err := search.SearchSessions(ctx, SearchQuery{Text: "authentication"}); err != nil || len(hits) != 0 {
		t.Fatalf("removed session remained indexed: %v %+v", err, hits)
	}
	if err := search.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteSearchNotifyCatchesUpWithoutACommitDependency(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	search, err := NewSQLiteSearchService(SQLiteSearchOptions{Repo: repo, Path: filepath.Join(t.TempDir(), "search.sqlite"), Debounce: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := search.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := session.AppendMessage(ctx, AgentMessage{Role: "user", Content: "notify me"}); err != nil {
		t.Fatal(err)
	}
	search.Notify(session.Metadata().ID)
	time.Sleep(50 * time.Millisecond)
	hits, err := search.SearchEntries(ctx, SearchQuery{Text: "notify"})
	if err != nil || len(hits) != 1 {
		t.Fatalf("notify did not catch up: %v %+v", err, hits)
	}
	if err := search.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
