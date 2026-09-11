package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type SQLiteSearchOptions struct {
	Repo         SessionRepo
	Path         string
	Debounce     time.Duration
	DefaultLimit int
}

type SQLiteSearchService struct {
	db           *sql.DB
	repo         SessionRepo
	debounce     time.Duration
	defaultLimit int
	fts          bool
	mu           sync.Mutex
	notifyTimers map[string]*time.Timer
	closed       bool
}

func NewSQLiteSearchService(options SQLiteSearchOptions) (*SQLiteSearchService, error) {
	if options.Repo == nil {
		return nil, fmt.Errorf("search repository is required")
	}
	if options.Path == "" {
		return nil, fmt.Errorf("search database path is required")
	}
	if err := os.MkdirAll(filepath.Dir(options.Path), 0o755); err != nil {
		return nil, err
	}
	debounce := options.Debounce
	if debounce <= 0 {
		debounce = 100 * time.Millisecond
	}
	defaultLimit := options.DefaultLimit
	if defaultLimit <= 0 {
		defaultLimit = 100
	}
	db, err := sql.Open("sqlite3", options.Path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	service := &SQLiteSearchService{db: db, repo: options.Repo, debounce: debounce, defaultLimit: defaultLimit, notifyTimers: make(map[string]*time.Timer)}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(searchSchema); err != nil {
		db.Close()
		return nil, err
	}
	fts := true
	if _, err := db.Exec(`CREATE VIRTUAL TABLE IF NOT EXISTS search_fts USING fts5(text, content='search_documents', content_rowid='rowid')`); err != nil {
		if !strings.Contains(err.Error(), "no such module: fts5") {
			db.Close()
			return nil, err
		}
		fts = false
	} else if _, err := db.Exec(`INSERT INTO search_fts(search_fts) VALUES('rebuild')`); err != nil {
		db.Close()
		return nil, err
	}
	service.fts = fts
	return service, nil
}

const searchSchema = `
CREATE TABLE IF NOT EXISTS search_documents (
  session_id TEXT NOT NULL,
  store_generation INTEGER NOT NULL,
  entry_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  text TEXT NOT NULL,
  PRIMARY KEY(session_id, store_generation, entry_id)
);
CREATE INDEX IF NOT EXISTS ix_search_documents_session ON search_documents(session_id, store_generation, seq);
CREATE TABLE IF NOT EXISTS search_cursors (
  session_id TEXT NOT NULL,
  store_generation INTEGER NOT NULL,
  last_seq INTEGER NOT NULL,
  PRIMARY KEY(session_id, store_generation)
);
`

func (s *SQLiteSearchService) Sync(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStorageClosed
	}
	metadata, err := s.repo.List(ctx, nil)
	if err != nil {
		return err
	}
	present := make(map[string]struct{}, len(metadata))
	for _, item := range metadata {
		present[item.ID] = struct{}{}
		if err := s.syncSessionLocked(ctx, item); err != nil {
			return err
		}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT session_id FROM search_cursors`)
	if err != nil {
		return err
	}
	var stale []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		if _, ok := present[id]; !ok {
			stale = append(stale, id)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, id := range stale {
		if err := s.removeLocked(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteSearchService) syncSessionLocked(ctx context.Context, metadata SessionMetadata) error {
	var generation int64
	var lastSeq int64
	if err := s.db.QueryRowContext(ctx, `SELECT store_generation, last_seq FROM search_cursors WHERE session_id = ? ORDER BY store_generation DESC LIMIT 1`, metadata.ID).Scan(&generation, &lastSeq); err == sql.ErrNoRows {
		generation = metadata.StoreGeneration
	} else if err != nil {
		return err
	}
	if generation != metadata.StoreGeneration {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM search_documents WHERE session_id = ?`, metadata.ID); err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, `DELETE FROM search_cursors WHERE session_id = ?`, metadata.ID); err != nil {
			return err
		}
		generation, lastSeq = metadata.StoreGeneration, 0
	}
	session, err := s.openSearchSession(ctx, metadata)
	if err != nil {
		return err
	}
	defer session.Close(context.Background())
	entries, err := sessionStorageEntries(ctx, session, EntryScan{FromSeq: lastSeq + 1, Order: OldestFirst})
	if err != nil {
		return err
	}
	nextSeq := lastSeq
	tx, err := beginSearchTransaction(ctx, s.db)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	for _, entry := range entries {
		if entry.Seq > nextSeq {
			nextSeq = entry.Seq
		}
		text := searchableEntryText(entry)
		if text == "" {
			continue
		}
		var rowID int64
		if err := tx.QueryRowContext(ctx, `SELECT rowid FROM search_documents WHERE session_id = ? AND store_generation = ? AND entry_id = ?`, metadata.ID, generation, entry.ID).Scan(&rowID); err == nil {
			if s.fts {
				if _, err := tx.ExecContext(ctx, `INSERT INTO search_fts(search_fts, rowid, text) VALUES('delete', ?, '')`, rowID); err != nil {
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM search_documents WHERE rowid = ?`, rowID); err != nil {
				return err
			}
		} else if err != sql.ErrNoRows {
			return err
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO search_documents(session_id, store_generation, entry_id, seq, timestamp, text) VALUES (?, ?, ?, ?, ?, ?)`, metadata.ID, generation, entry.ID, entry.Seq, entry.Timestamp, text)
		if err != nil {
			return err
		}
		rowID, err = result.LastInsertId()
		if err != nil {
			return err
		}
		if s.fts {
			if _, err := tx.ExecContext(ctx, `INSERT INTO search_fts(rowid, text) VALUES (?, ?)`, rowID, text); err != nil {
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO search_cursors(session_id, store_generation, last_seq) VALUES (?, ?, ?) ON CONFLICT(session_id, store_generation) DO UPDATE SET last_seq = MAX(search_cursors.last_seq, excluded.last_seq)`, metadata.ID, generation, nextSeq); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func (s *SQLiteSearchService) SearchSessions(ctx context.Context, query SearchQuery) ([]SessionSearchHit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errStorageClosed
	}
	rows, err := s.searchRows(ctx, query, true)
	if err != nil {
		return nil, err
	}
	result := make([]SessionSearchHit, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, ok := seen[row.sessionID]; ok {
			continue
		}
		seen[row.sessionID] = struct{}{}
		result = append(result, SessionSearchHit{SessionID: row.sessionID, Score: row.score, Top: &SessionSearchTop{EntryID: row.entryID, Snippet: row.snippet, Timestamp: row.timestamp}})
	}
	return result, nil
}

func (s *SQLiteSearchService) SearchEntries(ctx context.Context, query SearchQuery) ([]EntrySearchHit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errStorageClosed
	}
	rows, err := s.searchRows(ctx, query, false)
	if err != nil {
		return nil, err
	}
	result := make([]EntrySearchHit, len(rows))
	for i, row := range rows {
		result[i] = EntrySearchHit{SessionID: row.sessionID, EntryID: row.entryID, Timestamp: row.timestamp, Snippet: row.snippet, Score: row.score}
	}
	return result, nil
}

type searchRow struct {
	sessionID string
	entryID   string
	timestamp int64
	snippet   string
	score     float64
}

func (s *SQLiteSearchService) searchRows(ctx context.Context, query SearchQuery, sessions bool) ([]searchRow, error) {
	if strings.TrimSpace(query.Text) == "" || query.Limit < 0 {
		return []searchRow{}, nil
	}
	limit := query.Limit
	if limit == 0 {
		limit = s.defaultLimit
	}
	match := `"` + strings.ReplaceAll(strings.TrimSpace(query.Text), `"`, `""`) + `"`
	group := ""
	if sessions {
		group = `WHERE NOT EXISTS (SELECT 1 FROM matches newer WHERE newer.session_id = matches.session_id AND (newer.score < matches.score OR (newer.score = matches.score AND newer.entry_id < matches.entry_id)))`
	}
	querySQL := fmt.Sprintf(`WITH matches AS (
SELECT d.session_id, d.entry_id, d.timestamp, bm25(search_fts) AS score,
snippet(search_fts, 0, '[', ']', '...', 12) AS snippet
FROM search_fts JOIN search_documents d ON d.rowid = search_fts.rowid
JOIN search_cursors c ON c.session_id = d.session_id AND c.store_generation = d.store_generation AND c.store_generation = (SELECT MAX(c2.store_generation) FROM search_cursors c2 WHERE c2.session_id = d.session_id)
WHERE search_fts MATCH ?
)
SELECT session_id, entry_id, timestamp, snippet, score FROM matches %s ORDER BY score, timestamp DESC LIMIT ?`, group)
	if !s.fts {
		querySQL = fmt.Sprintf(`WITH matches AS (
SELECT session_id, entry_id, timestamp, 0.0 AS score, text AS snippet
FROM search_documents WHERE text LIKE ?
AND store_generation = (SELECT MAX(c.store_generation) FROM search_cursors c WHERE c.session_id = search_documents.session_id)
)
SELECT session_id, entry_id, timestamp, snippet, score FROM matches %s ORDER BY timestamp DESC LIMIT ?`, group)
		match = "%" + strings.TrimSpace(query.Text) + "%"
	}
	rows, err := s.db.QueryContext(ctx, querySQL, match, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]searchRow, 0)
	for rows.Next() {
		var row searchRow
		if err := rows.Scan(&row.sessionID, &row.entryID, &row.timestamp, &row.snippet, &row.score); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *SQLiteSearchService) Notify(sessionID string) {
	if sessionID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if timer := s.notifyTimers[sessionID]; timer != nil {
		timer.Stop()
	}
	s.notifyTimers[sessionID] = time.AfterFunc(s.debounce, func() {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		delete(s.notifyTimers, sessionID)
		s.mu.Unlock()
		_ = s.syncSession(sessionID)
	})
}

func (s *SQLiteSearchService) syncSession(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStorageClosed
	}
	metadata, err := s.repo.List(context.Background(), nil)
	if err != nil {
		return err
	}
	for _, item := range metadata {
		if item.ID == sessionID {
			return s.syncSessionLocked(context.Background(), item)
		}
	}
	return s.removeLocked(context.Background(), sessionID)
}

func (s *SQLiteSearchService) openSearchSession(ctx context.Context, metadata SessionMetadata) (Session, error) {
	if repo, ok := s.repo.(*SQLiteSessionRepo); ok {
		storage, stored, err := openSQLiteSnapshot(repo.path(metadata.ID), repo.options)
		if err != nil {
			return nil, err
		}
		return newSession(stored, storage), nil
	}
	return s.repo.Open(ctx, metadata)
}

func (s *SQLiteSearchService) Remove(ctx context.Context, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStorageClosed
	}
	return s.removeLocked(ctx, sessionID)
}

func (s *SQLiteSearchService) removeLocked(ctx context.Context, sessionID string) error {
	if timer := s.notifyTimers[sessionID]; timer != nil {
		timer.Stop()
		delete(s.notifyTimers, sessionID)
	}
	tx, err := beginSearchTransaction(ctx, s.db)
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT rowid FROM search_documents WHERE session_id = ?`, sessionID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	var rowIDs []int64
	for rows.Next() {
		var rowID int64
		if err := rows.Scan(&rowID); err != nil {
			rows.Close()
			_ = tx.Rollback()
			return err
		}
		rowIDs = append(rowIDs, rowID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		_ = tx.Rollback()
		return err
	}
	rows.Close()
	for _, rowID := range rowIDs {
		if s.fts {
			if _, err := tx.ExecContext(ctx, `INSERT INTO search_fts(search_fts, rowid, text) VALUES('delete', ?, '')`, rowID); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM search_documents WHERE session_id = ?`, sessionID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM search_cursors WHERE session_id = ?`, sessionID); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

type searchTransaction struct {
	conn *sql.Conn
}

func beginSearchTransaction(ctx context.Context, db *sql.DB) (*searchTransaction, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		conn.Close()
		return nil, err
	}
	return &searchTransaction{conn: conn}, nil
}

func (t *searchTransaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.conn.ExecContext(ctx, query, args...)
}

func (t *searchTransaction) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.conn.QueryContext(ctx, query, args...)
}

func (t *searchTransaction) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.conn.QueryRowContext(ctx, query, args...)
}

func (t *searchTransaction) Commit() error {
	_, err := t.conn.ExecContext(context.Background(), "COMMIT")
	closeErr := t.conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (t *searchTransaction) Rollback() error {
	if t.conn == nil {
		return nil
	}
	_, err := t.conn.ExecContext(context.Background(), "ROLLBACK")
	closeErr := t.conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (s *SQLiteSearchService) Close(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	for id, timer := range s.notifyTimers {
		timer.Stop()
		delete(s.notifyTimers, id)
	}
	s.mu.Unlock()
	return s.db.Close()
}

func searchableEntryText(entry Entry) string {
	parts := make([]string, 0, 4)
	if entry.Message != nil {
		if entry.Message.Name != "" {
			parts = append(parts, entry.Message.Name)
		}
		if text := jsonText(entry.Message.Content); text != "" {
			parts = append(parts, text)
		}
	}
	if entry.Summary != "" {
		parts = append(parts, entry.Summary)
	}
	if text := jsonText(entry.Data); text != "" {
		parts = append(parts, text)
	}
	for _, message := range entry.RetainedTail {
		if text := jsonText(message.Content); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, " ")
}

func jsonText(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return strings.Trim(string(data), `"`)
}

func sessionStorageEntries(ctx context.Context, session Session, query EntryScan) ([]Entry, error) {
	cursor := query.Cursor
	if query.FromSeq > 0 {
		cursor = &EntryCursor{AfterSeq: query.FromSeq - 1}
	}
	return session.FindEntries(ctx, EntryQuery{Type: query.Type, CustomType: query.CustomType, Order: query.Order, Limit: query.Limit, Cursor: cursor})
}
