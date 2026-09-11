package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteStorageOptions struct {
	Codec    SessionCodecOptions
	Now      func() time.Time
	OwnerID  string
	LeaseTTL time.Duration
}

type SQLiteStorage struct {
	db      *sql.DB
	path    string
	session SessionMetadata
	memory  *MemoryStorage
	codec   SessionCodec
	now     func() time.Time
	ownerID string
	lease   int64
	ttl     time.Duration
	leased  bool
	mu      sync.Mutex
	closed  bool
}

func CreateSQLiteStorage(path string, metadata SessionMetadata, options SQLiteStorageOptions) (*SQLiteStorage, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	storage, _, err := openSQLiteStorage(path, metadata, options, true, true)
	if err != nil {
		return nil, err
	}
	return storage, nil
}

func OpenSQLiteStorage(path string, options SQLiteStorageOptions) (*SQLiteStorage, SessionMetadata, error) {
	storage, metadata, err := openSQLiteStorage(path, SessionMetadata{}, options, false, true)
	return storage, metadata, err
}

func openSQLiteSnapshot(path string, options SQLiteStorageOptions) (*SQLiteStorage, SessionMetadata, error) {
	return openSQLiteStorage(path, SessionMetadata{}, options, false, false)
}

func openSQLiteStorage(path string, metadata SessionMetadata, options SQLiteStorageOptions, create, claimLease bool) (*SQLiteStorage, SessionMetadata, error) {
	if !create {
		if _, err := os.Stat(path); err != nil {
			return nil, SessionMetadata{}, err
		}
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	owner := options.OwnerID
	if owner == "" {
		owner = NewUUIDv7Generator().Next()
	}
	ttl := options.LeaseTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, SessionMetadata{}, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;`); err != nil {
		db.Close()
		return nil, SessionMetadata{}, err
	}
	if _, err := db.Exec(sqliteSchema); err != nil {
		db.Close()
		return nil, SessionMetadata{}, err
	}
	if create {
		if metadata.StorageVersion == 0 {
			metadata.StorageVersion = CurrentStorageVersion
		}
		if _, err := db.Exec(`INSERT INTO session (id, created_at, storage_version, cwd, parent_session_id, next_seq, message_count, cached_tokens, uncached_tokens, total_tokens, cost_total) VALUES (?, ?, ?, ?, ?, 1, 0, 0, 0, 0, 0)`, metadata.ID, metadata.CreatedAt, metadata.StorageVersion, metadata.CWD, nullString(metadata.ParentSessionID)); err != nil {
			db.Close()
			return nil, SessionMetadata{}, err
		}
	} else {
		row := db.QueryRow(`SELECT id, created_at, storage_version, cwd, parent_session_id FROM session LIMIT 1`)
		var parent sql.NullString
		if err := row.Scan(&metadata.ID, &metadata.CreatedAt, &metadata.StorageVersion, &metadata.CWD, &parent); err != nil {
			db.Close()
			return nil, SessionMetadata{}, err
		}
		if parent.Valid {
			metadata.ParentSessionID = parent.String
		}
		if metadata.StorageVersion > CurrentStorageVersion {
			db.Close()
			return nil, SessionMetadata{}, fmt.Errorf("session storage version %d is newer than binary version %d", metadata.StorageVersion, CurrentStorageVersion)
		}
	}
	memory := NewMemoryStorage(MemoryStorageOptions{Codec: options.Codec, Now: now})
	storage := &SQLiteStorage{db: db, path: path, session: metadata, memory: memory, codec: NewSessionCodec(options.Codec), now: now, ownerID: owner, ttl: ttl, leased: claimLease}
	if claimLease {
		if err := storage.acquireLease(); err != nil {
			db.Close()
			return nil, SessionMetadata{}, err
		}
	}
	if !create {
		if err := storage.loadShadow(); err != nil {
			storage.Close(context.Background())
			return nil, SessionMetadata{}, err
		}
	}
	return storage, metadata, nil
}

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS session (id TEXT PRIMARY KEY, created_at INTEGER NOT NULL, storage_version INTEGER NOT NULL, cwd TEXT NOT NULL DEFAULT '', parent_session_id TEXT, next_seq INTEGER NOT NULL, message_count INTEGER NOT NULL, cached_tokens INTEGER NOT NULL, uncached_tokens INTEGER NOT NULL, total_tokens INTEGER NOT NULL, cost_total REAL NOT NULL);
CREATE TABLE IF NOT EXISTS entries (id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE, parent_id TEXT, type TEXT NOT NULL, custom_type TEXT, timestamp INTEGER NOT NULL, payload TEXT NOT NULL) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_entries_parent ON entries(parent_id);
CREATE INDEX IF NOT EXISTS ix_entries_seq ON entries(seq, type);
CREATE TABLE IF NOT EXISTS registers (namespace TEXT NOT NULL, key TEXT NOT NULL, seq INTEGER NOT NULL, value TEXT NOT NULL, PRIMARY KEY(namespace, key));
CREATE TABLE IF NOT EXISTS usage_ledger (id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE, entry_id TEXT, adjustment INTEGER NOT NULL, usage TEXT NOT NULL, details TEXT) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_usage_seq ON usage_ledger(seq);
CREATE TABLE IF NOT EXISTS branch_entries (branch_id TEXT NOT NULL, entry_id TEXT NOT NULL, entry_seq INTEGER NOT NULL, entry_type TEXT NOT NULL, custom_type TEXT, PRIMARY KEY(branch_id, entry_id)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_branch_seq ON branch_entries(branch_id, entry_seq, entry_id, entry_type);
CREATE INDEX IF NOT EXISTS ix_branch_type ON branch_entries(branch_id, entry_type, entry_seq, entry_id);
CREATE INDEX IF NOT EXISTS ix_branch_entry ON branch_entries(entry_id);
CREATE TABLE IF NOT EXISTS branch_meta (branch_id TEXT PRIMARY KEY, tip_entry_id TEXT NOT NULL, tip_seq INTEGER NOT NULL, base_branch_id TEXT, base_seq INTEGER NOT NULL DEFAULT 0);
CREATE UNIQUE INDEX IF NOT EXISTS ix_branch_tip ON branch_meta(tip_entry_id);
CREATE TABLE IF NOT EXISTS writer_lease (owner_id TEXT NOT NULL, fence INTEGER NOT NULL, expires_at_ms INTEGER NOT NULL);
`

func (s *SQLiteStorage) acquireLease() error {
	ctx := context.Background()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	defer conn.ExecContext(ctx, "ROLLBACK")
	var owner string
	var fence, expires int64
	err = conn.QueryRowContext(ctx, `SELECT owner_id, fence, expires_at_ms FROM writer_lease LIMIT 1`).Scan(&owner, &fence, &expires)
	now := s.now().UnixMilli()
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == nil && expires > now && owner != s.ownerID {
		return fmt.Errorf("session writer lease is held by another owner")
	}
	if err == sql.ErrNoRows {
		fence = 0
	}
	fence++
	if _, err := conn.ExecContext(ctx, `DELETE FROM writer_lease`); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO writer_lease(owner_id, fence, expires_at_ms) VALUES (?, ?, ?)`, s.ownerID, fence, now+s.ttl.Milliseconds()); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	s.lease = fence
	return nil
}

func (s *SQLiteStorage) Commit(ctx context.Context, tx Transaction) (CommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return CommitResult{}, errStorageClosed
	}
	if !s.leased {
		return CommitResult{}, fmt.Errorf("SQLite snapshot is read-only")
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return CommitResult{}, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return CommitResult{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	result, err := s.memory.commitWithPublish(ctx, tx, func(result CommitResult) error {
		if err := s.applySQL(ctx, conn, tx, result); err != nil {
			return err
		}
		now := s.now().UnixMilli()
		updated, err := conn.ExecContext(ctx, `UPDATE writer_lease SET expires_at_ms = ? WHERE owner_id = ? AND fence = ? AND expires_at_ms > ?`, now+s.ttl.Milliseconds(), s.ownerID, s.lease, now)
		if err != nil {
			return err
		}
		count, err := updated.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("session writer lease was lost")
		}
		return nil
	})
	if err != nil {
		return CommitResult{}, err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return CommitResult{}, err
	}
	committed = true
	return result, nil
}

func (s *SQLiteStorage) applySQL(ctx context.Context, conn *sql.Conn, tx Transaction, result CommitResult) error {
	messageDelta := int64(0)
	var usageDelta SessionStats
	previousLeaves := make(map[string]*string)
	for _, write := range tx.Writes {
		if write.Kind != WriteRegister || write.Register.Operation != RegisterSet || write.Register.Namespace != RegisterLaneLeaf {
			continue
		}
		var value string
		err := conn.QueryRowContext(ctx, `SELECT value FROM registers WHERE namespace = ? AND key = ?`, RegisterLaneLeaf, write.Register.Key).Scan(&value)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return err
		}
		var leaf *string
		if err := json.Unmarshal([]byte(value), &leaf); err != nil {
			return err
		}
		previousLeaves[write.Register.Key] = leaf
	}
	for i, write := range tx.Writes {
		seq := result.Seqs[i]
		switch write.Kind {
		case WriteEntry:
			entry := cloneEntry(write.Entry.Entry)
			entry.Seq, entry.Timestamp = seq, result.Timestamp
			payload, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, `INSERT INTO entries(id, seq, parent_id, type, custom_type, timestamp, payload) VALUES (?, ?, ?, ?, ?, ?, ?)`, entry.ID, seq, nullableString(entry.ParentID), entry.Type, entry.CustomType, entry.Timestamp, payload); err != nil {
				return err
			}
			if entry.Type == EntryMessage {
				messageDelta++
			}
		case WriteUsage:
			row := cloneUsageRow(write.Usage.Row)
			row.Seq = seq
			usage, err := json.Marshal(row.Usage)
			if err != nil {
				return err
			}
			details, err := json.Marshal(row.Details)
			if err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, `INSERT INTO usage_ledger(id, seq, entry_id, adjustment, usage, details) VALUES (?, ?, ?, ?, ?, ?)`, row.ID, seq, nullableString(row.EntryID), boolInt(row.Adjustment), usage, details); err != nil {
				return err
			}
			addUsage(&usageDelta, row.Usage)
		case WriteRegister:
			rw := write.Register
			if rw.Operation == RegisterDelete {
				if _, err := conn.ExecContext(ctx, `DELETE FROM registers WHERE namespace = ? AND key = ?`, rw.Namespace, rw.Key); err != nil {
					return err
				}
			} else {
				value, err := json.Marshal(rw.Value)
				if err != nil {
					return err
				}
				if _, err := conn.ExecContext(ctx, `INSERT INTO registers(namespace, key, seq, value) VALUES (?, ?, ?, ?) ON CONFLICT(namespace, key) DO UPDATE SET seq = excluded.seq, value = excluded.value`, rw.Namespace, rw.Key, seq, value); err != nil {
					return err
				}
			}
		}
	}
	if len(result.Seqs) > 0 {
		if _, err := conn.ExecContext(ctx, `UPDATE session SET next_seq = ?, message_count = message_count + ?, cached_tokens = cached_tokens + ?, uncached_tokens = uncached_tokens + ?, total_tokens = total_tokens + ?, cost_total = cost_total + ?`, result.Seqs[len(result.Seqs)-1]+1, messageDelta, usageDelta.CachedTokens, usageDelta.UncachedTokens, usageDelta.TotalTokens, usageDelta.CostTotal); err != nil {
			return err
		}
	}
	return s.rebuildBranches(ctx, conn, tx, previousLeaves)
}

func (s *SQLiteStorage) rebuildBranches(ctx context.Context, conn *sql.Conn, tx Transaction, previousLeaves map[string]*string) error {
	for _, write := range tx.Writes {
		if write.Kind != WriteRegister || write.Register.Operation != RegisterSet || write.Register.Namespace != RegisterLaneLeaf {
			continue
		}
		leaf, _ := write.Register.Value.(*string)
		if leaf == nil {
			continue
		}
		var exists int
		err := conn.QueryRowContext(ctx, `SELECT 1 FROM branch_meta WHERE branch_id = ?`, *leaf).Scan(&exists)
		if err == nil {
			continue
		}
		if err != sql.ErrNoRows {
			return err
		}
		previous := previousLeaves[write.Register.Key]
		if previous != nil {
			var parent *string
			var seq int64
			if err := conn.QueryRowContext(ctx, `SELECT parent_id, seq FROM entries WHERE id = ?`, *leaf).Scan(&parent, &seq); err != nil {
				return err
			}
			if parent != nil && *parent == *previous {
				var baseSeq int64
				if err := conn.QueryRowContext(ctx, `SELECT seq FROM entries WHERE id = ?`, *previous).Scan(&baseSeq); err != nil {
					return err
				}
				if err := insertBranchSegment(ctx, conn, *leaf, *leaf, seq, previous, baseSeq); err != nil {
					return err
				}
				continue
			}
		}
		if err := insertFullBranch(ctx, conn, *leaf); err != nil {
			return err
		}
	}
	return nil
}

func insertBranchSegment(ctx context.Context, conn *sql.Conn, branchID, tipID string, tipSeq int64, baseID *string, baseSeq int64) error {
	if _, err := conn.ExecContext(ctx, `INSERT INTO branch_meta(branch_id, tip_entry_id, tip_seq, base_branch_id, base_seq) VALUES (?, ?, ?, ?, ?)`, branchID, tipID, tipSeq, nullableString(baseID), baseSeq); err != nil {
		return err
	}
	_, err := conn.ExecContext(ctx, `INSERT INTO branch_entries(branch_id, entry_id, entry_seq, entry_type, custom_type) SELECT ?, id, seq, type, custom_type FROM entries WHERE id = ?`, branchID, tipID)
	return err
}

func insertFullBranch(ctx context.Context, conn *sql.Conn, leaf string) error {
	if _, err := conn.ExecContext(ctx, `DELETE FROM branch_entries WHERE branch_id = ?`, leaf); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `DELETE FROM branch_meta WHERE branch_id = ?`, leaf); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO branch_meta(branch_id, tip_entry_id, tip_seq, base_branch_id, base_seq) SELECT id, id, seq, NULL, 0 FROM entries WHERE id = ?`, leaf); err != nil {
		return err
	}
	current := leaf
	for current != "" {
		if _, err := conn.ExecContext(ctx, `INSERT INTO branch_entries(branch_id, entry_id, entry_seq, entry_type, custom_type) SELECT ?, id, seq, type, custom_type FROM entries WHERE id = ?`, leaf, current); err != nil {
			return err
		}
		var parent *string
		if err := conn.QueryRowContext(ctx, `SELECT parent_id FROM entries WHERE id = ?`, current).Scan(&parent); err != nil {
			return err
		}
		if parent == nil {
			break
		}
		current = *parent
	}
	return nil
}

func (s *SQLiteStorage) loadShadow() error {
	rows, err := s.db.Query(`SELECT id, seq, parent_id, type, custom_type, timestamp, payload FROM entries ORDER BY seq`)
	if err != nil {
		return err
	}
	records := make([]jsonlRecord, 0)
	for rows.Next() {
		var entry Entry
		var id, typ, payload string
		var parent, custom sql.NullString
		var seq, timestamp int64
		if err := rows.Scan(&id, &seq, &parent, &typ, &custom, &timestamp, &payload); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal([]byte(payload), &entry); err != nil {
			rows.Close()
			return err
		}
		records = append(records, recordForEntry(entry))
	}
	rows.Close()
	usageRows, err := s.db.Query(`SELECT id, seq, entry_id, adjustment, usage, details FROM usage_ledger ORDER BY seq`)
	if err != nil {
		return err
	}
	for usageRows.Next() {
		var id, usageJSON string
		var seq int64
		var entryID sql.NullString
		var adjustment int
		var details sql.NullString
		if err := usageRows.Scan(&id, &seq, &entryID, &adjustment, &usageJSON, &details); err != nil {
			usageRows.Close()
			return err
		}
		var usage Usage
		if err := json.Unmarshal([]byte(usageJSON), &usage); err != nil {
			usageRows.Close()
			return err
		}
		var value JSONValue
		if details.Valid {
			_ = json.Unmarshal([]byte(details.String), &value)
		}
		var pointer *string
		if entryID.Valid {
			pointer = &entryID.String
		}
		records = append(records, jsonlRecord{Kind: string(WriteUsage), ID: id, Seq: seq, Usage: &usage, EntryID: pointer, Adjustment: adjustment != 0, Details: value})
	}
	usageRows.Close()
	registerRows, err := s.db.Query(`SELECT namespace, key, seq, value FROM registers ORDER BY seq`)
	if err != nil {
		return err
	}
	for registerRows.Next() {
		var namespace RegisterNamespace
		var key, valueJSON string
		var seq int64
		if err := registerRows.Scan(&namespace, &key, &seq, &valueJSON); err != nil {
			registerRows.Close()
			return err
		}
		var raw JSONValue
		if err := json.Unmarshal([]byte(valueJSON), &raw); err != nil {
			registerRows.Close()
			return err
		}
		records = append(records, jsonlRecord{Kind: string(WriteRegister), Namespace: namespace, Key: key, Seq: seq, Op: RegisterSet, Value: raw})
	}
	registerRows.Close()
	sort.Slice(records, func(i, j int) bool { return records[i].Seq < records[j].Seq })
	return s.memoryReplay(records)
}

func (s *SQLiteStorage) memoryReplay(records []jsonlRecord) error {
	// SQLite register values are decoded by replay through the same typed codec
	// path used by JSONL; convert their generic JSON values before applying.
	for i := range records {
		if records[i].Kind == string(WriteRegister) {
			raw, err := json.Marshal(records[i].Value)
			if err != nil {
				return err
			}
			value, err := decodeRegisterValue(records[i].Namespace, raw)
			if err != nil {
				return err
			}
			records[i].Value = value
		}
	}
	return (&JSONLStorage{memory: s.memory}).replay(records)
}

func (s *SQLiteStorage) GetEntries(ctx context.Context, ids []string) (map[string]Entry, error) {
	return s.memory.GetEntries(ctx, ids)
}
func (s *SQLiteStorage) GetRegister(ctx context.Context, namespace RegisterNamespace, key string) (*Register, error) {
	return s.memory.GetRegister(ctx, namespace, key)
}
func (s *SQLiteStorage) ListRegisters(ctx context.Context, namespace RegisterNamespace, prefix string) ([]Register, error) {
	return s.memory.ListRegisters(ctx, namespace, prefix)
}
func (s *SQLiteStorage) ScanEntries(ctx context.Context, query EntryScan) ([]Entry, error) {
	return s.memory.ScanEntries(ctx, query)
}
func (s *SQLiteStorage) ScanUsage(ctx context.Context, query UsageScan) ([]UsageRow, error) {
	return s.memory.ScanUsage(ctx, query)
}
func (s *SQLiteStorage) GetStats(ctx context.Context) (SessionStats, error) {
	return s.memory.GetStats(ctx)
}

func (s *SQLiteStorage) ScanBranch(ctx context.Context, query BranchScan) ([]Entry, error) {
	if query.Start == "" {
		return nil, fmt.Errorf("branch scan start is required")
	}
	entries := make([]Entry, 0)
	var upper int64
	if err := s.db.QueryRowContext(ctx, `SELECT seq FROM entries WHERE id = ?`, query.Start).Scan(&upper); err != nil {
		return nil, err
	}
	branchID := query.Start
	var branchExists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM branch_meta WHERE branch_id = ?`, branchID).Scan(&branchExists); err == sql.ErrNoRows {
		if err := s.db.QueryRowContext(ctx, `SELECT branch_id FROM branch_entries WHERE entry_id = ? LIMIT 1`, query.Start).Scan(&branchID); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	for branchID != "" {
		var base sql.NullString
		var baseSeq int64
		err := s.db.QueryRowContext(ctx, `SELECT base_branch_id, base_seq FROM branch_meta WHERE branch_id = ?`, branchID).Scan(&base, &baseSeq)
		if err == sql.ErrNoRows {
			base = sql.NullString{}
			baseSeq = 0
		} else if err != nil {
			return nil, err
		}
		segmentUpper := upper
		rows, err := s.db.QueryContext(ctx, `SELECT e.id, e.parent_id, e.payload FROM branch_entries b CROSS JOIN entries e ON e.id = b.entry_id WHERE b.branch_id = ? AND b.entry_seq > ? AND b.entry_seq <= ? ORDER BY b.entry_seq DESC`, branchID, int64(0), segmentUpper)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, payload string
			var parent sql.NullString
			if err := rows.Scan(&id, &parent, &payload); err != nil {
				rows.Close()
				return nil, err
			}
			var entry Entry
			if err := json.Unmarshal([]byte(payload), &entry); err != nil {
				rows.Close()
				return nil, err
			}
			if entry.ID != id || !sameStringPointer(entry.ParentID, nullablePointer(parent)) {
				rows.Close()
				return nil, sessionError(SessionInvalidEntry, fmt.Errorf("branch cache entry %q disagrees with canonical parent data", id))
			}
			entries = append(entries, entry)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
		if !base.Valid {
			break
		}
		if baseSeq > 0 && baseSeq < upper {
			upper = baseSeq
		}
		branchID = base.String
	}
	if err := validateBranchPath(entries, query.Start); err != nil {
		return nil, err
	}
	return filterBranchEntries(entries, query), nil
}

func nullablePointer(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}

func sameStringPointer(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func validateBranchPath(entries []Entry, start string) error {
	if len(entries) == 0 || entries[0].ID != start {
		return sessionError(SessionInvalidEntry, fmt.Errorf("branch cache has no complete path for %s", start))
	}
	seen := make(map[string]struct{}, len(entries))
	for i, entry := range entries {
		if _, ok := seen[entry.ID]; ok {
			return sessionError(SessionInvalidEntry, fmt.Errorf("branch cache repeats entry %s", entry.ID))
		}
		seen[entry.ID] = struct{}{}
		if i+1 < len(entries) {
			if entry.ParentID == nil || *entry.ParentID != entries[i+1].ID {
				return sessionError(SessionInvalidEntry, fmt.Errorf("branch cache has a broken parent chain at %s", entry.ID))
			}
		} else if entry.ParentID != nil {
			return sessionError(SessionInvalidEntry, fmt.Errorf("branch cache path does not reach the root at %s", entry.ID))
		}
	}
	return nil
}

// Repair rebuilds only SQLite's disposable branch cache from current lane leaves.
func (s *SQLiteStorage) Repair(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStorageClosed
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	if _, err := conn.ExecContext(ctx, `DELETE FROM branch_entries`); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `DELETE FROM branch_meta`); err != nil {
		return err
	}
	rows, err := conn.QueryContext(ctx, `SELECT value FROM registers WHERE namespace = ? AND key LIKE '%'`, RegisterLaneLeaf)
	if err != nil {
		return err
	}
	leaves := make([]string, 0)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			rows.Close()
			return err
		}
		var leaf *string
		if err := json.Unmarshal([]byte(value), &leaf); err != nil {
			rows.Close()
			return err
		}
		if leaf != nil {
			leaves = append(leaves, *leaf)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, leaf := range leaves {
		if err := insertFullBranch(ctx, conn, leaf); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}

// VacuumInto writes a coherent SQLite snapshot to a new path for administrative
// rewrite/fork tooling. The destination must not already exist.
func (s *SQLiteStorage) VacuumInto(ctx context.Context, destination string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStorageClosed
	}
	if destination == "" || filepath.Clean(destination) == filepath.Clean(s.path) {
		return fmt.Errorf("vacuum destination must be a different path")
	}
	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("vacuum destination already exists: %s", destination)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `VACUUM INTO ?`, destination)
	return err
}

func (s *SQLiteStorage) ScanBranchStructure(ctx context.Context, query BranchScan) ([]EntryStructure, error) {
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

func filterBranchEntries(entries []Entry, query BranchScan) []Entry {
	end := len(entries)
	for i, entry := range entries {
		if query.StopAtID != "" && entry.ID == query.StopAtID || query.StopAtType != nil && entry.Type == *query.StopAtType {
			end = i + 1
			break
		}
	}
	entries = entries[:end]
	if isOldestFirst(query.Order) {
		sort.Slice(entries, func(i, j int) bool { return entries[i].Seq < entries[j].Seq })
	}
	result := entries[:0]
	for _, entry := range entries {
		if query.Type != nil && entry.Type != *query.Type || query.CustomType != "" && entry.CustomType != query.CustomType {
			continue
		}
		if query.Cursor != nil && (isOldestFirst(query.Order) && entry.Seq <= query.Cursor.AfterSeq || !isOldestFirst(query.Order) && entry.Seq >= query.Cursor.AfterSeq) {
			continue
		}
		result = append(result, entry)
	}
	return limitEntries(result, query.Limit)
}

func (s *SQLiteStorage) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if !s.leased {
		return s.db.Close()
	}
	conn, err := s.db.Conn(ctx)
	if err == nil {
		if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err == nil {
			_, err = conn.ExecContext(ctx, `DELETE FROM writer_lease WHERE owner_id = ? AND fence = ?`, s.ownerID, s.lease)
			if err == nil {
				_, err = conn.ExecContext(ctx, "COMMIT")
			}
		}
		conn.Close()
	}
	closeErr := s.db.Close()
	if err != nil {
		return err
	}
	return closeErr
}

type SQLiteSessionRepo struct {
	dir     string
	options SQLiteStorageOptions
	mu      sync.Mutex
}

func NewSQLiteSessionRepo(dir string, options SQLiteStorageOptions) *SQLiteSessionRepo {
	return &SQLiteSessionRepo{dir: dir, options: options}
}
func (r *SQLiteSessionRepo) path(id string) string { return filepath.Join(r.dir, id+".sqlite") }

func readSQLiteMetadata(ctx context.Context, path string) (SessionMetadata, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return SessionMetadata{}, err
	}
	defer db.Close()
	var metadata SessionMetadata
	var parent sql.NullString
	err = db.QueryRowContext(ctx, `SELECT id, created_at, storage_version, cwd, parent_session_id FROM session LIMIT 1`).Scan(&metadata.ID, &metadata.CreatedAt, &metadata.StorageVersion, &metadata.CWD, &parent)
	if err != nil {
		return SessionMetadata{}, err
	}
	if parent.Valid {
		metadata.ParentSessionID = parent.String
	}
	if metadata.StorageVersion > CurrentStorageVersion {
		return SessionMetadata{}, fmt.Errorf("session storage version %d is newer than binary version %d", metadata.StorageVersion, CurrentStorageVersion)
	}
	return metadata, nil
}

func (r *SQLiteSessionRepo) Create(ctx context.Context, options SessionCreateOptions) (Session, error) {
	id := options.ID
	if id == "" {
		id = NewUUIDv7Generator().Next()
	}
	if !validSessionFileID(id) {
		return nil, fmt.Errorf("invalid session file id %q", id)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := os.Stat(r.path(id)); err == nil {
		return nil, sessionError(SessionAlreadyExists, fmt.Errorf("session already exists: %s", id))
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	metadata := SessionMetadata{ID: id, CreatedAt: time.Now().UnixMilli(), StorageVersion: CurrentStorageVersion, ParentSessionID: options.ParentSessionID}
	storage, err := CreateSQLiteStorage(r.path(id), metadata, r.options)
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
func (r *SQLiteSessionRepo) Open(ctx context.Context, metadata SessionMetadata) (Session, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	storage, stored, err := OpenSQLiteStorage(r.path(metadata.ID), r.options)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, sessionError(SessionNotFound, err)
		}
		return nil, err
	}
	return newSession(stored, storage), nil
}
func (r *SQLiteSessionRepo) List(ctx context.Context, _ JSONValue) ([]SessionMetadata, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	files, err := filepath.Glob(filepath.Join(r.dir, "*.sqlite"))
	if err != nil {
		return nil, err
	}
	result := make([]SessionMetadata, 0, len(files))
	for _, path := range files {
		metadata, err := readSQLiteMetadata(ctx, path)
		if err != nil {
			return nil, err
		}
		result = append(result, metadata)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt < result[j].CreatedAt })
	return result, nil
}
func (r *SQLiteSessionRepo) Delete(ctx context.Context, metadata SessionMetadata) error {
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
func (r *SQLiteSessionRepo) Fork(ctx context.Context, source SessionMetadata, options ForkOptions, create SessionCreateOptions) (Session, error) {
	sourceStorage, stored, err := openSQLiteSnapshot(r.path(source.ID), r.options)
	if err != nil {
		return nil, err
	}
	sourceSession := newSession(stored, sourceStorage)
	defer sourceSession.Close(ctx)
	entries, err := sourceSession.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil {
		return nil, err
	}
	selected := make(map[string]bool, len(entries))
	if options.Scope == "tree" {
		for _, entry := range entries {
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
	create.ParentSessionID = source.ID
	created, err := r.Create(ctx, create)
	if err != nil {
		return nil, err
	}
	writes := make([]Write, 0, len(entries)+8)
	for _, entry := range entries {
		if !selected[entry.ID] {
			continue
		}
		entry.Seq, entry.Timestamp = 0, 0
		if entry.ParentID != nil && !selected[*entry.ParentID] {
			entry.ParentID = nil
		}
		writes = append(writes, Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}})
	}
	mainLeaf := (*string)(nil)
	for i := len(entries) - 1; i >= 0; i-- {
		if selected[entries[i].ID] {
			mainLeaf = stringPointer(entries[i].ID)
			break
		}
	}
	copyRegister := func(namespace RegisterNamespace, key string) error {
		register, err := sourceSession.GetRegister(ctx, namespace, key)
		if err != nil {
			return err
		}
		if register != nil {
			writes = append(writes, registerSet(namespace, key, register.Value))
		}
		return nil
	}
	if err := copyRegister(RegisterFactName, ""); err != nil {
		return nil, err
	}
	facts, err := sourceSession.ListRegisters(ctx, RegisterFactCustom, "")
	if err != nil {
		return nil, err
	}
	for _, register := range facts {
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
			config, err := sourceSession.GetRegister(ctx, RegisterLaneConfig, leaf.Key)
			if err != nil {
				return nil, err
			}
			if config != nil {
				writes = append(writes, registerSet(RegisterLaneConfig, leaf.Key, config.Value))
			}
		}
	}
	writes = append(writes, registerSet(RegisterLaneLeaf, "main", mainLeaf), registerSet(RegisterLaneState, "main", LaneState{PendingNextRun: []string{}}))
	if err := copyRegister(RegisterLaneConfig, "main"); err != nil {
		return nil, err
	}
	if len(writes) > 0 {
		if _, err := created.Commit(ctx, Transaction{Writes: writes}); err != nil {
			_ = created.Close(ctx)
			return nil, err
		}
	}
	return created, nil
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
