package harness

import "context"

type SessionMetadata struct {
	ID                      string `json:"id"`
	CreatedAt               int64  `json:"createdAt"`
	StorageVersion          int    `json:"storageVersion"`
	CWD                     string `json:"cwd,omitempty"`
	ParentSessionID         string `json:"parentSessionId,omitempty"`
	LegacyParentSessionPath string `json:"legacyParentSessionPath,omitempty"`
}

type SessionStats struct {
	MessageCount   int64   `json:"messageCount"`
	CachedTokens   int64   `json:"cachedTokens"`
	UncachedTokens int64   `json:"uncachedTokens"`
	TotalTokens    int64   `json:"totalTokens"`
	CostTotal      float64 `json:"costTotal"`
}

type LanePointer struct {
	Lane   string  `json:"lane"`
	LeafID *string `json:"leafId,omitempty"`
}

type LogItem struct {
	Kind     string      `json:"kind"`
	Seq      int64       `json:"seq"`
	Entry    *Entry      `json:"entry,omitempty"`
	Record   *LaneRecord `json:"record,omitempty"`
	Lane     string      `json:"lane,omitempty"`
	LeafID   *string     `json:"leafId,omitempty"`
	Fact     string      `json:"fact,omitempty"`
	Name     *string     `json:"name,omitempty"`
	TargetID string      `json:"targetId,omitempty"`
	Label    *string     `json:"label,omitempty"`
}

type LogOptions struct {
	AfterSeq int64 `json:"afterSeq,omitempty"`
	Limit    int   `json:"limit,omitempty"`
}

type SessionCreateOptions struct {
	ID              string `json:"id,omitempty"`
	ParentSessionID string `json:"parentSessionId,omitempty"`
}

type ForkOptions struct {
	Scope    string `json:"scope,omitempty"`
	EntryID  string `json:"entryId,omitempty"`
	Position string `json:"position,omitempty"`
}

type SessionTree interface {
	GetLeafID(context.Context) (*string, error)
	GetEntry(context.Context, string) (*Entry, error)
	GetStats(context.Context) (SessionStats, error)
	GetName(context.Context) (*string, error)
	SetName(context.Context, *string) error
	GetLabel(context.Context, string) (*string, error)
	SetLabel(context.Context, string, *string) error
	FindEntries(context.Context, EntryQuery) ([]Entry, error)
	FindEntry(context.Context, EntryQuery) (*Entry, error)
	FindEntriesOnBranch(context.Context, BranchScan) ([]Entry, error)
	FindEntryOnBranch(context.Context, BranchScan) (*Entry, error)
	AppendMessage(context.Context, AgentMessage) (string, error)
	AppendCustomEntry(context.Context, string, JSONValue) (string, error)
}

type Session interface {
	SessionTree
	Metadata() SessionMetadata
	IDGenerator() IDGenerator
	View(string) SessionTree
	Commit(context.Context, Transaction) (CommitResult, error)
	GetEntries(context.Context, []string) (map[string]Entry, error)
	GetRegister(context.Context, RegisterNamespace, string) (*Register, error)
	ListRegisters(context.Context, RegisterNamespace, string) ([]Register, error)
	Close(context.Context) error
}

type IDGenerator interface {
	Next(timestampMs ...int64) string
}

type SessionRepo interface {
	Create(context.Context, SessionCreateOptions) (Session, error)
	Open(context.Context, SessionMetadata) (Session, error)
	List(context.Context, JSONValue) ([]SessionMetadata, error)
	Delete(context.Context, SessionMetadata) error
	Fork(context.Context, SessionMetadata, ForkOptions, SessionCreateOptions) (Session, error)
}

type SessionErrorCode string

const (
	SessionNotFound          SessionErrorCode = "not_found"
	SessionAlreadyExists     SessionErrorCode = "already_exists"
	SessionInvalidEntry      SessionErrorCode = "invalid_entry"
	SessionInvalidPayload    SessionErrorCode = "invalid_payload"
	SessionInvalidLane       SessionErrorCode = "invalid_lane"
	SessionInvalidQuery      SessionErrorCode = "invalid_query"
	SessionInvalidForkTarget SessionErrorCode = "invalid_fork_target"
	SessionStorageFailure    SessionErrorCode = "storage"
)

type SessionError struct {
	Code  SessionErrorCode
	Cause error
	Msg   string
}

func (e *SessionError) Error() string { return e.Msg }
func (e *SessionError) Unwrap() error { return e.Cause }
