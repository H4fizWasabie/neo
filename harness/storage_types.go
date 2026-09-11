package harness

import "context"

type EntryType string

const (
	EntryMessage       EntryType = "message"
	EntryCompaction    EntryType = "compaction"
	EntryBranchSummary EntryType = "branch_summary"
	EntryCustom        EntryType = "custom"
)

type EntryBase struct {
	ID        string    `json:"id"`
	ParentID  *string   `json:"parentId,omitempty"`
	Seq       int64     `json:"seq"`
	Timestamp int64     `json:"timestamp"`
	Type      EntryType `json:"type"`
}

// Entry is the Go representation of the harness discriminated union. Type
// selects the active payload fields; validation is Slice 2 behavior.
type Entry struct {
	EntryBase
	CustomType string `json:"customType,omitempty"`

	Message   *AgentMessage `json:"message,omitempty"`
	Terminate bool          `json:"terminate,omitempty"`

	Summary      string         `json:"summary,omitempty"`
	RetainedTail []AgentMessage `json:"retainedTail,omitempty"`
	TokensBefore int64          `json:"tokensBefore,omitempty"`
	Details      JSONValue      `json:"details,omitempty"`
	Usage        *Usage         `json:"usage,omitempty"`
	FromHook     bool           `json:"fromHook,omitempty"`
	FromID       string         `json:"fromId,omitempty"`
	Data         JSONValue      `json:"data,omitempty"`
}

type MessageEntry = Entry
type CompactionEntry = Entry
type BranchSummaryEntry = Entry
type CustomEntry = Entry

type PendingEntry struct {
	Type       EntryType `json:"type"`
	CustomType string    `json:"customType,omitempty"`
	Payload    JSONValue `json:"payload,omitempty"`
}

type DurableFileOperations struct {
	Read    []string `json:"read"`
	Written []string `json:"written"`
	Edited  []string `json:"edited"`
}

type DurableStructuralPreparation struct {
	Kind                EntryType             `json:"kind"`
	MessagesToSummarize []AgentMessage        `json:"messagesToSummarize,omitempty"`
	TurnPrefixMessages  []AgentMessage        `json:"turnPrefixMessages,omitempty"`
	RetainedTail        []AgentMessage        `json:"retainedTail,omitempty"`
	Messages            []AgentMessage        `json:"messages,omitempty"`
	IsSplitTurn         bool                  `json:"isSplitTurn,omitempty"`
	TokensBefore        int64                 `json:"tokensBefore,omitempty"`
	PreviousSummary     string                `json:"previousSummary,omitempty"`
	FileOps             DurableFileOperations `json:"fileOps"`
	Settings            CompactionSettings    `json:"settings,omitempty"`
	TotalTokens         int64                 `json:"totalTokens,omitempty"`
}

type UsageRow struct {
	ID         string    `json:"id"`
	Seq        int64     `json:"seq"`
	Usage      Usage     `json:"usage"`
	EntryID    *string   `json:"entryId,omitempty"`
	Adjustment bool      `json:"adjustment"`
	Details    JSONValue `json:"details,omitempty"`
}

type RegisterNamespace string

const (
	RegisterLaneLeaf       RegisterNamespace = "lane.leaf"
	RegisterLaneConfig     RegisterNamespace = "lane.config"
	RegisterLaneState      RegisterNamespace = "lane.state"
	RegisterLaneLastResult RegisterNamespace = "lane.lastResult"
	RegisterOpMeta         RegisterNamespace = "op.meta"
	RegisterOpState        RegisterNamespace = "op.state"
	RegisterOpToolArgs     RegisterNamespace = "op.tool_args"
	RegisterOpPreparation  RegisterNamespace = "op.preparation"
	RegisterPendingEntry   RegisterNamespace = "pending.entry"
	RegisterFactName       RegisterNamespace = "fact.name"
	RegisterFactLabel      RegisterNamespace = "fact.label"
	RegisterFactCustom     RegisterNamespace = "fact.custom"
)

// RegisterValue is the current value of a namespaced mutable cell. The
// namespace determines the concrete value and is validated before commit.
type RegisterValue any

type Register struct {
	Namespace RegisterNamespace `json:"namespace"`
	Key       string            `json:"key"`
	Value     RegisterValue     `json:"value"`
	Seq       int64             `json:"seq"`
}

type RegisterValues struct {
	LaneLeaf       *string
	LaneConfig     LaneConfiguration
	LaneState      LaneState
	LaneLastResult LaneLastResult
	OpMeta         Operation
	OpState        OperationState
	OpToolArgs     map[string]JSONValue
	OpPreparation  DurableStructuralPreparation
	PendingEntry   PendingEntry
	FactName       string
	FactLabel      string
	FactCustom     JSONValue
}

type RegisterOperation string

const (
	RegisterSet    RegisterOperation = "set"
	RegisterDelete RegisterOperation = "delete"
)

type EntryWrite struct {
	Entry Entry `json:"entry"`
}

type UsageWrite struct {
	Row UsageRow `json:"row"`
}

type RegisterWrite struct {
	Operation RegisterOperation `json:"op"`
	Namespace RegisterNamespace `json:"namespace"`
	Key       string            `json:"key"`
	Value     RegisterValue     `json:"value,omitempty"`
}

type WriteKind string

const (
	WriteEntry    WriteKind = "entry"
	WriteUsage    WriteKind = "usage"
	WriteRegister WriteKind = "register"
)

type Write struct {
	Kind     WriteKind      `json:"kind"`
	Entry    *EntryWrite    `json:"entry,omitempty"`
	Usage    *UsageWrite    `json:"usage,omitempty"`
	Register *RegisterWrite `json:"register,omitempty"`
}

type Transaction struct {
	Writes []Write `json:"writes"`
}

type CommitResult struct {
	FirstSeq  int64   `json:"firstSeq"`
	Seqs      []int64 `json:"seqs"`
	Timestamp int64   `json:"timestamp"`
}

type EntryStructure struct {
	ID         string    `json:"id"`
	ParentID   *string   `json:"parentId,omitempty"`
	Seq        int64     `json:"seq"`
	Timestamp  int64     `json:"timestamp"`
	Type       EntryType `json:"type"`
	CustomType string    `json:"customType,omitempty"`
}

type EntryOrder string

const (
	NewestFirst EntryOrder = "newestFirst"
	OldestFirst EntryOrder = "oldestFirst"
)

type EntryCursor struct {
	AfterSeq int64 `json:"afterSeq"`
}

type EntryQuery struct {
	Type       *EntryType   `json:"type,omitempty"`
	CustomType string       `json:"customType,omitempty"`
	Order      EntryOrder   `json:"order,omitempty"`
	Limit      int          `json:"limit,omitempty"`
	Cursor     *EntryCursor `json:"cursor,omitempty"`
}

type BranchScan struct {
	Start      string       `json:"start,omitempty"`
	StopAtType *EntryType   `json:"stopAtType,omitempty"`
	StopAtID   string       `json:"stopAtId,omitempty"`
	Type       *EntryType   `json:"type,omitempty"`
	CustomType string       `json:"customType,omitempty"`
	Order      EntryOrder   `json:"order,omitempty"`
	Limit      int          `json:"limit,omitempty"`
	Cursor     *EntryCursor `json:"cursor,omitempty"`
}

type EntryScan struct {
	Type       *EntryType   `json:"type,omitempty"`
	CustomType string       `json:"customType,omitempty"`
	FromSeq    int64        `json:"fromSeq,omitempty"`
	ToSeq      int64        `json:"toSeq,omitempty"`
	Order      EntryOrder   `json:"order,omitempty"`
	Limit      int          `json:"limit,omitempty"`
	Cursor     *EntryCursor `json:"cursor,omitempty"`
}

type UsageScan struct {
	FromSeq int64        `json:"fromSeq,omitempty"`
	ToSeq   int64        `json:"toSeq,omitempty"`
	Order   EntryOrder   `json:"order,omitempty"`
	Limit   int          `json:"limit,omitempty"`
	Cursor  *EntryCursor `json:"cursor,omitempty"`
}

type Storage interface {
	Commit(context.Context, Transaction) (CommitResult, error)
	GetEntries(context.Context, []string) (map[string]Entry, error)
	GetRegister(context.Context, RegisterNamespace, string) (*Register, error)
	ListRegisters(context.Context, RegisterNamespace, string) ([]Register, error)
	ScanBranch(context.Context, BranchScan) ([]Entry, error)
	ScanBranchStructure(context.Context, BranchScan) ([]EntryStructure, error)
	ScanEntries(context.Context, EntryScan) ([]Entry, error)
	ScanUsage(context.Context, UsageScan) ([]UsageRow, error)
	GetStats(context.Context) (SessionStats, error)
	Close(context.Context) error
}
