package harness

type TaggedError struct {
	Tag     string
	Message string
	Fields  map[string]JSONValue
}

func (e *TaggedError) Error() string { return e.Message }

type LaneBusy struct {
	TaggedError
	Lane          string
	OperationID   string
	OperationKind OperationKind
}

type MissingIdentities struct {
	TaggedError
	Lane   string
	Tools  []string
	Models []string
}

type NoActiveRun struct {
	TaggedError
	Lane string
}
type NoActiveOperation struct {
	TaggedError
	Lane string
}
type NothingToResume struct {
	TaggedError
	Lane string
}
type NothingToCompact struct {
	TaggedError
	Lane string
}
type InvalidMessage struct {
	TaggedError
	Lane, Reason string
}
type InvalidNavigation struct {
	TaggedError
	Lane, Reason string
}
type UnknownSkill struct {
	TaggedError
	Name string
}
type UnknownTemplate struct {
	TaggedError
	Name string
}
type UnknownTarget struct {
	TaggedError
	TargetID string
}
type UnknownQueueItem struct {
	TaggedError
	EntryID string
}
type LaneExists struct {
	TaggedError
	Lane string
}
type InvalidLane struct {
	TaggedError
	Lane, Reason string
}
type Closed struct{ TaggedError }

type HarnessFault struct {
	Message string
	Cause   error
}

func (e *HarnessFault) Error() string { return e.Message }
func (e *HarnessFault) Unwrap() error { return e.Cause }

type HarnessClosed struct{ Message string }

func (e *HarnessClosed) Error() string { return e.Message }

type RunOutcome struct {
	Kind         string          `json:"kind"`
	RunID        string          `json:"runId,omitempty"`
	LeafID       *string         `json:"leafId,omitempty"`
	FinalEntryID *string         `json:"finalEntryId,omitempty"`
	FinalMessage *AgentMessage   `json:"finalMessage,omitempty"`
	Reason       string          `json:"reason,omitempty"`
	Deferred     *DeferredHandle `json:"deferred,omitempty"`
	Error        *OperationError `json:"error,omitempty"`
}

type MissingIdentitySuspension struct {
	Reason        string   `json:"reason"`
	MissingTools  []string `json:"missingTools,omitempty"`
	MissingModels []string `json:"missingModels,omitempty"`
}

type RunResult = Result[RunOutcome, error]
type CompactionResult = Result[CompactionOutcome, error]
type NavigationResult = Result[NavigationOutcome, error]
type ResumeResult = Result[ResumeOutcome, error]
type QueueResult = Result[QueueOutcome, error]
type NextRunResult = Result[NextRunOutcome, error]
type CancelQueuedResult = Result[CancelQueuedOutcome, error]
type AbortResult = Result[AbortOutcome, error]
type RecordUsageResult = Result[RecordUsageOutcome, error]

type CompactionOutcome struct {
	Kind    string              `json:"kind"`
	LeafID  *string             `json:"leafId,omitempty"`
	Entry   *Entry              `json:"entry,omitempty"`
	Error   *OperationError     `json:"error,omitempty"`
	Missing *SuspendedOperation `json:"missing,omitempty"`
}

type NavigationOutcome struct {
	Kind         string          `json:"kind"`
	OldLeafID    *string         `json:"oldLeafId,omitempty"`
	NewLeafID    *string         `json:"newLeafId,omitempty"`
	LeafID       *string         `json:"leafId,omitempty"`
	SummaryEntry *Entry          `json:"summaryEntry,omitempty"`
	Error        *OperationError `json:"error,omitempty"`
}

type ResumeOutcome struct {
	Operation  OperationKind      `json:"operation"`
	RunID      string             `json:"runId"`
	Run        *RunOutcome        `json:"run,omitempty"`
	Compaction *CompactionOutcome `json:"compaction,omitempty"`
	Navigation *NavigationOutcome `json:"navigation,omitempty"`
}

type QueueOutcome struct {
	EntryID string `json:"entryId"`
}
type NextRunOutcome struct {
	EntryID string `json:"entryId"`
}
type CancelQueuedOutcome struct {
	Kind string `json:"kind"`
}
type AbortOutcome struct {
	RunID    string         `json:"runId"`
	Steer    []AgentMessage `json:"steer"`
	FollowUp []AgentMessage `json:"followUp"`
}
type RecordUsageOutcome struct {
	UsageID string `json:"usageId"`
}
