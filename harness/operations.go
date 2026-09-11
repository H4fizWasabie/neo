package harness

type QueueMode string

const (
	QueueAll        QueueMode = "all"
	QueueOneAtATime QueueMode = "one-at-a-time"
)

type RetryPolicy struct {
	Enabled     bool  `json:"enabled"`
	MaxRetries  int64 `json:"maxRetries"`
	BaseDelayMs int64 `json:"baseDelayMs"`
}

type NormalizedRetryPolicy struct {
	MaxAttempts int64 `json:"maxAttempts"`
	BaseDelayMs int64 `json:"baseDelayMs"`
}

type CompactionSettings struct {
	Enabled          bool  `json:"enabled"`
	ReserveTokens    int64 `json:"reserveTokens"`
	KeepRecentTokens int64 `json:"keepRecentTokens"`
}

type ToolExecutionMode string

const (
	ToolExecutionSequential ToolExecutionMode = "sequential"
	ToolExecutionParallel   ToolExecutionMode = "parallel"
)

type OperationKind string

const (
	OperationRun        OperationKind = "run"
	OperationCompaction OperationKind = "compaction"
	OperationNavigation OperationKind = "navigation"
)

type Operation struct {
	OperationID  string          `json:"operationId"`
	Lane         string          `json:"lane"`
	SourceLeafID *string         `json:"sourceLeafId,omitempty"`
	StartedAt    int64           `json:"startedAt"`
	Intent       OperationIntent `json:"intent"`
}

type OperationIntent struct {
	Kind                 OperationKind        `json:"kind"`
	PromptEntryIDs       []string             `json:"promptEntryIds,omitempty"`
	SystemPromptOverride string               `json:"systemPromptOverride,omitempty"`
	ResumeData           map[string]JSONValue `json:"resumeData,omitempty"`
	CustomInstructions   string               `json:"customInstructions,omitempty"`
	TargetID             *string              `json:"targetId,omitempty"`
	Summarize            bool                 `json:"summarize,omitempty"`
	Label                string               `json:"label,omitempty"`
}

type ControlStatus string

const (
	ControlRunning         ControlStatus = "running"
	ControlCancelRequested ControlStatus = "cancel_requested"
)

type Control struct {
	Status          ControlStatus `json:"status"`
	RequestedAt     int64         `json:"requestedAt,omitempty"`
	DrainedSteer    []string      `json:"drainedSteer,omitempty"`
	DrainedFollowUp []string      `json:"drainedFollowUp,omitempty"`
}

type ContinuationKind string

const (
	ContinuationNeedAssistant ContinuationKind = "need_assistant"
	ContinuationMayFinish     ContinuationKind = "may_finish"
)

type Continuation struct {
	Kind                  ContinuationKind `json:"kind"`
	OverflowRecoveryUsed  bool             `json:"overflowRecoveryUsed,omitempty"`
	IncludeFinalAssistant bool             `json:"includeFinalAssistant,omitempty"`
}

type CheckpointPhase struct {
	Continuation                   Continuation `json:"continuation"`
	TriggerEntryID                 string       `json:"triggerEntryId"`
	ThresholdCheckedTriggerEntryID string       `json:"thresholdCheckedTriggerEntryId,omitempty"`
	SkipInboxOnce                  bool         `json:"skipInboxOnce,omitempty"`
}

type Inbox struct {
	Steer    []string `json:"steer"`
	FollowUp []string `json:"followUp"`
	Writes   []string `json:"writes"`
}

type OperationError struct {
	Code    string    `json:"code"`
	Message string    `json:"message"`
	Details JSONValue `json:"details,omitempty"`
}

type RunSettings struct {
	Compaction    CompactionSettings `json:"compaction"`
	SteeringMode  QueueMode          `json:"steeringMode"`
	FollowUpMode  QueueMode          `json:"followUpMode"`
	ToolExecution ToolExecutionMode  `json:"toolExecution"`
}

type RunState struct {
	Kind                   OperationKind `json:"kind"`
	Control                Control       `json:"control"`
	Settings               RunSettings   `json:"settings"`
	Phase                  RunPhase      `json:"phase"`
	Inbox                  Inbox         `json:"inbox"`
	LatestAssistantEntryID *string       `json:"latestAssistantEntryId,omitempty"`
}

type RunPhaseKind string

const (
	PhaseCheckpoint   RunPhaseKind = "checkpoint"
	PhaseAssistant    RunPhaseKind = "assistant"
	PhaseTools        RunPhaseKind = "tools"
	PhaseCompaction   RunPhaseKind = "compaction"
	PhaseDeferred     RunPhaseKind = "deferred"
	PhaseFailureDrain RunPhaseKind = "failure_drain"
)

// RunPhase is a tagged value. The concrete payload is selected by Kind.
type RunPhase struct {
	Kind        RunPhaseKind        `json:"kind"`
	Checkpoint  *CheckpointPhase    `json:"checkpoint,omitempty"`
	Generation  *Generation         `json:"generation,omitempty"`
	ToolBatch   *ToolBatch          `json:"batch,omitempty"`
	Reason      string              `json:"reason,omitempty"`
	Structural  *StructuralDecision `json:"structural,omitempty"`
	ResumeAfter *CheckpointPhase    `json:"resumeAfter,omitempty"`
	Deferred    *Deferred           `json:"deferred,omitempty"`
	Error       *OperationError     `json:"error,omitempty"`
	Provenance  *FailureProvenance  `json:"provenance,omitempty"`
}

type FailureProvenance struct {
	Kind    string `json:"kind"`
	EntryID string `json:"entryId,omitempty"`
	TaskID  string `json:"taskId,omitempty"`
}

type LaneState struct {
	CurrentOperationID *string  `json:"currentOperationId,omitempty"`
	PendingNextRun     []string `json:"pendingNextRun"`
}

type LaneConfiguration struct {
	Model           Model         `json:"model"`
	ThinkingLevel   ThinkingLevel `json:"thinkingLevel"`
	ActiveToolNames []string      `json:"activeToolNames"`
}

type GenerationContext struct {
	StepID               string                    `json:"stepId"`
	TriggerEntryID       string                    `json:"triggerEntryId"`
	Configuration        LaneConfiguration         `json:"configuration"`
	StreamOptions        AgentHarnessStreamOptions `json:"streamOptions"`
	RetryPolicy          NormalizedRetryPolicy     `json:"retryPolicy"`
	OverflowRecoveryUsed bool                      `json:"overflowRecoveryUsed"`
}

type GenerationStatus string

const (
	GenerationReady         GenerationStatus = "ready"
	GenerationEffectPending GenerationStatus = "effect_pending"
	GenerationRetryWait     GenerationStatus = "retry_wait"
)

type Generation struct {
	Status              GenerationStatus  `json:"status"`
	Context             GenerationContext `json:"context"`
	Attempt             int64             `json:"attempt,omitempty"`
	NextAttempt         int64             `json:"nextAttempt,omitempty"`
	ResponseEntryID     string            `json:"responseEntryId,omitempty"`
	UsageID             string            `json:"usageId,omitempty"`
	IntendedOutputLimit int64             `json:"intendedOutputLimit,omitempty"`
	ContextWindow       int64             `json:"contextWindow,omitempty"`
	NotBefore           int64             `json:"notBefore,omitempty"`
	ErrorMessage        string            `json:"errorMessage,omitempty"`
}

type ToolBatch struct {
	AssistantEntryID string            `json:"assistantEntryId"`
	Configuration    LaneConfiguration `json:"configuration"`
	StepID           string            `json:"stepId,omitempty"`
	TurnID           string            `json:"turnId"`
	Calls            []ToolCall        `json:"calls"`
}

type ToolCall struct {
	Status        string       `json:"status"`
	SourceIndex   int          `json:"sourceIndex"`
	ResultEntryID string       `json:"resultEntryId"`
	Replay        ReplayPolicy `json:"replay,omitempty"`
	Terminate     bool         `json:"terminate,omitempty"`
}

type DeferredStatus string

const (
	DeferredSuspended     DeferredStatus = "suspended"
	DeferredEffectPending DeferredStatus = "effect_pending"
)

type Deferred struct {
	Status          DeferredStatus            `json:"status"`
	StepID          string                    `json:"stepId"`
	SourceEntryID   string                    `json:"sourceEntryId"`
	Poll            int64                     `json:"poll"`
	ResponseEntryID string                    `json:"responseEntryId,omitempty"`
	UsageID         string                    `json:"usageId,omitempty"`
	Configuration   LaneConfiguration         `json:"configuration"`
	StreamOptions   AgentHarnessStreamOptions `json:"streamOptions"`
}

type StructuralDecision struct {
	TaskID     string             `json:"taskId"`
	Status     string             `json:"status"`
	Generation *SummaryGeneration `json:"generation,omitempty"`
}

type SummaryContext struct {
	TaskID        string                    `json:"taskId"`
	ResultEntryID string                    `json:"resultEntryId"`
	Kind          EntryType                 `json:"kind"`
	Configuration LaneConfiguration         `json:"configuration"`
	StreamOptions AgentHarnessStreamOptions `json:"streamOptions"`
	RetryPolicy   NormalizedRetryPolicy     `json:"retryPolicy"`
	Reason        string                    `json:"reason,omitempty"`
}

type SummaryGeneration struct {
	Status       GenerationStatus `json:"status"`
	Context      SummaryContext   `json:"context"`
	NextAttempt  int64            `json:"nextAttempt,omitempty"`
	Attempt      int64            `json:"attempt,omitempty"`
	Request      *SummaryRequest  `json:"request,omitempty"`
	UsageIDs     []string         `json:"usageIds"`
	NotBefore    int64            `json:"notBefore,omitempty"`
	ErrorMessage string           `json:"errorMessage,omitempty"`
}

type SummaryRequest struct {
	Index   int    `json:"index"`
	UsageID string `json:"usageId"`
}

type CompactionState struct {
	Kind               OperationKind      `json:"kind"`
	Control            Control            `json:"control"`
	CustomInstructions string             `json:"customInstructions,omitempty"`
	Structural         StructuralDecision `json:"structural"`
}

type NavigationPhase struct {
	Kind       string              `json:"kind"`
	Structural *StructuralDecision `json:"structural,omitempty"`
}

type NavigationState struct {
	Kind               OperationKind   `json:"kind"`
	Control            Control         `json:"control"`
	TargetID           *string         `json:"targetId,omitempty"`
	Label              string          `json:"label,omitempty"`
	CustomInstructions string          `json:"customInstructions,omitempty"`
	Summarize          bool            `json:"summarize"`
	Phase              NavigationPhase `json:"phase"`
}

type OperationState struct {
	Kind       OperationKind    `json:"kind"`
	Run        *RunState        `json:"run,omitempty"`
	Compaction *CompactionState `json:"compaction,omitempty"`
	Navigation *NavigationState `json:"navigation,omitempty"`
}

type LaneLastResult struct {
	OperationID           string          `json:"operationId"`
	Kind                  OperationKind   `json:"kind"`
	Outcome               string          `json:"outcome"`
	LeafID                *string         `json:"leafId,omitempty"`
	FinalAssistantEntryID *string         `json:"finalAssistantEntryId,omitempty"`
	RunCompletion         string          `json:"runCompletion,omitempty"`
	Error                 *OperationError `json:"error,omitempty"`
}
