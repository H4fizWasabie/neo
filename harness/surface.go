package harness

import "context"

type Result[T any, E error] struct {
	OK    bool
	Value T
	Err   E
}

func Ok[T any, E error](value T) Result[T, E] { return Result[T, E]{OK: true, Value: value} }
func Err[T any, E error](err E) Result[T, E]  { return Result[T, E]{Err: err} }

type Skill struct {
	Name                   string `json:"name"`
	Description            string `json:"description"`
	Content                string `json:"content"`
	FilePath               string `json:"filePath"`
	DisableModelInvocation bool   `json:"disableModelInvocation,omitempty"`
}

type PromptTemplate struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Content     string `json:"content"`
}

type Resources struct {
	PromptTemplates []PromptTemplate `json:"promptTemplates,omitempty"`
	Skills          []Skill          `json:"skills,omitempty"`
}

type AgentHarnessStreamOptions struct {
	Transport       Transport            `json:"transport,omitempty"`
	TimeoutMs       int64                `json:"timeoutMs,omitempty"`
	MaxRetries      int64                `json:"maxRetries,omitempty"`
	MaxRetryDelayMs int64                `json:"maxRetryDelayMs,omitempty"`
	Headers         map[string]string    `json:"headers,omitempty"`
	Metadata        map[string]JSONValue `json:"metadata,omitempty"`
	CacheRetention  CacheRetention       `json:"cacheRetention,omitempty"`
	Deferred        JSONValue            `json:"deferred,omitempty"`
}

type AgentHarnessStreamOptionsPatch struct {
	Transport       *Transport            `json:"transport,omitempty"`
	TimeoutMs       *int64                `json:"timeoutMs,omitempty"`
	MaxRetries      *int64                `json:"maxRetries,omitempty"`
	MaxRetryDelayMs *int64                `json:"maxRetryDelayMs,omitempty"`
	Headers         map[string]*string    `json:"headers,omitempty"`
	Metadata        map[string]*JSONValue `json:"metadata,omitempty"`
	CacheRetention  *CacheRetention       `json:"cacheRetention,omitempty"`
	Deferred        *JSONValue            `json:"deferred,omitempty"`
}

type AgentHarnessTool interface {
	Name() string
	Description() string
	Replay() ReplayPolicy
	Execute(context.Context, string, map[string]JSONValue, any, func(AgentToolResult)) (AgentToolResult, error)
}

type AgentHarnessToolContextSource any

type EntryProjector func(context.Context, Entry) ([]AgentMessage, error)

type FileKind string

const (
	FileKindFile      FileKind = "file"
	FileKindDirectory FileKind = "directory"
	FileKindSymlink   FileKind = "symlink"
)

type FileErrorCode string

const (
	FileAborted          FileErrorCode = "aborted"
	FileAlreadyExists    FileErrorCode = "already_exists"
	FileNotFound         FileErrorCode = "not_found"
	FilePermissionDenied FileErrorCode = "permission_denied"
	FileNotDirectory     FileErrorCode = "not_directory"
	FileIsDirectory      FileErrorCode = "is_directory"
	FileInvalid          FileErrorCode = "invalid"
	FileNotSupported     FileErrorCode = "not_supported"
	FileUnknown          FileErrorCode = "unknown"
)

type FileError struct {
	Code  FileErrorCode
	Path  string
	Msg   string
	Cause error
}

func (e *FileError) Error() string { return e.Msg }
func (e *FileError) Unwrap() error { return e.Cause }

type FileInfo struct {
	Name    string   `json:"name"`
	Path    string   `json:"path"`
	Kind    FileKind `json:"kind"`
	Size    int64    `json:"size"`
	MTimeMs int64    `json:"mtimeMs"`
}

type FileSystem interface {
	CWD() string
	AbsolutePath(context.Context, string) Result[string, *FileError]
	JoinPath(context.Context, []string) Result[string, *FileError]
	ReadTextFile(context.Context, string) Result[string, *FileError]
	ReadTextLines(context.Context, string, int) Result[[]string, *FileError]
	ReadBinaryFile(context.Context, string) Result[[]byte, *FileError]
	WriteFile(context.Context, string, []byte) Result[struct{}, *FileError]
	AppendFile(context.Context, string, []byte) Result[struct{}, *FileError]
	RenameFile(context.Context, string, string) Result[struct{}, *FileError]
	FileInfo(context.Context, string) Result[FileInfo, *FileError]
	ListDir(context.Context, string) Result[[]FileInfo, *FileError]
	CanonicalPath(context.Context, string) Result[string, *FileError]
	Exists(context.Context, string) Result[bool, *FileError]
	CreateDir(context.Context, string, bool) Result[struct{}, *FileError]
	Remove(context.Context, string, bool, bool) Result[struct{}, *FileError]
	CreateTempDir(context.Context, string) Result[string, *FileError]
	CreateTempFile(context.Context, string, string, string) Result[string, *FileError]
	Cleanup() error
}

type ExecutionErrorCode string

const (
	ExecutionAborted          ExecutionErrorCode = "aborted"
	ExecutionTimeout          ExecutionErrorCode = "timeout"
	ExecutionShellUnavailable ExecutionErrorCode = "shell_unavailable"
	ExecutionSpawnError       ExecutionErrorCode = "spawn_error"
	ExecutionCallbackError    ExecutionErrorCode = "callback_error"
	ExecutionUnknown          ExecutionErrorCode = "unknown"
)

type ExecutionError struct {
	Code  ExecutionErrorCode
	Msg   string
	Cause error
}

func (e *ExecutionError) Error() string { return e.Msg }
func (e *ExecutionError) Unwrap() error { return e.Cause }

type ShellExecOptions struct {
	CWD         string
	Environment map[string]string
	InheritEnv  bool
	Timeout     int64
	OnStdout    func(string)
	OnStderr    func(string)
}

type Shell interface {
	Exec(context.Context, string, ShellExecOptions) Result[ShellResult, *ExecutionError]
	Cleanup() error
}

type ShellResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

type ExecutionEnv interface {
	FileSystem
	Shell
}

type CompactionErrorCode string

const (
	CompactionAborted             CompactionErrorCode = "aborted"
	CompactionSummarizationFailed CompactionErrorCode = "summarization_failed"
)

type CompactionError struct {
	Code  CompactionErrorCode
	Msg   string
	Cause error
}

func (e *CompactionError) Error() string { return e.Msg }
func (e *CompactionError) Unwrap() error { return e.Cause }

type BranchSummaryErrorCode string

const (
	BranchSummaryAborted             BranchSummaryErrorCode = "aborted"
	BranchSummarySummarizationFailed BranchSummaryErrorCode = "summarization_failed"
)

type BranchSummaryError struct {
	Code  BranchSummaryErrorCode
	Msg   string
	Cause error
}

func (e *BranchSummaryError) Error() string { return e.Msg }
func (e *BranchSummaryError) Unwrap() error { return e.Cause }

type FileOperations struct {
	Read    []string `json:"read"`
	Written []string `json:"written"`
	Edited  []string `json:"edited"`
}

type CompactionPreparation struct {
	MessagesToSummarize []AgentMessage     `json:"messagesToSummarize"`
	TurnPrefixMessages  []AgentMessage     `json:"turnPrefixMessages"`
	RetainedTail        []AgentMessage     `json:"retainedTail"`
	IsSplitTurn         bool               `json:"isSplitTurn"`
	TokensBefore        int64              `json:"tokensBefore"`
	PreviousSummary     string             `json:"previousSummary,omitempty"`
	FileOps             FileOperations     `json:"fileOps"`
	Settings            CompactionSettings `json:"settings"`
}

type BranchPreparation struct {
	Messages    []AgentMessage `json:"messages"`
	FileOps     FileOperations `json:"fileOps"`
	TotalTokens int64          `json:"totalTokens"`
}

type CompactResult struct {
	Summary       string   `json:"summary"`
	Usage         *Usage   `json:"usage,omitempty"`
	ReadFiles     []string `json:"readFiles"`
	ModifiedFiles []string `json:"modifiedFiles"`
}

type BranchSummaryResult struct {
	Summary       string   `json:"summary"`
	Usage         *Usage   `json:"usage,omitempty"`
	ReadFiles     []string `json:"readFiles"`
	ModifiedFiles []string `json:"modifiedFiles"`
}

type PromptInput struct {
	Text     string
	Images   []ImageContent
	Messages []AgentMessage
}

type AgentHarnessOptions struct {
	Session            Session
	Models             Models
	Model              Model
	ThinkingLevel      ThinkingLevel
	ActiveToolNames    []string
	Tools              []AgentHarnessTool
	ToolContext        AgentHarnessToolContextSource
	SystemPrompt       func(any) (string, error)
	Resources          Resources
	StreamOptions      AgentHarnessStreamOptions
	Retry              RetryPolicy
	Compaction         CompactionSettings
	SteeringMode       QueueMode
	FollowUpMode       QueueMode
	ToolExecution      ToolExecutionMode
	Drive              string
	ToProviderMessages func([]AgentMessage) ([]Message, error)
	EntryProjectors    map[string]EntryProjector
	Telemetry          TelemetryContext
}

type LaneInfo struct {
	Name      string         `json:"name"`
	LeafID    *string        `json:"leafId,omitempty"`
	Operation *OperationInfo `json:"operation,omitempty"`
}

type OperationInfo struct {
	ID     string        `json:"id"`
	Kind   OperationKind `json:"kind"`
	Status string        `json:"status"`
}

type SuspendedOperation struct {
	Lane          string          `json:"lane"`
	OperationID   string          `json:"operationId"`
	Kind          OperationKind   `json:"kind"`
	Reason        string          `json:"reason"`
	StartedAt     int64           `json:"startedAt"`
	Prompt        []AgentMessage  `json:"prompt,omitempty"`
	Deferred      *DeferredHandle `json:"deferred,omitempty"`
	Aborting      *AbortingItems  `json:"aborting,omitempty"`
	MissingTools  []string        `json:"missingTools,omitempty"`
	MissingModels []string        `json:"missingModels,omitempty"`
}

type AbortingItems struct {
	Steer    []AgentMessage `json:"steer"`
	FollowUp []AgentMessage `json:"followUp"`
}

type LaneSnapshot struct {
	Lane          string                 `json:"lane"`
	Transcript    []Entry                `json:"transcript"`
	LeafID        *string                `json:"leafId,omitempty"`
	Operation     *OperationSnapshot     `json:"operation,omitempty"`
	Queues        QueueSnapshot          `json:"queues"`
	PendingWrites []PendingEntrySnapshot `json:"pendingWrites,omitempty"`
	Faulted       bool                   `json:"faulted"`
}

type OperationSnapshot struct {
	ID               string              `json:"id"`
	Kind             OperationKind       `json:"kind"`
	Status           string              `json:"status"`
	StartedAt        int64               `json:"startedAt"`
	Suspended        *SuspendedOperation `json:"suspended,omitempty"`
	StreamingMessage *AgentMessage       `json:"streamingMessage,omitempty"`
	RunningTools     []RunningTool       `json:"runningTools,omitempty"`
	Retry            *RetrySnapshot      `json:"retry,omitempty"`
}

type RunningTool struct {
	ToolCallID    string           `json:"toolCallId"`
	ToolName      string           `json:"toolName"`
	Args          JSONValue        `json:"args,omitempty"`
	PartialResult *AgentToolResult `json:"partialResult,omitempty"`
}

type RetrySnapshot struct {
	Attempt       int64 `json:"attempt"`
	MaxAttempts   int64 `json:"maxAttempts"`
	NextAttemptAt int64 `json:"nextAttemptAt"`
}

type QueueItem struct {
	EntryID string       `json:"entryId"`
	Message AgentMessage `json:"message"`
}

type QueueSnapshot struct {
	Steer    []QueueItem `json:"steer"`
	FollowUp []QueueItem `json:"followUp"`
	NextRun  []QueueItem `json:"nextRun"`
}

type PendingEntrySnapshot struct {
	EntryID    string        `json:"entryId"`
	Type       EntryType     `json:"type"`
	CustomType string        `json:"customType,omitempty"`
	Message    *AgentMessage `json:"message,omitempty"`
	Data       JSONValue     `json:"data,omitempty"`
}

type SessionSnapshot struct {
	Lanes   []LaneInfo `json:"lanes"`
	Faulted bool       `json:"faulted"`
}

type ActionInfo struct {
	Kind        string    `json:"kind"`
	Description string    `json:"description"`
	Details     JSONValue `json:"details,omitempty"`
}

type WatchHandle[T any] struct {
	Snapshot    T
	Start       func(func(HarnessEvent))
	Unsubscribe func()
}

type AgentLane interface {
	Name() string
	GetLeafID(context.Context) (*string, error)
	GetLastResult(context.Context) (*LaneLastResult, error)
	Prompt(context.Context, PromptInput) (Result[RunOutcome, error], error)
	Skill(context.Context, string, string) (Result[RunOutcome, error], error)
	PromptFromTemplate(context.Context, string, []string) (Result[RunOutcome, error], error)
	Compact(context.Context, string) (Result[CompactionOutcome, error], error)
	NavigateTree(context.Context, *string, NavigateOptions) (Result[NavigationOutcome, error], error)
	Resume(context.Context) (Result[ResumeOutcome, error], error)
	Abort(context.Context) (Result[AbortOutcome, error], error)
	Steer(context.Context, PromptInput) (Result[QueueOutcome, error], error)
	FollowUp(context.Context, PromptInput) (Result[QueueOutcome, error], error)
	NextRun(context.Context, PromptInput) (Result[NextRunOutcome, error], error)
	CancelQueued(context.Context, string) (Result[CancelQueuedOutcome, error], error)
	RecordUsage(context.Context, Usage, *string, JSONValue) (Result[RecordUsageOutcome, error], error)
	WaitForIdle(context.Context) error
	RunWhenIdle(context.Context, func() error) error
	PeekAction(context.Context) (*ActionInfo, error)
	ExecuteAction(context.Context) (*ActionInfo, error)
	RunToCompletion(context.Context) error
	GetModel(context.Context) (*Model, error)
	SetModel(context.Context, Model) error
	GetThinkingLevel(context.Context) (ThinkingLevel, error)
	SetThinkingLevel(context.Context, ThinkingLevel) error
	GetActiveTools(context.Context) ([]string, error)
	SetActiveTools(context.Context, []string) error
	Session() SessionTree
	Watch(context.Context) (WatchHandle[LaneSnapshot], error)
}

type NavigateOptions struct {
	Summarize          bool   `json:"summarize,omitempty"`
	Label              string `json:"label,omitempty"`
	CustomInstructions string `json:"customInstructions,omitempty"`
}

type AgentHarness interface {
	AgentLane
	Lane(context.Context, string) (AgentLane, error)
	CreateLane(context.Context, string, *string) (Result[AgentLane, error], error)
	Lanes(context.Context) ([]LaneInfo, error)
	GetTools(context.Context) ([]AgentHarnessTool, error)
	SetTools(context.Context, []AgentHarnessTool) error
	GetResources(context.Context) (Resources, error)
	SetResources(context.Context, Resources) error
	GetStreamOptions(context.Context) (AgentHarnessStreamOptions, error)
	SetStreamOptions(context.Context, AgentHarnessStreamOptions) error
	GetRetryPolicy(context.Context) (RetryPolicy, error)
	SetRetryPolicy(context.Context, RetryPolicy) error
	GetCompactionSettings(context.Context) (CompactionSettings, error)
	SetCompactionSettings(context.Context, CompactionSettings) error
	GetSteeringMode(context.Context) (QueueMode, error)
	SetSteeringMode(context.Context, QueueMode) error
	GetFollowUpMode(context.Context) (QueueMode, error)
	SetFollowUpMode(context.Context, QueueMode) error
	WatchSession(context.Context) (WatchHandle[SessionSnapshot], error)
	Hooks() Hooks
	Events() Events
	Close(context.Context) error
}
