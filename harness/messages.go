package harness

import "context"

// JSONValue is deliberately opaque at the type-surface stage. Storage and
// hook boundaries validate it as JSON in Slice 2 and later runtime slices.
type JSONValue any

type StopReason string

const (
	StopReasonStop     StopReason = "stop"
	StopReasonLength   StopReason = "length"
	StopReasonToolUse  StopReason = "tool_use"
	StopReasonError    StopReason = "error"
	StopReasonAborted  StopReason = "aborted"
	StopReasonDeferred StopReason = "deferred"
)

type ThinkingLevel string

const (
	ThinkingOff    ThinkingLevel = "off"
	ThinkingLow    ThinkingLevel = "low"
	ThinkingMedium ThinkingLevel = "medium"
	ThinkingHigh   ThinkingLevel = "high"
)

type Transport string

type CacheRetention string

// Usage is the provider-reported usage snapshot carried by entries and the
// append-only ledger. Cost fields are optional because providers differ.
type Usage struct {
	Input      int64 `json:"input,omitempty"`
	Output     int64 `json:"output,omitempty"`
	CacheRead  int64 `json:"cacheRead,omitempty"`
	CacheWrite int64 `json:"cacheWrite,omitempty"`
	Reasoning  int64 `json:"reasoning,omitempty"`
	Total      int64 `json:"total,omitempty"`
	Cost       *Cost `json:"cost,omitempty"`
}

type Cost struct {
	Input      float64 `json:"input,omitempty"`
	Output     float64 `json:"output,omitempty"`
	CacheRead  float64 `json:"cacheRead,omitempty"`
	CacheWrite float64 `json:"cacheWrite,omitempty"`
	Total      float64 `json:"total,omitempty"`
}

// AgentMessage is the provider-neutral message shape used by the harness.
// Content and provider-specific metadata remain JSON values by contract.
type AgentMessage struct {
	Role       string               `json:"role"`
	Content    JSONValue            `json:"content,omitempty"`
	Timestamp  int64                `json:"timestamp,omitempty"`
	StopReason StopReason           `json:"stopReason,omitempty"`
	ToolCalls  []AgentToolCall      `json:"toolCalls,omitempty"`
	ToolCallID string               `json:"toolCallId,omitempty"`
	Name       string               `json:"name,omitempty"`
	Usage      *Usage               `json:"usage,omitempty"`
	Metadata   map[string]JSONValue `json:"metadata,omitempty"`
}

type AssistantMessage = AgentMessage
type Message = AgentMessage

type ImageContent struct {
	Type     string `json:"type"`
	Data     string `json:"data,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
	URL      string `json:"url,omitempty"`
}

type AgentToolCall struct {
	ID        string               `json:"id"`
	Name      string               `json:"name"`
	Arguments map[string]JSONValue `json:"arguments,omitempty"`
}

type AgentToolResult struct {
	Content   JSONValue `json:"content,omitempty"`
	Details   JSONValue `json:"details,omitempty"`
	IsError   bool      `json:"isError,omitempty"`
	Usage     *Usage    `json:"usage,omitempty"`
	Terminate bool      `json:"terminate,omitempty"`
}

type ToolResultMessage struct {
	Role       string    `json:"role"`
	ToolCallID string    `json:"toolCallId"`
	Content    JSONValue `json:"content,omitempty"`
	Details    JSONValue `json:"details,omitempty"`
	IsError    bool      `json:"isError,omitempty"`
	Terminate  bool      `json:"terminate,omitempty"`
	Usage      *Usage    `json:"usage,omitempty"`
}

type StreamOptions struct {
	MaxTokens int64 `json:"maxTokens,omitempty"`
}

type Model struct {
	Provider      string `json:"provider"`
	ModelID       string `json:"modelId"`
	API           string `json:"api,omitempty"`
	ContextWindow int64  `json:"contextWindow,omitempty"`
	OutputLimit   int64  `json:"outputLimit,omitempty"`
}

type Models interface {
	Resolve(ctx context.Context, provider, modelID string) (Model, error)
	Stream(ctx context.Context, model Model, messages []Message, options AgentHarnessStreamOptions) (<-chan AgentEvent, error)
	FetchDeferred(ctx context.Context, model Model, handle DeferredHandle, options AgentHarnessStreamOptions) (DeferredResponse, error)
	CancelDeferred(ctx context.Context, model Model, handle DeferredHandle) error
}

type AgentEvent struct {
	Kind    string        `json:"kind"`
	Message *AgentMessage `json:"message,omitempty"`
	Update  JSONValue     `json:"update,omitempty"`
}

type DeferredHandle struct {
	Provider string    `json:"provider"`
	ModelID  string    `json:"modelId"`
	ID       string    `json:"id"`
	Data     JSONValue `json:"data,omitempty"`
}

type DeferredResponse struct {
	Kind    string          `json:"kind"` // pending, ready, or error
	Handle  *DeferredHandle `json:"handle,omitempty"`
	Message *AgentMessage   `json:"message,omitempty"`
	Error   string          `json:"error,omitempty"`
}

type AgentTool struct {
	Name        string
	Description string
	Parameters  JSONValue
	Replay      ReplayPolicy
	Execute     func(context.Context, string, map[string]JSONValue, context.Context, func(AgentToolResult)) (AgentToolResult, error)
}

type ReplayPolicy string

const (
	ReplayNever ReplayPolicy = "never"
	ReplaySafe  ReplayPolicy = "safe"
)
