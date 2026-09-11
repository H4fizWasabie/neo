package harness

import "context"

type AgentEventSink func(context.Context, AgentEvent)

type StreamAssistantConfig struct {
	Model              Model
	ThinkingLevel      ThinkingLevel
	SystemPrompt       string
	Tools              []AgentHarnessTool
	TransformContext   func(context.Context, []AgentMessage) ([]AgentMessage, error)
	ToProviderMessages func([]AgentMessage) ([]Message, error)
	Models             Models
	StreamOptions      AgentHarnessStreamOptions
	TransformPayload   func(context.Context, JSONValue, Model) (JSONValue, error)
	TransformResponse  func(context.Context, AgentMessage, map[string]string) (AgentMessage, error)
	Telemetry          TelemetryContext
}

type PreparedToolCall struct {
	Call ToolCall
	Tool AgentHarnessTool
	Args map[string]JSONValue
}

type ImmediateOutcome struct {
	Result    AgentToolResult
	IsError   bool
	Terminate bool
}

type FinalizedToolCall struct {
	Call      ToolCall
	Result    AgentToolResult
	IsError   bool
	Terminate bool
}

type ToolCallbacks struct {
	BeforeToolCall func(context.Context, ToolCall, map[string]JSONValue) (JSONValue, error)
	AfterToolCall  func(context.Context, ToolCall, map[string]JSONValue, AgentToolResult, bool) (JSONValue, error)
	ExecuteTool    func(context.Context, PreparedToolCall) (AgentToolResult, bool, error)
	OnToolStart    func(context.Context, ToolCall, map[string]JSONValue) error
	OnToolResult   func(context.Context, ToolCall, ToolResultMessage, bool) error
}
