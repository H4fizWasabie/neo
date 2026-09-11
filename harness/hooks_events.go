package harness

import "context"

type HarnessEvent struct {
	Type     string    `json:"type"`
	Lane     string    `json:"lane,omitempty"`
	Recovery bool      `json:"recovery,omitempty"`
	Payload  JSONValue `json:"payload,omitempty"`
}

type HarnessEventType string

const (
	EventRunStart        HarnessEventType = "run_start"
	EventRunResume       HarnessEventType = "run_resume"
	EventRunSuspend      HarnessEventType = "run_suspend"
	EventRunAbort        HarnessEventType = "run_abort"
	EventRunEnd          HarnessEventType = "run_end"
	EventFault           HarnessEventType = "fault"
	EventHandlerError    HarnessEventType = "handler_error"
	EventTurnStart       HarnessEventType = "turn_start"
	EventTurnEnd         HarnessEventType = "turn_end"
	EventRetryScheduled  HarnessEventType = "retry_scheduled"
	EventRetryStart      HarnessEventType = "retry_start"
	EventRetryEnd        HarnessEventType = "retry_end"
	EventMessageStart    HarnessEventType = "message_start"
	EventMessageUpdate   HarnessEventType = "message_update"
	EventMessageEnd      HarnessEventType = "message_end"
	EventToolStart       HarnessEventType = "tool_start"
	EventToolUpdate      HarnessEventType = "tool_update"
	EventToolEnd         HarnessEventType = "tool_end"
	EventEntryAdded      HarnessEventType = "entry_added"
	EventWritePending    HarnessEventType = "write_pending"
	EventQueueUpdate     HarnessEventType = "queue_update"
	EventFactUpdate      HarnessEventType = "fact_update"
	EventConfigUpdate    HarnessEventType = "config_update"
	EventCompactionStart HarnessEventType = "compaction_start"
	EventCompactionEnd   HarnessEventType = "compaction_end"
	EventNavigationStart HarnessEventType = "navigation_start"
	EventNavigationEnd   HarnessEventType = "navigation_end"
	EventLaneCreated     HarnessEventType = "lane_created"
	EventUsage           HarnessEventType = "usage"
)

type Events interface {
	On(eventType string, listener func(context.Context, HarnessEvent)) (unsubscribe func())
}

type HookName string

const (
	HookBeforeRun        HookName = "before_run"
	HookBeforeResume     HookName = "before_resume"
	HookBeforeRunEnd     HookName = "before_run_end"
	HookTransformContext HookName = "transform_context"
	HookBeforeRequest    HookName = "before_request"
	HookBeforePayload    HookName = "before_payload"
	HookAfterResponse    HookName = "after_response"
	HookBeforeTool       HookName = "before_tool"
	HookAfterTool        HookName = "after_tool"
	HookBeforeCompaction HookName = "before_compaction"
	HookBeforeNavigation HookName = "before_navigation"
)

type BeforeResumePrepared struct {
	Kind                 OperationKind  `json:"kind"`
	Prompt               []AgentMessage `json:"prompt,omitempty"`
	SystemPromptOverride string         `json:"systemPromptOverride,omitempty"`
	SourceLeafID         *string        `json:"sourceLeafId,omitempty"`
	TargetID             *string        `json:"targetId,omitempty"`
	Summarize            bool           `json:"summarize,omitempty"`
	Label                string         `json:"label,omitempty"`
	CustomInstructions   string         `json:"customInstructions,omitempty"`
}

type HookInvocation struct {
	Name  HookName  `json:"name"`
	Lane  string    `json:"lane"`
	RunID string    `json:"runId"`
	Event JSONValue `json:"event,omitempty"`
}

type HookHandler func(context.Context, HookInvocation) (JSONValue, error)

type Hooks interface {
	On(name HookName, handler HookHandler, id string) (unsubscribe func(), err error)
}
