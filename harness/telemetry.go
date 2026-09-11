package harness

import "context"

type AttributeValue any

type SpanStatus string

const (
	SpanOK    SpanStatus = "ok"
	SpanError SpanStatus = "error"
)

type SpanOptions struct {
	Name       string
	Attributes map[string]AttributeValue
}

type TelemetrySpan interface {
	SetAttributes(map[string]AttributeValue)
	SetStatus(SpanStatus, string)
	AddEvent(string, map[string]AttributeValue)
	End()
}

type TelemetryContext interface {
	StartSpan(context.Context, SpanOptions, func(TelemetrySpan) error) error
}

const (
	SpanAIRequest           = "theoses.ai.request"
	SpanHarnessRun          = "theoses.harness.run"
	SpanHarnessCompaction   = "theoses.harness.compaction"
	SpanHarnessNavigation   = "theoses.harness.navigation"
	SpanHarnessCheckpoint   = "theoses.harness.checkpoint"
	SpanHarnessTurn         = "theoses.harness.turn"
	SpanHarnessStep         = "theoses.harness.step"
	SpanHarnessTool         = "theoses.harness.tool"
	SpanHarnessHook         = "theoses.harness.hook"
	SpanHarnessSleep        = "theoses.harness.sleep"
	SpanHarnessEventHandler = "theoses.harness.event_handler"
	SpanSessionWrite        = "theoses.session.write"
)
