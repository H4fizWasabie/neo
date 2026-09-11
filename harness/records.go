package harness

type LaneRecord struct {
	Type        string    `json:"type"`
	ID          string    `json:"id"`
	Seq         int64     `json:"seq"`
	Lane        string    `json:"lane"`
	Timestamp   int64     `json:"timestamp"`
	RunID       string    `json:"runId,omitempty"`
	OperationID string    `json:"operationId,omitempty"`
	Payload     JSONValue `json:"payload,omitempty"`
}

type OperationStartedRecord = LaneRecord
type AbortRequestedRecord = LaneRecord
type OperationFinishedRecord = LaneRecord
type StepAttemptRecord = LaneRecord
type ToolStartedRecord = LaneRecord
type QueueEnqueuedRecord = LaneRecord
type QueueCancelledRecord = LaneRecord
type WriteDeferredRecord = LaneRecord
type UsageRecord = LaneRecord
