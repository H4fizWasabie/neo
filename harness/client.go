package harness

import "context"

// AgentClient is the deliberately small one-lane boundary used by the dev TUI.
type AgentClient interface {
	Snapshot(context.Context) (LaneSnapshot, error)
	Watch(context.Context) (WatchHandle[LaneSnapshot], error)
	Prompt(context.Context, PromptInput) (Result[RunOutcome, error], error)
	Steer(context.Context, PromptInput) (Result[QueueOutcome, error], error)
	FollowUp(context.Context, PromptInput) (Result[QueueOutcome, error], error)
	Abort(context.Context) (Result[AbortOutcome, error], error)
	Resume(context.Context) (Result[ResumeOutcome, error], error)
	CancelQueued(context.Context, string) (Result[CancelQueuedOutcome, error], error)
	LastResult(context.Context) (*LaneLastResult, error)
}

type laneAgentClient struct{ lane AgentLane }

func NewAgentClient(lane AgentLane) AgentClient { return &laneAgentClient{lane: lane} }

func (c *laneAgentClient) Snapshot(ctx context.Context) (LaneSnapshot, error) {
	handle, err := c.lane.Watch(ctx)
	if err != nil {
		return LaneSnapshot{}, err
	}
	handle.Unsubscribe()
	return handle.Snapshot, nil
}

func (c *laneAgentClient) Watch(ctx context.Context) (WatchHandle[LaneSnapshot], error) {
	return c.lane.Watch(ctx)
}
func (c *laneAgentClient) Prompt(ctx context.Context, input PromptInput) (Result[RunOutcome, error], error) {
	return c.lane.Prompt(ctx, input)
}
func (c *laneAgentClient) Steer(ctx context.Context, input PromptInput) (Result[QueueOutcome, error], error) {
	return c.lane.Steer(ctx, input)
}
func (c *laneAgentClient) FollowUp(ctx context.Context, input PromptInput) (Result[QueueOutcome, error], error) {
	return c.lane.FollowUp(ctx, input)
}
func (c *laneAgentClient) Abort(ctx context.Context) (Result[AbortOutcome, error], error) {
	return c.lane.Abort(ctx)
}
func (c *laneAgentClient) Resume(ctx context.Context) (Result[ResumeOutcome, error], error) {
	return c.lane.Resume(ctx)
}
func (c *laneAgentClient) CancelQueued(ctx context.Context, id string) (Result[CancelQueuedOutcome, error], error) {
	return c.lane.CancelQueued(ctx, id)
}
func (c *laneAgentClient) LastResult(ctx context.Context) (*LaneLastResult, error) {
	return c.lane.GetLastResult(ctx)
}
