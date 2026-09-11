package harness

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

type deferredFetch struct {
	response DeferredResponse
	err      error
}

type deferredModels struct {
	mu      sync.Mutex
	initial AgentMessage
	fetches []deferredFetch
	options []AgentHarnessStreamOptions
	handles []DeferredHandle
	cancels []DeferredHandle
}

func (m *deferredModels) Resolve(context.Context, string, string) (Model, error) {
	return Model{Provider: "provider", ModelID: "model"}, nil
}

func (m *deferredModels) Stream(context.Context, Model, []Message, AgentHarnessStreamOptions) (<-chan AgentEvent, error) {
	stream := make(chan AgentEvent, 1)
	message := m.initial
	stream <- AgentEvent{Kind: "done", Message: &message}
	close(stream)
	return stream, nil
}

func (m *deferredModels) FetchDeferred(_ context.Context, _ Model, handle DeferredHandle, options AgentHarnessStreamOptions) (DeferredResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handles = append(m.handles, handle)
	m.options = append(m.options, options)
	if len(m.fetches) == 0 {
		return DeferredResponse{}, fmt.Errorf("unexpected deferred poll")
	}
	next := m.fetches[0]
	m.fetches = m.fetches[1:]
	return next.response, next.err
}

func (m *deferredModels) CancelDeferred(_ context.Context, _ Model, handle DeferredHandle) error {
	m.mu.Lock()
	m.cancels = append(m.cancels, handle)
	m.mu.Unlock()
	return nil
}

func (m *deferredModels) pollOptions() []AgentHarnessStreamOptions {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]AgentHarnessStreamOptions(nil), m.options...)
}

func (m *deferredModels) cancelledHandles() []DeferredHandle {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DeferredHandle(nil), m.cancels...)
}

func deferredPending(handle DeferredHandle, content JSONValue) DeferredResponse {
	message := AgentMessage{Role: "assistant", Content: content, StopReason: StopReasonDeferred}
	return DeferredResponse{Kind: "pending", Handle: &handle, Message: &message}
}

func deferredReady(content JSONValue) DeferredResponse {
	message := AgentMessage{Role: "assistant", Content: content, StopReason: StopReasonStop, Usage: &Usage{Input: 2, Output: 3, Total: 5}}
	return DeferredResponse{Kind: "ready", Message: &message}
}

func newDeferredTestHarness(t *testing.T, models *deferredModels, options AgentHarnessStreamOptions) (*Harness, Session) {
	t.Helper()
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	harness, suspended, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}, StreamOptions: options})
	if err != nil || len(suspended) != 0 {
		t.Fatalf("harness creation failed: %v %+v", err, suspended)
	}
	return harness, session
}

func TestHarnessDeferredPollsOnceAndUsesCapturedSourceAndOptions(t *testing.T) {
	handle := DeferredHandle{Provider: "provider", ModelID: "model", ID: "request", Data: map[string]JSONValue{"token": "a"}}
	models := &deferredModels{
		initial: deferredMessage(handle),
		fetches: []deferredFetch{
			{response: deferredPending(handle, "waiting")},
			{response: deferredReady("ready")},
		},
	}
	base := AgentHarnessStreamOptions{TimeoutMs: 17, Headers: map[string]string{"x-test": "captured"}, Deferred: true}
	harness, session := newDeferredTestHarness(t, models, base)
	ctx := context.Background()
	var deferredAttempts []int64
	if _, err := harness.Hooks().On(HookBeforeRequest, func(_ context.Context, invocation HookInvocation) (JSONValue, error) {
		event := invocation.Event.(map[string]JSONValue)
		if event["step"] == "deferred" {
			deferredAttempts = append(deferredAttempts, event["attempt"].(int64))
		}
		return nil, nil
	}, "r7-deferred-request"); err != nil {
		t.Fatal(err)
	}
	first, err := harness.Prompt(ctx, PromptInput{Text: "prompt"})
	if err != nil || !first.OK || first.Value.Kind != "suspended" {
		t.Fatalf("initial deferred result = %v %+v", err, first)
	}
	if err := harness.SetStreamOptions(ctx, AgentHarnessStreamOptions{TimeoutMs: 99, Headers: map[string]string{"x-test": "current"}}); err != nil {
		t.Fatal(err)
	}
	one, err := harness.Resume(ctx)
	if err != nil || !one.OK || one.Value.Run == nil || one.Value.Run.Kind != "suspended" || one.Value.Run.Reason != "deferred" {
		t.Fatalf("pending deferred result = %v %+v", err, one)
	}
	restored, err := Restore(ctx, session, "main")
	if err != nil || restored.Current == nil || restored.Current.State.Run.Phase.Deferred == nil {
		t.Fatalf("pending deferred state = %v %+v", err, restored)
	}
	state := restored.Current.State.Run.Phase.Deferred
	if state.Poll != 1 || first.Value.FinalEntryID == nil || state.SourceEntryID == *first.Value.FinalEntryID {
		t.Fatalf("pending deferred state = %+v", state)
	}
	entries, err := session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 3 || entries[2].ParentID == nil || *entries[2].ParentID != *first.Value.FinalEntryID {
		t.Fatalf("deferred lineage = %v %+v", err, entries)
	}
	two, err := harness.Resume(ctx)
	if err != nil || !two.OK || two.Value.Run == nil || two.Value.Run.Kind != "completed" {
		t.Fatalf("ready deferred result = %v %+v", err, two)
	}
	options := models.pollOptions()
	if len(options) != 2 || options[0].TimeoutMs != 17 || options[1].TimeoutMs != 17 || options[0].Deferred != false || options[1].Deferred != false || options[0].Headers["x-test"] != "captured" {
		t.Fatalf("captured poll options = %+v", options)
	}
	if len(deferredAttempts) != 2 || deferredAttempts[0] != 1 || deferredAttempts[1] != 2 {
		t.Fatalf("deferred request attempts = %+v", deferredAttempts)
	}
}

func TestHarnessDeferredMismatchBecomesDurableErrorWithoutRetry(t *testing.T) {
	first := DeferredHandle{Provider: "provider", ModelID: "model", ID: "first"}
	other := DeferredHandle{Provider: "provider", ModelID: "model", ID: "other"}
	models := &deferredModels{initial: deferredMessage(first), fetches: []deferredFetch{{response: deferredPending(other, "wrong")}}}
	harness, session := newDeferredTestHarness(t, models, AgentHarnessStreamOptions{})
	ctx := context.Background()
	if result, err := harness.Prompt(ctx, PromptInput{Text: "prompt"}); err != nil || !result.OK || result.Value.Kind != "suspended" {
		t.Fatalf("initial result = %v %+v", err, result)
	}
	result, err := harness.Resume(ctx)
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "failed" {
		t.Fatalf("mismatch result = %v %+v", err, result)
	}
	entries, err := session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 3 || entries[2].Message == nil || entries[2].Message.StopReason != StopReasonError {
		t.Fatalf("mismatch entry = %v %+v", err, entries)
	}
	if content, ok := entries[2].Message.Content.(string); !ok || content == "" {
		t.Fatalf("mismatch error content = %#v", entries[2].Message.Content)
	}
	if len(models.pollOptions()) != 1 {
		t.Fatalf("mismatch poll count = %d", len(models.pollOptions()))
	}
}

func TestHarnessDeferredEffectPendingRecoveryUsesSamePollNumber(t *testing.T) {
	handle := DeferredHandle{Provider: "provider", ModelID: "model", ID: "recovered"}
	models := &deferredModels{fetches: []deferredFetch{{response: deferredPending(handle, "still waiting")}}}
	harness, session := newDeferredTestHarness(t, models, AgentHarnessStreamOptions{TimeoutMs: 23})
	ctx := context.Background()
	source, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "assistant", Content: handle, StopReason: StopReasonDeferred})
	if err != nil {
		t.Fatal(err)
	}
	deferred := Deferred{Status: DeferredEffectPending, StepID: "step", SourceEntryID: source, Poll: 4, ResponseEntryID: session.IDGenerator().Next(), UsageID: session.IDGenerator().Next(), Configuration: LaneConfiguration{Model: Model{Provider: "provider", ModelID: "model"}}, StreamOptions: AgentHarnessStreamOptions{TimeoutMs: 23}}
	seedRunOperation(t, session, RunPhase{Kind: PhaseDeferred, Deferred: &deferred}, &source)
	result, err := harness.Resume(ctx)
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "suspended" {
		t.Fatalf("recovered deferred result = %v %+v", err, result)
	}
	entries, err := session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 2 || entries[1].Message == nil {
		t.Fatalf("recovered entries = %v %+v", err, entries)
	}
	restored, err := Restore(ctx, session, "main")
	if err != nil || restored.Current == nil || restored.Current.State.Run.Phase.Deferred.Poll != 4 {
		t.Fatalf("recovered state = %v %+v", err, restored)
	}
}

func TestHarnessDeferredCancellationSettlesReservedEffectPending(t *testing.T) {
	handle := DeferredHandle{Provider: "provider", ModelID: "model", ID: "cancel-me"}
	models := &deferredModels{}
	harness, session := newDeferredTestHarness(t, models, AgentHarnessStreamOptions{})
	ctx := context.Background()
	source, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "assistant", Content: handle, StopReason: StopReasonDeferred})
	if err != nil {
		t.Fatal(err)
	}
	responseID, usageID := session.IDGenerator().Next(), session.IDGenerator().Next()
	deferred := Deferred{Status: DeferredEffectPending, StepID: "step", SourceEntryID: source, Poll: 1, ResponseEntryID: responseID, UsageID: usageID, Configuration: LaneConfiguration{Model: Model{Provider: "provider", ModelID: "model"}}}
	seedRunOperation(t, session, RunPhase{Kind: PhaseDeferred, Deferred: &deferred}, &source)
	if result, err := harness.Abort(ctx); err != nil || !result.OK {
		t.Fatalf("abort = %v %+v", err, result)
	}
	result, err := harness.Resume(ctx)
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "aborted" {
		t.Fatalf("cancelled deferred result = %v %+v", err, result)
	}
	if len(models.cancelledHandles()) != 1 || models.cancelledHandles()[0] != handle {
		t.Fatalf("cancelled handles = %+v", models.cancelledHandles())
	}
	entries, err := session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 2 || entries[1].ID != responseID || entries[1].Message == nil || entries[1].Message.StopReason != StopReasonAborted {
		t.Fatalf("cancelled response = %v %+v", err, entries)
	}
}

func deferredMessage(handle DeferredHandle) AgentMessage {
	return AgentMessage{Role: "assistant", Content: handle, StopReason: StopReasonDeferred}
}
