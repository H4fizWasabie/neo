package harness

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type scriptedModels struct {
	mu       sync.Mutex
	streamed []Message
	calls    int
}

type retryModels struct {
	mu         sync.Mutex
	model      Model
	outcomes   []retryOutcome
	requests   [][]Message
	resolveErr error
	calls      int
	cancelled  int
}

type blockingModels struct {
	started chan struct{}
	once    sync.Once
}

func (*blockingModels) Resolve(context.Context, string, string) (Model, error) {
	return Model{Provider: "provider", ModelID: "model"}, nil
}

func (m *blockingModels) Stream(ctx context.Context, _ Model, _ []Message, _ AgentHarnessStreamOptions) (<-chan AgentEvent, error) {
	m.once.Do(func() { close(m.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*blockingModels) FetchDeferred(context.Context, Model, DeferredHandle, AgentHarnessStreamOptions) (DeferredResponse, error) {
	return DeferredResponse{}, nil
}

func (*blockingModels) CancelDeferred(context.Context, Model, DeferredHandle) error { return nil }

type retryOutcome struct {
	err     error
	message AgentMessage
}

type namedTool struct {
	calls int
}

func (*namedTool) Name() string         { return "echo" }
func (*namedTool) Description() string  { return "echoes input" }
func (*namedTool) Replay() ReplayPolicy { return ReplaySafe }
func (t *namedTool) Execute(_ context.Context, _ string, args map[string]JSONValue, _ any, _ func(AgentToolResult)) (AgentToolResult, error) {
	t.calls++
	return AgentToolResult{Content: args["value"]}, nil
}

type toolModels struct {
	mu    sync.Mutex
	calls int
}

func (*toolModels) Resolve(context.Context, string, string) (Model, error) {
	return Model{Provider: "provider", ModelID: "model"}, nil
}

func (m *toolModels) Stream(_ context.Context, _ Model, _ []Message, _ AgentHarnessStreamOptions) (<-chan AgentEvent, error) {
	m.mu.Lock()
	m.calls++
	call := m.calls
	m.mu.Unlock()
	message := AgentMessage{Role: "assistant", StopReason: StopReasonStop, Content: "done"}
	if call == 1 {
		message.StopReason = StopReasonToolUse
		message.ToolCalls = []AgentToolCall{{ID: "call-1", Name: "echo", Arguments: map[string]JSONValue{"value": "tool result"}}}
	}
	stream := make(chan AgentEvent, 1)
	stream <- AgentEvent{Kind: "done", Message: &message}
	close(stream)
	return stream, nil
}
func (*toolModels) FetchDeferred(context.Context, Model, DeferredHandle, AgentHarnessStreamOptions) (DeferredResponse, error) {
	return DeferredResponse{}, nil
}
func (*toolModels) CancelDeferred(context.Context, Model, DeferredHandle) error { return nil }

func (m *retryModels) Resolve(context.Context, string, string) (Model, error) {
	if m.resolveErr != nil {
		return Model{}, m.resolveErr
	}
	if m.model.Provider != "" {
		return m.model, nil
	}
	return Model{Provider: "provider", ModelID: "model"}, nil
}

func (m *retryModels) Stream(_ context.Context, _ Model, messages []Message, _ AgentHarnessStreamOptions) (<-chan AgentEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	index := m.calls - 1
	if index >= len(m.outcomes) {
		return nil, fmt.Errorf("unexpected provider call %d", m.calls)
	}
	outcome := m.outcomes[index]
	m.requests = append(m.requests, append([]Message(nil), messages...))
	if outcome.err != nil {
		return nil, outcome.err
	}
	stream := make(chan AgentEvent, 1)
	message := outcome.message
	stream <- AgentEvent{Kind: "done", Message: &message}
	close(stream)
	return stream, nil
}

func (*retryModels) FetchDeferred(_ context.Context, _ Model, handle DeferredHandle, _ AgentHarnessStreamOptions) (DeferredResponse, error) {
	message := AgentMessage{Role: "assistant", Content: handle, StopReason: StopReasonDeferred}
	return DeferredResponse{Kind: "pending", Handle: &handle, Message: &message}, nil
}

func (m *retryModels) CancelDeferred(context.Context, Model, DeferredHandle) error {
	m.mu.Lock()
	m.cancelled++
	m.mu.Unlock()
	return nil
}

func (m *retryModels) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *retryModels) Cancelled() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cancelled
}

func (m *scriptedModels) Resolve(context.Context, string, string) (Model, error) {
	return Model{Provider: "provider", ModelID: "model"}, nil
}

func (m *scriptedModels) Stream(_ context.Context, _ Model, messages []Message, _ AgentHarnessStreamOptions) (<-chan AgentEvent, error) {
	m.mu.Lock()
	m.calls++
	m.streamed = append([]Message(nil), messages...)
	m.mu.Unlock()
	result := make(chan AgentEvent, 1)
	result <- AgentEvent{Kind: "done", Message: &AgentMessage{Role: "assistant", Content: "answer", StopReason: StopReasonStop, Usage: &Usage{Input: 3, Output: 2, Total: 5}}}
	close(result)
	return result, nil
}

func (*scriptedModels) FetchDeferred(context.Context, Model, DeferredHandle, AgentHarnessStreamOptions) (DeferredResponse, error) {
	return DeferredResponse{}, nil
}

func (*scriptedModels) CancelDeferred(context.Context, Model, DeferredHandle) error { return nil }

func (m *scriptedModels) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func TestHarnessRunsNoToolPromptAndCleansUpTerminalState(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	models := &scriptedModels{}
	harness, suspended, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}})
	if err != nil || len(suspended) != 0 {
		t.Fatalf("harness creation failed: %v %+v", err, suspended)
	}
	var mu sync.Mutex
	var events []string
	harness.Events().On("*", func(_ context.Context, event HarnessEvent) {
		mu.Lock()
		events = append(events, event.Type)
		mu.Unlock()
	})
	harness.Hooks().On(HookBeforeRun, func(_ context.Context, invocation HookInvocation) (JSONValue, error) {
		return map[string]JSONValue{"messages": []AgentMessage{{Role: "user", Content: "injected"}}}, nil
	}, "inject")
	result, err := harness.Prompt(ctx, PromptInput{Text: "prompt"})
	if err != nil || !result.OK || result.Value.Kind != "completed" {
		t.Fatalf("prompt failed: %v %+v", err, result)
	}
	if models.Calls() != 1 {
		t.Fatalf("provider call count = %d, want 1", models.Calls())
	}
	models.mu.Lock()
	streamed := append([]Message(nil), models.streamed...)
	models.mu.Unlock()
	if len(streamed) != 2 || streamed[0].Content != "prompt" || streamed[1].Content != "injected" {
		t.Fatalf("captured prompt = %+v", streamed)
	}
	last, err := harness.GetLastResult(ctx)
	if err != nil || last == nil || last.Outcome != "completed" {
		t.Fatalf("missing last result: %v %+v", err, last)
	}
	if register, err := session.GetRegister(ctx, RegisterOpState, last.OperationID); err != nil {
		t.Fatal(err)
	} else if register != nil {
		t.Fatalf("operation state survived terminal commit: %+v", register)
	}
	mu.Lock()
	gotEvents := append([]string(nil), events...)
	mu.Unlock()
	want := []string{string(EventRunStart), string(EventTurnStart), string(EventMessageStart), string(EventMessageUpdate), string(EventMessageEnd), string(EventEntryAdded), string(EventUsage), string(EventTurnEnd), string(EventRunEnd)}
	if len(gotEvents) != len(want) {
		t.Fatalf("event count = %v, want %v", gotEvents, want)
	}
	for i := range want {
		if gotEvents[i] != want[i] {
			t.Fatalf("event order = %v, want %v", gotEvents, want)
		}
	}
}

func TestHarnessManualDriveParksBeforeProvider(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	models := &scriptedModels{}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}, Drive: "manual"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan Result[RunOutcome, error], 1)
	go func() {
		result, _ := harness.Prompt(ctx, PromptInput{Text: "prompt"})
		done <- result
	}()
	waitForAction(t, harness)
	if calls := models.Calls(); calls != 0 {
		t.Fatalf("provider ran while manual action was parked: %d", calls)
	}
	for i := 0; i < 12; i++ {
		if _, err := harness.ExecuteAction(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case result := <-done:
			if !result.OK {
				t.Fatalf("manual prompt failed: %+v", result)
			}
			if models.Calls() != 1 {
				t.Fatalf("provider calls = %d, want 1", models.Calls())
			}
			return
		default:
		}
		select {
		case result := <-done:
			if !result.OK {
				t.Fatalf("manual prompt failed: %+v", result)
			}
			if models.Calls() != 1 {
				t.Fatalf("provider calls = %d, want 1", models.Calls())
			}
			return
		default:
			if waitForActionOrDone(t, harness, done) {
				return
			}
		}
	}
	t.Fatal("manual prompt did not complete")
}

func TestHarnessExecutesToolBatchAndContinues(t *testing.T) {
	tool := &namedTool{}
	models := &toolModels{}
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}, ActiveToolNames: []string{"echo"}, Tools: []AgentHarnessTool{tool}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := harness.Prompt(ctx, PromptInput{Text: "prompt"})
	if err != nil || !result.OK || result.Value.Kind != "completed" {
		t.Fatalf("tool run = %v %+v", err, result)
	}
	if tool.calls != 1 {
		t.Fatalf("tool calls = %d, want 1", tool.calls)
	}
	entries, err := session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 4 || entries[2].Message == nil || entries[2].Message.Role != "tool" {
		t.Fatalf("tool entries = %v %+v", err, entries)
	}
	if registers, err := session.ListRegisters(ctx, RegisterOpToolArgs, ""); err != nil || len(registers) != 0 {
		t.Fatalf("tool args registers = %v %+v", err, registers)
	}
}

func newRetryHarness(t *testing.T, models Models, retry RetryPolicy) (*Harness, Session) {
	t.Helper()
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	harness, suspended, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}, Retry: retry})
	if err != nil || len(suspended) != 0 {
		t.Fatalf("harness creation failed: %v %+v", err, suspended)
	}
	return harness, session
}

func seedRunOperation(t *testing.T, session Session, phase RunPhase, latest *string) string {
	t.Helper()
	ctx := context.Background()
	source := latest
	if source == nil {
		id, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "user", Content: "prompt"})
		if err != nil {
			t.Fatal(err)
		}
		source = &id
	}
	configRegister, err := session.GetRegister(ctx, RegisterLaneConfig, "main")
	if err != nil {
		t.Fatal(err)
	}
	config := configRegister.Value.(LaneConfiguration)
	if phase.Generation != nil {
		if phase.Generation.Context.TriggerEntryID == "" {
			phase.Generation.Context.TriggerEntryID = *source
		}
		phase.Generation.Context.Configuration = config
	}
	opID := session.IDGenerator().Next()
	state := OperationState{Kind: OperationRun, Run: &RunState{Kind: OperationRun, Control: Control{Status: ControlRunning}, Phase: phase, Inbox: Inbox{}, LatestAssistantEntryID: latest}}
	op := Operation{OperationID: opID, Lane: "main", SourceLeafID: cloneStringPointer(source), StartedAt: time.Now().UnixMilli(), Intent: OperationIntent{Kind: OperationRun, PromptEntryIDs: []string{*source}}}
	laneState := LaneState{CurrentOperationID: &opID, PendingNextRun: []string{}}
	if _, err := session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpMeta, opID, op), registerSet(RegisterOpState, opID, state), registerSet(RegisterLaneLeaf, "main", source), registerSet(RegisterLaneState, "main", laneState)}}); err != nil {
		t.Fatal(err)
	}
	return opID
}

func TestHarnessRetriesProviderErrorsWithCapturedPolicy(t *testing.T) {
	models := &retryModels{outcomes: []retryOutcome{{err: fmt.Errorf("temporary")}, {message: AgentMessage{Role: "assistant", Content: "answer", StopReason: StopReasonStop}}}}
	harness, session := newRetryHarness(t, models, RetryPolicy{Enabled: true, MaxRetries: 1})
	result, err := harness.Prompt(context.Background(), PromptInput{Text: "prompt"})
	if err != nil || !result.OK || result.Value.Kind != "completed" {
		t.Fatalf("retry failed: %v %+v", err, result)
	}
	if models.Calls() != 2 {
		t.Fatalf("provider calls = %d, want 2", models.Calls())
	}
	entries, err := session.FindEntries(context.Background(), EntryQuery{Order: OldestFirst})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 || entries[1].Message == nil || entries[1].Message.StopReason != StopReasonError {
		t.Fatalf("retry ledger = %+v", entries)
	}
}

func TestHarnessWatchBuffersEventsAfterSnapshot(t *testing.T) {
	harness, session := newRetryHarness(t, &retryModels{outcomes: []retryOutcome{{message: AgentMessage{Role: "assistant", Content: "answer", StopReason: StopReasonStop}}}}, RetryPolicy{})
	watch, err := harness.Watch(context.Background())
	if err != nil || len(watch.Snapshot.Transcript) != 0 {
		t.Fatalf("watch snapshot = %v %+v", err, watch.Snapshot)
	}
	if result, err := harness.Prompt(context.Background(), PromptInput{Text: "prompt"}); err != nil || !result.OK {
		t.Fatalf("watched prompt = %v %+v", err, result)
	}
	var events []string
	watch.Start(func(event HarnessEvent) { events = append(events, event.Type) })
	watch.Unsubscribe()
	if len(events) == 0 || events[0] != string(EventRunStart) || events[len(events)-1] != string(EventRunEnd) {
		t.Fatalf("watch event order = %+v", events)
	}
	if entries, err := session.FindEntries(context.Background(), EntryQuery{Order: OldestFirst}); err != nil || len(entries) != 2 {
		t.Fatalf("watched entries = %v %+v", err, entries)
	}
}

func TestHarnessRestoresAllPersistedLanes(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	config := LaneConfiguration{Model: Model{Provider: "provider", ModelID: "model"}, ThinkingLevel: ThinkingLow, ActiveToolNames: []string{}}
	if err := session.(*MemorySession).CreateLane(ctx, "branch", nil, config); err != nil {
		t.Fatal(err)
	}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: &scriptedModels{}, Model: config.Model})
	if err != nil {
		t.Fatal(err)
	}
	lanes, err := harness.Lanes(ctx)
	if err != nil || len(lanes) != 2 || lanes[0].Name != "branch" || lanes[1].Name != "main" {
		t.Fatalf("restored lanes = %v %+v", err, lanes)
	}
	if _, err := harness.Lane(ctx, "branch"); err != nil {
		t.Fatalf("restored branch lookup = %v", err)
	}
}

func TestHarnessPreservesProviderErrorResponseAndUsage(t *testing.T) {
	models := &retryModels{outcomes: []retryOutcome{{message: AgentMessage{Role: "assistant", Content: "quota exceeded", StopReason: StopReasonError, Usage: &Usage{Input: 4, Output: 1, Total: 5}}}}}
	harness, session := newRetryHarness(t, models, RetryPolicy{})
	result, err := harness.Prompt(context.Background(), PromptInput{Text: "prompt"})
	if err != nil || !result.OK || result.Value.Kind != "failed" || result.Value.FinalMessage == nil || result.Value.FinalMessage.Content != "quota exceeded" {
		t.Fatalf("provider error result = %v %+v", err, result)
	}
	entries, err := session.FindEntries(context.Background(), EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 2 || entries[1].Message == nil || entries[1].Message.Content != "quota exceeded" {
		t.Fatalf("provider error entry = %v %+v", err, entries)
	}
}

func TestHarnessDoesNotRetryMalformedAssistantResponse(t *testing.T) {
	models := &retryModels{outcomes: []retryOutcome{{message: AgentMessage{Role: "user", StopReason: StopReasonStop}}}}
	harness, _ := newRetryHarness(t, models, RetryPolicy{Enabled: true, MaxRetries: 3})
	result, err := harness.Prompt(context.Background(), PromptInput{Text: "prompt"})
	if err != nil || !result.OK || result.Value.Kind != "failed" {
		t.Fatalf("malformed response = %v %+v", err, result)
	}
	if models.Calls() != 1 {
		t.Fatalf("malformed response calls = %d, want 1", models.Calls())
	}
}

func TestRetryDelaySaturatesAtSafeInteger(t *testing.T) {
	if got := retryDelay(NormalizedRetryPolicy{BaseDelayMs: maxSafeInteger}, AgentHarnessStreamOptions{}, 1); got != maxSafeInteger {
		t.Fatalf("base delay = %d, want %d", got, maxSafeInteger)
	}
	if got := retryDelay(NormalizedRetryPolicy{BaseDelayMs: maxSafeInteger}, AgentHarnessStreamOptions{}, 2); got != maxSafeInteger {
		t.Fatalf("exponential delay = %d, want %d", got, maxSafeInteger)
	}
	if got := retryNotBefore(maxSafeInteger-1, 10); got != maxSafeInteger {
		t.Fatalf("not-before = %d, want %d", got, maxSafeInteger)
	}
}

func TestHarnessRetryCapSettlesSyntheticFailure(t *testing.T) {
	models := &retryModels{outcomes: []retryOutcome{{err: fmt.Errorf("first")}, {err: fmt.Errorf("second")}}}
	harness, session := newRetryHarness(t, models, RetryPolicy{Enabled: true, MaxRetries: 1})
	result, err := harness.Prompt(context.Background(), PromptInput{Text: "prompt"})
	if err != nil || !result.OK || result.Value.Kind != "failed" {
		t.Fatalf("capped retry = %v %+v", err, result)
	}
	if models.Calls() != 2 {
		t.Fatalf("provider calls = %d, want 2", models.Calls())
	}
	last, err := harness.GetLastResult(context.Background())
	if err != nil || last == nil || last.Outcome != "failed" || last.RunCompletion != "" {
		t.Fatalf("last result = %v %+v", err, last)
	}
	if entries, err := session.FindEntries(context.Background(), EntryQuery{Order: OldestFirst}); err != nil || len(entries) != 3 {
		t.Fatalf("failed retry entries = %v %+v", err, entries)
	}
}

func TestHarnessReopensEffectPendingAtRetryCap(t *testing.T) {
	models := &retryModels{}
	harness, session := newRetryHarness(t, models, RetryPolicy{})
	responseID, usageID := session.IDGenerator().Next(), session.IDGenerator().Next()
	seedRunOperation(t, session, RunPhase{Kind: PhaseAssistant, Generation: &Generation{Status: GenerationEffectPending, Attempt: 1, ResponseEntryID: responseID, UsageID: usageID, Context: GenerationContext{RetryPolicy: NormalizedRetryPolicy{MaxAttempts: 1}}}}, nil)
	result, err := harness.Resume(context.Background())
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "failed" {
		t.Fatalf("recovered cap = %v %+v", err, result)
	}
	if models.Calls() != 0 {
		t.Fatalf("recovered cap provider calls = %d, want 0", models.Calls())
	}
}

func TestHarnessResumeReturnsMissingIdentitiesAfterAcceptance(t *testing.T) {
	models := &retryModels{resolveErr: fmt.Errorf("provider unavailable")}
	harness, session := newRetryHarness(t, models, RetryPolicy{})
	seedRunOperation(t, session, RunPhase{Kind: PhaseAssistant, Generation: &Generation{Status: GenerationReady, NextAttempt: 1, Context: GenerationContext{RetryPolicy: NormalizedRetryPolicy{MaxAttempts: 1}}}}, nil)
	result, err := harness.Resume(context.Background())
	if err != nil || result.OK {
		t.Fatalf("missing identity resume = %v %+v", err, result)
	}
	if _, ok := result.Err.(*MissingIdentities); !ok {
		t.Fatalf("missing identity error = %T %v", result.Err, result.Err)
	}
}

func TestHarnessReopensMayFinishWithoutProviderIdentity(t *testing.T) {
	models := &retryModels{resolveErr: fmt.Errorf("provider unavailable")}
	harness, session := newRetryHarness(t, models, RetryPolicy{})
	entryID, err := session.View("main").AppendMessage(context.Background(), AgentMessage{Role: "assistant", Content: "done", StopReason: StopReasonStop})
	if err != nil {
		t.Fatal(err)
	}
	seedRunOperation(t, session, RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationMayFinish}, TriggerEntryID: entryID}}, &entryID)
	result, err := harness.Resume(context.Background())
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "completed" {
		t.Fatalf("may-finish resume = %v %+v", err, result)
	}
}

func TestHarnessClassifiesDeferredAndLeavesOperationSuspended(t *testing.T) {
	handle := DeferredHandle{Provider: "provider", ModelID: "model", ID: "request"}
	models := &retryModels{outcomes: []retryOutcome{{message: AgentMessage{Role: "assistant", StopReason: StopReasonDeferred, Content: handle}}}}
	harness, session := newRetryHarness(t, models, RetryPolicy{})
	result, err := harness.Prompt(context.Background(), PromptInput{Text: "prompt"})
	if err != nil || !result.OK || result.Value.Kind != "suspended" || result.Value.Reason != "deferred" || result.Value.Deferred == nil {
		t.Fatalf("deferred result = %v %+v", err, result)
	}
	restored, err := Restore(context.Background(), session, "main")
	if err != nil || restored.Current == nil || restored.Current.State.Run.Phase.Kind != PhaseDeferred {
		t.Fatalf("deferred restore = %v %+v", err, restored)
	}
	resumed, err := harness.Resume(context.Background())
	if err != nil || !resumed.OK || resumed.Value.Run == nil || resumed.Value.Run.Kind != "suspended" || resumed.Value.Run.Reason != "deferred" {
		t.Fatalf("deferred resume = %v %+v", err, resumed)
	}
}

func TestHarnessRejectsProviderAbortedWhileControlRunning(t *testing.T) {
	models := &retryModels{outcomes: []retryOutcome{{message: AgentMessage{Role: "assistant", StopReason: StopReasonAborted}}}}
	harness, _ := newRetryHarness(t, models, RetryPolicy{})
	result, err := harness.Prompt(context.Background(), PromptInput{Text: "prompt"})
	if err != nil || result.OK || result.Err == nil {
		t.Fatalf("aborted classification = %v %+v", err, result)
	}
}

func TestHarnessReportsMissingModelIdentityBeforeAcceptance(t *testing.T) {
	models := &retryModels{resolveErr: fmt.Errorf("provider unavailable")}
	harness, session := newRetryHarness(t, models, RetryPolicy{})
	result, err := harness.Prompt(context.Background(), PromptInput{Text: "prompt"})
	if err != nil || result.OK {
		t.Fatalf("missing identity result = %v %+v", err, result)
	}
	entries, err := session.FindEntries(context.Background(), EntryQuery{Order: OldestFirst})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("pre-acceptance wrote entries: %+v", entries)
	}
}

func TestHarnessRecoversUnknownAssistantEffectAfterReopen(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	models := &retryModels{outcomes: []retryOutcome{{message: AgentMessage{Role: "assistant", Content: "recovered", StopReason: StopReasonStop}}}}
	first, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}, Retry: RetryPolicy{Enabled: true, MaxRetries: 1}})
	if err != nil {
		t.Fatal(err)
	}
	promptID, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "user", Content: "prompt"})
	if err != nil {
		t.Fatal(err)
	}
	opID := session.IDGenerator().Next()
	responseID := session.IDGenerator().Next()
	usageID := session.IDGenerator().Next()
	configRegister, err := session.GetRegister(ctx, RegisterLaneConfig, "main")
	if err != nil {
		t.Fatal(err)
	}
	config := configRegister.Value.(LaneConfiguration)
	state := OperationState{Kind: OperationRun, Run: &RunState{Kind: OperationRun, Control: Control{Status: ControlRunning}, Phase: RunPhase{Kind: PhaseAssistant, Generation: &Generation{Status: GenerationEffectPending, Attempt: 1, ResponseEntryID: responseID, UsageID: usageID, Context: GenerationContext{StepID: opID + ":step:1", TriggerEntryID: promptID, Configuration: config, RetryPolicy: NormalizedRetryPolicy{MaxAttempts: 2}}}}, Inbox: Inbox{}}}
	laneState := LaneState{CurrentOperationID: &opID, PendingNextRun: []string{}}
	op := Operation{OperationID: opID, Lane: "main", SourceLeafID: &promptID, StartedAt: time.Now().UnixMilli(), Intent: OperationIntent{Kind: OperationRun, PromptEntryIDs: []string{promptID}}}
	if _, err := session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpMeta, opID, op), registerSet(RegisterOpState, opID, state), registerSet(RegisterLaneLeaf, "main", &promptID), registerSet(RegisterLaneState, "main", laneState)}}); err != nil {
		t.Fatal(err)
	}
	first.cancel()
	first.scheduler.Close()
	second, suspended, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}, Retry: RetryPolicy{Enabled: true, MaxRetries: 1}})
	if err != nil || len(suspended) != 1 {
		t.Fatalf("reopen = %v %+v", err, suspended)
	}
	result, err := second.Resume(ctx)
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "completed" {
		t.Fatalf("recovered resume = %v %+v", err, result)
	}
	if models.Calls() != 1 {
		t.Fatalf("recovery provider calls = %d, want 1", models.Calls())
	}
}

func waitForAction(t *testing.T, harness *Harness) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if harness.scheduler.Peek() != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("manual action was not scheduled")
}

func waitForActionOrDone(t *testing.T, harness *Harness, done <-chan Result[RunOutcome, error]) bool {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if harness.scheduler.Peek() != nil {
			return false
		}
		select {
		case result := <-done:
			if !result.OK {
				t.Fatalf("manual prompt failed: %+v", result)
			}
			return true
		default:
			time.Sleep(time.Millisecond)
		}
	}
	t.Fatal("manual action was not scheduled")
	return false
}

func TestHarnessNextRunCaptureAndCancellation(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	models := &scriptedModels{}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}})
	if err != nil {
		t.Fatal(err)
	}
	lane := harness.runtimeLane
	queued, err := lane.NextRun(ctx, PromptInput{Text: "cancel me"})
	if err != nil || !queued.OK || queued.Value.EntryID == "" {
		t.Fatalf("next-run = %v %+v", err, queued)
	}
	cancelled, err := lane.CancelQueued(ctx, queued.Value.EntryID)
	if err != nil || !cancelled.OK || cancelled.Value.Kind != "cancelled" {
		t.Fatalf("cancel = %v %+v", err, cancelled)
	}
	retry, err := lane.CancelQueued(ctx, queued.Value.EntryID)
	if err != nil || !retry.OK || retry.Value.Kind != "not_found" {
		t.Fatalf("cancel retry = %v %+v", err, retry)
	}

	queued, err = lane.NextRun(ctx, PromptInput{Text: "run me"})
	if err != nil || !queued.OK {
		t.Fatalf("next-run for capture = %v %+v", err, queued)
	}
	result, err := lane.Prompt(ctx, PromptInput{})
	if err != nil || !result.OK || result.Value.Kind != "completed" {
		t.Fatalf("captured next-run prompt = %v %+v", err, result)
	}
	entries, err := session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 2 || entries[0].Message == nil || entries[0].Message.Content != "run me" {
		t.Fatalf("captured entries = %v %+v", err, entries)
	}
	consumed, err := lane.CancelQueued(ctx, queued.Value.EntryID)
	if err != nil || !consumed.OK || consumed.Value.Kind != "already_consumed" {
		t.Fatalf("consumed cancellation = %v %+v", err, consumed)
	}
}

func TestHarnessDefersActiveTreeWritesUntilCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: &scriptedModels{}, Model: Model{Provider: "provider", ModelID: "model"}})
	if err != nil {
		t.Fatal(err)
	}
	seedRunOperation(t, session, RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationNeedAssistant}}}, nil)
	id, err := harness.runtimeLane.Session().AppendMessage(ctx, AgentMessage{Role: "user", Content: "deferred"})
	if err != nil || id == "" {
		t.Fatalf("deferred append = %v %q", err, id)
	}
	register, err := session.GetRegister(ctx, RegisterPendingEntry, id)
	if err != nil || register == nil {
		t.Fatalf("pending register = %v %+v", err, register)
	}
	current, err := harness.currentOperation(ctx, harness.runtimeLane)
	if err != nil || len(current.State.Run.Inbox.Writes) != 1 || current.State.Run.Inbox.Writes[0] != id {
		t.Fatalf("pending write state = %v %+v", err, current)
	}
	result, err := harness.Resume(ctx)
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "completed" {
		t.Fatalf("deferred write resume = %v %+v", err, result)
	}
	entries, err := session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 3 || entries[1].ID != id || entries[1].Message == nil || entries[1].Message.Content != "deferred" {
		t.Fatalf("deferred write entries = %v %+v", err, entries)
	}
	register, err = session.GetRegister(ctx, RegisterPendingEntry, id)
	if err != nil || register != nil {
		t.Fatalf("pending register survived consumption = %v %+v", err, register)
	}
}

func TestHarnessConfigurationSettersValidateAndApply(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: &scriptedModels{}, Model: Model{Provider: "provider", ModelID: "model"}})
	if err != nil {
		t.Fatal(err)
	}
	settings := CompactionSettings{Enabled: true, ReserveTokens: 10, KeepRecentTokens: 20}
	if err := harness.SetCompactionSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}
	if got, err := harness.GetCompactionSettings(ctx); err != nil || got != settings {
		t.Fatalf("compaction settings = %v %+v", err, got)
	}
	if err := harness.SetSteeringMode(ctx, QueueOneAtATime); err != nil {
		t.Fatal(err)
	}
	if err := harness.SetFollowUpMode(ctx, QueueOneAtATime); err != nil {
		t.Fatal(err)
	}
	if got, _ := harness.GetSteeringMode(ctx); got != QueueOneAtATime {
		t.Fatalf("steering mode = %q", got)
	}
	if got, _ := harness.GetFollowUpMode(ctx); got != QueueOneAtATime {
		t.Fatalf("follow-up mode = %q", got)
	}
	if err := harness.SetSteeringMode(ctx, QueueMode("invalid")); err == nil {
		t.Fatal("invalid steering mode was accepted")
	}
	if got, _ := harness.GetSteeringMode(ctx); got != QueueOneAtATime {
		t.Fatalf("invalid setter changed steering mode = %q", got)
	}
	if err := harness.SetCompactionSettings(ctx, CompactionSettings{ReserveTokens: -1}); err == nil {
		t.Fatal("invalid compaction settings were accepted")
	}
}

func TestHarnessOneAtATimeSteerSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	models := &scriptedModels{}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}, SteeringMode: QueueOneAtATime})
	if err != nil {
		t.Fatal(err)
	}
	opID := seedRunOperation(t, session, RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationNeedAssistant}}}, nil)
	ids := []string{session.IDGenerator().Next(), session.IDGenerator().Next()}
	stateRegister, err := session.GetRegister(ctx, RegisterOpState, opID)
	if err != nil || stateRegister == nil {
		t.Fatalf("operation state = %v %+v", err, stateRegister)
	}
	state := stateRegister.Value.(OperationState)
	state.Run.Settings.SteeringMode = QueueOneAtATime
	state.Run.Inbox.Steer = append([]string(nil), ids...)
	writes := make([]Write, 0, len(ids)+1)
	for _, id := range ids {
		writes = append(writes, registerSet(RegisterPendingEntry, id, PendingEntry{Type: EntryMessage, Payload: AgentMessage{Role: "user", Content: id}}))
	}
	writes = append(writes, registerSet(RegisterOpState, opID, state))
	if _, err := session.Commit(ctx, Transaction{Writes: writes}); err != nil {
		t.Fatal(err)
	}
	current, err := harness.currentOperation(ctx, harness.runtimeLane)
	if err != nil {
		t.Fatal(err)
	}
	consumed, current, err := harness.consumeCheckpointInput(ctx, harness.runtimeLane, current)
	if err != nil || !consumed || len(current.State.Run.Inbox.Steer) != 1 || current.State.Run.Inbox.Steer[0] != ids[1] {
		t.Fatalf("one-at-a-time drain = %v %+v", err, current)
	}
	if current.State.Run.Phase.Checkpoint == nil || !current.State.Run.Phase.Checkpoint.SkipInboxOnce {
		t.Fatalf("one-at-a-time drain did not set skip marker: %+v", current.State.Run.Phase)
	}
	if register, err := session.GetRegister(ctx, RegisterPendingEntry, ids[0]); err != nil || register != nil {
		t.Fatalf("consumed pending register = %v %+v", err, register)
	}
	if register, err := session.GetRegister(ctx, RegisterPendingEntry, ids[1]); err != nil || register == nil {
		t.Fatalf("remaining pending register = %v %+v", err, register)
	}
	metadata := session.Metadata()
	if err := harness.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := repo.Open(ctx, metadata)
	if err != nil {
		t.Fatal(err)
	}
	restarted, suspended, err := NewHarness(ctx, AgentHarnessOptions{Session: reopened, Models: models, Model: Model{Provider: "provider", ModelID: "model"}})
	if err != nil || len(suspended) != 1 {
		t.Fatalf("reopen = %v %+v", err, suspended)
	}
	result, err := restarted.Resume(ctx)
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "completed" {
		t.Fatalf("reopened drain = %v %+v", err, result)
	}
	entries, err := reopened.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 5 || entries[1].ID != ids[0] || entries[3].ID != ids[1] {
		t.Fatalf("reopened one-at-a-time entries = %v %+v", err, entries)
	}
}

func TestHarnessAbortDrainsQueuesAndCleansThemOnResume(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: &scriptedModels{}, Model: Model{Provider: "provider", ModelID: "model"}})
	if err != nil {
		t.Fatal(err)
	}
	seedRunOperation(t, session, RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationNeedAssistant}}}, nil)
	steer, err := harness.runtimeLane.Steer(ctx, PromptInput{Text: "steer"})
	if err != nil || !steer.OK {
		t.Fatalf("steer = %v %+v", err, steer)
	}
	followUp, err := harness.runtimeLane.FollowUp(ctx, PromptInput{Text: "follow-up"})
	if err != nil || !followUp.OK {
		t.Fatalf("follow-up = %v %+v", err, followUp)
	}
	aborted, err := harness.runtimeLane.Abort(ctx)
	if err != nil || !aborted.OK || aborted.Value.RunID == "" || len(aborted.Value.Steer) != 1 || len(aborted.Value.FollowUp) != 1 {
		t.Fatalf("abort = %v %+v", err, aborted)
	}
	repeated, err := harness.runtimeLane.Abort(ctx)
	if err != nil || !repeated.OK || len(repeated.Value.Steer) != 1 || len(repeated.Value.FollowUp) != 1 {
		t.Fatalf("repeated abort = %v %+v", err, repeated)
	}
	for _, id := range []string{steer.Value.EntryID, followUp.Value.EntryID} {
		register, err := session.GetRegister(ctx, RegisterPendingEntry, id)
		if err != nil || register == nil {
			t.Fatalf("drained register %s = %v %+v", id, err, register)
		}
	}
	result, err := harness.runtimeLane.Resume(ctx)
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "aborted" {
		t.Fatalf("aborted resume = %v %+v", err, result)
	}
	for _, id := range []string{steer.Value.EntryID, followUp.Value.EntryID} {
		register, err := session.GetRegister(ctx, RegisterPendingEntry, id)
		if err != nil || register != nil {
			t.Fatalf("drained register survived terminal cleanup %s = %v %+v", id, err, register)
		}
	}
	last, err := harness.GetLastResult(ctx)
	if err != nil || last == nil || last.Outcome != "aborted" {
		t.Fatalf("aborted last result = %v %+v", err, last)
	}
}

func TestHarnessAbortSignalsLiveProvider(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	models := &blockingModels{started: make(chan struct{})}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan Result[RunOutcome, error], 1)
	go func() {
		result, _ := harness.Prompt(ctx, PromptInput{Text: "interrupt"})
		done <- result
	}()
	select {
	case <-models.started:
	case <-time.After(time.Second):
		t.Fatal("provider did not start")
	}
	aborted, err := harness.Abort(ctx)
	if err != nil || !aborted.OK {
		t.Fatalf("live abort = %v %+v", err, aborted)
	}
	select {
	case result := <-done:
		if !result.OK || result.Value.Kind != "aborted" {
			t.Fatalf("live abort result = %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("live provider did not reconcile after abort")
	}
}

func TestHarnessAbortBestEffortCancelsDeferredSource(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	handle := DeferredHandle{Provider: "provider", ModelID: "model", ID: "deferred"}
	source, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "assistant", Content: handle, StopReason: StopReasonDeferred})
	if err != nil {
		t.Fatal(err)
	}
	models := &retryModels{}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}})
	if err != nil {
		t.Fatal(err)
	}
	seedRunOperation(t, session, RunPhase{Kind: PhaseDeferred, Deferred: &Deferred{Status: DeferredSuspended, SourceEntryID: source, Configuration: LaneConfiguration{Model: Model{Provider: "provider", ModelID: "model"}}}}, &source)
	if _, err := harness.runtimeLane.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := harness.runtimeLane.Resume(ctx)
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "aborted" {
		t.Fatalf("deferred abort resume = %v %+v", err, result)
	}
	if models.Cancelled() != 1 {
		t.Fatalf("deferred cancellations = %d, want 1", models.Cancelled())
	}
}

func TestHarnessAbortSettlesPlannedToolWithoutExecutingIt(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: &scriptedModels{}, Model: Model{Provider: "provider", ModelID: "model"}, ActiveToolNames: []string{"echo"}})
	if err != nil {
		t.Fatal(err)
	}
	source, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "user", Content: "prompt"})
	if err != nil {
		t.Fatal(err)
	}
	opID := seedRunOperation(t, session, RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationNeedAssistant}}}, &source)
	assistant, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "assistant", StopReason: StopReasonToolUse, ToolCalls: []AgentToolCall{{ID: "call", Name: "echo", Arguments: map[string]JSONValue{"value": "unused"}}}})
	if err != nil {
		t.Fatal(err)
	}
	resultID := session.IDGenerator().Next()
	stateRegister, err := session.GetRegister(ctx, RegisterOpState, opID)
	if err != nil || stateRegister == nil {
		t.Fatalf("tool operation state = %v %+v", err, stateRegister)
	}
	state := stateRegister.Value.(OperationState)
	state.Run.Phase = RunPhase{Kind: PhaseTools, ToolBatch: &ToolBatch{AssistantEntryID: assistant, Configuration: LaneConfiguration{Model: Model{Provider: "provider", ModelID: "model"}, ActiveToolNames: []string{"echo"}}, StepID: "step", TurnID: "turn", Calls: []ToolCall{{Status: "planned", SourceIndex: 0, ResultEntryID: resultID}}}}
	if _, err := session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterLaneLeaf, "main", &assistant), registerSet(RegisterOpState, opID, state)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.runtimeLane.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := harness.runtimeLane.Resume(ctx)
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "aborted" {
		t.Fatalf("planned tool abort = %v %+v", err, result)
	}
	entries, err := session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 3 || entries[2].ID != resultID || entries[2].Message == nil || !entries[2].Message.Metadata["isError"].(bool) {
		t.Fatalf("planned tool entries = %v %+v", err, entries)
	}
}

func TestHarnessFailureDrainAppliesWritesAndRevivesOnInput(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	models := &scriptedModels{}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}})
	if err != nil {
		t.Fatal(err)
	}
	source := session.IDGenerator().Next()
	if _, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "user", Content: "source"}); err != nil {
		t.Fatal(err)
	}
	entries, err := session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 1 {
		t.Fatalf("source entry = %v %+v", err, entries)
	}
	source = entries[0].ID
	opID := seedRunOperation(t, session, RunPhase{Kind: PhaseFailureDrain, Error: &OperationError{Code: "provider", Message: "temporary"}, Provenance: &FailureProvenance{Kind: "assistant", EntryID: source}}, &source)
	writeID := session.IDGenerator().Next()
	steerID := session.IDGenerator().Next()
	stateRegister, err := session.GetRegister(ctx, RegisterOpState, opID)
	if err != nil || stateRegister == nil {
		t.Fatalf("failure state = %v %+v", err, stateRegister)
	}
	state := stateRegister.Value.(OperationState)
	state.Run.Inbox.Writes = []string{writeID}
	state.Run.Inbox.Steer = []string{steerID}
	if _, err := session.Commit(ctx, Transaction{Writes: []Write{
		registerSet(RegisterPendingEntry, writeID, PendingEntry{Type: EntryCustom, CustomType: "marker", Payload: map[string]JSONValue{"ok": true}}),
		registerSet(RegisterPendingEntry, steerID, PendingEntry{Type: EntryMessage, Payload: AgentMessage{Role: "user", Content: "revive"}}),
		registerSet(RegisterOpState, opID, state),
	}}); err != nil {
		t.Fatal(err)
	}
	result, err := harness.Resume(ctx)
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "completed" {
		t.Fatalf("failure drain revival = %v %+v", err, result)
	}
	entries, err = session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 4 || entries[1].ID != writeID || entries[1].CustomType != "marker" || entries[2].ID != steerID {
		t.Fatalf("failure drain entries = %v %+v", err, entries)
	}
}

func TestHarnessCancelledWritesSurviveUntilTerminalCleanup(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: &scriptedModels{}, Model: Model{Provider: "provider", ModelID: "model"}})
	if err != nil {
		t.Fatal(err)
	}
	seedRunOperation(t, session, RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationNeedAssistant}}}, nil)
	id, err := harness.runtimeLane.Session().AppendCustomEntry(ctx, "deferred", map[string]JSONValue{"value": "kept"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.runtimeLane.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := harness.runtimeLane.Resume(ctx)
	if err != nil || !result.OK || result.Value.Run == nil || result.Value.Run.Kind != "aborted" {
		t.Fatalf("cancelled write resume = %v %+v", err, result)
	}
	entries, err := session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 2 || entries[1].ID != id || entries[1].CustomType != "deferred" {
		t.Fatalf("cancelled write entries = %v %+v", err, entries)
	}
	if register, err := session.GetRegister(ctx, RegisterPendingEntry, id); err != nil || register != nil {
		t.Fatalf("cancelled write register = %v %+v", err, register)
	}
}

func TestHarnessCloseStopsNewWorkAndPreservesAcceptedState(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	metadata := session.Metadata()
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: &scriptedModels{}, Model: Model{Provider: "provider", ModelID: "model"}, Drive: "manual"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan Result[RunOutcome, error], 1)
	go func() {
		result, _ := harness.Prompt(ctx, PromptInput{Text: "accepted"})
		done <- result
	}()
	waitForAction(t, harness)
	if err := harness.Close(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.OK || result.Err == nil {
		t.Fatalf("closed prompt = %+v", result)
	}
	rejected, err := harness.Prompt(ctx, PromptInput{Text: "rejected"})
	if err != nil || rejected.Err == nil {
		t.Fatalf("closed new prompt = %v %+v", err, rejected)
	}
	reopened, err := repo.Open(ctx, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if restored, err := Restore(ctx, reopened, "main"); err != nil || restored.Current == nil {
		t.Fatalf("accepted state after close = %v %+v", err, restored)
	}
}

func TestHarnessDriveResolvesExternallyFinalizedOperation(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: &scriptedModels{}, Model: Model{Provider: "provider", ModelID: "model"}, Drive: "manual"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan Result[RunOutcome, error], 1)
	go func() {
		result, _ := harness.Prompt(ctx, PromptInput{Text: "externally finalized"})
		done <- result
	}()
	waitForAction(t, harness)
	restored, err := Restore(ctx, session, "main")
	if err != nil || restored.Current == nil {
		t.Fatalf("restore before external finalization = %v %+v", err, restored)
	}
	current := restored.Current
	laneState := current.LaneState
	laneState.CurrentOperationID = nil
	if _, err := session.Commit(ctx, Transaction{Writes: []Write{
		registerDelete(RegisterOpMeta, current.Operation.OperationID),
		registerDelete(RegisterOpState, current.Operation.OperationID),
		registerSet(RegisterLaneLastResult, "main", LaneLastResult{OperationID: current.Operation.OperationID, Kind: OperationRun, Outcome: "completed", LeafID: current.LeafID}),
		registerSet(RegisterLaneState, "main", laneState),
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.ExecuteAction(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if !result.OK || result.Value.Kind != "completed" {
			t.Fatalf("external finalization result = %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("drive did not stop after external finalization")
	}
}
