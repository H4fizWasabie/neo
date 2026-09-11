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
	outcomes   []retryOutcome
	resolveErr error
	calls      int
}

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
	return Model{Provider: "provider", ModelID: "model"}, nil
}

func (m *retryModels) Stream(_ context.Context, _ Model, _ []Message, _ AgentHarnessStreamOptions) (<-chan AgentEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	index := m.calls - 1
	if index >= len(m.outcomes) {
		return nil, fmt.Errorf("unexpected provider call %d", m.calls)
	}
	outcome := m.outcomes[index]
	if outcome.err != nil {
		return nil, outcome.err
	}
	stream := make(chan AgentEvent, 1)
	message := outcome.message
	stream <- AgentEvent{Kind: "done", Message: &message}
	close(stream)
	return stream, nil
}

func (*retryModels) FetchDeferred(context.Context, Model, DeferredHandle, AgentHarnessStreamOptions) (DeferredResponse, error) {
	return DeferredResponse{}, nil
}

func (*retryModels) CancelDeferred(context.Context, Model, DeferredHandle) error { return nil }

func (m *retryModels) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
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
