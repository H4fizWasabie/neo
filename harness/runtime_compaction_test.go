package harness

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type blockingSummaryModels struct {
	started chan struct{}
}

func (*blockingSummaryModels) Resolve(context.Context, string, string) (Model, error) {
	return Model{Provider: "provider", ModelID: "model"}, nil
}

func (m *blockingSummaryModels) Stream(ctx context.Context, _ Model, _ []Message, _ AgentHarnessStreamOptions) (<-chan AgentEvent, error) {
	close(m.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*blockingSummaryModels) FetchDeferred(context.Context, Model, DeferredHandle, AgentHarnessStreamOptions) (DeferredResponse, error) {
	return DeferredResponse{}, nil
}

func (*blockingSummaryModels) CancelDeferred(context.Context, Model, DeferredHandle) error {
	return nil
}

func newCompactionHarness(t *testing.T, models Models) (*Harness, Session, string) {
	t.Helper()
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	user, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "user", Content: "old"})
	if err != nil {
		t.Fatal(err)
	}
	assistant, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "assistant", Content: "answer", StopReason: StopReasonStop})
	if err != nil {
		t.Fatal(err)
	}
	harness, suspended, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: Model{Provider: "provider", ModelID: "model"}})
	if err != nil || len(suspended) != 0 {
		t.Fatalf("harness creation failed: %v %+v", err, suspended)
	}
	_ = user
	return harness, session, assistant
}

func TestHarnessManualCompactionPublishesOneSummaryWithoutMessageEvents(t *testing.T) {
	harness, session, oldLeaf := newCompactionHarness(t, &scriptedModels{})
	ctx := context.Background()
	var events []string
	harness.Events().On("*", func(_ context.Context, event HarnessEvent) { events = append(events, event.Type) })
	result, err := harness.Compact(ctx, "remember the important facts")
	if err != nil || !result.OK || result.Value.Kind != "completed" || result.Value.Entry == nil {
		t.Fatalf("compaction result = %v %+v", err, result)
	}
	if result.Value.Entry.Type != EntryCompaction || result.Value.Entry.Summary != "answer" || result.Value.Entry.ParentID == nil || *result.Value.Entry.ParentID != oldLeaf {
		t.Fatalf("compaction entry = %+v", result.Value.Entry)
	}
	for _, event := range events {
		if event == string(EventMessageStart) || event == string(EventMessageUpdate) || event == string(EventMessageEnd) {
			t.Fatalf("summary emitted public message event %q", event)
		}
	}
	leaf, err := session.View("main").GetLeafID(ctx)
	if err != nil || leaf == nil || *leaf != result.Value.Entry.ID {
		t.Fatalf("compaction leaf = %v %v", err, leaf)
	}
	if registers, err := session.ListRegisters(ctx, RegisterOpPreparation, ""); err != nil || len(registers) != 0 {
		t.Fatalf("preparation cleanup = %v %+v", err, registers)
	}
	if restored, err := Restore(ctx, session, "main"); err != nil || restored.Current != nil {
		t.Fatalf("compaction restore = %v %+v", err, restored)
	}
}

func TestHarnessManualCompactionHookCanDeclineOrSupplyResult(t *testing.T) {
	ctx := context.Background()
	harness, session, oldLeaf := newCompactionHarness(t, &scriptedModels{})
	if _, err := harness.Hooks().On(HookBeforeCompaction, func(context.Context, HookInvocation) (JSONValue, error) {
		return map[string]JSONValue{"decline": true}, nil
	}, "decline"); err != nil {
		t.Fatal(err)
	}
	declined, err := harness.Compact(ctx, "")
	if err != nil || !declined.OK || declined.Value.Kind != "declined" {
		t.Fatalf("declined compaction = %v %+v", err, declined)
	}
	leaf, err := session.View("main").GetLeafID(ctx)
	if err != nil || leaf == nil || *leaf != oldLeaf {
		t.Fatalf("declined leaf = %v %v", err, leaf)
	}

	harness2, _, oldLeaf2 := newCompactionHarness(t, &scriptedModels{})
	if _, err := harness2.Hooks().On(HookBeforeCompaction, func(context.Context, HookInvocation) (JSONValue, error) {
		return map[string]JSONValue{"compaction": CompactResult{Summary: "hook summary", Usage: &Usage{Total: 4}}}, nil
	}, "result"); err != nil {
		t.Fatal(err)
	}
	completed, err := harness2.Compact(ctx, "")
	if err != nil || !completed.OK || completed.Value.Kind != "completed" || completed.Value.Entry == nil || completed.Value.Entry.Summary != "hook summary" {
		t.Fatalf("hook compaction = %v %+v", err, completed)
	}
	if completed.Value.Entry.ParentID == nil || *completed.Value.Entry.ParentID != oldLeaf2 {
		t.Fatalf("hook compaction parent = %+v", completed.Value.Entry)
	}
}

func TestHarnessManualCompactionAbortStopsSummaryGeneration(t *testing.T) {
	models := &blockingSummaryModels{started: make(chan struct{})}
	harness, session, _ := newCompactionHarness(t, models)
	ctx := context.Background()
	result := make(chan Result[CompactionOutcome, error], 1)
	go func() {
		value, _ := harness.Compact(ctx, "")
		result <- value
	}()
	select {
	case <-models.started:
	case <-time.After(time.Second):
		t.Fatal("summary request did not start")
	}
	if aborted, err := harness.Abort(ctx); err != nil || !aborted.OK {
		t.Fatalf("abort = %v %+v", err, aborted)
	}
	select {
	case value := <-result:
		if !value.OK || value.Value.Kind != "aborted" {
			t.Fatalf("aborted compaction = %+v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("aborted compaction did not finish")
	}
	entries, err := session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 2 {
		t.Fatalf("aborted compaction entries = %v %+v", err, entries)
	}
}

func TestHarnessReopensCompactionEffectPendingWithCapturedRetryPolicy(t *testing.T) {
	harness, session, source := newCompactionHarness(t, &scriptedModels{})
	ctx := context.Background()
	configRegister, err := session.GetRegister(ctx, RegisterLaneConfig, "main")
	if err != nil || configRegister == nil {
		t.Fatalf("config = %v %+v", err, configRegister)
	}
	config := configRegister.Value.(LaneConfiguration)
	opID := session.IDGenerator().Next()
	taskID := "task:1"
	resultID, usageID := session.IDGenerator().Next(), session.IDGenerator().Next()
	prep := DurableStructuralPreparation{Kind: EntryCompaction, MessagesToSummarize: []AgentMessage{{Role: "user", Content: "old"}}, RetainedTail: []AgentMessage{{Role: "assistant", Content: "answer", StopReason: StopReasonStop}}, TokensBefore: 2}
	state := OperationState{Kind: OperationCompaction, Compaction: &CompactionState{Kind: OperationCompaction, Control: Control{Status: ControlRunning}, Structural: StructuralDecision{TaskID: taskID, Status: "generating", Generation: &SummaryGeneration{Status: GenerationEffectPending, Attempt: 1, NextAttempt: 1, Request: &SummaryRequest{Index: 0, UsageID: usageID}, Context: SummaryContext{TaskID: taskID, ResultEntryID: resultID, Kind: EntryCompaction, Configuration: config, RetryPolicy: NormalizedRetryPolicy{MaxAttempts: 2}}}}}}
	op := Operation{OperationID: opID, Lane: "main", SourceLeafID: &source, StartedAt: 1, Intent: OperationIntent{Kind: OperationCompaction}}
	if _, err := session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpMeta, opID, op), registerSet(RegisterOpPreparation, opID+":"+taskID, prep), registerSet(RegisterOpState, opID, state), registerSet(RegisterLaneState, "main", LaneState{CurrentOperationID: &opID, PendingNextRun: []string{}})}}); err != nil {
		t.Fatal(err)
	}
	result, err := harness.Resume(ctx)
	if err != nil || !result.OK || result.Value.Compaction == nil || result.Value.Compaction.Kind != "completed" {
		t.Fatalf("recovered compaction = %v %+v", err, result)
	}
	entries, err := session.FindEntries(ctx, EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 3 || entries[2].Type != EntryCompaction {
		t.Fatalf("recovered compaction entries = %v %+v", err, entries)
	}
}

func newR9Harness(t *testing.T, models *retryModels) (*Harness, Session) {
	t.Helper()
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "user", Content: "old"}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "assistant", Content: "answer", StopReason: StopReasonStop}); err != nil {
		t.Fatal(err)
	}
	harness, suspended, err := NewHarness(ctx, AgentHarnessOptions{
		Session: session,
		Models:  models,
		Model:   models.model,
		Compaction: CompactionSettings{
			Enabled:       true,
			ReserveTokens: 1,
		},
	})
	if err != nil || len(suspended) != 0 {
		t.Fatalf("harness creation failed: %v %+v", err, suspended)
	}
	return harness, session
}

func TestHarnessThresholdCompactionRunsOnceBeforeAssistant(t *testing.T) {
	models := &retryModels{
		model: Model{Provider: "provider", ModelID: "model", ContextWindow: 5},
		outcomes: []retryOutcome{
			{message: AgentMessage{Role: "assistant", Content: "summary", StopReason: StopReasonStop}},
			{message: AgentMessage{Role: "assistant", Content: "final", StopReason: StopReasonStop}},
		},
	}
	harness, session := newR9Harness(t, models)
	result, err := harness.Prompt(context.Background(), PromptInput{Text: "prompt"})
	if err != nil || !result.OK || result.Value.Kind != "completed" {
		t.Fatalf("threshold run = %v %+v", err, result)
	}
	if models.Calls() != 2 {
		t.Fatalf("threshold provider calls = %d, want 2", models.Calls())
	}
	entries, err := session.FindEntries(context.Background(), EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 5 {
		t.Fatalf("threshold entries = %v %+v", err, entries)
	}
	compactions := 0
	for _, entry := range entries {
		if entry.Type == EntryCompaction {
			compactions++
		}
	}
	if compactions != 1 || entries[3].Summary != "summary" || entries[4].Message == nil || entries[4].Message.Content != "final" {
		t.Fatalf("threshold ledger = %+v", entries)
	}
	if len(models.requests) != 2 || len(models.requests[1]) != 2 || models.requests[1][0].Content != "summary" || models.requests[1][1].Content != "prompt" {
		t.Fatalf("threshold resumed context = %+v", models.requests)
	}
}

func TestOverflowClassifierLeavesToolPlanAsGenuineLength(t *testing.T) {
	generation := Generation{IntendedOutputLimit: 10}
	message := AgentMessage{Role: "assistant", StopReason: StopReasonLength, Usage: &Usage{Output: 1}, ToolCalls: []AgentToolCall{{ID: "call", Name: "echo"}}}
	if overflow, _ := overflowMessage(message, generation); overflow {
		t.Fatal("length response with tool calls was classified as overflow")
	}
	message.ToolCalls = nil
	if overflow, _ := overflowMessage(message, generation); !overflow {
		t.Fatal("truncated length response was not classified as overflow")
	}
}

func TestHarnessOverflowCompactsAndContinues(t *testing.T) {
	models := &retryModels{
		model: Model{Provider: "provider", ModelID: "model"},
		outcomes: []retryOutcome{
			{err: fmt.Errorf("context window exceeded")},
			{message: AgentMessage{Role: "assistant", Content: "recovered summary", StopReason: StopReasonStop}},
			{message: AgentMessage{Role: "assistant", Content: "final", StopReason: StopReasonStop}},
		},
	}
	harness, session := newR9Harness(t, models)
	result, err := harness.Prompt(context.Background(), PromptInput{Text: "prompt"})
	if err != nil || !result.OK || result.Value.Kind != "completed" {
		t.Fatalf("overflow run = %v %+v", err, result)
	}
	if models.Calls() != 3 {
		t.Fatalf("overflow provider calls = %d, want 3", models.Calls())
	}
	entries, err := session.FindEntries(context.Background(), EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 6 || entries[3].Message == nil || entries[3].Message.StopReason != StopReasonError || entries[4].Type != EntryCompaction || entries[5].Message == nil || entries[5].Message.Content != "final" {
		t.Fatalf("overflow ledger = %v %+v", err, entries)
	}
}

func TestHarnessSecondOverflowUsesBoundedFailure(t *testing.T) {
	models := &retryModels{
		model: Model{Provider: "provider", ModelID: "model"},
		outcomes: []retryOutcome{
			{err: fmt.Errorf("context length exceeded")},
			{message: AgentMessage{Role: "assistant", Content: "recovered summary", StopReason: StopReasonStop}},
			{err: fmt.Errorf("maximum context exceeded again")},
		},
	}
	harness, session := newR9Harness(t, models)
	result, err := harness.Prompt(context.Background(), PromptInput{Text: "prompt"})
	if err != nil || !result.OK || result.Value.Kind != "failed" {
		t.Fatalf("second overflow run = %v %+v", err, result)
	}
	if models.Calls() != 3 {
		t.Fatalf("second overflow provider calls = %d, want 3", models.Calls())
	}
	entries, err := session.FindEntries(context.Background(), EntryQuery{Order: OldestFirst})
	if err != nil || len(entries) != 6 {
		t.Fatalf("second overflow entries = %v %+v", err, entries)
	}
	compactions := 0
	for _, entry := range entries {
		if entry.Type == EntryCompaction {
			compactions++
		}
	}
	if compactions != 1 || entries[len(entries)-1].Message == nil || entries[len(entries)-1].Message.StopReason != StopReasonError {
		t.Fatalf("second overflow ledger = %+v", entries)
	}
}

func newNavigationHarness(t *testing.T, models *retryModels) (*Harness, Session, string, string) {
	t.Helper()
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "user", Content: "root"}); err != nil {
		t.Fatal(err)
	}
	target, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "assistant", Content: "target", StopReason: StopReasonStop})
	if err != nil {
		t.Fatal(err)
	}
	source, err := session.View("main").AppendMessage(ctx, AgentMessage{Role: "user", Content: "source"})
	if err != nil {
		t.Fatal(err)
	}
	harness, suspended, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: models, Model: models.model})
	if err != nil || len(suspended) != 0 {
		t.Fatalf("harness creation failed: %v %+v", err, suspended)
	}
	return harness, session, target, source
}

func TestHarnessNavigationMovesAndLabelsInOneResult(t *testing.T) {
	harness, session, target, source := newNavigationHarness(t, &retryModels{model: Model{Provider: "provider", ModelID: "model"}})
	result, err := harness.NavigateTree(context.Background(), &target, NavigateOptions{Label: "checkpoint"})
	if err != nil || !result.OK || result.Value.Kind != "completed" || result.Value.OldLeafID == nil || *result.Value.OldLeafID != source || result.Value.NewLeafID == nil || *result.Value.NewLeafID != target {
		t.Fatalf("navigation result = %v %+v", err, result)
	}
	leaf, err := session.View("main").GetLeafID(context.Background())
	if err != nil || leaf == nil || *leaf != target {
		t.Fatalf("navigation leaf = %v %v", err, leaf)
	}
	label, err := session.View("main").GetLabel(context.Background(), target)
	if err != nil || label == nil || *label != "checkpoint" {
		t.Fatalf("navigation label = %v %v", err, label)
	}
	last, err := harness.GetLastResult(context.Background())
	if err != nil || last == nil || last.Kind != OperationNavigation || last.Outcome != "completed" {
		t.Fatalf("navigation last result = %v %+v", err, last)
	}
}

func TestHarnessSummarizedNavigationUsesHookAndPublishesBranchSummary(t *testing.T) {
	models := &retryModels{model: Model{Provider: "provider", ModelID: "model"}}
	harness, session, target, source := newNavigationHarness(t, models)
	if _, err := harness.Hooks().On(HookBeforeNavigation, func(context.Context, HookInvocation) (JSONValue, error) {
		return map[string]JSONValue{"summary": BranchSummaryResult{Summary: "branch summary", Usage: &Usage{Total: 3}}}, nil
	}, "summary"); err != nil {
		t.Fatal(err)
	}
	result, err := harness.NavigateTree(context.Background(), &target, NavigateOptions{Summarize: true})
	if err != nil || !result.OK || result.Value.Kind != "completed" || result.Value.SummaryEntry == nil {
		t.Fatalf("summarized navigation result = %v %+v", err, result)
	}
	if models.Calls() != 0 || result.Value.SummaryEntry.Summary != "branch summary" || result.Value.SummaryEntry.ParentID == nil || *result.Value.SummaryEntry.ParentID != target || result.Value.SummaryEntry.FromID != source {
		t.Fatalf("summarized navigation = calls %d %+v", models.Calls(), result.Value.SummaryEntry)
	}
	leaf, err := session.View("main").GetLeafID(context.Background())
	if err != nil || leaf == nil || *leaf != result.Value.SummaryEntry.ID {
		t.Fatalf("summarized navigation leaf = %v %v", err, leaf)
	}
}

func TestHarnessSummarizedNavigationGeneratesSummary(t *testing.T) {
	models := &retryModels{
		model:    Model{Provider: "provider", ModelID: "model"},
		outcomes: []retryOutcome{{message: AgentMessage{Role: "assistant", Content: "generated", StopReason: StopReasonStop}}},
	}
	harness, _, target, _ := newNavigationHarness(t, models)
	result, err := harness.NavigateTree(context.Background(), &target, NavigateOptions{Summarize: true, CustomInstructions: "keep decisions"})
	if err != nil || !result.OK || result.Value.Kind != "completed" || result.Value.SummaryEntry == nil || result.Value.SummaryEntry.Summary != "generated" || models.Calls() != 1 {
		t.Fatalf("generated navigation = %v %+v calls=%d", err, result, models.Calls())
	}
}
