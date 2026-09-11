package harness

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestRestoreValidatesIdleAndOpenLaneInventory(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.(*MemorySession).SetLaneConfiguration(ctx, "main", LaneConfiguration{Model: Model{Provider: "provider", ModelID: "model"}, ThinkingLevel: ThinkingLow, ActiveToolNames: []string{}}); err != nil {
		t.Fatal(err)
	}
	id, err := session.AppendMessage(ctx, AgentMessage{Role: "user", Content: "restore"})
	if err != nil {
		t.Fatal(err)
	}
	idle, err := Restore(ctx, session, "main")
	if err != nil || !idle.Idle || idle.Current != nil {
		t.Fatalf("unexpected idle restore: %v %+v", err, idle)
	}
	opID := "operation"
	operation := Operation{OperationID: opID, Lane: "main", SourceLeafID: stringPointer(id), Intent: OperationIntent{Kind: OperationRun}}
	state := OperationState{Kind: OperationRun, Run: &RunState{Kind: OperationRun, Control: Control{Status: ControlRunning}, Phase: RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationMayFinish}, TriggerEntryID: id}}, Inbox: Inbox{}}}
	laneState := LaneState{CurrentOperationID: &opID, PendingNextRun: []string{}}
	if _, err := session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpMeta, opID, operation), registerSet(RegisterOpState, opID, state), registerSet(RegisterLaneState, "main", laneState)}}); err != nil {
		t.Fatal(err)
	}
	open, err := Restore(ctx, session, "main")
	if err != nil || open.Current == nil || open.Current.OperationStateSeq == 0 || open.Current.LaneStateSeq == 0 {
		t.Fatalf("unexpected open restore: %v %+v", err, open)
	}
	if open.Current.Operation.OperationID != opID || open.Current.State.Run == nil || open.Current.LeafID == nil {
		t.Fatalf("restore lost operation inventory: %+v", open.Current)
	}
}

func TestManualSchedulerAndRuntimePrimitives(t *testing.T) {
	ctx := context.Background()
	scheduler := NewManualScheduler(true)
	started := make(chan struct{})
	done, err := scheduler.Enqueue(ctx, ScheduledAction{Info: ActionInfo{Kind: "transition"}, Run: func(context.Context) error { close(started); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if info := scheduler.Peek(); info == nil || info.Kind != "transition" {
		t.Fatalf("manual action was not parked: %+v", info)
	}
	select {
	case <-started:
		t.Fatal("manual action ran while parked")
	default:
	}
	info, err := scheduler.Execute(ctx)
	if err != nil || info == nil || info.Kind != "transition" || <-done != nil {
		t.Fatalf("manual action did not execute: %v %+v", err, info)
	}

	bus := NewEventBus()
	var events []string
	unsub := bus.On("entry_added", func(_ context.Context, event HarnessEvent) { events = append(events, event.Type) })
	bus.Emit(ctx, HarnessEvent{Type: "entry_added"})
	unsub()
	bus.Emit(ctx, HarnessEvent{Type: "entry_added"})
	if !reflect.DeepEqual(events, []string{"entry_added"}) {
		t.Fatalf("event subscription failed: %v", events)
	}
	hooks := NewHookRunner()
	var order []string
	if _, err := hooks.On(HookBeforeRun, func(context.Context, HookInvocation) (JSONValue, error) { order = append(order, "b"); return "b", nil }, "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := hooks.On(HookBeforeRun, func(context.Context, HookInvocation) (JSONValue, error) { order = append(order, "a"); return "a", nil }, "a"); err != nil {
		t.Fatal(err)
	}
	values, err := hooks.Run(ctx, HookInvocation{Name: HookBeforeRun})
	if err != nil || !reflect.DeepEqual(order, []string{"b", "a"}) || !reflect.DeepEqual(values, "a") {
		t.Fatalf("hook aggregation/order failed: %v %v %v", err, order, values)
	}
	values, err = hooks.Run(ctx, HookInvocation{Name: HookBeforeRun, Event: map[string]JSONValue{"messages": []AgentMessage{{Role: "user", Content: "one"}}}})
	if err != nil || values != "a" {
		t.Fatalf("hook result aggregation failed: %v %v", err, values)
	}
	bus.Emit(ctx, HarnessEvent{Type: "buffered", Payload: map[string]JSONValue{"value": "kept"}})
	if buffered := bus.Drain(); len(buffered) != 3 || buffered[2].Type != "buffered" {
		t.Fatalf("event buffering failed: %+v", buffered)
	}
	settings := NewSettingsSnapshot(AgentHarnessStreamOptions{}, NormalizedRetryPolicy{MaxAttempts: 1})
	updated := settings.Update(AgentHarnessStreamOptions{TimeoutMs: 10}, NormalizedRetryPolicy{MaxAttempts: 2})
	if updated.SettingsRevision != 1 || updated.RetryPolicy.MaxAttempts != 2 {
		t.Fatalf("settings revision failed: %+v", updated)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); scheduler.Close() }()
	wg.Wait()
}

func TestEventBusReportsPassiveHandlerErrors(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus()
	var got []string
	bus.On(string(EventHandlerError), func(_ context.Context, event HarnessEvent) { got = append(got, event.Type) })
	bus.On("message", func(context.Context, HarnessEvent) { panic("handler failed") })
	bus.Emit(ctx, HarnessEvent{Type: "message", Lane: "main", Payload: "private"})
	if !reflect.DeepEqual(got, []string{string(EventHandlerError)}) {
		t.Fatalf("handler error events = %v", got)
	}
	buffered := bus.Drain()
	if len(buffered) != 2 || buffered[1].Type != string(EventHandlerError) || buffered[1].Lane != "main" {
		t.Fatalf("handler error buffer = %+v", buffered)
	}
}

func TestLaneMutationCASAndNestedManualGate(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	state, err := session.GetRegister(ctx, RegisterLaneState, "main")
	if err != nil {
		t.Fatal(err)
	}
	line := &LaneMutationLine{}
	ok, err := line.Commit(ctx, session, []RegisterToken{{Namespace: RegisterLaneState, Key: "main", Seq: state.Seq, Exists: true}}, Transaction{Writes: []Write{registerSet(RegisterLaneState, "main", LaneState{PendingNextRun: []string{"queued"}})}})
	if err != nil || !ok {
		t.Fatalf("expected CAS commit: %v %v", err, ok)
	}
	ok, err = line.Commit(ctx, session, []RegisterToken{{Namespace: RegisterLaneState, Key: "main", Seq: state.Seq, Exists: true}}, Transaction{Writes: []Write{registerSet(RegisterLaneState, "main", LaneState{PendingNextRun: []string{"stale"}})}})
	if err != nil || ok {
		t.Fatalf("stale CAS committed: %v %v", err, ok)
	}

	scheduler := NewManualScheduler(true)
	parentStarted := make(chan struct{})
	parentDone := make(chan struct{})
	childDone, err := scheduler.Enqueue(ctx, ScheduledAction{Info: ActionInfo{Kind: "parent"}, Run: func(context.Context) error {
		close(parentStarted)
		done, err := scheduler.Enqueue(ctx, ScheduledAction{Info: ActionInfo{Kind: "child"}, Run: func(context.Context) error { close(parentDone); return nil }})
		if err != nil {
			return err
		}
		return <-done
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Execute(ctx); err != nil {
		t.Fatal(err)
	}
	<-parentStarted
	if info := scheduler.Peek(); info == nil || info.Kind != "child" {
		t.Fatalf("nested action was not parked: %+v", info)
	}
	if _, err := scheduler.Execute(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-childDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-parentDone:
	case <-time.After(time.Second):
		t.Fatal("nested manual action did not complete parent")
	}
}

func TestRestoreAcceptsUnmaterializedReservedResponse(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	config := LaneConfiguration{Model: Model{Provider: "provider", ModelID: "model"}, ThinkingLevel: ThinkingLow, ActiveToolNames: []string{}}
	if err := session.(*MemorySession).SetLaneConfiguration(ctx, "main", config); err != nil {
		t.Fatal(err)
	}
	prompt, err := session.AppendMessage(ctx, AgentMessage{Role: "user", Content: "prompt"})
	if err != nil {
		t.Fatal(err)
	}
	opID := "operation"
	state := OperationState{Kind: OperationRun, Run: &RunState{Kind: OperationRun, Control: Control{Status: ControlRunning}, Phase: RunPhase{Kind: PhaseAssistant, Generation: &Generation{Status: GenerationEffectPending, ResponseEntryID: "reserved-response", Context: GenerationContext{StepID: "step", TriggerEntryID: prompt, Configuration: config}}}, Inbox: Inbox{}}}
	operation := Operation{OperationID: opID, Lane: "main", SourceLeafID: stringPointer(prompt), Intent: OperationIntent{Kind: OperationRun, PromptEntryIDs: []string{prompt}}}
	if _, err := session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpMeta, opID, operation), registerSet(RegisterOpState, opID, state), registerSet(RegisterLaneState, "main", LaneState{CurrentOperationID: &opID, PendingNextRun: []string{}})}}); err != nil {
		t.Fatal(err)
	}
	result, err := Restore(ctx, session, "main")
	if err != nil || result.Current == nil {
		t.Fatalf("restore rejected unmaterialized reservation: %v %+v", err, result)
	}
}
