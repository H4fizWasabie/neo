package harness

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

func TestRestoreValidatesIdleAndOpenLaneInventory(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: "session"})
	if err != nil {
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
	if err != nil || !reflect.DeepEqual(order, []string{"a", "b"}) || len(values) != 2 {
		t.Fatalf("hook aggregation/order failed: %v %v %v", err, order, values)
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
