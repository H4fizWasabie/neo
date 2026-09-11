package harness

import (
	"context"
	"sync"
	"testing"
	"time"
)

type scriptedModels struct {
	mu       sync.Mutex
	streamed []Message
	calls    int
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
	want := []string{string(EventMessageStart), string(EventMessageUpdate), string(EventMessageEnd), string(EventEntryAdded), string(EventUsage)}
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
	for i := 0; i < 8; i++ {
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
		if models.Calls() == 1 {
			for {
				select {
				case result := <-done:
					if !result.OK {
						t.Fatalf("manual prompt failed: %+v", result)
					}
					return
				case <-time.After(time.Second):
					t.Fatal("manual prompt did not finish")
				}
			}
		}
		waitForAction(t, harness)
	}
	t.Fatal("manual prompt did not complete")
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
