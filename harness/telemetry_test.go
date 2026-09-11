package harness

import (
	"context"
	"sync"
	"testing"
)

type telemetryTestSpan struct {
	attributes map[string]AttributeValue
}

func (s telemetryTestSpan) SetAttributes(attributes map[string]AttributeValue) {
	for key, value := range attributes {
		s.attributes[key] = value
	}
}

func (s telemetryTestSpan) SetStatus(SpanStatus, string)               {}
func (s telemetryTestSpan) AddEvent(string, map[string]AttributeValue) {}
func (s telemetryTestSpan) End()                                       {}

type telemetryTestRecorder struct {
	mu    sync.Mutex
	spans []SpanOptions
}

func (r *telemetryTestRecorder) StartSpan(_ context.Context, options SpanOptions, fn func(TelemetrySpan) error) error {
	attributes := cloneAttributes(options.Attributes)
	span := telemetryTestSpan{attributes: attributes}
	err := fn(span)
	r.mu.Lock()
	options.Attributes = attributes
	r.spans = append(r.spans, options)
	r.mu.Unlock()
	return err
}

func cloneAttributes(attributes map[string]AttributeValue) map[string]AttributeValue {
	copy := make(map[string]AttributeValue, len(attributes))
	for key, value := range attributes {
		copy[key] = value
	}
	return copy
}

func (r *telemetryTestRecorder) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, len(r.spans))
	for i, span := range r.spans {
		names[i] = span.Name
	}
	return names
}

func TestHarnessTelemetryUsesRequiredNamesAndSafeStorageAttributes(t *testing.T) {
	ctx := context.Background()
	repo := NewMemorySessionRepo(SessionCodecOptions{})
	session, err := repo.Create(ctx, SessionCreateOptions{ID: "telemetry"})
	if err != nil {
		t.Fatal(err)
	}
	recorder := &telemetryTestRecorder{}
	harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: &scriptedModels{}, Model: Model{Provider: "provider", ModelID: "model"}, Telemetry: recorder})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.Session().AppendMessage(ctx, AgentMessage{Role: "user", Content: "direct write"}); err != nil {
		t.Fatal(err)
	}
	result, err := harness.Prompt(ctx, PromptInput{Text: "private prompt"})
	if err != nil || !result.OK {
		t.Fatalf("prompt = %v %+v", err, result)
	}
	names := recorder.names()
	for _, required := range []string{SpanSessionWrite, SpanHarnessRun, SpanHarnessTurn, SpanHarnessCheckpoint, SpanHarnessStep, SpanAIRequest} {
		found := false
		for _, name := range names {
			if name == required {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("telemetry names = %v, missing %q", names, required)
		}
	}
	for _, span := range recorder.spans {
		for key := range span.Attributes {
			if key == "prompt" || key == "completion" || key == "payload" || key == "headers" {
				t.Fatalf("sensitive telemetry attribute %q in %q", key, span.Name)
			}
		}
	}
}

func TestHarnessTelemetryStorageParity(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		repo SessionRepo
	}{
		{name: "memory", repo: NewMemorySessionRepo(SessionCodecOptions{})},
		{name: "jsonl", repo: NewJSONLSessionRepo(t.TempDir(), JSONLStorageOptions{})},
		{name: "sqlite", repo: NewSQLiteSessionRepo(t.TempDir(), SQLiteStorageOptions{OwnerID: "telemetry"})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			session, err := tc.repo.Create(ctx, SessionCreateOptions{ID: tc.name})
			if err != nil {
				t.Fatal(err)
			}
			recorder := &telemetryTestRecorder{}
			harness, _, err := NewHarness(ctx, AgentHarnessOptions{Session: session, Models: &scriptedModels{}, Model: Model{Provider: "provider", ModelID: "model"}, Telemetry: recorder})
			if err != nil {
				t.Fatal(err)
			}
			result, err := harness.Prompt(ctx, PromptInput{Text: "prompt"})
			if err != nil || !result.OK {
				t.Fatalf("prompt = %v %+v", err, result)
			}
			if err := harness.Close(ctx); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, name := range recorder.names() {
				if name == SpanSessionWrite {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("%s emitted no session write span: %v", tc.name, recorder.names())
			}
		})
	}
}
