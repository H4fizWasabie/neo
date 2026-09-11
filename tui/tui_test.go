package tui

import (
	"context"
	"strings"
	"testing"

	"github.com/H4fizWasabie/neo/harness"
)

type fakeClient struct {
	prompts []string
}

func (f *fakeClient) Snapshot(context.Context) (harness.LaneSnapshot, error) {
	return harness.LaneSnapshot{Lane: "main"}, nil
}
func (f *fakeClient) Watch(context.Context) (harness.WatchHandle[harness.LaneSnapshot], error) {
	return harness.WatchHandle[harness.LaneSnapshot]{Snapshot: harness.LaneSnapshot{Lane: "main"}, Start: func(func(harness.HarnessEvent)) {}, Unsubscribe: func() {}}, nil
}
func (f *fakeClient) Prompt(_ context.Context, input harness.PromptInput) (harness.Result[harness.RunOutcome, error], error) {
	f.prompts = append(f.prompts, input.Text)
	return harness.Ok[harness.RunOutcome, error](harness.RunOutcome{}), nil
}
func (f *fakeClient) Abort(context.Context) (harness.Result[harness.AbortOutcome, error], error) {
	return harness.Ok[harness.AbortOutcome, error](harness.AbortOutcome{}), nil
}

func TestFakeClientSmoke(t *testing.T) {
	fake := &fakeClient{}
	var out strings.Builder
	app := App{Client: fake, In: strings.NewReader("hello\n:q\n"), Out: &out}
	if err := app.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fake.prompts) != 1 || fake.prompts[0] != "hello" || !strings.Contains(out.String(), "lane main") {
		t.Fatalf("fake TUI did not run: prompts=%v output=%q", fake.prompts, out.String())
	}
}
