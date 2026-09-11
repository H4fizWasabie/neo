package tui

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/H4fizWasabie/neo/harness"
)

type Client interface {
	Snapshot(context.Context) (harness.LaneSnapshot, error)
	Watch(context.Context) (harness.WatchHandle[harness.LaneSnapshot], error)
	Prompt(context.Context, harness.PromptInput) (harness.Result[harness.RunOutcome, error], error)
	Abort(context.Context) (harness.Result[harness.AbortOutcome, error], error)
}

type App struct {
	Client Client
	In     io.Reader
	Out    io.Writer
}

func (a App) Run(ctx context.Context) error {
	if a.Client == nil || a.In == nil || a.Out == nil {
		return fmt.Errorf("tui client, input, and output are required")
	}
	snapshot, err := a.Client.Snapshot(ctx)
	if err != nil {
		return err
	}
	handle, err := a.Client.Watch(ctx)
	if err != nil {
		return err
	}
	defer handle.Unsubscribe()
	events := make(chan harness.HarnessEvent, 16)
	handle.Start(func(event harness.HarnessEvent) {
		select {
		case events <- event:
		default:
		}
	})
	input := make(chan string)
	go readLines(a.In, input)
	if err := render(a.Out, snapshot); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-events:
			if _, err := fmt.Fprintf(a.Out, "\r[event] %s\n", event.Type); err != nil {
				return err
			}
		case line, ok := <-input:
			if !ok || strings.TrimSpace(line) == ":q" {
				return nil
			}
			line = strings.TrimSpace(line)
			if line == ":abort" {
				if _, err := a.Client.Abort(ctx); err != nil {
					return err
				}
				continue
			}
			if line == "" {
				continue
			}
			if _, err := a.Client.Prompt(ctx, harness.PromptInput{Text: line}); err != nil {
				return err
			}
		}
	}
}

func readLines(input io.Reader, output chan<- string) {
	defer close(output)
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		output <- scanner.Text()
	}
}

func render(out io.Writer, snapshot harness.LaneSnapshot) error {
	if _, err := fmt.Fprint(out, "\x1b[?1049h\x1b[2J\x1b[H"); err != nil {
		return err
	}
	defer fmt.Fprint(out, "\x1b[?1049l")
	if _, err := fmt.Fprintf(out, "Neo — lane %s\n\n", snapshot.Lane); err != nil {
		return err
	}
	for _, entry := range snapshot.Transcript {
		if entry.Message != nil {
			if _, err := fmt.Fprintf(out, "%s: %v\n", entry.Message.Role, entry.Message.Content); err != nil {
				return err
			}
		}
	}
	_, err := fmt.Fprint(out, "\n> ")
	return err
}
