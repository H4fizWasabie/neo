package harness

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Harness struct {
	*runtimeLane
	session      Session
	models       Models
	systemPrompt func(any) (string, error)
	settings     *SettingsSnapshot
	tools        []AgentHarnessTool
	resources    Resources
	lines        sync.Map
	scheduler    *ManualScheduler
	hooks        *HookRunner
	events       *EventBus
	lifecycle    RuntimeLifecycle
	lanes        sync.Map
}

type runtimeLane struct {
	harness *Harness
	name    string
	view    SessionTree
	idle    chan struct{}
	mu      sync.Mutex
}

func NewHarness(ctx context.Context, options AgentHarnessOptions) (*Harness, []SuspendedOperation, error) {
	if options.Session == nil || options.Models == nil {
		return nil, nil, fmt.Errorf("session and models are required")
	}
	if err := contextError(ctx); err != nil {
		return nil, nil, err
	}
	if options.Drive == "" {
		options.Drive = "automatic"
	}
	if options.ToolExecution == "" {
		options.ToolExecution = ToolExecutionParallel
	}
	if options.ThinkingLevel == "" {
		options.ThinkingLevel = ThinkingOff
	}
	if options.Retry.MaxRetries < 0 {
		return nil, nil, fmt.Errorf("retry max cannot be negative")
	}
	h := &Harness{
		session:      options.Session,
		models:       options.Models,
		systemPrompt: options.SystemPrompt,
		settings:     NewSettingsSnapshot(options.StreamOptions, NormalizedRetryPolicy{MaxAttempts: options.Retry.MaxRetries + 1, BaseDelayMs: options.Retry.BaseDelayMs}),
		tools:        append([]AgentHarnessTool(nil), options.Tools...),
		resources:    cloneValue(options.Resources).(Resources),
		scheduler:    NewManualScheduler(options.Drive == "manual"),
		hooks:        NewHookRunner(),
		events:       NewEventBus(),
	}
	main, err := h.attachLane(ctx, "main", options)
	if err != nil {
		return nil, nil, err
	}
	h.runtimeLane = main
	suspended := make([]SuspendedOperation, 0)
	if restored, err := Restore(ctx, options.Session, "main"); err == nil && restored.Current != nil {
		suspended = append(suspended, SuspendedOperation{Lane: "main", OperationID: restored.Current.Operation.OperationID, Kind: restored.Current.Operation.Intent.Kind, Reason: "crash", StartedAt: restored.Current.Operation.StartedAt})
	}
	return h, suspended, nil
}

func NewAgentHarness(ctx context.Context, options AgentHarnessOptions) (*Harness, []SuspendedOperation, error) {
	return NewHarness(ctx, options)
}

func (h *Harness) attachLane(ctx context.Context, name string, options AgentHarnessOptions) (*runtimeLane, error) {
	if name == "" {
		return nil, fmt.Errorf("lane name is required")
	}
	if register, err := h.session.GetRegister(ctx, RegisterLaneState, name); err != nil {
		return nil, err
	} else if register == nil {
		config := LaneConfiguration{Model: options.Model, ThinkingLevel: options.ThinkingLevel, ActiveToolNames: append([]string{}, options.ActiveToolNames...)}
		if _, err := h.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterLaneConfig, name, config), registerSet(RegisterLaneLeaf, name, (*string)(nil)), registerSet(RegisterLaneState, name, LaneState{PendingNextRun: []string{}})}}); err != nil {
			return nil, err
		}
	} else if config, err := h.session.GetRegister(ctx, RegisterLaneConfig, name); err != nil {
		return nil, err
	} else if config == nil {
		seed := LaneConfiguration{Model: options.Model, ThinkingLevel: options.ThinkingLevel, ActiveToolNames: append([]string{}, options.ActiveToolNames...)}
		if _, err := h.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterLaneConfig, name, seed)}}); err != nil {
			return nil, err
		}
	}
	lane := &runtimeLane{harness: h, name: name, view: h.session.View(name), idle: make(chan struct{})}
	close(lane.idle)
	h.lanes.Store(name, lane)
	h.lines.Store(name, &LaneMutationLine{})
	return lane, nil
}

func (h *Harness) lane(name string) (*runtimeLane, error) {
	if value, ok := h.lanes.Load(name); ok {
		return value.(*runtimeLane), nil
	}
	return nil, sessionError(SessionInvalidLane, fmt.Errorf("unknown lane: %s", name))
}

func (h *Harness) line(name string) *LaneMutationLine {
	value, _ := h.lines.LoadOrStore(name, &LaneMutationLine{})
	return value.(*LaneMutationLine)
}

func (h *Harness) effect(ctx context.Context, info ActionInfo, fn func(context.Context) error) error {
	done, err := h.scheduler.Enqueue(ctx, ScheduledAction{Info: info, Run: fn})
	if err != nil {
		return err
	}
	return <-done
}

func (h *Harness) Hooks() Hooks   { return h.hooks }
func (h *Harness) Events() Events { return h.events }

func (h *Harness) Close(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	h.lifecycle.Close()
	h.scheduler.Close()
	return h.session.Close(ctx)
}

func (h *Harness) Name() string         { return h.runtimeLane.name }
func (h *Harness) Session() SessionTree { return h.runtimeLane.view }

func (l *runtimeLane) Name() string         { return l.name }
func (l *runtimeLane) Session() SessionTree { return l.view }

func (l *runtimeLane) GetLeafID(ctx context.Context) (*string, error) { return l.view.GetLeafID(ctx) }

func (l *runtimeLane) GetLastResult(ctx context.Context) (*LaneLastResult, error) {
	register, err := l.harness.session.GetRegister(ctx, RegisterLaneLastResult, l.name)
	if err != nil || register == nil {
		return nil, err
	}
	result, ok := register.Value.(LaneLastResult)
	if !ok {
		return nil, sessionError(SessionInvalidEntry, fmt.Errorf("lane last result has invalid type"))
	}
	return &result, nil
}

func (l *runtimeLane) Prompt(ctx context.Context, input PromptInput) (Result[RunOutcome, error], error) {
	messages := append([]AgentMessage(nil), input.Messages...)
	if input.Text != "" {
		messages = append(messages, AgentMessage{Role: "user", Content: input.Text})
	}
	if len(messages) == 0 {
		return Err[RunOutcome](fmt.Errorf("prompt is empty")), nil
	}
	if err := l.harness.lifecycle.Err(); err != nil {
		return Result[RunOutcome, error]{Err: err}, nil
	}
	model := mustModel(ctx, l)
	if _, err := l.harness.models.Resolve(ctx, model.Provider, model.ModelID); err != nil {
		return Result[RunOutcome, error]{Err: err}, nil
	}
	operationID := l.harness.session.IDGenerator().Next()
	if err := l.harness.effect(ctx, ActionInfo{Kind: "hook", Description: "before_run"}, func(ctx context.Context) error {
		systemPrompt := ""
		if l.harness.systemPrompt != nil {
			var err error
			systemPrompt, err = l.harness.systemPrompt(nil)
			if err != nil {
				return err
			}
		}
		result, err := l.harness.hooks.Run(ctx, HookInvocation{Name: HookBeforeRun, Lane: l.name, RunID: operationID, Event: map[string]JSONValue{"prompt": append([]AgentMessage(nil), messages...), "systemPrompt": systemPrompt, "resources": l.harness.resources}})
		if err != nil {
			return err
		}
		if values, ok := result.(map[string]JSONValue); ok {
			if injected, ok := values["messages"].([]AgentMessage); ok {
				messages = append(messages, injected...)
			}
		}
		return nil
	}); err != nil {
		return Result[RunOutcome, error]{Err: err}, nil
	}
	var accepted Operation
	var acceptedState OperationState
	if err := l.harness.line(l.name).Do(ctx, func() error {
		laneStateRegister, err := l.harness.session.GetRegister(ctx, RegisterLaneState, l.name)
		if err != nil {
			return err
		}
		if laneStateRegister == nil {
			return fmt.Errorf("lane does not exist: %s", l.name)
		}
		laneState, ok := laneStateRegister.Value.(LaneState)
		if !ok || laneState.CurrentOperationID != nil {
			return &LaneBusy{TaggedError: TaggedError{Message: "lane is busy"}, Lane: l.name}
		}
		leafRegister, err := l.harness.session.GetRegister(ctx, RegisterLaneLeaf, l.name)
		if err != nil {
			return err
		}
		var leaf *string
		if leafRegister != nil {
			leaf, ok = leafRegister.Value.(*string)
			if !ok {
				return fmt.Errorf("lane leaf has invalid type")
			}
		}
		configRegister, err := l.harness.session.GetRegister(ctx, RegisterLaneConfig, l.name)
		if err != nil {
			return err
		}
		if configRegister == nil {
			return fmt.Errorf("lane has no configuration: %s", l.name)
		}
		config, ok := configRegister.Value.(LaneConfiguration)
		if !ok {
			return fmt.Errorf("lane configuration has invalid type")
		}
		writes := make([]Write, 0, len(messages)+4)
		parent := leaf
		promptIDs := make([]string, 0, len(messages))
		for _, message := range messages {
			id := l.harness.session.IDGenerator().Next()
			entry := Entry{EntryBase: EntryBase{ID: id, ParentID: cloneStringPointer(parent), Type: EntryMessage}, Message: messageCopy(message)}
			writes = append(writes, Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}})
			promptIDs = append(promptIDs, id)
			parent = &id
		}
		if len(promptIDs) == 0 {
			return fmt.Errorf("prompt is empty")
		}
		accepted = Operation{OperationID: operationID, Lane: l.name, SourceLeafID: cloneStringPointer(leaf), StartedAt: time.Now().UnixMilli(), Intent: OperationIntent{Kind: OperationRun, PromptEntryIDs: promptIDs}}
		acceptedState = OperationState{Kind: OperationRun, Run: &RunState{Kind: OperationRun, Control: Control{Status: ControlRunning}, Settings: RunSettings{ToolExecution: ToolExecutionParallel}, Phase: RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationNeedAssistant}, TriggerEntryID: *parent, SkipInboxOnce: true}}, Inbox: Inbox{}}}
		writes = append(writes, registerSet(RegisterOpMeta, operationID, accepted), registerSet(RegisterOpState, operationID, acceptedState), registerSet(RegisterLaneLeaf, l.name, parent), registerSet(RegisterLaneState, l.name, LaneState{CurrentOperationID: &operationID, PendingNextRun: laneState.PendingNextRun}))
		_, err = l.harness.session.Commit(ctx, Transaction{Writes: writes})
		_ = config
		return err
	}); err != nil {
		return Result[RunOutcome, error]{Err: err}, nil
	}
	result, err := l.harness.drive(ctx, l, accepted, acceptedState)
	if err != nil {
		return Result[RunOutcome, error]{Err: err}, nil
	}
	return Ok[RunOutcome, error](result), nil
}

func mustModel(ctx context.Context, l *runtimeLane) Model {
	register, err := l.harness.session.GetRegister(ctx, RegisterLaneConfig, l.name)
	if err != nil || register == nil {
		return Model{}
	}
	config, ok := register.Value.(LaneConfiguration)
	if !ok {
		return Model{}
	}
	return config.Model
}

func (h *Harness) drive(ctx context.Context, lane *runtimeLane, operation Operation, state OperationState) (RunOutcome, error) {
	configRegister, err := h.session.GetRegister(ctx, RegisterLaneConfig, lane.name)
	if err != nil {
		return RunOutcome{}, err
	}
	config := configRegister.Value.(LaneConfiguration)
	settings := h.settings.Snapshot()
	ready := state
	ready.Run.Phase = RunPhase{Kind: PhaseAssistant, Generation: &Generation{Status: GenerationReady, NextAttempt: 1, Context: GenerationContext{StepID: operation.OperationID + ":step:1", TriggerEntryID: ready.Run.Phase.Checkpoint.TriggerEntryID, Configuration: config, StreamOptions: settings.StreamOptions, RetryPolicy: settings.RetryPolicy}}}
	if err := h.commitState(ctx, lane, operation.OperationID, ready); err != nil {
		return RunOutcome{}, err
	}
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "before_request"}, func(ctx context.Context) error {
		_, err := h.hooks.Run(ctx, HookInvocation{Name: HookBeforeRequest, Lane: lane.name, RunID: operation.OperationID, Event: map[string]JSONValue{"step": "assistant", "attempt": int64(1), "streamOptions": settings.StreamOptions}})
		return err
	}); err != nil {
		return RunOutcome{}, err
	}
	responseID := h.session.IDGenerator().Next()
	usageID := h.session.IDGenerator().Next()
	pending := ready
	pending.Run.Phase.Generation.Status = GenerationEffectPending
	pending.Run.Phase.Generation.Attempt = 1
	pending.Run.Phase.Generation.ResponseEntryID = responseID
	pending.Run.Phase.Generation.UsageID = usageID
	if err := h.commitState(ctx, lane, operation.OperationID, pending); err != nil {
		return RunOutcome{}, err
	}
	message := AgentMessage{Role: "assistant"}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventMessageStart), Lane: lane.name, Payload: cloneValue(message)})
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "before_payload"}, func(ctx context.Context) error {
		_, err := h.hooks.Run(ctx, HookInvocation{Name: HookBeforePayload, Lane: lane.name, RunID: operation.OperationID, Event: map[string]JSONValue{"model": pending.Run.Phase.Generation.Context.Configuration.Model, "payload": "provider-request"}})
		return err
	}); err != nil {
		return RunOutcome{}, err
	}
	if err := h.effect(ctx, ActionInfo{Kind: "provider", Description: "assistant stream"}, func(ctx context.Context) error {
		entries, err := lane.view.FindEntriesOnBranch(ctx, BranchScan{Start: pending.Run.Phase.Generation.Context.TriggerEntryID, Order: OldestFirst})
		if err != nil {
			return err
		}
		messages := make([]AgentMessage, 0, len(entries))
		for _, entry := range entries {
			if entry.Message != nil {
				messages = append(messages, *entry.Message)
			}
		}
		model := pending.Run.Phase.Generation.Context.Configuration.Model
		stream, err := h.models.Stream(ctx, model, messages, pending.Run.Phase.Generation.Context.StreamOptions)
		if err != nil {
			return err
		}
		for event := range stream {
			if event.Message != nil {
				message = *event.Message
				h.events.Emit(ctx, HarnessEvent{Type: string(EventMessageUpdate), Lane: lane.name, Payload: cloneValue(message)})
			}
		}
		if message.Role == "" {
			message = AgentMessage{Role: "assistant", Content: ""}
		}
		return nil
	}); err != nil {
		return RunOutcome{}, err
	}
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "after_response"}, func(ctx context.Context) error {
		result, err := h.hooks.Run(ctx, HookInvocation{Name: HookAfterResponse, Lane: lane.name, RunID: operation.OperationID, Event: map[string]JSONValue{"message": message}})
		if err != nil {
			return err
		}
		if patch, ok := result.(map[string]JSONValue); ok {
			if replacement, ok := patch["message"].(AgentMessage); ok {
				message = replacement
			}
		}
		return nil
	}); err != nil {
		return RunOutcome{}, err
	}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventMessageEnd), Lane: lane.name, Payload: cloneValue(message)})
	entry := Entry{EntryBase: EntryBase{ID: responseID, ParentID: stringPointer(pending.Run.Phase.Generation.Context.TriggerEntryID), Type: EntryMessage}, Message: messageCopy(message)}
	usage := Usage{}
	if message.Usage != nil {
		usage = *message.Usage
	}
	if _, err := h.session.Commit(ctx, Transaction{Writes: []Write{Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}}, Write{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: usageID, Usage: usage, EntryID: &responseID}}}, registerSet(RegisterLaneLeaf, lane.name, stringPointer(responseID)), registerSet(RegisterOpState, operation.OperationID, OperationState{Kind: OperationRun, Run: &RunState{Kind: OperationRun, Control: Control{Status: ControlRunning}, Phase: RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationMayFinish}, TriggerEntryID: responseID}}, Inbox: Inbox{}, LatestAssistantEntryID: &responseID}})}}); err != nil {
		return RunOutcome{}, err
	}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventEntryAdded), Lane: lane.name, Payload: cloneValue(entry)})
	h.events.Emit(ctx, HarnessEvent{Type: string(EventUsage), Lane: lane.name, Payload: cloneValue(usage)})
	result := RunOutcome{Kind: "completed", LeafID: stringPointer(responseID), FinalEntryID: stringPointer(responseID), FinalMessage: &message}
	laneStateRegister, err := h.session.GetRegister(ctx, RegisterLaneState, lane.name)
	if err != nil {
		return RunOutcome{}, err
	}
	laneState := LaneState{PendingNextRun: []string{}}
	if laneStateRegister != nil {
		if current, ok := laneStateRegister.Value.(LaneState); ok {
			laneState = current
			laneState.CurrentOperationID = nil
		}
	}
	if _, err := h.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterLaneLastResult, lane.name, LaneLastResult{OperationID: operation.OperationID, Kind: OperationRun, Outcome: result.Kind, LeafID: result.LeafID, FinalAssistantEntryID: result.FinalEntryID}), registerDelete(RegisterOpMeta, operation.OperationID), registerDelete(RegisterOpState, operation.OperationID), registerSet(RegisterLaneState, lane.name, laneState)}}); err != nil {
		return RunOutcome{}, err
	}
	return result, nil
}

func (h *Harness) commitState(ctx context.Context, lane *runtimeLane, operationID string, state OperationState) error {
	return h.line(lane.name).Do(ctx, func() error {
		_, err := h.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpState, operationID, state)}})
		return err
	})
}

func messageCopy(message AgentMessage) *AgentMessage {
	copy := cloneValue(message).(AgentMessage)
	return &copy
}

func registerDelete(namespace RegisterNamespace, key string) Write {
	return Write{Kind: WriteRegister, Register: &RegisterWrite{Operation: RegisterDelete, Namespace: namespace, Key: key}}
}

func (l *runtimeLane) Skill(context.Context, string, string) (Result[RunOutcome, error], error) {
	return Err[RunOutcome](fmt.Errorf("skill is not implemented")), nil
}
func (l *runtimeLane) PromptFromTemplate(context.Context, string, []string) (Result[RunOutcome, error], error) {
	return Err[RunOutcome](fmt.Errorf("prompt template is not implemented")), nil
}
func (l *runtimeLane) Compact(context.Context, string) (Result[CompactionOutcome, error], error) {
	return Err[CompactionOutcome](fmt.Errorf("compaction is not implemented")), nil
}
func (l *runtimeLane) NavigateTree(context.Context, *string, NavigateOptions) (Result[NavigationOutcome, error], error) {
	return Err[NavigationOutcome](fmt.Errorf("navigation is not implemented")), nil
}
func (l *runtimeLane) Resume(context.Context) (Result[ResumeOutcome, error], error) {
	return Err[ResumeOutcome](fmt.Errorf("resume is not implemented")), nil
}
func (l *runtimeLane) Abort(context.Context) (Result[AbortOutcome, error], error) {
	return Err[AbortOutcome](fmt.Errorf("abort is not implemented")), nil
}
func (l *runtimeLane) Steer(context.Context, PromptInput) (Result[QueueOutcome, error], error) {
	return Err[QueueOutcome](fmt.Errorf("steer is not implemented")), nil
}
func (l *runtimeLane) FollowUp(context.Context, PromptInput) (Result[QueueOutcome, error], error) {
	return Err[QueueOutcome](fmt.Errorf("follow-up is not implemented")), nil
}
func (l *runtimeLane) NextRun(context.Context, PromptInput) (Result[NextRunOutcome, error], error) {
	return Err[NextRunOutcome](fmt.Errorf("next-run is not implemented")), nil
}
func (l *runtimeLane) CancelQueued(context.Context, string) (Result[CancelQueuedOutcome, error], error) {
	return Err[CancelQueuedOutcome](fmt.Errorf("queue cancellation is not implemented")), nil
}
func (l *runtimeLane) RecordUsage(context.Context, Usage, *string, JSONValue) (Result[RecordUsageOutcome, error], error) {
	return Err[RecordUsageOutcome](fmt.Errorf("usage recording is not implemented")), nil
}
func (l *runtimeLane) WaitForIdle(ctx context.Context) error {
	select {
	case <-l.idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (l *runtimeLane) RunWhenIdle(ctx context.Context, callback func() error) error {
	if err := l.WaitForIdle(ctx); err != nil {
		return err
	}
	return callback()
}
func (l *runtimeLane) PeekAction(context.Context) (*ActionInfo, error) {
	return l.harness.scheduler.Peek(), nil
}
func (l *runtimeLane) ExecuteAction(ctx context.Context) (*ActionInfo, error) {
	return l.harness.scheduler.Execute(ctx)
}
func (l *runtimeLane) RunToCompletion(ctx context.Context) error {
	return l.harness.scheduler.RunToCompletion(ctx)
}
func (l *runtimeLane) GetModel(ctx context.Context) (*Model, error) {
	model := mustModel(ctx, l)
	if model.Provider == "" || model.ModelID == "" {
		return nil, nil
	}
	return &model, nil
}
func (l *runtimeLane) SetModel(ctx context.Context, model Model) error {
	config, err := l.configuration(ctx)
	if err != nil {
		return err
	}
	config.Model = model
	return l.setConfiguration(ctx, config)
}
func (l *runtimeLane) GetThinkingLevel(ctx context.Context) (ThinkingLevel, error) {
	config, err := l.configuration(ctx)
	return config.ThinkingLevel, err
}
func (l *runtimeLane) SetThinkingLevel(ctx context.Context, level ThinkingLevel) error {
	config, err := l.configuration(ctx)
	if err != nil {
		return err
	}
	config.ThinkingLevel = level
	return l.setConfiguration(ctx, config)
}
func (l *runtimeLane) GetActiveTools(ctx context.Context) ([]string, error) {
	config, err := l.configuration(ctx)
	return append([]string(nil), config.ActiveToolNames...), err
}
func (l *runtimeLane) SetActiveTools(ctx context.Context, names []string) error {
	config, err := l.configuration(ctx)
	if err != nil {
		return err
	}
	config.ActiveToolNames = append([]string(nil), names...)
	return l.setConfiguration(ctx, config)
}
func (l *runtimeLane) configuration(ctx context.Context) (LaneConfiguration, error) {
	register, err := l.harness.session.GetRegister(ctx, RegisterLaneConfig, l.name)
	if err != nil {
		return LaneConfiguration{}, err
	}
	if register == nil {
		return LaneConfiguration{}, fmt.Errorf("lane has no configuration")
	}
	config, ok := register.Value.(LaneConfiguration)
	if !ok {
		return LaneConfiguration{}, fmt.Errorf("lane configuration has invalid type")
	}
	return config, nil
}
func (l *runtimeLane) setConfiguration(ctx context.Context, config LaneConfiguration) error {
	return l.harness.line(l.name).Do(ctx, func() error {
		_, err := l.harness.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterLaneConfig, l.name, config)}})
		return err
	})
}
func (l *runtimeLane) Watch(ctx context.Context) (WatchHandle[LaneSnapshot], error) {
	snapshot, err := l.snapshot(ctx)
	return WatchHandle[LaneSnapshot]{Snapshot: snapshot, Start: func(_ func(HarnessEvent)) {}, Unsubscribe: func() {}}, err
}
func (l *runtimeLane) snapshot(ctx context.Context) (LaneSnapshot, error) {
	leaf, err := l.GetLeafID(ctx)
	if err != nil {
		return LaneSnapshot{}, err
	}
	entries, err := l.view.FindEntriesOnBranch(ctx, BranchScan{Order: OldestFirst})
	if err != nil {
		return LaneSnapshot{}, err
	}
	return LaneSnapshot{Lane: l.name, Transcript: entries, LeafID: leaf}, nil
}

func (h *Harness) Lane(ctx context.Context, name string) (AgentLane, error) {
	lane, err := h.lane(name)
	return lane, err
}
func (h *Harness) CreateLane(context.Context, string, *string) (Result[AgentLane, error], error) {
	return Err[AgentLane](fmt.Errorf("lane creation is not implemented")), nil
}
func (h *Harness) Lanes(ctx context.Context) ([]LaneInfo, error) {
	var result []LaneInfo
	h.lanes.Range(func(_, value any) bool {
		lane := value.(*runtimeLane)
		leaf, _ := lane.GetLeafID(ctx)
		result = append(result, LaneInfo{Name: lane.name, LeafID: leaf})
		return true
	})
	return result, nil
}
func (h *Harness) GetTools(context.Context) ([]AgentHarnessTool, error) {
	return append([]AgentHarnessTool(nil), h.tools...), nil
}
func (h *Harness) SetTools(context.Context, []AgentHarnessTool) error {
	return fmt.Errorf("tool registry replacement is not implemented")
}
func (h *Harness) GetResources(context.Context) (Resources, error) {
	return cloneValue(h.resources).(Resources), nil
}
func (h *Harness) SetResources(context.Context, Resources) error {
	return fmt.Errorf("resource replacement is not implemented")
}
func (h *Harness) GetStreamOptions(context.Context) (AgentHarnessStreamOptions, error) {
	return h.settings.Snapshot().StreamOptions, nil
}
func (h *Harness) SetStreamOptions(_ context.Context, options AgentHarnessStreamOptions) error {
	h.settings.Update(options, h.settings.Snapshot().RetryPolicy)
	return nil
}
func (h *Harness) GetRetryPolicy(context.Context) (RetryPolicy, error) {
	snapshot := h.settings.Snapshot()
	return RetryPolicy{Enabled: snapshot.RetryPolicy.MaxAttempts > 1, MaxRetries: snapshot.RetryPolicy.MaxAttempts - 1, BaseDelayMs: snapshot.RetryPolicy.BaseDelayMs}, nil
}
func (h *Harness) SetRetryPolicy(_ context.Context, policy RetryPolicy) error {
	h.settings.Update(h.settings.Snapshot().StreamOptions, NormalizedRetryPolicy{MaxAttempts: policy.MaxRetries + 1, BaseDelayMs: policy.BaseDelayMs})
	return nil
}
func (h *Harness) GetCompactionSettings(context.Context) (CompactionSettings, error) {
	return CompactionSettings{}, nil
}
func (h *Harness) SetCompactionSettings(context.Context, CompactionSettings) error {
	return fmt.Errorf("compaction settings are not implemented")
}
func (h *Harness) GetSteeringMode(context.Context) (QueueMode, error) { return QueueAll, nil }
func (h *Harness) SetSteeringMode(context.Context, QueueMode) error   { return nil }
func (h *Harness) GetFollowUpMode(context.Context) (QueueMode, error) { return QueueAll, nil }
func (h *Harness) SetFollowUpMode(context.Context, QueueMode) error   { return nil }
func (h *Harness) WatchSession(context.Context) (WatchHandle[SessionSnapshot], error) {
	return WatchHandle[SessionSnapshot]{Snapshot: SessionSnapshot{}, Start: func(_ func(HarnessEvent)) {}, Unsubscribe: func() {}}, nil
}

var _ AgentHarness = (*Harness)(nil)
