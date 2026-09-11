package harness

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Harness struct {
	*runtimeLane
	session            Session
	models             Models
	systemPrompt       func(any) (string, error)
	toProviderMessages func([]AgentMessage) ([]Message, error)
	telemetry          TelemetryContext
	settings           *SettingsSnapshot
	tools              []AgentHarnessTool
	resources          Resources
	compaction         CompactionSettings
	steeringMode       QueueMode
	followUpMode       QueueMode
	toolExecution      ToolExecutionMode
	lines              sync.Map
	scheduler          *ManualScheduler
	hooks              *HookRunner
	events             *EventBus
	lifecycle          RuntimeLifecycle
	lanes              sync.Map
	rootCtx            context.Context
	cancel             context.CancelFunc
	effects            sync.WaitGroup
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
	rootCtx, cancel := context.WithCancel(ctx)
	h := &Harness{
		session:            options.Session,
		models:             options.Models,
		systemPrompt:       options.SystemPrompt,
		toProviderMessages: options.ToProviderMessages,
		telemetry:          options.Telemetry,
		settings:           NewSettingsSnapshot(options.StreamOptions, NormalizedRetryPolicy{MaxAttempts: options.Retry.MaxRetries + 1, BaseDelayMs: options.Retry.BaseDelayMs}),
		tools:              append([]AgentHarnessTool(nil), options.Tools...),
		resources:          cloneValue(options.Resources).(Resources),
		compaction:         options.Compaction,
		steeringMode:       options.SteeringMode,
		followUpMode:       options.FollowUpMode,
		toolExecution:      options.ToolExecution,
		scheduler:          NewManualScheduler(options.Drive == "manual"),
		hooks:              NewHookRunner(),
		events:             NewEventBus(),
		rootCtx:            rootCtx,
		cancel:             cancel,
	}
	main, err := h.attachLane(ctx, "main", options)
	if err != nil {
		return nil, nil, err
	}
	h.runtimeLane = main
	suspended := make([]SuspendedOperation, 0)
	if restored, err := Restore(ctx, options.Session, "main"); err != nil {
		cancel()
		return nil, nil, err
	} else if restored.Current != nil {
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
	effectCtx, cancel := context.WithCancel(h.rootCtx)
	defer cancel()
	stopCaller := context.AfterFunc(ctx, cancel)
	defer stopCaller()
	h.effects.Add(1)
	run := fn
	if h.telemetry != nil {
		run = func(ctx context.Context) error {
			return h.telemetry.StartSpan(ctx, SpanOptions{Name: info.Description}, func(TelemetrySpan) error { return fn(ctx) })
		}
	}
	done, err := h.scheduler.Enqueue(effectCtx, ScheduledAction{Info: info, Run: run})
	if err != nil {
		h.effects.Done()
		return err
	}
	err = <-done
	h.effects.Done()
	return err
}

func (h *Harness) Hooks() Hooks   { return h.hooks }
func (h *Harness) Events() Events { return h.events }

func (h *Harness) Close(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	h.lifecycle.Close()
	h.cancel()
	h.scheduler.Close()
	h.effects.Wait()
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
		content := JSONValue(input.Text)
		if len(input.Images) != 0 {
			content = map[string]JSONValue{"text": input.Text, "images": append([]ImageContent(nil), input.Images...)}
		}
		messages = append(messages, AgentMessage{Role: "user", Content: content})
	} else if len(input.Images) != 0 {
		messages = append(messages, AgentMessage{Role: "user", Content: append([]ImageContent(nil), input.Images...)})
	}
	if len(messages) == 0 {
		pending, err := l.hasPendingNextRun(ctx)
		if err != nil {
			return Result[RunOutcome, error]{Err: err}, nil
		}
		if !pending {
			return Err[RunOutcome, error](&InvalidMessage{TaggedError: TaggedError{Message: "prompt is empty"}, Lane: l.name, Reason: "no messages"}), nil
		}
	}
	callerMessages := append([]AgentMessage(nil), messages...)
	if err := l.harness.lifecycle.Err(); err != nil {
		return Result[RunOutcome, error]{Err: err}, nil
	}
	model := mustModel(ctx, l)
	if _, err := l.harness.models.Resolve(ctx, model.Provider, model.ModelID); err != nil {
		return Result[RunOutcome, error]{Err: &MissingIdentities{TaggedError: TaggedError{Message: err.Error()}, Lane: l.name, Models: []string{model.Provider + "/" + model.ModelID}}}, nil
	}
	if missing := missingActiveTools(l.harness.tools, mustActiveTools(ctx, l)); len(missing) != 0 {
		return Result[RunOutcome, error]{Err: &MissingIdentities{TaggedError: TaggedError{Message: "active tool is unavailable"}, Lane: l.name, Tools: missing}}, nil
	}
	operationID := l.harness.session.IDGenerator().Next()
	fx := &runtimeEffects{harness: l.harness, lane: l}
	capturedSystemPrompt := ""
	var resumeData map[string]JSONValue
	if l.harness.systemPrompt != nil {
		var err error
		capturedSystemPrompt, err = l.harness.systemPrompt(nil)
		if err != nil {
			return Result[RunOutcome, error]{Err: err}, nil
		}
	}
	if l.harness.hooks.Has(HookBeforeRun) {
		if err := l.harness.effect(ctx, ActionInfo{Kind: "hook", Description: "before_run"}, func(ctx context.Context) error {
			output, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(operationID), HookName: HookBeforeRun, Event: map[string]JSONValue{"prompt": append([]AgentMessage(nil), messages...), "systemPrompt": capturedSystemPrompt, "resources": l.harness.resources}})
			if err != nil {
				return err
			}
			result := output.Result
			if values, ok := result.(map[string]JSONValue); ok {
				if injected, ok := values["messages"].([]AgentMessage); ok {
					messages = append(messages, injected...)
				}
				if override, ok := values["systemPrompt"].(string); ok {
					capturedSystemPrompt = override
				}
				if data, ok := values["resumeData"].(map[string]JSONValue); ok {
					resumeData = data
				}
			}
			return nil
		}); err != nil {
			return Result[RunOutcome, error]{Err: err}, nil
		}
	}
	var accepted Operation
	var acceptedState OperationState
	l.begin()
	acceptedInLine := false
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
		writes := make([]Write, 0, len(messages)+len(laneState.PendingNextRun)+6)
		parent := leaf
		for _, id := range laneState.PendingNextRun {
			register, err := l.harness.session.GetRegister(ctx, RegisterPendingEntry, id)
			if err != nil {
				return err
			}
			if register == nil {
				return fmt.Errorf("pending next-run item %s has no payload register", id)
			}
			pending, ok := register.Value.(PendingEntry)
			if !ok || pending.Type != EntryMessage {
				return fmt.Errorf("pending next-run item %s has invalid payload", id)
			}
			message, ok := pending.Payload.(AgentMessage)
			if !ok {
				return fmt.Errorf("pending next-run item %s has invalid message", id)
			}
			entry := Entry{EntryBase: EntryBase{ID: id, ParentID: cloneStringPointer(parent), Type: EntryMessage}, Message: messageCopy(message)}
			writes = append(writes, Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}}, registerDelete(RegisterPendingEntry, id))
			parent = &id
		}
		promptIDs := make([]string, 0, len(callerMessages))
		for _, message := range messages {
			id := l.harness.session.IDGenerator().Next()
			entry := Entry{EntryBase: EntryBase{ID: id, ParentID: cloneStringPointer(parent), Type: EntryMessage}, Message: messageCopy(message)}
			writes = append(writes, Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}})
			if len(promptIDs) < len(callerMessages) {
				promptIDs = append(promptIDs, id)
			}
			parent = &id
		}
		if len(promptIDs) == 0 {
			return fmt.Errorf("prompt is empty")
		}
		accepted = Operation{OperationID: operationID, Lane: l.name, SourceLeafID: cloneStringPointer(leaf), StartedAt: time.Now().UnixMilli(), Intent: OperationIntent{Kind: OperationRun, PromptEntryIDs: promptIDs, SystemPromptOverride: capturedSystemPrompt, ResumeData: resumeData}}
		acceptedState = OperationState{Kind: OperationRun, Run: &RunState{Kind: OperationRun, Control: Control{Status: ControlRunning}, Settings: RunSettings{Compaction: l.harness.compaction, SteeringMode: l.harness.steeringMode, FollowUpMode: l.harness.followUpMode, ToolExecution: l.harness.toolExecution}, Phase: RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationNeedAssistant}, TriggerEntryID: *parent, SkipInboxOnce: true}}, Inbox: Inbox{}}}
		laneState.CurrentOperationID = &operationID
		laneState.PendingNextRun = []string{}
		writes = append(writes, registerSet(RegisterOpMeta, operationID, accepted), registerSet(RegisterOpState, operationID, acceptedState), registerSet(RegisterLaneLeaf, l.name, parent), registerSet(RegisterLaneState, l.name, laneState))
		_, err = l.harness.session.Commit(ctx, Transaction{Writes: writes})
		_ = config
		acceptedInLine = err == nil
		return err
	}); err != nil {
		l.finish()
		return Result[RunOutcome, error]{Err: err}, nil
	}
	if !acceptedInLine {
		l.finish()
		return Result[RunOutcome, error]{Err: fmt.Errorf("prompt was not accepted")}, nil
	}
	l.harness.events.Emit(ctx, HarnessEvent{Type: string(EventRunStart), Lane: l.name, Payload: map[string]JSONValue{"runId": operationID}})
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

func mustActiveTools(ctx context.Context, l *runtimeLane) []string {
	register, err := l.harness.session.GetRegister(ctx, RegisterLaneConfig, l.name)
	if err != nil || register == nil {
		return nil
	}
	config, ok := register.Value.(LaneConfiguration)
	if !ok {
		return nil
	}
	return config.ActiveToolNames
}

func (l *runtimeLane) hasPendingNextRun(ctx context.Context) (bool, error) {
	register, err := l.harness.session.GetRegister(ctx, RegisterLaneState, l.name)
	if err != nil || register == nil {
		return false, err
	}
	state, ok := register.Value.(LaneState)
	if !ok {
		return false, fmt.Errorf("lane state has invalid type")
	}
	return len(state.PendingNextRun) != 0, nil
}

func missingActiveTools(tools []AgentHarnessTool, active []string) []string {
	available := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		if tool != nil {
			available[tool.Name()] = struct{}{}
		}
	}
	missing := make([]string, 0)
	for _, name := range active {
		if _, ok := available[name]; !ok {
			missing = append(missing, name)
		}
	}
	return missing
}

func applyStreamOptions(options AgentHarnessStreamOptions, patch AgentHarnessStreamOptionsPatch) AgentHarnessStreamOptions {
	if patch.Transport != nil {
		options.Transport = *patch.Transport
	}
	if patch.TimeoutMs != nil {
		options.TimeoutMs = *patch.TimeoutMs
	}
	if patch.MaxRetries != nil {
		options.MaxRetries = *patch.MaxRetries
	}
	if patch.MaxRetryDelayMs != nil {
		options.MaxRetryDelayMs = *patch.MaxRetryDelayMs
	}
	if patch.CacheRetention != nil {
		options.CacheRetention = *patch.CacheRetention
	}
	if patch.Deferred != nil {
		options.Deferred = cloneValue(*patch.Deferred)
	}
	if patch.Headers != nil {
		if options.Headers == nil {
			options.Headers = make(map[string]string)
		}
		for key, value := range patch.Headers {
			if value == nil {
				delete(options.Headers, key)
			} else {
				options.Headers[key] = *value
			}
		}
	}
	if patch.Metadata != nil {
		if options.Metadata == nil {
			options.Metadata = make(map[string]JSONValue)
		}
		for key, value := range patch.Metadata {
			if value == nil {
				delete(options.Metadata, key)
			} else {
				options.Metadata[key] = cloneValue(*value)
			}
		}
	}
	return options
}

func (h *Harness) drive(ctx context.Context, lane *runtimeLane, operation Operation, state OperationState) (RunOutcome, error) {
	fx := &runtimeEffects{harness: h, lane: lane}
	configRegister, err := h.session.GetRegister(ctx, RegisterLaneConfig, lane.name)
	if err != nil {
		return RunOutcome{}, err
	}
	config := configRegister.Value.(LaneConfiguration)
	settings := h.settings.Snapshot()
	ready := state
	ready.Run.Phase = RunPhase{Kind: PhaseAssistant, Generation: &Generation{Status: GenerationReady, NextAttempt: 1, Context: GenerationContext{StepID: operation.OperationID + ":step:1", TriggerEntryID: ready.Run.Phase.Checkpoint.TriggerEntryID, Configuration: config, StreamOptions: settings.StreamOptions, RetryPolicy: settings.RetryPolicy}}}
	if _, err := fx.CommitTransition(ctx, CurrentOperation{Operation: operation}, ready, h.telemetry, nil, nil); err != nil {
		return RunOutcome{}, err
	}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventTurnStart), Lane: lane.name, Payload: map[string]JSONValue{"runId": operation.OperationID, "turnId": ready.Run.Phase.Generation.Context.StepID}})
	requestOptions := settings.StreamOptions
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "before_request"}, func(ctx context.Context) error {
		output, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(operation.OperationID + ":before_request"), HookName: HookBeforeRequest, Event: map[string]JSONValue{"step": "assistant", "attempt": int64(1), "streamOptions": requestOptions}})
		result := output.Result
		if values, ok := result.(map[string]JSONValue); ok {
			if patch, ok := values["streamOptions"].(AgentHarnessStreamOptionsPatch); ok {
				requestOptions = applyStreamOptions(requestOptions, patch)
			}
		}
		return err
	}); err != nil {
		return h.failOperation(ctx, lane, operation, ready, err)
	}
	responseID := h.session.IDGenerator().Next()
	usageID := h.session.IDGenerator().Next()
	pending := ready
	pending.Run.Phase.Generation.Status = GenerationEffectPending
	pending.Run.Phase.Generation.Attempt = 1
	pending.Run.Phase.Generation.ResponseEntryID = responseID
	pending.Run.Phase.Generation.UsageID = usageID
	pending.Run.Phase.Generation.Context.StreamOptions = requestOptions
	if _, err := fx.CommitTransition(ctx, CurrentOperation{Operation: operation, State: ready}, pending, h.telemetry, nil, nil); err != nil {
		return RunOutcome{}, err
	}
	message := AgentMessage{Role: "assistant"}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventMessageStart), Lane: lane.name, Payload: cloneValue(message)})
	payload := JSONValue("provider-request")
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "before_payload"}, func(ctx context.Context) error {
		output, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(operation.OperationID + ":before_payload"), HookName: HookBeforePayload, Event: map[string]JSONValue{"model": pending.Run.Phase.Generation.Context.Configuration.Model, "payload": payload}})
		if values, ok := output.Result.(map[string]JSONValue); ok {
			if replacement, ok := values["payload"]; ok {
				payload = replacement
			}
		}
		return err
	}); err != nil {
		return h.failOperation(ctx, lane, operation, pending, err)
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
		if operation.Intent.SystemPromptOverride != "" {
			messages = append([]AgentMessage{{Role: "system", Content: operation.Intent.SystemPromptOverride}}, messages...)
		}
		providerMessages := make([]Message, len(messages))
		copy(providerMessages, messages)
		if h.toProviderMessages != nil {
			providerMessages, err = h.toProviderMessages(messages)
			if err != nil {
				return err
			}
		}
		output, err := fx.Run(ctx, EffectPlan{Kind: EffectAssistant, Key: EffectKey(operation.OperationID + ":assistant"), Model: pending.Run.Phase.Generation.Context.Configuration.Model, Messages: providerMessages, StreamOptions: pending.Run.Phase.Generation.Context.StreamOptions})
		if err != nil {
			return err
		}
		if output.Message == nil {
			return fmt.Errorf("provider returned no assistant message")
		}
		message = *output.Message
		return nil
	}); err != nil {
		return h.failOperation(ctx, lane, operation, pending, err)
	}
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "after_response"}, func(ctx context.Context) error {
		output, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(operation.OperationID + ":after_response"), HookName: HookAfterResponse, Event: map[string]JSONValue{"message": message}})
		if err != nil {
			return err
		}
		result := output.Result
		if patch, ok := result.(map[string]JSONValue); ok {
			if replacement, ok := patch["message"].(AgentMessage); ok {
				message = replacement
			}
		}
		return nil
	}); err != nil {
		return h.failOperation(ctx, lane, operation, pending, err)
	}
	if message.StopReason == "" {
		message.StopReason = StopReasonStop
	}
	if message.StopReason != StopReasonStop && message.StopReason != StopReasonLength {
		return h.failOperation(ctx, lane, operation, pending, fmt.Errorf("unsupported assistant stop reason %q", message.StopReason))
	}
	if message.Role != "assistant" {
		return h.failOperation(ctx, lane, operation, pending, fmt.Errorf("provider response has role %q", message.Role))
	}
	if len(message.ToolCalls) != 0 {
		return h.failOperation(ctx, lane, operation, pending, fmt.Errorf("tool calls are not supported by the no-tool run"))
	}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventMessageEnd), Lane: lane.name, Payload: cloneValue(message)})
	usage := Usage{}
	if message.Usage != nil {
		usage = *message.Usage
	}
	if _, err := fx.CommitEffectSettlement(ctx, CurrentOperation{Operation: operation, State: pending}, EffectPlan{Kind: EffectAssistant, Key: EffectKey(operation.OperationID), Generation: pending.Run.Phase.Generation}, SettlementOutput{Kind: "assistant", Key: EffectKey(operation.OperationID), Message: &message}, h.telemetry); err != nil {
		return RunOutcome{}, err
	}
	entry := Entry{EntryBase: EntryBase{ID: responseID, ParentID: stringPointer(pending.Run.Phase.Generation.Context.TriggerEntryID), Type: EntryMessage}, Message: messageCopy(message)}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventEntryAdded), Lane: lane.name, Payload: cloneValue(entry)})
	h.events.Emit(ctx, HarnessEvent{Type: string(EventUsage), Lane: lane.name, Payload: cloneValue(usage)})
	h.events.Emit(ctx, HarnessEvent{Type: string(EventTurnEnd), Lane: lane.name, Payload: map[string]JSONValue{"runId": operation.OperationID, "turnId": pending.Run.Phase.Generation.Context.StepID}})
	result := RunOutcome{Kind: "completed", RunID: operation.OperationID, LeafID: stringPointer(responseID), FinalEntryID: stringPointer(responseID), FinalMessage: &message}
	if _, err := fx.CommitTerminal(ctx, CurrentOperation{Operation: operation, State: pending}, result); err != nil {
		return RunOutcome{}, err
	}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventRunEnd), Lane: lane.name, Payload: cloneValue(result)})
	return result, nil
}

func (h *Harness) commitState(ctx context.Context, lane *runtimeLane, operationID string, state OperationState) error {
	return h.line(lane.name).Do(ctx, func() error {
		_, err := h.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpState, operationID, state)}})
		return err
	})
}

type runtimeEffects struct {
	harness *Harness
	lane    *runtimeLane
}

var _ Effects = (*runtimeEffects)(nil)

func (e *runtimeEffects) CommitTransition(ctx context.Context, current CurrentOperation, next OperationState, _ TelemetryContext, _, _ *int64) (*CurrentOperation, error) {
	if current.Operation.OperationID == "" {
		return nil, fmt.Errorf("transition has no operation")
	}
	if err := e.harness.commitState(ctx, e.lane, current.Operation.OperationID, next); err != nil {
		return nil, err
	}
	current.State = next
	return &current, nil
}

func (e *runtimeEffects) CommitEffectSettlement(ctx context.Context, current CurrentOperation, plan EffectPlan, output SettlementOutput, _ TelemetryContext) (SettlementResult, error) {
	if plan.Generation == nil || output.Message == nil {
		return SettlementResult{}, fmt.Errorf("assistant settlement is incomplete")
	}
	responseID := plan.Generation.ResponseEntryID
	usageID := plan.Generation.UsageID
	if responseID == "" || usageID == "" {
		return SettlementResult{}, fmt.Errorf("assistant settlement has no reserved ids")
	}
	message := *output.Message
	usage := Usage{}
	if message.Usage != nil {
		usage = *message.Usage
	}
	entry := Entry{EntryBase: EntryBase{ID: responseID, ParentID: stringPointer(plan.Generation.Context.TriggerEntryID), Type: EntryMessage}, Message: messageCopy(message)}
	next := OperationState{Kind: OperationRun, Run: &RunState{Kind: OperationRun, Control: Control{Status: ControlRunning}, Phase: RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationMayFinish}, TriggerEntryID: responseID}}, Inbox: Inbox{}, LatestAssistantEntryID: &responseID}}
	if err := e.harness.line(e.lane.name).Do(ctx, func() error {
		_, err := e.harness.session.Commit(ctx, Transaction{Writes: []Write{Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}}, Write{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: usageID, Usage: usage, EntryID: &responseID}}}, registerSet(RegisterLaneLeaf, e.lane.name, stringPointer(responseID)), registerSet(RegisterOpState, current.Operation.OperationID, next)}})
		return err
	}); err != nil {
		return SettlementResult{}, err
	}
	current.State = next
	current.LeafID = stringPointer(responseID)
	return SettlementResult{Current: current}, nil
}

func (e *runtimeEffects) CommitTerminal(ctx context.Context, current CurrentOperation, result OperationResult) (*CurrentOperation, error) {
	run, ok := result.(RunOutcome)
	if !ok {
		return nil, fmt.Errorf("unsupported terminal result")
	}
	cleanup, err := e.harness.operationCleanupWrites(ctx, current.Operation.OperationID, current.State)
	if err != nil {
		return nil, err
	}
	laneState := LaneState{PendingNextRun: []string{}}
	if register, err := e.harness.session.GetRegister(ctx, RegisterLaneState, e.lane.name); err != nil {
		return nil, err
	} else if register != nil {
		if value, ok := register.Value.(LaneState); ok {
			laneState = value
			laneState.CurrentOperationID = nil
		}
	}
	writes := append(cleanup, registerSet(RegisterLaneLastResult, e.lane.name, LaneLastResult{OperationID: current.Operation.OperationID, Kind: OperationRun, Outcome: run.Kind, LeafID: run.LeafID, FinalAssistantEntryID: run.FinalEntryID, RunCompletion: "assistant"}), registerSet(RegisterLaneState, e.lane.name, laneState))
	if err := e.harness.line(e.lane.name).Do(ctx, func() error { _, err := e.harness.session.Commit(ctx, Transaction{Writes: writes}); return err }); err != nil {
		return nil, err
	}
	e.lane.finish()
	return nil, nil
}

func (e *runtimeEffects) FinalizeTool(context.Context, EffectPlan, EffectOutput) (SettlementOutput, error) {
	return SettlementOutput{}, fmt.Errorf("tools are not implemented")
}

func (e *runtimeEffects) RunSummaryRequest(context.Context, SummaryRequestPlan) (SummaryRequestOutput, error) {
	return SummaryRequestOutput{}, fmt.Errorf("summaries are not implemented")
}

func (e *runtimeEffects) SettleSummaryRequest(context.Context, CurrentOperation, SummaryRequestPlan, *AgentMessage, TelemetryContext) (CurrentOperation, error) {
	return CurrentOperation{}, fmt.Errorf("summaries are not implemented")
}

func (e *runtimeEffects) Run(ctx context.Context, plan EffectPlan) (EffectOutput, error) {
	switch plan.Kind {
	case EffectHook:
		result, err := e.harness.hooks.Run(ctx, HookInvocation{Name: plan.HookName, Lane: e.lane.name, RunID: string(plan.Key), Event: plan.Event})
		return EffectOutput{Kind: "hook", Key: plan.Key, Result: result}, err
	case EffectAssistant:
		if _, err := e.harness.models.Resolve(ctx, plan.Model.Provider, plan.Model.ModelID); err != nil {
			return EffectOutput{}, err
		}
		stream, err := e.harness.models.Stream(ctx, plan.Model, plan.Messages, plan.StreamOptions)
		if err != nil {
			return EffectOutput{}, err
		}
		if stream == nil {
			return EffectOutput{}, fmt.Errorf("provider returned a nil stream")
		}
		var message *AgentMessage
		for event := range stream {
			if event.Message == nil {
				continue
			}
			copy := *event.Message
			message = &copy
			e.harness.events.Emit(ctx, HarnessEvent{Type: string(EventMessageUpdate), Lane: e.lane.name, Payload: cloneValue(copy)})
		}
		if message == nil {
			return EffectOutput{}, fmt.Errorf("provider returned no assistant message")
		}
		if message.Role == "" {
			message.Role = "assistant"
		}
		return EffectOutput{Kind: "assistant", Key: plan.Key, Message: message}, nil
	default:
		return EffectOutput{}, fmt.Errorf("unsupported effect kind %q", plan.Kind)
	}
}

func (e *runtimeEffects) Sleep(ctx context.Context, delayMs int64, _ TelemetryContext) error {
	timer := time.NewTimer(time.Duration(delayMs) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Harness) failOperation(ctx context.Context, lane *runtimeLane, operation Operation, state OperationState, cause error) (RunOutcome, error) {
	if cause == nil {
		cause = fmt.Errorf("operation failed")
	}
	errorValue := &OperationError{Code: "runtime", Message: cause.Error()}
	var leaf *string
	if current, err := lane.GetLeafID(ctx); err != nil {
		return RunOutcome{}, err
	} else {
		leaf = current
	}
	result := RunOutcome{Kind: "failed", RunID: operation.OperationID, LeafID: cloneStringPointer(leaf), Error: errorValue}
	writes := make([]Write, 0, 8)
	if state.Run != nil && state.Run.Phase.Generation != nil && state.Run.Phase.Generation.ResponseEntryID != "" {
		responseID := state.Run.Phase.Generation.ResponseEntryID
		usageID := state.Run.Phase.Generation.UsageID
		if usageID == "" {
			usageID = h.session.IDGenerator().Next()
		}
		message := AgentMessage{Role: "assistant", Content: cause.Error(), StopReason: StopReasonError}
		parent := state.Run.Phase.Generation.Context.TriggerEntryID
		entry := Entry{EntryBase: EntryBase{ID: responseID, ParentID: stringPointer(parent), Type: EntryMessage}, Message: messageCopy(message)}
		writes = append(writes, Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}}, Write{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: usageID, Usage: Usage{}, EntryID: &responseID}}}, registerSet(RegisterLaneLeaf, lane.name, stringPointer(responseID)))
		result.LeafID = stringPointer(responseID)
		result.FinalEntryID = stringPointer(responseID)
		result.FinalMessage = &message
	}
	cleanup, err := h.operationCleanupWrites(ctx, operation.OperationID, state)
	if err != nil {
		return RunOutcome{}, err
	}
	writes = append(writes, cleanup...)
	laneState := LaneState{PendingNextRun: []string{}}
	if register, err := h.session.GetRegister(ctx, RegisterLaneState, lane.name); err != nil {
		return RunOutcome{}, err
	} else if register != nil {
		if current, ok := register.Value.(LaneState); ok {
			laneState = current
			laneState.CurrentOperationID = nil
		}
	}
	writes = append(writes, registerSet(RegisterLaneLastResult, lane.name, LaneLastResult{OperationID: operation.OperationID, Kind: OperationRun, Outcome: result.Kind, LeafID: result.LeafID, FinalAssistantEntryID: result.FinalEntryID, Error: errorValue}), registerSet(RegisterLaneState, lane.name, laneState))
	if err := h.line(lane.name).Do(ctx, func() error { _, err := h.session.Commit(ctx, Transaction{Writes: writes}); return err }); err != nil {
		return RunOutcome{}, err
	}
	lane.finish()
	h.events.Emit(ctx, HarnessEvent{Type: string(EventRunEnd), Lane: lane.name, Payload: cloneValue(result)})
	return result, nil
}

func (h *Harness) operationCleanupWrites(ctx context.Context, operationID string, state OperationState) ([]Write, error) {
	writes := []Write{registerDelete(RegisterOpMeta, operationID), registerDelete(RegisterOpState, operationID)}
	for _, namespace := range []RegisterNamespace{RegisterOpToolArgs, RegisterOpPreparation} {
		registers, err := h.session.ListRegisters(ctx, namespace, operationID+":")
		if err != nil {
			return nil, err
		}
		for _, register := range registers {
			writes = append(writes, registerDelete(register.Namespace, register.Key))
		}
	}
	if state.Run != nil {
		ids := append(append(append([]string{}, state.Run.Inbox.Steer...), state.Run.Inbox.FollowUp...), state.Run.Inbox.Writes...)
		for _, id := range append(ids, append(state.Run.Control.DrainedSteer, state.Run.Control.DrainedFollowUp...)...) {
			writes = append(writes, registerDelete(RegisterPendingEntry, id))
		}
	}
	return writes, nil
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
func (l *runtimeLane) begin() {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-l.idle:
		l.idle = make(chan struct{})
	default:
	}
}

func (l *runtimeLane) finish() {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-l.idle:
	default:
		close(l.idle)
	}
}

func (l *runtimeLane) WaitForIdle(ctx context.Context) error {
	l.mu.Lock()
	idle := l.idle
	l.mu.Unlock()
	select {
	case <-idle:
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
