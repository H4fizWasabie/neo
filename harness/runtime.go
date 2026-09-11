package harness

import (
	"context"
	"errors"
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
	toolContext        AgentHarnessToolContextSource
	resources          Resources
	compaction         CompactionSettings
	steeringMode       QueueMode
	followUpMode       QueueMode
	toolExecution      ToolExecutionMode
	configurationMu    sync.RWMutex
	lines              sync.Map
	scheduler          *ManualScheduler
	hooks              *HookRunner
	events             *EventBus
	lifecycle          RuntimeLifecycle
	lanes              sync.Map
	rootCtx            context.Context
	cancel             context.CancelFunc
	effects            sync.WaitGroup
	admission          sync.RWMutex
}

const maxSafeInteger = int64(9007199254740991)

type runtimeLane struct {
	harness         *Harness
	name            string
	view            SessionTree
	idle            chan struct{}
	mu              sync.Mutex
	operationCtx    context.Context
	operationCancel context.CancelFunc
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
	if options.SteeringMode == "" {
		options.SteeringMode = QueueAll
	}
	if options.FollowUpMode == "" {
		options.FollowUpMode = QueueAll
	}
	if err := validateCompactionSettings(options.Compaction); err != nil {
		return nil, nil, err
	}
	if err := validateQueueMode(options.SteeringMode); err != nil {
		return nil, nil, err
	}
	if err := validateQueueMode(options.FollowUpMode); err != nil {
		return nil, nil, err
	}
	if options.ToolExecution != ToolExecutionSequential && options.ToolExecution != ToolExecutionParallel {
		return nil, nil, fmt.Errorf("invalid tool execution mode %q", options.ToolExecution)
	}
	if options.ThinkingLevel == "" {
		options.ThinkingLevel = ThinkingOff
	}
	if options.Retry.MaxRetries < 0 {
		return nil, nil, fmt.Errorf("retry max cannot be negative")
	}
	if options.Retry.BaseDelayMs < 0 || options.Retry.BaseDelayMs > maxSafeInteger || options.Retry.MaxRetries > maxSafeInteger-1 {
		return nil, nil, fmt.Errorf("retry policy exceeds safe limits")
	}
	maxAttempts := int64(1)
	if options.Retry.Enabled {
		maxAttempts = options.Retry.MaxRetries + 1
	}
	rootCtx, cancel := context.WithCancel(context.Background())
	h := &Harness{
		session:            options.Session,
		models:             options.Models,
		systemPrompt:       options.SystemPrompt,
		toProviderMessages: options.ToProviderMessages,
		telemetry:          options.Telemetry,
		settings:           NewSettingsSnapshot(options.StreamOptions, NormalizedRetryPolicy{MaxAttempts: maxAttempts, BaseDelayMs: options.Retry.BaseDelayMs}),
		tools:              append([]AgentHarnessTool(nil), options.Tools...),
		toolContext:        options.ToolContext,
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
	h.lifecycle.done = make(chan struct{})
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
		main.begin()
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
	h.admission.RLock()
	if err := h.lifecycle.Err(); err != nil {
		h.admission.RUnlock()
		return err
	}
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
	h.admission.RUnlock()
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
	h.admission.Lock()
	h.lifecycle.Close()
	h.cancel()
	h.scheduler.Close()
	h.admission.Unlock()
	h.effects.Wait()
	return h.session.Close(ctx)
}

func (h *Harness) Name() string         { return h.runtimeLane.name }
func (h *Harness) Session() SessionTree { return h.runtimeLane.Session() }

func (l *runtimeLane) Name() string         { return l.name }
func (l *runtimeLane) Session() SessionTree { return &laneTree{SessionTree: l.view, lane: l} }

type laneTree struct {
	SessionTree
	lane *runtimeLane
}

func (t *laneTree) AppendMessage(ctx context.Context, message AgentMessage) (string, error) {
	if err := t.lane.harness.lifecycle.Err(); err != nil {
		return "", err
	}
	deferred, err := t.lane.hasActiveOperation(ctx)
	if err != nil {
		return "", err
	}
	if deferred {
		return t.lane.enqueueTreeEntry(ctx, PendingEntry{Type: EntryMessage, Payload: message})
	}
	return t.SessionTree.AppendMessage(ctx, message)
}

func (t *laneTree) AppendCustomEntry(ctx context.Context, customType string, data JSONValue) (string, error) {
	if err := t.lane.harness.lifecycle.Err(); err != nil {
		return "", err
	}
	deferred, err := t.lane.hasActiveOperation(ctx)
	if err != nil {
		return "", err
	}
	if deferred {
		return t.lane.enqueueTreeEntry(ctx, PendingEntry{Type: EntryCustom, CustomType: customType, Payload: data})
	}
	return t.SessionTree.AppendCustomEntry(ctx, customType, data)
}

func (l *runtimeLane) hasActiveOperation(ctx context.Context) (bool, error) {
	register, err := l.harness.session.GetRegister(ctx, RegisterLaneState, l.name)
	if err != nil {
		return false, err
	}
	if register == nil {
		return false, fmt.Errorf("lane state is missing")
	}
	state, ok := register.Value.(LaneState)
	if !ok {
		return false, fmt.Errorf("lane state has invalid type")
	}
	return state.CurrentOperationID != nil, nil
}

func (l *runtimeLane) enqueueTreeEntry(ctx context.Context, pending PendingEntry) (string, error) {
	if err := l.harness.lifecycle.Err(); err != nil {
		return "", err
	}
	id := l.harness.session.IDGenerator().Next()
	operationID := ""
	err := l.harness.line(l.name).Do(ctx, func() error {
		if err := l.harness.lifecycle.Err(); err != nil {
			return err
		}
		current, err := l.harness.currentOperation(ctx, l)
		if err != nil {
			return err
		}
		state := current.State
		if state.Run == nil {
			return &NoActiveRun{TaggedError: TaggedError{Message: "no active run"}, Lane: l.name}
		}
		operationID = current.Operation.OperationID
		state.Run.Inbox.Writes = append(state.Run.Inbox.Writes, id)
		_, err = l.harness.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterPendingEntry, id, pending), registerSet(RegisterOpState, current.Operation.OperationID, state)}})
		return err
	})
	if err != nil {
		return "", err
	}
	l.harness.events.Emit(ctx, HarnessEvent{Type: string(EventWritePending), Lane: l.name, Payload: map[string]JSONValue{"runId": operationID, "entryId": id, "entryType": pending.Type}})
	return id, nil
}

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
	acceptedInLine := false
	begun := false
	capturedNextRun := false
	if err := l.harness.line(l.name).Do(ctx, func() error {
		if err := l.harness.lifecycle.Err(); err != nil {
			return err
		}
		laneStateRegister, err := l.harness.session.GetRegister(ctx, RegisterLaneState, l.name)
		if err != nil {
			return err
		}
		if laneStateRegister == nil {
			return fmt.Errorf("lane does not exist: %s", l.name)
		}
		laneState, ok := laneStateRegister.Value.(LaneState)
		if !ok || laneState.CurrentOperationID != nil {
			busyID := ""
			busyKind := OperationRun
			if laneState.CurrentOperationID != nil {
				busyID = *laneState.CurrentOperationID
			}
			return &LaneBusy{TaggedError: TaggedError{Message: "lane is busy"}, Lane: l.name, OperationID: busyID, OperationKind: busyKind}
		}
		l.begin()
		begun = true
		capturedNextRun = len(laneState.PendingNextRun) != 0
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
			if !capturedNextRun {
				return fmt.Errorf("prompt is empty")
			}
		}
		accepted = Operation{OperationID: operationID, Lane: l.name, SourceLeafID: cloneStringPointer(leaf), StartedAt: time.Now().UnixMilli(), Intent: OperationIntent{Kind: OperationRun, PromptEntryIDs: promptIDs, SystemPromptOverride: capturedSystemPrompt, ResumeData: resumeData}}
		runSettings := l.harness.runSettings()
		acceptedState = OperationState{Kind: OperationRun, Run: &RunState{Kind: OperationRun, Control: Control{Status: ControlRunning}, Settings: runSettings, Phase: RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationNeedAssistant}, TriggerEntryID: *parent, SkipInboxOnce: true}}, Inbox: Inbox{}}}
		laneState.CurrentOperationID = &operationID
		laneState.PendingNextRun = []string{}
		writes = append(writes, registerSet(RegisterOpMeta, operationID, accepted), registerSet(RegisterOpState, operationID, acceptedState), registerSet(RegisterLaneLeaf, l.name, parent), registerSet(RegisterLaneState, l.name, laneState))
		_, err = l.harness.session.Commit(ctx, Transaction{Writes: writes})
		_ = config
		acceptedInLine = err == nil
		return err
	}); err != nil {
		if begun {
			l.finish()
		}
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
	for {
		restored, err := Restore(ctx, h.session, lane.name)
		if err != nil {
			if isOperationMissing(err) {
				return h.resolveExternalFinalization(ctx, lane, operation)
			}
			return RunOutcome{}, err
		}
		if restored.Current == nil {
			return h.resolveExternalFinalization(ctx, lane, operation)
		}
		current := *restored.Current
		state = current.State
		if state.Run == nil {
			return RunOutcome{}, fmt.Errorf("run state disappeared before drive")
		}
		if state.Run.Control.Status == ControlCancelRequested {
			return h.reconcileCancelled(ctx, lane, operation, current)
		}
		phase := state.Run.Phase
		if phase.Kind == PhaseFailureDrain {
			return h.driveFailureDrain(ctx, lane, operation, current)
		}
		if phase.Kind == PhaseTools {
			return h.driveTools(ctx, lane, operation, current)
		}
		if phase.Kind == PhaseDeferred {
			if phase.Deferred == nil || phase.Deferred.Status != DeferredSuspended {
				return RunOutcome{}, fmt.Errorf("deferred state is not resumable")
			}
			entry, err := lane.view.GetEntry(ctx, phase.Deferred.SourceEntryID)
			if err != nil || entry == nil || entry.Message == nil {
				if err == nil {
					err = fmt.Errorf("deferred source entry is missing")
				}
				return RunOutcome{}, err
			}
			handle, ok := deferredHandle(*entry.Message)
			if !ok {
				return RunOutcome{}, fmt.Errorf("deferred source entry has no valid handle")
			}
			return RunOutcome{Kind: "suspended", RunID: operation.OperationID, LeafID: stringPointer(phase.Deferred.SourceEntryID), FinalEntryID: stringPointer(phase.Deferred.SourceEntryID), FinalMessage: messageCopy(*entry.Message), Reason: "deferred", Deferred: handle}, nil
		}
		if phase.Kind == PhaseCheckpoint {
			if phase.Checkpoint == nil {
				return h.finishFailure(ctx, lane, operation, current, fmt.Errorf("checkpoint has no payload"))
			}
			if !phase.Checkpoint.SkipInboxOnce {
				consumed, next, err := h.consumeCheckpointInput(ctx, lane, current)
				if err != nil {
					return RunOutcome{}, err
				}
				if consumed {
					current = next
					state = next.State
					continue
				}
			}
			if phase.Checkpoint.Continuation.Kind == ContinuationMayFinish {
				if !phase.Checkpoint.Continuation.IncludeFinalAssistant {
					result := RunOutcome{Kind: "completed", RunID: operation.OperationID, LeafID: current.LeafID, Reason: "terminated_tools"}
					return h.finishOutcome(ctx, lane, operation, current, result)
				}
				if state.Run.LatestAssistantEntryID == nil {
					return h.finishFailure(ctx, lane, operation, current, fmt.Errorf("checkpoint has no final assistant"))
				}
				entry, err := lane.view.GetEntry(ctx, *state.Run.LatestAssistantEntryID)
				if err != nil || entry == nil || entry.Message == nil {
					if err == nil {
						err = fmt.Errorf("final assistant entry is missing")
					}
					return RunOutcome{}, err
				}
				result := RunOutcome{Kind: "completed", RunID: operation.OperationID, LeafID: current.LeafID, FinalEntryID: state.Run.LatestAssistantEntryID, FinalMessage: messageCopy(*entry.Message)}
				return h.finishOutcome(ctx, lane, operation, current, result)
			}
			if phase.Checkpoint.Continuation.Kind != ContinuationNeedAssistant {
				return h.finishFailure(ctx, lane, operation, current, fmt.Errorf("unsupported checkpoint continuation"))
			}
			config := current.Configuration
			settings := h.settings.Snapshot()
			generation := &Generation{Status: GenerationReady, NextAttempt: 1, Context: GenerationContext{StepID: operation.OperationID + ":step:1", TriggerEntryID: phase.Checkpoint.TriggerEntryID, Configuration: config, StreamOptions: settings.StreamOptions, RetryPolicy: settings.RetryPolicy}}
			next := state
			next.Run.Phase = RunPhase{Kind: PhaseAssistant, Generation: generation}
			var transition *CurrentOperation
			configurationSeq, settingsRevision := current.ConfigurationSeq, settings.SettingsRevision
			if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "assistant ready"}, func(ctx context.Context) error {
				var err error
				transition, err = fx.CommitTransition(ctx, current, next, h.telemetry, &configurationSeq, &settingsRevision)
				return err
			}); err != nil {
				if isOperationMissing(err) {
					return h.resolveExternalFinalization(ctx, lane, operation)
				}
				return RunOutcome{}, err
			}
			if transition == nil {
				return RunOutcome{}, fmt.Errorf("assistant ready transition lost its compare-and-swap")
			}
			h.events.Emit(ctx, HarnessEvent{Type: string(EventTurnStart), Lane: lane.name, Payload: map[string]JSONValue{"runId": operation.OperationID, "turnId": generation.Context.StepID}})
			continue
		}
		generation := phase.Generation
		if phase.Kind != PhaseAssistant || generation == nil {
			return h.finishFailure(ctx, lane, operation, current, fmt.Errorf("unsupported run phase %q", phase.Kind))
		}
		if generation.Status == GenerationRetryWait {
			if delay := generation.NotBefore - time.Now().UnixMilli(); delay > 0 {
				if err := h.effect(ctx, ActionInfo{Kind: "wait", Description: "assistant retry wait"}, func(ctx context.Context) error { return fx.Sleep(ctx, delay, h.telemetry) }); err != nil {
					return RunOutcome{}, err
				}
			}
			next := state
			next.Run.Phase.Generation.Status = GenerationReady
			var transition *CurrentOperation
			if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "assistant retry ready"}, func(ctx context.Context) error {
				var err error
				transition, err = fx.CommitTransition(ctx, current, next, h.telemetry, nil, nil)
				return err
			}); err != nil {
				if isOperationMissing(err) {
					return h.resolveExternalFinalization(ctx, lane, operation)
				}
				return RunOutcome{}, err
			}
			if transition == nil {
				return RunOutcome{}, fmt.Errorf("assistant retry transition lost its compare-and-swap")
			}
			continue
		}
		if generation.Status == GenerationEffectPending {
			if generation.Attempt >= generation.Context.RetryPolicy.MaxAttempts {
				return h.settleGenerationError(ctx, lane, operation, current, *generation, fmt.Errorf("recovered assistant effect reached retry cap"))
			}
			next := state
			next.Run.Phase.Generation.Status = GenerationReady
			next.Run.Phase.Generation.NextAttempt = generation.Attempt + 1
			next.Run.Phase.Generation.Attempt = 0
			next.Run.Phase.Generation.ResponseEntryID = ""
			next.Run.Phase.Generation.UsageID = ""
			var transition *CurrentOperation
			if err := h.effect(ctx, ActionInfo{Kind: "recovery", Description: "recover assistant effect"}, func(ctx context.Context) error {
				var err error
				transition, err = fx.CommitTransition(ctx, current, next, h.telemetry, nil, nil)
				return err
			}); err != nil {
				if isOperationMissing(err) {
					return h.resolveExternalFinalization(ctx, lane, operation)
				}
				return RunOutcome{}, err
			}
			if transition == nil {
				return RunOutcome{}, fmt.Errorf("assistant recovery transition lost its compare-and-swap")
			}
			continue
		}
		if generation.Status != GenerationReady {
			return h.finishFailure(ctx, lane, operation, current, fmt.Errorf("unsupported generation status %q", generation.Status))
		}
		missingTools := missingActiveTools(h.tools, generation.Context.Configuration.ActiveToolNames)
		_, modelErr := h.models.Resolve(ctx, generation.Context.Configuration.Model.Provider, generation.Context.Configuration.Model.ModelID)
		if modelErr != nil || len(missingTools) != 0 {
			missing := &MissingIdentitySuspension{Reason: "missing_identities", Tools: missingTools}
			if modelErr != nil {
				missing.Models = []string{generation.Context.Configuration.Model.Provider + "/" + generation.Context.Configuration.Model.ModelID}
			}
			return RunOutcome{Kind: "suspended", RunID: operation.OperationID, LeafID: current.LeafID, Reason: "missing_identities", Missing: missing}, nil
		}
		attempt := generation.NextAttempt
		if attempt == 0 {
			attempt = generation.Attempt + 1
		}
		if attempt > 1 {
			h.events.Emit(ctx, HarnessEvent{Type: string(EventTurnStart), Lane: lane.name, Payload: map[string]JSONValue{"runId": operation.OperationID, "turnId": generation.Context.StepID}})
		}
		requestOptions := generation.Context.StreamOptions
		if err := h.runBeforeRequest(ctx, fx, operation, attempt, &requestOptions); err != nil {
			return h.finishFailure(ctx, lane, operation, current, err)
		}
		responseID, usageID := h.session.IDGenerator().Next(), h.session.IDGenerator().Next()
		pending := state
		pending.Run.Phase.Generation.Status = GenerationEffectPending
		pending.Run.Phase.Generation.Attempt = attempt
		pending.Run.Phase.Generation.NextAttempt = attempt
		pending.Run.Phase.Generation.ResponseEntryID = responseID
		pending.Run.Phase.Generation.UsageID = usageID
		pending.Run.Phase.Generation.AttemptStreamOptions = &requestOptions
		var transition *CurrentOperation
		if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "assistant effect pending"}, func(ctx context.Context) error {
			var err error
			transition, err = fx.CommitTransition(ctx, current, pending, h.telemetry, nil, nil)
			return err
		}); err != nil {
			if isOperationMissing(err) {
				return h.resolveExternalFinalization(ctx, lane, operation)
			}
			return RunOutcome{}, err
		}
		if transition == nil {
			return RunOutcome{}, fmt.Errorf("assistant intent lost its compare-and-swap")
		}
		current = *transition
		message, err := h.runAssistantAttempt(ctx, lane, operation, pending, fx)
		if err != nil {
			latest, reloadErr := h.currentOperation(ctx, lane)
			if reloadErr == nil && latest.State.Run != nil && latest.State.Run.Control.Status == ControlCancelRequested {
				return h.settleAborted(ctx, lane, operation, latest, *pending.Run.Phase.Generation, AgentMessage{Role: "assistant", Content: err.Error()})
			}
			if isOperationMissing(reloadErr) {
				return h.resolveExternalFinalization(ctx, lane, operation)
			}
			return h.settleGenerationError(ctx, lane, operation, current, *pending.Run.Phase.Generation, err)
		}
		latest, reloadErr := h.currentOperation(ctx, lane)
		if reloadErr == nil && latest.State.Run != nil && latest.State.Run.Control.Status == ControlCancelRequested {
			message.StopReason = StopReasonAborted
			return h.settleAborted(ctx, lane, operation, latest, *pending.Run.Phase.Generation, message)
		}
		if isOperationMissing(reloadErr) {
			return h.resolveExternalFinalization(ctx, lane, operation)
		}
		if err := validateAssistantMessage(message); err != nil {
			return h.settleGenerationError(ctx, lane, operation, current, *pending.Run.Phase.Generation, terminalGenerationError{err})
		}
		if message.StopReason == StopReasonAborted {
			return RunOutcome{}, h.lifecycle.Fault(fmt.Errorf("provider returned aborted while control is running"))
		}
		if message.StopReason == StopReasonError {
			return h.settleGenerationErrorMessage(ctx, lane, operation, current, *pending.Run.Phase.Generation, message, fmt.Errorf("provider returned an error"))
		}
		if message.StopReason == StopReasonDeferred {
			handle, ok := deferredHandle(message)
			if !ok {
				return h.settleGenerationError(ctx, lane, operation, current, *pending.Run.Phase.Generation, terminalGenerationError{fmt.Errorf("deferred response has no valid handle")})
			}
			return h.settleDeferred(ctx, lane, operation, current, *pending.Run.Phase.Generation, message, handle)
		}
		if len(message.ToolCalls) != 0 {
			calls := make([]ToolCall, len(message.ToolCalls))
			for i := range message.ToolCalls {
				calls[i] = ToolCall{Status: "planned", SourceIndex: i, ResultEntryID: h.session.IDGenerator().Next()}
			}
			batch := RunPhase{Kind: PhaseTools, ToolBatch: &ToolBatch{AssistantEntryID: pending.Run.Phase.Generation.ResponseEntryID, Configuration: pending.Run.Phase.Generation.Context.Configuration, StepID: pending.Run.Phase.Generation.Context.StepID, TurnID: pending.Run.Phase.Generation.Context.StepID, Calls: calls}}
			settled, err := h.settleGeneration(ctx, lane, operation, current, *pending.Run.Phase.Generation, message, batch)
			if err != nil {
				return RunOutcome{}, err
			}
			return h.driveTools(ctx, lane, operation, settled)
		}
		if message.StopReason == StopReasonToolUse {
			return h.settleGenerationError(ctx, lane, operation, current, *pending.Run.Phase.Generation, terminalGenerationError{fmt.Errorf("tool_use response has no tool calls")})
		}
		settled, err := h.settleGeneration(ctx, lane, operation, current, *pending.Run.Phase.Generation, message, RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationMayFinish, IncludeFinalAssistant: true}, TriggerEntryID: pending.Run.Phase.Generation.ResponseEntryID}})
		if err != nil {
			return RunOutcome{}, err
		}
		return h.drive(ctx, lane, operation, settled.State)
	}
}

func (h *Harness) driveFailureDrain(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation) (RunOutcome, error) {
	run := current.State.Run
	if run == nil || run.Phase.Error == nil {
		return h.finishFailure(ctx, lane, operation, current, fmt.Errorf("operation failed"))
	}
	failureError := run.Phase.Error
	failureProvenance := run.Phase.Provenance
	if len(run.Inbox.Writes) == 0 && len(run.Inbox.Steer) == 0 && len(run.Inbox.FollowUp) == 0 {
		return h.finishFailureValue(ctx, lane, operation, current, failureError)
	}
	next := current.State
	next.Run.Phase = RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationMayFinish}, TriggerEntryID: stringValue(current.LeafID)}}
	var transition *CurrentOperation
	if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "begin failure drain"}, func(ctx context.Context) error {
		var err error
		transition, err = (&runtimeEffects{harness: h, lane: lane}).CommitTransition(ctx, current, next, h.telemetry, nil, nil)
		return err
	}); err != nil {
		if isOperationMissing(err) {
			return h.resolveExternalFinalization(ctx, lane, operation)
		}
		return RunOutcome{}, err
	}
	consumed, _, err := h.consumeCheckpointInput(ctx, lane, *transition)
	if err != nil {
		return RunOutcome{}, err
	}
	if !consumed {
		return h.finishFailureValue(ctx, lane, operation, current, run.Phase.Error)
	}
	reloaded, err := h.currentOperation(ctx, lane)
	if err != nil {
		return RunOutcome{}, err
	}
	if reloaded.State.Run.Phase.Checkpoint != nil && reloaded.State.Run.Phase.Checkpoint.Continuation.Kind == ContinuationMayFinish {
		reloaded.State.Run.Phase = RunPhase{Kind: PhaseFailureDrain, Error: failureError, Provenance: failureProvenance}
		if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "continue failure drain"}, func(ctx context.Context) error {
			_, err := (&runtimeEffects{harness: h, lane: lane}).CommitTransition(ctx, reloaded, reloaded.State, h.telemetry, nil, nil)
			return err
		}); err != nil {
			return RunOutcome{}, err
		}
		return h.drive(ctx, lane, operation, reloaded.State)
	}
	return h.drive(ctx, lane, operation, reloaded.State)
}

func (h *Harness) reconcileCancelled(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation) (RunOutcome, error) {
	run := current.State.Run
	if run == nil {
		return RunOutcome{}, fmt.Errorf("cancelled operation has no run state")
	}
	phase := run.Phase
	if phase.Kind == PhaseAssistant && phase.Generation != nil && phase.Generation.Status == GenerationEffectPending {
		return h.settleAborted(ctx, lane, operation, current, *phase.Generation, AgentMessage{Role: "assistant", Content: "assistant request aborted"})
	}
	if phase.Kind == PhaseDeferred && phase.Deferred != nil {
		h.cancelDeferredSource(ctx, lane, *phase.Deferred)
	}
	if phase.Kind == PhaseTools && phase.ToolBatch != nil {
		for index, call := range phase.ToolBatch.Calls {
			if call.Status == "completed" {
				continue
			}
			_, source, err := h.toolCall(ctx, lane, *phase.ToolBatch, index)
			if err != nil {
				return RunOutcome{}, err
			}
			result, err := h.settleTool(ctx, lane, operation, current, index, call, source.ID, AgentToolResult{Content: "tool call aborted", IsError: true})
			if err != nil || result.Kind != "" {
				return result, err
			}
			return h.drive(ctx, lane, operation, current.State)
		}
	}
	if phase.Kind != PhaseCheckpoint && len(run.Inbox.Writes) != 0 {
		next := current.State
		next.Run.Phase = RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationMayFinish}, TriggerEntryID: stringValue(current.LeafID)}}
		if _, err := (&runtimeEffects{harness: h, lane: lane}).CommitTransition(ctx, current, next, h.telemetry, nil, nil); err != nil {
			return RunOutcome{}, err
		}
		return h.drive(ctx, lane, operation, next)
	}
	if phase.Kind == PhaseCheckpoint && len(run.Inbox.Writes) != 0 {
		consumed, next, err := h.consumeCheckpointInput(ctx, lane, current)
		if err != nil {
			return RunOutcome{}, err
		}
		if consumed {
			return h.drive(ctx, lane, operation, next.State)
		}
	}
	result := RunOutcome{Kind: "aborted", RunID: operation.OperationID, LeafID: current.LeafID, Reason: "cancelled"}
	return h.finishOutcome(ctx, lane, operation, current, result)
}

func (h *Harness) cancelDeferredSource(ctx context.Context, lane *runtimeLane, deferred Deferred) {
	entry, err := lane.view.GetEntry(ctx, deferred.SourceEntryID)
	if err != nil || entry == nil || entry.Message == nil {
		return
	}
	handle, ok := deferredHandle(*entry.Message)
	if !ok {
		return
	}
	model, err := h.models.Resolve(ctx, handle.Provider, handle.ModelID)
	if err != nil {
		return
	}
	_ = h.effect(ctx, ActionInfo{Kind: "cancel_deferred", Description: "cancel deferred request"}, func(ctx context.Context) error {
		return h.models.CancelDeferred(ctx, model, *handle)
	})
}

func (h *Harness) resolveExternalFinalization(ctx context.Context, lane *runtimeLane, operation Operation) (RunOutcome, error) {
	last, err := lane.GetLastResult(ctx)
	if err != nil {
		return RunOutcome{}, err
	}
	if last == nil || last.OperationID != operation.OperationID {
		return RunOutcome{}, fmt.Errorf("operation disappeared without a matching last result")
	}
	result := RunOutcome{Kind: last.Outcome, RunID: last.OperationID, LeafID: cloneStringPointer(last.LeafID), FinalEntryID: cloneStringPointer(last.FinalAssistantEntryID), Error: last.Error}
	if result.FinalEntryID != nil {
		if entry, err := lane.view.GetEntry(ctx, *result.FinalEntryID); err == nil && entry != nil && entry.Message != nil {
			result.FinalMessage = messageCopy(*entry.Message)
		}
	}
	lane.signalOperation()
	lane.finish()
	h.events.Emit(ctx, HarnessEvent{Type: string(EventRunEnd), Lane: lane.name, Recovery: true, Payload: cloneValue(result)})
	return result, nil
}

func (h *Harness) runBeforeRequest(ctx context.Context, fx *runtimeEffects, operation Operation, attempt int64, options *AgentHarnessStreamOptions) error {
	return h.effect(ctx, ActionInfo{Kind: "hook", Description: "before_request"}, func(ctx context.Context) error {
		output, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(fmt.Sprintf("%s:before_request:%d", operation.OperationID, attempt)), HookName: HookBeforeRequest, Event: map[string]JSONValue{"step": "assistant", "attempt": attempt, "streamOptions": *options}})
		if values, ok := output.Result.(map[string]JSONValue); ok {
			if patch, ok := values["streamOptions"].(AgentHarnessStreamOptionsPatch); ok {
				*options = applyStreamOptions(*options, patch)
			}
		}
		return err
	})
}

func (h *Harness) consumeCheckpointInput(ctx context.Context, lane *runtimeLane, current CurrentOperation) (bool, CurrentOperation, error) {
	if current.State.Run == nil || current.State.Run.Phase.Checkpoint == nil {
		return false, current, nil
	}
	run := current.State.Run
	ids := append([]string(nil), run.Inbox.Writes...)
	queue := "writes"
	project := false
	if len(ids) == 0 {
		if len(run.Inbox.Steer) != 0 {
			ids = queueIDs(run.Inbox.Steer, run.Settings.SteeringMode)
			queue = "steer"
			project = true
		} else if run.Phase.Checkpoint.Continuation.Kind == ContinuationMayFinish {
			if len(run.Inbox.FollowUp) == 0 {
				return false, current, nil
			}
			ids = queueIDs(run.Inbox.FollowUp, run.Settings.FollowUpMode)
			queue = "followUp"
			project = true
		} else {
			return false, current, nil
		}
	}
	parent := current.LeafID
	entries := make([]Write, 0, len(ids)*2+3)
	applied := make([]Entry, 0, len(ids))
	for _, id := range ids {
		register, err := h.session.GetRegister(ctx, RegisterPendingEntry, id)
		if err != nil {
			return false, current, err
		}
		if register == nil {
			return false, current, fmt.Errorf("queued item %s has no payload register", id)
		}
		pending, ok := register.Value.(PendingEntry)
		if !ok {
			return false, current, fmt.Errorf("queued item %s has invalid payload", id)
		}
		entry := Entry{EntryBase: EntryBase{ID: id, ParentID: cloneStringPointer(parent), Type: pending.Type}, CustomType: pending.CustomType, Data: cloneValue(pending.Payload)}
		if pending.Type == EntryMessage {
			message, ok := pending.Payload.(AgentMessage)
			if !ok {
				return false, current, fmt.Errorf("queued message %s has invalid payload", id)
			}
			entry.Message = messageCopy(message)
			entry.Data = nil
			project = true
		}
		entries = append(entries, Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}}, registerDelete(RegisterPendingEntry, id))
		applied = append(applied, entry)
		parent = &id
	}
	next := current.State
	switch queue {
	case "writes":
		next.Run.Inbox.Writes = removeIDs(next.Run.Inbox.Writes, ids)
	case "steer":
		next.Run.Inbox.Steer = removeIDs(next.Run.Inbox.Steer, ids)
	case "followUp":
		next.Run.Inbox.FollowUp = removeIDs(next.Run.Inbox.FollowUp, ids)
	}
	continuation := next.Run.Phase.Checkpoint.Continuation
	if project {
		continuation = Continuation{Kind: ContinuationNeedAssistant}
	}
	next.Run.Phase = RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: continuation, TriggerEntryID: stringValue(parent), SkipInboxOnce: project}}
	if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "consume queued input"}, func(ctx context.Context) error {
		return h.line(lane.name).Do(ctx, func() error {
			valid, err := (&runtimeEffects{harness: h, lane: lane}).current(ctx, current)
			if err != nil || !valid {
				if err == nil {
					err = fmt.Errorf("stale operation state")
				}
				return err
			}
			latestLane, err := h.session.GetRegister(ctx, RegisterLaneState, lane.name)
			if err != nil || latestLane == nil {
				return err
			}
			laneState, ok := latestLane.Value.(LaneState)
			if !ok {
				return fmt.Errorf("lane state has invalid type")
			}
			writes := append(entries, registerSet(RegisterLaneLeaf, lane.name, cloneStringPointer(parent)), registerSet(RegisterLaneState, lane.name, laneState), registerSet(RegisterOpState, current.Operation.OperationID, next))
			_, err = h.session.Commit(ctx, Transaction{Writes: writes})
			return err
		})
	}); err != nil {
		return false, current, err
	}
	current.State = next
	current.LeafID = cloneStringPointer(parent)
	for _, entry := range applied {
		h.events.Emit(ctx, HarnessEvent{Type: string(EventEntryAdded), Lane: lane.name, Payload: cloneValue(entry)})
	}
	h.eventsQueueUpdate(ctx, lane)
	return true, current, nil
}

func queueIDs(ids []string, mode QueueMode) []string {
	if mode == QueueOneAtATime && len(ids) > 1 {
		return append([]string(nil), ids[:1]...)
	}
	return append([]string(nil), ids...)
}

func removeIDs(ids, remove []string) []string {
	set := make(map[string]struct{}, len(remove))
	for _, id := range remove {
		set[id] = struct{}{}
	}
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := set[id]; !ok {
			result = append(result, id)
		}
	}
	return result
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (h *Harness) driveTools(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation) (RunOutcome, error) {
	if current.State.Run != nil && current.State.Run.Settings.ToolExecution == ToolExecutionParallel {
		return h.driveToolsParallel(ctx, lane, operation, current)
	}
	toolContext, err := h.resolveToolContext(ctx)
	if err != nil {
		return RunOutcome{}, err
	}
	for {
		batch := current.State.Run.Phase.ToolBatch
		if batch == nil {
			return RunOutcome{}, fmt.Errorf("tool phase has no batch")
		}
		index := -1
		for i, call := range batch.Calls {
			if call.Status != "completed" {
				index = i
				break
			}
		}
		if index == -1 {
			return RunOutcome{}, fmt.Errorf("tool batch has no completion transition")
		}
		call, source, err := h.toolCall(ctx, lane, *batch, index)
		if err != nil {
			return RunOutcome{}, err
		}
		args := map[string]JSONValue{}
		if call.Status == "effect_pending" {
			if sourceEntry, sourceErr := lane.view.GetEntry(ctx, batch.AssistantEntryID); sourceErr == nil && sourceEntry != nil && sourceEntry.Message != nil && sourceEntry.Message.StopReason == StopReasonLength {
				return h.settleTool(ctx, lane, operation, current, index, call, source.ID, AgentToolResult{Content: "tool call was not executed because the assistant reached its output limit", IsError: true})
			}
			key := toolArgsKey(operation.OperationID, batch.StepID, call.SourceIndex)
			register, err := h.session.GetRegister(ctx, RegisterOpToolArgs, key)
			if err != nil {
				return RunOutcome{}, err
			}
			if register == nil {
				return RunOutcome{}, fmt.Errorf("tool arguments are missing for %s", key)
			}
			value, ok := register.Value.(map[string]JSONValue)
			if !ok {
				return RunOutcome{}, fmt.Errorf("tool arguments have invalid type for %s", key)
			}
			args = value
			tool, active := h.activeTool(batch.Configuration, source.Name)
			if !active || tool.Replay() != call.Replay || call.Replay != ReplaySafe {
				result := AgentToolResult{Content: "tool effect was interrupted and is not safe to replay", IsError: true}
				return h.settleTool(ctx, lane, operation, current, index, call, source.ID, result)
			}
		} else {
			if sourceEntry, sourceErr := lane.view.GetEntry(ctx, batch.AssistantEntryID); sourceErr == nil && sourceEntry != nil && sourceEntry.Message != nil && sourceEntry.Message.StopReason == StopReasonLength {
				return h.settleTool(ctx, lane, operation, current, index, call, source.ID, AgentToolResult{Content: "tool call was not executed because the assistant reached its output limit", IsError: true})
			}
			if source.Arguments != nil {
				args = cloneValue(source.Arguments).(map[string]JSONValue)
			}
			tool, ok := h.activeTool(batch.Configuration, source.Name)
			if !ok {
				return h.settleTool(ctx, lane, operation, current, index, call, source.ID, AgentToolResult{Content: fmt.Sprintf("unknown tool %q", source.Name), IsError: true})
			}
			output, err := h.harnessToolHook(ctx, lane, operation, source, args)
			if err != nil {
				return h.settleTool(ctx, lane, operation, current, index, call, source.ID, AgentToolResult{Content: err.Error(), IsError: true})
			}
			if blocked, terminate, reason := blockedTool(output); blocked {
				return h.settleTool(ctx, lane, operation, current, index, call, source.ID, AgentToolResult{Content: reason, IsError: true, Terminate: terminate})
			}
			if replacement, ok := output["arguments"].(map[string]JSONValue); ok {
				args = replacement
			}
			if err := h.commitToolIntent(ctx, lane, operation, current, index, call, args, tool.Replay()); err != nil {
				return RunOutcome{}, err
			}
			current, err = h.currentOperation(ctx, lane)
			if err != nil {
				return RunOutcome{}, err
			}
			call = current.State.Run.Phase.ToolBatch.Calls[index]
		}
		h.events.Emit(ctx, HarnessEvent{Type: string(EventToolStart), Lane: lane.name, Payload: map[string]JSONValue{"toolCallId": source.ID, "name": source.Name, "arguments": cloneValue(args)}})
		output, runErr := h.runTool(ctx, lane, operation, source, args, toolContext)
		if runErr != nil {
			output = AgentToolResult{Content: runErr.Error(), IsError: true}
		}
		current, err = h.currentOperation(ctx, lane)
		if err != nil {
			return RunOutcome{}, err
		}
		if current.State.Run.Control.Status != ControlCancelRequested {
			output, err = h.afterTool(ctx, lane, operation, source, args, output)
			if err != nil {
				output = AgentToolResult{Content: err.Error(), IsError: true}
			}
		} else {
			output.Terminate = false
		}
		result, err := h.settleTool(ctx, lane, operation, current, index, call, source.ID, output)
		if err != nil || result.Kind == "completed" || result.Kind == "failed" || result.Kind == "suspended" {
			return result, err
		}
		current, err = h.currentOperation(ctx, lane)
		if err != nil {
			return RunOutcome{}, err
		}
	}
}

func (h *Harness) driveToolsParallel(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation) (RunOutcome, error) {
	toolContext, err := h.resolveToolContext(ctx)
	if err != nil {
		return RunOutcome{}, err
	}
	batch := current.State.Run.Phase.ToolBatch
	if batch == nil {
		return RunOutcome{}, fmt.Errorf("tool phase has no batch")
	}
	type pendingTool struct {
		index  int
		call   ToolCall
		source AgentToolCall
		args   map[string]JSONValue
		result AgentToolResult
		err    error
	}
	pending := make([]*pendingTool, 0, len(batch.Calls))
	for index := range batch.Calls {
		call, source, err := h.toolCall(ctx, lane, *batch, index)
		if err != nil {
			return RunOutcome{}, err
		}
		if call.Status == "completed" {
			continue
		}
		if entry, entryErr := lane.view.GetEntry(ctx, batch.AssistantEntryID); entryErr == nil && entry != nil && entry.Message != nil && entry.Message.StopReason == StopReasonLength {
			result, settleErr := h.settleTool(ctx, lane, operation, current, index, call, source.ID, AgentToolResult{Content: "tool call was not executed because the assistant reached its output limit", IsError: true})
			if settleErr != nil || result.Kind != "" {
				return result, settleErr
			}
			current, err = h.currentOperation(ctx, lane)
			if err != nil {
				return RunOutcome{}, err
			}
			batch = current.State.Run.Phase.ToolBatch
			continue
		}
		args := map[string]JSONValue{}
		if call.Status == "effect_pending" {
			register, getErr := h.session.GetRegister(ctx, RegisterOpToolArgs, toolArgsKey(operation.OperationID, batch.StepID, call.SourceIndex))
			if getErr != nil {
				return RunOutcome{}, getErr
			}
			if register == nil {
				return RunOutcome{}, fmt.Errorf("tool arguments are missing")
			}
			var ok bool
			args, ok = register.Value.(map[string]JSONValue)
			if !ok {
				return RunOutcome{}, fmt.Errorf("tool arguments have invalid type")
			}
			tool, active := h.activeTool(batch.Configuration, source.Name)
			if !active || tool.Replay() != call.Replay || call.Replay != ReplaySafe {
				result, settleErr := h.settleTool(ctx, lane, operation, current, index, call, source.ID, AgentToolResult{Content: "tool effect was interrupted and is not safe to replay", IsError: true})
				if settleErr != nil || result.Kind != "" {
					return result, settleErr
				}
				current, err = h.currentOperation(ctx, lane)
				if err != nil {
					return RunOutcome{}, err
				}
				batch = current.State.Run.Phase.ToolBatch
				continue
			}
		} else {
			if source.Arguments != nil {
				args = cloneValue(source.Arguments).(map[string]JSONValue)
			}
			tool, active := h.activeTool(batch.Configuration, source.Name)
			if !active {
				result, settleErr := h.settleTool(ctx, lane, operation, current, index, call, source.ID, AgentToolResult{Content: fmt.Sprintf("unknown tool %q", source.Name), IsError: true})
				if settleErr != nil || result.Kind != "" {
					return result, settleErr
				}
				current, err = h.currentOperation(ctx, lane)
				if err != nil {
					return RunOutcome{}, err
				}
				batch = current.State.Run.Phase.ToolBatch
				continue
			}
			values, hookErr := h.harnessToolHook(ctx, lane, operation, source, args)
			if hookErr != nil {
				result, settleErr := h.settleTool(ctx, lane, operation, current, index, call, source.ID, AgentToolResult{Content: hookErr.Error(), IsError: true})
				if settleErr != nil || result.Kind != "" {
					return result, settleErr
				}
				current, err = h.currentOperation(ctx, lane)
				if err != nil {
					return RunOutcome{}, err
				}
				batch = current.State.Run.Phase.ToolBatch
				continue
			}
			if blocked, terminate, reason := blockedTool(values); blocked {
				result, settleErr := h.settleTool(ctx, lane, operation, current, index, call, source.ID, AgentToolResult{Content: reason, IsError: true, Terminate: terminate})
				if settleErr != nil || result.Kind != "" {
					return result, settleErr
				}
				current, err = h.currentOperation(ctx, lane)
				if err != nil {
					return RunOutcome{}, err
				}
				batch = current.State.Run.Phase.ToolBatch
				continue
			}
			if replacement, ok := values["arguments"].(map[string]JSONValue); ok {
				args = replacement
			}
			if err := h.commitToolIntent(ctx, lane, operation, current, index, call, args, tool.Replay()); err != nil {
				return RunOutcome{}, err
			}
			current, err = h.currentOperation(ctx, lane)
			if err != nil {
				return RunOutcome{}, err
			}
			call = current.State.Run.Phase.ToolBatch.Calls[index]
			batch = current.State.Run.Phase.ToolBatch
		}
		pending = append(pending, &pendingTool{index: index, call: call, source: source, args: args})
		h.events.Emit(ctx, HarnessEvent{Type: string(EventToolStart), Lane: lane.name, Payload: map[string]JSONValue{"toolCallId": source.ID, "name": source.Name, "arguments": cloneValue(args)}})
	}
	var wg sync.WaitGroup
	for _, item := range pending {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			item.result, item.err = h.runTool(ctx, lane, operation, item.source, item.args, toolContext)
		}()
	}
	wg.Wait()
	for _, item := range pending {
		if item.err != nil {
			item.result = AgentToolResult{Content: item.err.Error(), IsError: true}
		}
		item.result, err = h.afterTool(ctx, lane, operation, item.source, item.args, item.result)
		if err != nil {
			item.result = AgentToolResult{Content: err.Error(), IsError: true}
		}
		current, err = h.currentOperation(ctx, lane)
		if err != nil {
			return RunOutcome{}, err
		}
		result, settleErr := h.settleTool(ctx, lane, operation, current, item.index, current.State.Run.Phase.ToolBatch.Calls[item.index], item.source.ID, item.result)
		if settleErr != nil || result.Kind != "" {
			return result, settleErr
		}
	}
	return RunOutcome{}, nil
}

func (h *Harness) currentOperation(ctx context.Context, lane *runtimeLane) (CurrentOperation, error) {
	restored, err := Restore(ctx, h.session, lane.name)
	if err != nil || restored.Current == nil {
		if err == nil {
			err = fmt.Errorf("%w: %s", errOperationMissing, lane.name)
		}
		return CurrentOperation{}, err
	}
	return *restored.Current, nil
}

func (h *Harness) resolveToolContext(ctx context.Context) (AgentHarnessToolContextSource, error) {
	switch source := h.toolContext.(type) {
	case func(context.Context) (AgentHarnessToolContextSource, error):
		return source(ctx)
	case func(context.Context) (any, error):
		return source(ctx)
	case func(context.Context) any:
		return source(ctx), nil
	default:
		return h.toolContext, nil
	}
}

func (h *Harness) toolCall(ctx context.Context, lane *runtimeLane, batch ToolBatch, index int) (ToolCall, AgentToolCall, error) {
	entry, err := lane.view.GetEntry(ctx, batch.AssistantEntryID)
	if err != nil || entry == nil || entry.Message == nil {
		if err == nil {
			err = fmt.Errorf("tool assistant entry is missing")
		}
		return ToolCall{}, AgentToolCall{}, err
	}
	if index < 0 || index >= len(entry.Message.ToolCalls) {
		return ToolCall{}, AgentToolCall{}, fmt.Errorf("tool source index %d is missing", index)
	}
	return batch.Calls[index], entry.Message.ToolCalls[index], nil
}

func (h *Harness) activeTool(config LaneConfiguration, name string) (AgentHarnessTool, bool) {
	for _, active := range config.ActiveToolNames {
		if active == name {
			for _, tool := range h.tools {
				if tool != nil && tool.Name() == name {
					return tool, true
				}
			}
			return nil, false
		}
	}
	return nil, false
}

func toolArgsKey(operationID, stepID string, index int) string {
	return fmt.Sprintf("%s:%s:%d", operationID, stepID, index)
}

func (h *Harness) harnessToolHook(ctx context.Context, lane *runtimeLane, operation Operation, call AgentToolCall, args map[string]JSONValue) (map[string]JSONValue, error) {
	var output JSONValue
	err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "before_tool"}, func(ctx context.Context) error {
		var err error
		output, err = h.hooks.Run(ctx, HookInvocation{Name: HookBeforeTool, Lane: lane.name, RunID: operation.OperationID, Event: map[string]JSONValue{"toolCallId": call.ID, "name": call.Name, "arguments": cloneValue(args)}})
		return err
	})
	if err != nil {
		return nil, err
	}
	values, _ := output.(map[string]JSONValue)
	if output != nil && values == nil {
		return map[string]JSONValue{"block": map[string]JSONValue{"reason": "invalid before_tool output"}}, nil
	}
	return values, nil
}

func blockedTool(values map[string]JSONValue) (bool, bool, string) {
	block, ok := values["block"].(map[string]JSONValue)
	if !ok {
		return false, false, ""
	}
	reason, _ := block["reason"].(string)
	terminate, _ := block["terminate"].(bool)
	if reason == "" {
		reason = "tool blocked"
	}
	return true, terminate, reason
}

func (h *Harness) commitToolIntent(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, index int, call ToolCall, args map[string]JSONValue, replay ReplayPolicy) error {
	next := current.State
	batch := next.Run.Phase.ToolBatch
	if batch == nil {
		return fmt.Errorf("tool intent has no batch")
	}
	nextBatch := *batch
	nextBatch.Calls = append([]ToolCall(nil), batch.Calls...)
	nextBatch.Calls[index].Status = "effect_pending"
	nextBatch.Calls[index].Replay = replay
	next.Run.Phase.ToolBatch = &nextBatch
	key := toolArgsKey(operation.OperationID, batch.StepID, call.SourceIndex)
	return h.effect(ctx, ActionInfo{Kind: "transition", Description: "tool effect pending"}, func(ctx context.Context) error {
		return h.line(lane.name).Do(ctx, func() error {
			valid, err := (&runtimeEffects{harness: h, lane: lane}).current(ctx, current)
			if err != nil || !valid {
				if err == nil {
					err = fmt.Errorf("stale operation state")
				}
				return err
			}
			_, err = h.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpToolArgs, key, args), registerSet(RegisterOpState, operation.OperationID, next)}})
			return err
		})
	})
}

func (h *Harness) runTool(ctx context.Context, lane *runtimeLane, operation Operation, call AgentToolCall, args map[string]JSONValue, toolContext AgentHarnessToolContextSource) (AgentToolResult, error) {
	_, ok := h.activeTool(mustActiveConfiguration(ctx, lane), call.Name)
	if !ok {
		return AgentToolResult{}, fmt.Errorf("unknown active tool %q", call.Name)
	}
	var output EffectOutput
	err := h.effect(ctx, ActionInfo{Kind: "tool", Description: "tool effect"}, func(ctx context.Context) error {
		var err error
		output, err = (&runtimeEffects{harness: h, lane: lane}).Run(ctx, EffectPlan{Kind: EffectTool, Key: EffectKey(fmt.Sprintf("%s:tool:%s", operation.OperationID, call.ID)), ToolName: call.Name, ToolCallID: call.ID, ToolArgs: args, ToolContext: toolContext})
		return err
	})
	if err != nil {
		return AgentToolResult{}, err
	}
	if output.ToolResult == nil {
		return AgentToolResult{}, fmt.Errorf("tool returned no result")
	}
	return *output.ToolResult, nil
}

func (h *Harness) afterTool(ctx context.Context, lane *runtimeLane, operation Operation, call AgentToolCall, args map[string]JSONValue, result AgentToolResult) (AgentToolResult, error) {
	var output JSONValue
	err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "after_tool"}, func(ctx context.Context) error {
		var err error
		output, err = h.hooks.Run(ctx, HookInvocation{Name: HookAfterTool, Lane: lane.name, RunID: operation.OperationID, Event: map[string]JSONValue{"toolCallId": call.ID, "name": call.Name, "arguments": cloneValue(args), "result": cloneValue(result), "isError": result.IsError}})
		return err
	})
	if err != nil {
		return result, err
	}
	values, _ := output.(map[string]JSONValue)
	if replacement, ok := values["result"].(AgentToolResult); ok {
		result = replacement
	}
	if terminate, ok := values["terminate"].(bool); ok {
		result.Terminate = terminate
	}
	return result, nil
}

func mustActiveConfiguration(ctx context.Context, lane *runtimeLane) LaneConfiguration {
	config, err := lane.configuration(ctx)
	if err != nil {
		return LaneConfiguration{}
	}
	return config
}

func (h *Harness) settleTool(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, index int, call ToolCall, toolCallID string, result AgentToolResult) (RunOutcome, error) {
	settlement := SettlementResult{}
	err := h.effect(ctx, ActionInfo{Kind: "settlement", Description: "tool result"}, func(ctx context.Context) error {
		var err error
		settlement, err = (&runtimeEffects{harness: h, lane: lane}).CommitEffectSettlement(ctx, current, EffectPlan{Kind: EffectTool, Key: EffectKey(fmt.Sprintf("%s:tool:%d", operation.OperationID, index)), AssistantEntryID: current.State.Run.Phase.ToolBatch.AssistantEntryID, SourceIndex: index, ToolCallID: toolCallID, ToolResultEntryID: call.ResultEntryID}, SettlementOutput{Kind: "tool", Key: EffectKey(operation.OperationID), ToolResult: &result}, h.telemetry)
		return err
	})
	if err != nil {
		if isOperationMissing(err) {
			return h.resolveExternalFinalization(ctx, lane, operation)
		}
		return RunOutcome{}, err
	}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventToolEnd), Lane: lane.name, Payload: cloneValue(result)})
	if result.Usage != nil {
		h.events.Emit(ctx, HarnessEvent{Type: string(EventUsage), Lane: lane.name, Payload: cloneValue(*result.Usage)})
	}
	if settlement.Current.State.Run.Phase.Kind == PhaseCheckpoint {
		if settlement.Current.State.Run.Phase.Checkpoint.Continuation.Kind == ContinuationMayFinish {
			return h.finishOutcome(ctx, lane, operation, settlement.Current, RunOutcome{Kind: "completed", RunID: operation.OperationID, LeafID: settlement.Current.LeafID, Reason: "terminated_tools"})
		}
		return h.drive(ctx, lane, operation, settlement.Current.State)
	}
	return RunOutcome{}, nil
}

func (h *Harness) runAssistantAttempt(ctx context.Context, lane *runtimeLane, operation Operation, pending OperationState, fx *runtimeEffects) (AgentMessage, error) {
	generation := pending.Run.Phase.Generation
	message := AgentMessage{Role: "assistant"}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventMessageStart), Lane: lane.name, Payload: cloneValue(message)})
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "before_payload"}, func(ctx context.Context) error {
		_, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(operation.OperationID + ":before_payload"), HookName: HookBeforePayload, Event: map[string]JSONValue{"model": generation.Context.Configuration.Model, "payload": JSONValue("provider-request")}})
		return err
	}); err != nil {
		return AgentMessage{}, terminalGenerationError{err}
	}
	if err := h.effect(ctx, ActionInfo{Kind: "provider", Description: "assistant stream"}, func(ctx context.Context) error {
		entries, err := lane.view.FindEntriesOnBranch(ctx, BranchScan{Start: generation.Context.TriggerEntryID, Order: OldestFirst})
		if err != nil {
			return err
		}
		messages := make([]AgentMessage, 0, len(entries))
		for _, entry := range entries {
			if entry.Message != nil && includeInContext(*entry.Message) {
				messages = append(messages, *entry.Message)
			}
		}
		if operation.Intent.SystemPromptOverride != "" {
			messages = append([]AgentMessage{{Role: "system", Content: operation.Intent.SystemPromptOverride}}, messages...)
		}
		providerMessages := append([]Message(nil), messages...)
		if h.toProviderMessages != nil {
			providerMessages, err = h.toProviderMessages(messages)
			if err != nil {
				return terminalGenerationError{err}
			}
		}
		streamOptions := generation.Context.StreamOptions
		if generation.AttemptStreamOptions != nil {
			streamOptions = *generation.AttemptStreamOptions
		}
		output, err := fx.Run(ctx, EffectPlan{Kind: EffectAssistant, Key: EffectKey(fmt.Sprintf("%s:assistant:%d", operation.OperationID, generation.Attempt)), Model: generation.Context.Configuration.Model, Messages: providerMessages, StreamOptions: streamOptions})
		if err != nil {
			return err
		}
		if output.Message == nil {
			return fmt.Errorf("provider returned no assistant message")
		}
		message = *output.Message
		return nil
	}); err != nil {
		return AgentMessage{}, err
	}
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "after_response"}, func(ctx context.Context) error {
		output, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(operation.OperationID + ":after_response"), HookName: HookAfterResponse, Event: map[string]JSONValue{"message": message}})
		if values, ok := output.Result.(map[string]JSONValue); ok {
			if replacement, ok := values["message"].(AgentMessage); ok {
				message = replacement
			}
		}
		return err
	}); err != nil {
		return AgentMessage{}, terminalGenerationError{err}
	}
	if message.Role == "" {
		message.Role = "assistant"
	}
	if message.StopReason == "" {
		message.StopReason = StopReasonStop
	}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventMessageEnd), Lane: lane.name, Payload: cloneValue(message)})
	return message, nil
}

func validateAssistantMessage(message AgentMessage) error {
	if message.Role != "assistant" {
		return fmt.Errorf("provider response has role %q", message.Role)
	}
	switch message.StopReason {
	case StopReasonStop, StopReasonLength, StopReasonError, StopReasonAborted, StopReasonDeferred, StopReasonToolUse:
		return nil
	default:
		return fmt.Errorf("unsupported assistant stop reason %q", message.StopReason)
	}
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

type terminalGenerationError struct{ error }

var errOperationMissing = errors.New("operation is no longer current")

func (e terminalGenerationError) Unwrap() error { return e.error }

var _ Effects = (*runtimeEffects)(nil)

func (e *runtimeEffects) current(ctx context.Context, expected CurrentOperation) (bool, error) {
	if expected.OperationStateSeq == 0 {
		return true, nil
	}
	register, err := e.harness.session.GetRegister(ctx, RegisterOpState, expected.Operation.OperationID)
	if err != nil {
		return false, err
	}
	return register != nil && register.Seq == expected.OperationStateSeq, nil
}

func isOperationMissing(err error) bool {
	return errors.Is(err, errOperationMissing)
}

func (e *runtimeEffects) staleOperationError(ctx context.Context, operationID string) error {
	register, err := e.harness.session.GetRegister(ctx, RegisterOpState, operationID)
	if err != nil {
		return err
	}
	if register == nil {
		return errOperationMissing
	}
	return fmt.Errorf("stale operation state")
}

func (e *runtimeEffects) CommitTransition(ctx context.Context, current CurrentOperation, next OperationState, _ TelemetryContext, expectedConfigurationSeq, expectedSettingsRevision *int64) (*CurrentOperation, error) {
	if current.Operation.OperationID == "" {
		return nil, fmt.Errorf("transition has no operation")
	}
	if expectedConfigurationSeq != nil && *expectedConfigurationSeq != current.ConfigurationSeq {
		return nil, fmt.Errorf("stale lane configuration")
	}
	if expectedSettingsRevision != nil && *expectedSettingsRevision != e.harness.settings.Snapshot().SettingsRevision {
		return nil, fmt.Errorf("stale settings revision")
	}
	var commit CommitResult
	if err := e.harness.line(e.lane.name).Do(ctx, func() error {
		if expectedConfigurationSeq != nil {
			config, err := e.harness.session.GetRegister(ctx, RegisterLaneConfig, e.lane.name)
			if err != nil {
				return err
			}
			if config == nil || config.Seq != *expectedConfigurationSeq {
				return fmt.Errorf("stale lane configuration")
			}
		}
		if expectedSettingsRevision != nil && e.harness.settings.Snapshot().SettingsRevision != *expectedSettingsRevision {
			return fmt.Errorf("stale settings revision")
		}
		valid, err := e.current(ctx, current)
		if err != nil || !valid {
			if err == nil {
				err = e.staleOperationError(ctx, current.Operation.OperationID)
			}
			return err
		}
		commit, err = e.harness.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpState, current.Operation.OperationID, next)}})
		return err
	}); err != nil {
		return nil, err
	}
	current.State = next
	if len(commit.Seqs) != 0 {
		current.OperationStateSeq = commit.Seqs[len(commit.Seqs)-1]
	}
	return &current, nil
}

func (e *runtimeEffects) CommitEffectSettlement(ctx context.Context, current CurrentOperation, plan EffectPlan, output SettlementOutput, _ TelemetryContext) (SettlementResult, error) {
	if plan.Kind == EffectTool {
		return e.commitToolSettlement(ctx, current, plan, output)
	}
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
	phase := output.Phase
	if phase.Kind == "" {
		phase = RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationMayFinish}, TriggerEntryID: responseID}}
	}
	next := current.State
	if next.Run == nil {
		return SettlementResult{}, fmt.Errorf("assistant settlement has no run state")
	}
	next.Run.Phase = phase
	next.Run.LatestAssistantEntryID = &responseID
	if err := e.harness.line(e.lane.name).Do(ctx, func() error {
		valid, err := e.current(ctx, current)
		if err != nil || !valid {
			if err == nil {
				err = fmt.Errorf("stale operation state")
			}
			return err
		}
		commit, err := e.harness.session.Commit(ctx, Transaction{Writes: []Write{Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}}, Write{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: usageID, Usage: usage, EntryID: &responseID}}}, registerSet(RegisterLaneLeaf, e.lane.name, stringPointer(responseID)), registerSet(RegisterOpState, current.Operation.OperationID, next)}})
		if len(commit.Seqs) != 0 {
			current.OperationStateSeq = commit.Seqs[len(commit.Seqs)-1]
		}
		return err
	}); err != nil {
		return SettlementResult{}, err
	}
	current.State = next
	current.LeafID = stringPointer(responseID)
	return SettlementResult{Current: current}, nil
}

func (e *runtimeEffects) commitToolSettlement(ctx context.Context, current CurrentOperation, plan EffectPlan, output SettlementOutput) (SettlementResult, error) {
	if output.ToolResult == nil || current.State.Run == nil || current.State.Run.Phase.ToolBatch == nil {
		return SettlementResult{}, fmt.Errorf("tool settlement is incomplete")
	}
	batch := current.State.Run.Phase.ToolBatch
	if plan.SourceIndex < 0 || plan.SourceIndex >= len(batch.Calls) {
		return SettlementResult{}, fmt.Errorf("tool source index %d is out of range", plan.SourceIndex)
	}
	call := batch.Calls[plan.SourceIndex]
	if call.Status == "completed" {
		return SettlementResult{Current: current}, nil
	}
	result := *output.ToolResult
	resultID := call.ResultEntryID
	if plan.ToolResultEntryID != "" {
		resultID = plan.ToolResultEntryID
	}
	metadata := map[string]JSONValue{"isError": result.IsError}
	if result.Details != nil {
		metadata["details"] = cloneValue(result.Details)
	}
	message := AgentMessage{Role: "tool", ToolCallID: plan.ToolCallID, Content: cloneValue(result.Content), Metadata: metadata, Usage: result.Usage}
	entry := Entry{EntryBase: EntryBase{ID: resultID, ParentID: stringPointer(batch.AssistantEntryID), Type: EntryMessage}, Message: messageCopy(message), Terminate: result.Terminate}
	next := current.State
	nextBatch := *batch
	nextBatch.Calls = append([]ToolCall(nil), batch.Calls...)
	nextBatch.Calls[plan.SourceIndex].Status = "completed"
	nextBatch.Calls[plan.SourceIndex].Terminate = result.Terminate
	allCompleted := true
	allTerminate := true
	for _, item := range nextBatch.Calls {
		if item.Status != "completed" {
			allCompleted = false
			break
		}
		if !item.Terminate {
			allTerminate = false
		}
	}
	if allCompleted {
		continuation := Continuation{Kind: ContinuationNeedAssistant}
		if allTerminate {
			continuation = Continuation{Kind: ContinuationMayFinish}
		}
		next.Run.Phase = RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: continuation, TriggerEntryID: resultID}}
	} else {
		nextBatch.Calls = append([]ToolCall(nil), nextBatch.Calls...)
		next.Run.Phase.ToolBatch = &nextBatch
	}
	writes := []Write{{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}}}
	if result.Usage != nil {
		usageID := e.harness.session.IDGenerator().Next()
		writes = append(writes, Write{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: usageID, Usage: *result.Usage, EntryID: &resultID}}})
	}
	writes = append(writes, registerSet(RegisterLaneLeaf, e.lane.name, stringPointer(resultID)))
	if allCompleted {
		for _, item := range batch.Calls {
			writes = append(writes, registerDelete(RegisterOpToolArgs, fmt.Sprintf("%s:%s:%d", current.Operation.OperationID, batch.StepID, item.SourceIndex)))
		}
	}
	writes = append(writes, registerSet(RegisterOpState, current.Operation.OperationID, next))
	var commit CommitResult
	if err := e.harness.line(e.lane.name).Do(ctx, func() error {
		valid, err := e.current(ctx, current)
		if err != nil || !valid {
			if err == nil {
				err = fmt.Errorf("stale operation state")
			}
			return err
		}
		commit, err = e.harness.session.Commit(ctx, Transaction{Writes: writes})
		return err
	}); err != nil {
		return SettlementResult{}, err
	}
	current.State = next
	current.LeafID = stringPointer(resultID)
	if len(commit.Seqs) != 0 {
		current.OperationStateSeq = commit.Seqs[len(commit.Seqs)-1]
	}
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
	completion := ""
	if run.Kind == "completed" {
		completion = "assistant"
		if run.Reason == "terminated_tools" {
			completion = "terminated_tools"
		}
	}
	writes := append(cleanup, registerSet(RegisterLaneLastResult, e.lane.name, LaneLastResult{OperationID: current.Operation.OperationID, Kind: OperationRun, Outcome: run.Kind, LeafID: run.LeafID, FinalAssistantEntryID: run.FinalEntryID, RunCompletion: completion, Error: run.Error}), registerSet(RegisterLaneState, e.lane.name, laneState))
	if err := e.harness.line(e.lane.name).Do(ctx, func() error {
		valid, err := e.current(ctx, current)
		if err != nil || !valid {
			if err == nil {
				err = fmt.Errorf("stale operation state")
			}
			return err
		}
		_, err = e.harness.session.Commit(ctx, Transaction{Writes: writes})
		return err
	}); err != nil {
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
		effectCtx, stop := e.lane.cancellableEffectContext(ctx)
		defer stop()
		if _, err := e.harness.models.Resolve(effectCtx, plan.Model.Provider, plan.Model.ModelID); err != nil {
			return EffectOutput{}, err
		}
		stream, err := e.harness.models.Stream(effectCtx, plan.Model, plan.Messages, plan.StreamOptions)
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
	case EffectTool:
		effectCtx, stop := e.lane.cancellableEffectContext(ctx)
		defer stop()
		tool, err := e.tool(plan.ToolName)
		if err != nil {
			return EffectOutput{}, err
		}
		updates := func(result AgentToolResult) {
			e.harness.events.Emit(ctx, HarnessEvent{Type: string(EventToolUpdate), Lane: e.lane.name, Payload: cloneValue(result)})
		}
		args := plan.ToolArgs
		if args == nil {
			args = map[string]JSONValue{}
		}
		result, err := tool.Execute(effectCtx, plan.ToolCallID, cloneValue(args).(map[string]JSONValue), plan.ToolContext, updates)
		return EffectOutput{Kind: "tool", Key: plan.Key, ToolResult: &result, IsError: result.IsError}, err
	default:
		return EffectOutput{}, fmt.Errorf("unsupported effect kind %q", plan.Kind)
	}
}

func (l *runtimeLane) cancellableEffectContext(ctx context.Context) (context.Context, func()) {
	l.mu.Lock()
	signal := l.operationCtx
	l.mu.Unlock()
	if signal == nil {
		return ctx, func() {}
	}
	derived, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(signal, cancel)
	return derived, func() {
		stop()
		cancel()
	}
}

func (e *runtimeEffects) tool(name string) (AgentHarnessTool, error) {
	for _, tool := range e.harness.tools {
		if tool != nil && tool.Name() == name {
			return tool, nil
		}
	}
	return nil, fmt.Errorf("unknown tool %q", name)
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
	stateRegister, err := h.session.GetRegister(ctx, RegisterOpState, operation.OperationID)
	if err != nil {
		return RunOutcome{}, err
	}
	if stateRegister == nil {
		return RunOutcome{}, fmt.Errorf("operation disappeared before failure settlement")
	}
	expectedStateSeq := stateRegister.Seq
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
	if err := h.effect(ctx, ActionInfo{Kind: "terminal", Description: "failed operation"}, func(ctx context.Context) error {
		return h.line(lane.name).Do(ctx, func() error {
			current, err := h.session.GetRegister(ctx, RegisterOpState, operation.OperationID)
			if err != nil {
				return err
			}
			if current == nil || current.Seq != expectedStateSeq {
				if current == nil {
					return errOperationMissing
				}
				return fmt.Errorf("stale operation state")
			}
			_, err = h.session.Commit(ctx, Transaction{Writes: writes})
			return err
		})
	}); err != nil {
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
func (l *runtimeLane) Resume(ctx context.Context) (Result[ResumeOutcome, error], error) {
	if err := l.harness.lifecycle.Err(); err != nil {
		return Result[ResumeOutcome, error]{Err: err}, nil
	}
	restored, err := Restore(ctx, l.harness.session, l.name)
	if err != nil {
		return Result[ResumeOutcome, error]{Err: err}, nil
	}
	if restored.Current == nil {
		return Result[ResumeOutcome, error]{Err: &NothingToResume{TaggedError: TaggedError{Message: "nothing to resume"}, Lane: l.name}}, nil
	}
	current := *restored.Current
	if current.Operation.Intent.Kind != OperationRun || current.State.Run == nil {
		return Result[ResumeOutcome, error]{Err: fmt.Errorf("resume for %s is not implemented", current.Operation.Intent.Kind)}, nil
	}
	if current.State.Run.Phase.Kind == PhaseAssistant && current.State.Run.Phase.Generation != nil && current.State.Run.Phase.Generation.Status != GenerationEffectPending {
		if missing := l.missingIdentities(ctx, current.Configuration); missing != nil {
			return Result[ResumeOutcome, error]{Err: missing}, nil
		}
	}
	if err := l.harness.line(l.name).Do(ctx, func() error {
		l.begin()
		return nil
	}); err != nil {
		return Result[ResumeOutcome, error]{Err: err}, nil
	}
	outcome, err := l.harness.drive(ctx, l, current.Operation, current.State)
	if err != nil {
		return Result[ResumeOutcome, error]{Err: err}, nil
	}
	if outcome.Kind == "suspended" && outcome.Reason == "missing_identities" {
		if missing := l.missingIdentities(ctx, current.Configuration); missing != nil {
			return Result[ResumeOutcome, error]{Err: missing}, nil
		}
	}
	return Ok[ResumeOutcome, error](ResumeOutcome{Operation: OperationRun, RunID: current.Operation.OperationID, Run: &outcome}), nil
}

func (l *runtimeLane) missingIdentities(ctx context.Context, config LaneConfiguration) *MissingIdentities {
	missing := &MissingIdentities{TaggedError: TaggedError{Message: "required identity is unavailable"}, Lane: l.name}
	if _, err := l.harness.models.Resolve(ctx, config.Model.Provider, config.Model.ModelID); err != nil {
		missing.Models = []string{config.Model.Provider + "/" + config.Model.ModelID}
	}
	missing.Tools = missingActiveTools(l.harness.tools, config.ActiveToolNames)
	if len(missing.Models) == 0 && len(missing.Tools) == 0 {
		return nil
	}
	return missing
}
func (l *runtimeLane) Abort(ctx context.Context) (Result[AbortOutcome, error], error) {
	if err := l.harness.lifecycle.Err(); err != nil {
		return Result[AbortOutcome, error]{Err: err}, nil
	}
	var outcome AbortOutcome
	var shouldSignal bool
	if err := l.harness.line(l.name).Do(ctx, func() error {
		current, err := l.harness.currentOperation(ctx, l)
		if err != nil {
			if isOperationMissing(err) {
				return &NoActiveOperation{TaggedError: TaggedError{Message: "no active operation"}, Lane: l.name}
			}
			return err
		}
		if current.State.Run == nil {
			return &NoActiveOperation{TaggedError: TaggedError{Message: "operation is not a run"}, Lane: l.name}
		}
		run := current.State.Run
		if run.Control.Status == ControlRunning {
			run.Control.Status = ControlCancelRequested
			run.Control.RequestedAt = time.Now().UnixMilli()
			run.Control.DrainedSteer = append([]string(nil), run.Inbox.Steer...)
			run.Control.DrainedFollowUp = append([]string(nil), run.Inbox.FollowUp...)
			run.Inbox.Steer = nil
			run.Inbox.FollowUp = nil
			if _, err := l.harness.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpState, current.Operation.OperationID, current.State)}}); err != nil {
				return err
			}
			shouldSignal = true
		}
		outcome.RunID = current.Operation.OperationID
		outcome.Steer, err = l.pendingMessages(ctx, run.Control.DrainedSteer)
		if err != nil {
			return err
		}
		outcome.FollowUp, err = l.pendingMessages(ctx, run.Control.DrainedFollowUp)
		return err
	}); err != nil {
		return Result[AbortOutcome, error]{Err: err}, nil
	}
	if shouldSignal {
		l.signalOperation()
	}
	l.harness.events.Emit(ctx, HarnessEvent{Type: string(EventRunAbort), Lane: l.name, Payload: outcome})
	return Ok[AbortOutcome, error](outcome), nil
}

func (l *runtimeLane) pendingMessages(ctx context.Context, ids []string) ([]AgentMessage, error) {
	messages := make([]AgentMessage, 0, len(ids))
	for _, id := range ids {
		register, err := l.harness.session.GetRegister(ctx, RegisterPendingEntry, id)
		if err != nil {
			return nil, err
		}
		if register == nil {
			return nil, fmt.Errorf("drained item %s has no payload register", id)
		}
		pending, ok := register.Value.(PendingEntry)
		if !ok || pending.Type != EntryMessage {
			return nil, fmt.Errorf("drained item %s has invalid payload", id)
		}
		message, ok := pending.Payload.(AgentMessage)
		if !ok {
			return nil, fmt.Errorf("drained item %s has invalid message", id)
		}
		messages = append(messages, *messageCopy(message))
	}
	return messages, nil
}

func (l *runtimeLane) signalOperation() {
	l.mu.Lock()
	cancel := l.operationCancel
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}
func (l *runtimeLane) Steer(ctx context.Context, input PromptInput) (Result[QueueOutcome, error], error) {
	return l.enqueueQueue(ctx, input, false)
}
func (l *runtimeLane) FollowUp(ctx context.Context, input PromptInput) (Result[QueueOutcome, error], error) {
	return l.enqueueQueue(ctx, input, true)
}
func (l *runtimeLane) NextRun(ctx context.Context, input PromptInput) (Result[NextRunOutcome, error], error) {
	if err := l.harness.lifecycle.Err(); err != nil {
		return Result[NextRunOutcome, error]{Err: err}, nil
	}
	message, err := queueMessage(input)
	if err != nil {
		return Result[NextRunOutcome, error]{Err: err}, nil
	}
	id := l.harness.session.IDGenerator().Next()
	if err := l.harness.line(l.name).Do(ctx, func() error {
		register, err := l.harness.session.GetRegister(ctx, RegisterLaneState, l.name)
		if err != nil {
			return err
		}
		if register == nil {
			return fmt.Errorf("lane state is missing")
		}
		state, ok := register.Value.(LaneState)
		if !ok {
			return fmt.Errorf("lane state has invalid type")
		}
		state.PendingNextRun = append(state.PendingNextRun, id)
		_, err = l.harness.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterPendingEntry, id, PendingEntry{Type: EntryMessage, Payload: message}), registerSet(RegisterLaneState, l.name, state)}})
		return err
	}); err != nil {
		return Result[NextRunOutcome, error]{Err: err}, nil
	}
	l.harness.eventsQueueUpdate(ctx, l)
	return Ok[NextRunOutcome, error](NextRunOutcome{EntryID: id}), nil
}
func (l *runtimeLane) CancelQueued(ctx context.Context, id string) (Result[CancelQueuedOutcome, error], error) {
	if err := l.harness.lifecycle.Err(); err != nil {
		return Result[CancelQueuedOutcome, error]{Err: err}, nil
	}
	if id == "" {
		return Ok[CancelQueuedOutcome, error](CancelQueuedOutcome{Kind: "not_found"}), nil
	}
	var outcome CancelQueuedOutcome
	if err := l.harness.line(l.name).Do(ctx, func() error {
		laneRegister, err := l.harness.session.GetRegister(ctx, RegisterLaneState, l.name)
		if err != nil || laneRegister == nil {
			return err
		}
		laneState, ok := laneRegister.Value.(LaneState)
		if !ok {
			return fmt.Errorf("lane state has invalid type")
		}
		if removeID(&laneState.PendingNextRun, id) {
			outcome.Kind = "cancelled"
			_, err = l.harness.session.Commit(ctx, Transaction{Writes: []Write{registerDelete(RegisterPendingEntry, id), registerSet(RegisterLaneState, l.name, laneState)}})
			return err
		}
		operation, operationErr := l.harness.currentOperation(ctx, l)
		if operationErr == nil && operation.State.Run != nil {
			for _, queue := range []*[]string{&operation.State.Run.Inbox.Steer, &operation.State.Run.Inbox.FollowUp, &operation.State.Run.Inbox.Writes} {
				if removeID(queue, id) {
					_, err = l.harness.session.Commit(ctx, Transaction{Writes: []Write{registerDelete(RegisterPendingEntry, id), registerSet(RegisterOpState, operation.Operation.OperationID, operation.State)}})
					outcome.Kind = "cancelled"
					return err
				}
			}
		}
		entries, err := l.harness.session.GetEntries(ctx, []string{id})
		if err != nil {
			return err
		}
		if _, ok := entries[id]; ok {
			outcome.Kind = "already_consumed"
		} else {
			outcome.Kind = "not_found"
		}
		return nil
	}); err != nil {
		return Result[CancelQueuedOutcome, error]{Err: err}, nil
	}
	l.harness.eventsQueueUpdate(ctx, l)
	return Ok[CancelQueuedOutcome, error](outcome), nil
}
func (l *runtimeLane) RecordUsage(context.Context, Usage, *string, JSONValue) (Result[RecordUsageOutcome, error], error) {
	return Err[RecordUsageOutcome](fmt.Errorf("usage recording is not implemented")), nil
}
func (l *runtimeLane) begin() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.operationCtx == nil {
		l.operationCtx, l.operationCancel = context.WithCancel(l.harness.rootCtx)
	}
	select {
	case <-l.idle:
		l.idle = make(chan struct{})
	default:
	}
}

func (l *runtimeLane) finish() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.operationCancel != nil {
		l.operationCancel()
		l.operationCancel = nil
		l.operationCtx = nil
	}
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
	case <-l.harness.lifecycle.Done():
		return l.harness.lifecycle.Err()
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
	if err := l.harness.lifecycle.Err(); err != nil {
		return err
	}
	return l.harness.line(l.name).Do(ctx, func() error {
		if err := l.harness.lifecycle.Err(); err != nil {
			return err
		}
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
	h.settings.UpdateStream(options)
	return nil
}

func deferredHandle(message AgentMessage) (*DeferredHandle, bool) {
	value := message.Content
	if values, ok := message.Metadata["deferred"]; ok {
		value = values
	}
	switch handle := value.(type) {
	case DeferredHandle:
		if handle.Provider != "" && handle.ModelID != "" && handle.ID != "" {
			return &handle, true
		}
	case *DeferredHandle:
		if handle != nil && handle.Provider != "" && handle.ModelID != "" && handle.ID != "" {
			return cloneValue(*handle).(*DeferredHandle), true
		}
	case map[string]JSONValue:
		provider, providerOK := handle["provider"].(string)
		modelID, modelOK := handle["modelId"].(string)
		id, idOK := handle["id"].(string)
		if providerOK && modelOK && idOK && provider != "" && modelID != "" && id != "" {
			return &DeferredHandle{Provider: provider, ModelID: modelID, ID: id, Data: handle["data"]}, true
		}
	}
	return nil, false
}

func (h *Harness) settleGeneration(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, generation Generation, message AgentMessage, phase RunPhase) (CurrentOperation, error) {
	if generation.ResponseEntryID == "" || generation.UsageID == "" {
		return CurrentOperation{}, fmt.Errorf("assistant settlement has no reserved ids")
	}
	usage := Usage{}
	if message.Usage != nil {
		usage = *message.Usage
	}
	entry := Entry{EntryBase: EntryBase{ID: generation.ResponseEntryID, ParentID: stringPointer(generation.Context.TriggerEntryID), Type: EntryMessage}, Message: messageCopy(message)}
	var settlement SettlementResult
	if err := h.effect(ctx, ActionInfo{Kind: "settlement", Description: "assistant response"}, func(ctx context.Context) error {
		var err error
		settlement, err = (&runtimeEffects{harness: h, lane: lane}).CommitEffectSettlement(ctx, current, EffectPlan{Kind: EffectAssistant, Key: EffectKey(operation.OperationID), Generation: &generation}, SettlementOutput{Kind: "assistant", Key: EffectKey(operation.OperationID), Message: &message, Phase: phase}, h.telemetry)
		return err
	}); err != nil {
		if isOperationMissing(err) {
			return CurrentOperation{}, errOperationMissing
		}
		return CurrentOperation{}, err
	}
	settled := settlement.Current
	h.events.Emit(ctx, HarnessEvent{Type: string(EventEntryAdded), Lane: lane.name, Payload: cloneValue(entry)})
	h.events.Emit(ctx, HarnessEvent{Type: string(EventUsage), Lane: lane.name, Payload: cloneValue(usage)})
	h.events.Emit(ctx, HarnessEvent{Type: string(EventTurnEnd), Lane: lane.name, Payload: map[string]JSONValue{"runId": operation.OperationID, "turnId": generation.Context.StepID}})
	return settled, nil
}

func retryDelay(policy NormalizedRetryPolicy, options AgentHarnessStreamOptions, attempt int64) int64 {
	if policy.BaseDelayMs <= 0 {
		return 0
	}
	delay := policy.BaseDelayMs
	if delay > maxSafeInteger {
		delay = maxSafeInteger
	}
	for i := int64(1); i < attempt; i++ {
		if delay > maxSafeInteger/2 {
			delay = maxSafeInteger
			break
		}
		delay *= 2
	}
	maxDelay := options.MaxRetryDelayMs
	if maxDelay > maxSafeInteger {
		maxDelay = maxSafeInteger
	}
	if maxDelay > 0 && delay > maxDelay {
		return maxDelay
	}
	return delay
}

func retryNotBefore(now, delay int64) int64 {
	if delay > maxSafeInteger-now {
		return maxSafeInteger
	}
	return now + delay
}

func (h *Harness) settleGenerationError(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, generation Generation, cause error) (RunOutcome, error) {
	return h.settleGenerationErrorMessage(ctx, lane, operation, current, generation, AgentMessage{}, cause)
}

func (h *Harness) settleGenerationErrorMessage(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, generation Generation, message AgentMessage, cause error) (RunOutcome, error) {
	if cause == nil {
		cause = fmt.Errorf("generation failed")
	}
	if message.Role == "" && message.Content == nil && message.Usage == nil {
		message = AgentMessage{Role: "assistant", Content: cause.Error()}
	}
	message.Role = "assistant"
	message.StopReason = StopReasonError
	next := RunPhase{Kind: PhaseFailureDrain, Error: &OperationError{Code: "runtime", Message: cause.Error()}, Provenance: &FailureProvenance{Kind: "assistant", EntryID: generation.ResponseEntryID}}
	var terminal terminalGenerationError
	if !errors.As(cause, &terminal) && generation.Attempt < generation.Context.RetryPolicy.MaxAttempts {
		delay := retryDelay(generation.Context.RetryPolicy, generation.Context.StreamOptions, generation.Attempt)
		next = RunPhase{Kind: PhaseAssistant, Generation: &Generation{Status: GenerationRetryWait, NextAttempt: generation.Attempt + 1, Context: generation.Context, NotBefore: retryNotBefore(time.Now().UnixMilli(), delay), ErrorMessage: cause.Error()}}
	}
	settled, err := h.settleGeneration(ctx, lane, operation, current, generation, message, next)
	if err != nil {
		return RunOutcome{}, err
	}
	if next.Kind == PhaseAssistant {
		return h.drive(ctx, lane, operation, settled.State)
	}
	return h.drive(ctx, lane, operation, settled.State)
}

func (h *Harness) settleDeferred(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, generation Generation, message AgentMessage, handle *DeferredHandle) (RunOutcome, error) {
	next := RunPhase{Kind: PhaseDeferred, Deferred: &Deferred{Status: DeferredSuspended, StepID: generation.Context.StepID, SourceEntryID: generation.ResponseEntryID, Configuration: generation.Context.Configuration, StreamOptions: generation.Context.StreamOptions}}
	settled, err := h.settleGeneration(ctx, lane, operation, current, generation, message, next)
	if err != nil {
		return RunOutcome{}, err
	}
	return RunOutcome{Kind: "suspended", RunID: operation.OperationID, LeafID: settled.LeafID, FinalEntryID: settled.LeafID, FinalMessage: &message, Reason: "deferred", Deferred: handle}, nil
}

func (h *Harness) settleAborted(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, generation Generation, message AgentMessage) (RunOutcome, error) {
	message.Role = "assistant"
	message.StopReason = StopReasonAborted
	phase := RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationMayFinish, IncludeFinalAssistant: true}, TriggerEntryID: generation.ResponseEntryID}}
	settled, err := h.settleGeneration(ctx, lane, operation, current, generation, message, phase)
	if err != nil {
		return RunOutcome{}, err
	}
	return h.drive(ctx, lane, operation, settled.State)
}

func (h *Harness) finishOutcome(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, result RunOutcome) (RunOutcome, error) {
	fx := &runtimeEffects{harness: h, lane: lane}
	if err := h.effect(ctx, ActionInfo{Kind: "terminal", Description: "assistant terminal"}, func(ctx context.Context) error {
		_, err := fx.CommitTerminal(ctx, current, result)
		return err
	}); err != nil {
		if isOperationMissing(err) {
			return h.resolveExternalFinalization(ctx, lane, operation)
		}
		return RunOutcome{}, err
	}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventRunEnd), Lane: lane.name, Payload: cloneValue(result)})
	return result, nil
}

func (h *Harness) finishFailure(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, cause error) (RunOutcome, error) {
	if cause == nil {
		cause = fmt.Errorf("operation failed")
	}
	return h.finishFailureValue(ctx, lane, operation, current, &OperationError{Code: "runtime", Message: cause.Error()})
}

func (h *Harness) finishFailureValue(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, errorValue *OperationError) (RunOutcome, error) {
	if errorValue == nil {
		errorValue = &OperationError{Code: "runtime", Message: "operation failed"}
	}
	result := RunOutcome{Kind: "failed", RunID: operation.OperationID, LeafID: current.LeafID, Error: errorValue}
	if current.State.Run != nil {
		result.FinalEntryID = current.State.Run.LatestAssistantEntryID
		if result.FinalEntryID != nil {
			if entry, err := lane.view.GetEntry(ctx, *result.FinalEntryID); err == nil && entry != nil && entry.Message != nil {
				result.FinalMessage = messageCopy(*entry.Message)
			}
		}
	}
	return h.finishOutcome(ctx, lane, operation, current, result)
}

func queueMessage(input PromptInput) (AgentMessage, error) {
	if len(input.Messages) > 1 || input.Text != "" && len(input.Messages) != 0 {
		return AgentMessage{}, fmt.Errorf("queued input must contain one message")
	}
	if len(input.Messages) == 1 {
		return *messageCopy(input.Messages[0]), nil
	}
	if input.Text == "" && len(input.Images) == 0 {
		return AgentMessage{}, fmt.Errorf("queued input is empty")
	}
	var content JSONValue = input.Text
	if len(input.Images) != 0 {
		content = append([]ImageContent(nil), input.Images...)
		if input.Text != "" {
			content = map[string]JSONValue{"text": input.Text, "images": append([]ImageContent(nil), input.Images...)}
		}
	}
	return AgentMessage{Role: "user", Content: content}, nil
}

func (l *runtimeLane) enqueueQueue(ctx context.Context, input PromptInput, followUp bool) (Result[QueueOutcome, error], error) {
	if err := l.harness.lifecycle.Err(); err != nil {
		return Result[QueueOutcome, error]{Err: err}, nil
	}
	message, err := queueMessage(input)
	if err != nil {
		return Result[QueueOutcome, error]{Err: err}, nil
	}
	id := l.harness.session.IDGenerator().Next()
	if err := l.harness.line(l.name).Do(ctx, func() error {
		current, err := l.harness.currentOperation(ctx, l)
		if err != nil {
			return &NoActiveRun{TaggedError: TaggedError{Message: "no active run"}, Lane: l.name}
		}
		if current.State.Run == nil || current.State.Run.Control.Status != ControlRunning {
			return &NoActiveRun{TaggedError: TaggedError{Message: "run is not accepting queued input"}, Lane: l.name}
		}
		state := current.State
		if followUp {
			state.Run.Inbox.FollowUp = append(state.Run.Inbox.FollowUp, id)
		} else {
			state.Run.Inbox.Steer = append(state.Run.Inbox.Steer, id)
		}
		_, err = l.harness.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterPendingEntry, id, PendingEntry{Type: EntryMessage, Payload: message}), registerSet(RegisterOpState, current.Operation.OperationID, state)}})
		return err
	}); err != nil {
		return Result[QueueOutcome, error]{Err: err}, nil
	}
	l.harness.eventsQueueUpdate(ctx, l)
	return Ok[QueueOutcome, error](QueueOutcome{EntryID: id}), nil
}

func (h *Harness) eventsQueueUpdate(ctx context.Context, lane *runtimeLane) {
	snapshot, err := h.queueSnapshot(ctx, lane)
	if err != nil {
		return
	}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventQueueUpdate), Lane: lane.name, Payload: snapshot})
}

func (h *Harness) queueSnapshot(ctx context.Context, lane *runtimeLane) (QueueSnapshot, error) {
	restored, err := Restore(ctx, h.session, lane.name)
	if err != nil {
		return QueueSnapshot{}, err
	}
	steer, followUp := []string(nil), []string(nil)
	if restored.Current != nil && restored.Current.State.Run != nil {
		steer = restored.Current.State.Run.Inbox.Steer
		followUp = restored.Current.State.Run.Inbox.FollowUp
	}
	stateRegister, err := h.session.GetRegister(ctx, RegisterLaneState, lane.name)
	if err != nil {
		return QueueSnapshot{}, err
	}
	if stateRegister == nil {
		return QueueSnapshot{}, fmt.Errorf("lane state is missing")
	}
	state, ok := stateRegister.Value.(LaneState)
	if !ok {
		return QueueSnapshot{}, fmt.Errorf("lane state has invalid type")
	}
	item := func(id string) (QueueItem, error) {
		register, err := h.session.GetRegister(ctx, RegisterPendingEntry, id)
		if err != nil {
			return QueueItem{}, err
		}
		if register == nil {
			return QueueItem{}, fmt.Errorf("queued item %s has no payload register", id)
		}
		pending, ok := register.Value.(PendingEntry)
		if !ok || pending.Type != EntryMessage {
			return QueueItem{}, fmt.Errorf("queued item %s is not a message", id)
		}
		message, ok := pending.Payload.(AgentMessage)
		if !ok {
			return QueueItem{}, fmt.Errorf("queued item %s has invalid message", id)
		}
		return QueueItem{EntryID: id, Message: *messageCopy(message)}, nil
	}
	items := func(ids []string) ([]QueueItem, error) {
		result := make([]QueueItem, 0, len(ids))
		for _, id := range ids {
			queued, err := item(id)
			if err != nil {
				return nil, err
			}
			result = append(result, queued)
		}
		return result, nil
	}
	steerItems, err := items(steer)
	if err != nil {
		return QueueSnapshot{}, err
	}
	followUpItems, err := items(followUp)
	if err != nil {
		return QueueSnapshot{}, err
	}
	nextRunItems, err := items(state.PendingNextRun)
	if err != nil {
		return QueueSnapshot{}, err
	}
	return QueueSnapshot{Steer: steerItems, FollowUp: followUpItems, NextRun: nextRunItems}, nil
}

func removeID(ids *[]string, wanted string) bool {
	for i, id := range *ids {
		if id == wanted {
			*ids = append((*ids)[:i], (*ids)[i+1:]...)
			return true
		}
	}
	return false
}
func (h *Harness) GetRetryPolicy(context.Context) (RetryPolicy, error) {
	snapshot := h.settings.Snapshot()
	return RetryPolicy{Enabled: snapshot.RetryPolicy.MaxAttempts > 1, MaxRetries: snapshot.RetryPolicy.MaxAttempts - 1, BaseDelayMs: snapshot.RetryPolicy.BaseDelayMs}, nil
}
func (h *Harness) SetRetryPolicy(_ context.Context, policy RetryPolicy) error {
	if err := h.lifecycle.Err(); err != nil {
		return err
	}
	if policy.MaxRetries < 0 || policy.BaseDelayMs < 0 || policy.BaseDelayMs > maxSafeInteger || policy.MaxRetries > maxSafeInteger-1 {
		return fmt.Errorf("retry policy exceeds safe limits")
	}
	maxAttempts := int64(1)
	if policy.Enabled {
		maxAttempts = policy.MaxRetries + 1
	}
	h.settings.UpdateRetry(NormalizedRetryPolicy{MaxAttempts: maxAttempts, BaseDelayMs: policy.BaseDelayMs})
	return nil
}
func (h *Harness) GetCompactionSettings(context.Context) (CompactionSettings, error) {
	h.configurationMu.RLock()
	defer h.configurationMu.RUnlock()
	return h.compaction, nil
}
func (h *Harness) SetCompactionSettings(_ context.Context, settings CompactionSettings) error {
	if err := h.lifecycle.Err(); err != nil {
		return err
	}
	if err := validateCompactionSettings(settings); err != nil {
		return err
	}
	h.configurationMu.Lock()
	h.compaction = settings
	h.configurationMu.Unlock()
	return nil
}
func (h *Harness) GetSteeringMode(context.Context) (QueueMode, error) {
	h.configurationMu.RLock()
	defer h.configurationMu.RUnlock()
	return h.steeringMode, nil
}
func (h *Harness) SetSteeringMode(_ context.Context, mode QueueMode) error {
	if err := h.lifecycle.Err(); err != nil {
		return err
	}
	if err := validateQueueMode(mode); err != nil {
		return err
	}
	h.configurationMu.Lock()
	h.steeringMode = mode
	h.configurationMu.Unlock()
	return nil
}
func (h *Harness) GetFollowUpMode(context.Context) (QueueMode, error) {
	h.configurationMu.RLock()
	defer h.configurationMu.RUnlock()
	return h.followUpMode, nil
}
func (h *Harness) SetFollowUpMode(_ context.Context, mode QueueMode) error {
	if err := h.lifecycle.Err(); err != nil {
		return err
	}
	if err := validateQueueMode(mode); err != nil {
		return err
	}
	h.configurationMu.Lock()
	h.followUpMode = mode
	h.configurationMu.Unlock()
	return nil
}

func (h *Harness) runSettings() RunSettings {
	h.configurationMu.RLock()
	defer h.configurationMu.RUnlock()
	return RunSettings{Compaction: h.compaction, SteeringMode: h.steeringMode, FollowUpMode: h.followUpMode, ToolExecution: h.toolExecution}
}

func validateCompactionSettings(settings CompactionSettings) error {
	if settings.ReserveTokens < 0 || settings.ReserveTokens > maxSafeInteger || settings.KeepRecentTokens < 0 || settings.KeepRecentTokens > maxSafeInteger {
		return fmt.Errorf("compaction token settings exceed safe limits")
	}
	return nil
}

func validateQueueMode(mode QueueMode) error {
	if mode != QueueAll && mode != QueueOneAtATime {
		return fmt.Errorf("invalid queue mode %q", mode)
	}
	return nil
}
func (h *Harness) WatchSession(context.Context) (WatchHandle[SessionSnapshot], error) {
	return WatchHandle[SessionSnapshot]{Snapshot: SessionSnapshot{}, Start: func(_ func(HarnessEvent)) {}, Unsubscribe: func() {}}, nil
}

var _ AgentHarness = (*Harness)(nil)
