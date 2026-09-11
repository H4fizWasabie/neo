package harness

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
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
	entryProjectors    map[string]EntryProjector
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
	laneCreationMu     sync.Mutex
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
	suspension      *SuspendedOperation
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
		entryProjectors:    options.EntryProjectors,
		compaction:         options.Compaction,
		steeringMode:       options.SteeringMode,
		followUpMode:       options.FollowUpMode,
		toolExecution:      options.ToolExecution,
		scheduler:          NewManualScheduler(options.Drive == "manual"),
		hooks:              NewHookRunner(),
		events:             NewEventBus(options.Telemetry),
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
	restore := func(lane *runtimeLane) error {
		current, err := Restore(ctx, options.Session, lane.name)
		if err != nil {
			return err
		}
		if current.Current == nil {
			return nil
		}
		descriptor := SuspendedOperation{Lane: lane.name, OperationID: current.Current.Operation.OperationID, Kind: current.Current.Operation.Intent.Kind, Reason: "crash", StartedAt: current.Current.Operation.StartedAt}
		lane.begin()
		lane.mu.Lock()
		lane.suspension = &descriptor
		lane.mu.Unlock()
		suspended = append(suspended, descriptor)
		return nil
	}
	if err := restore(main); err != nil {
		cancel()
		return nil, nil, err
	}
	states, err := options.Session.ListRegisters(ctx, RegisterLaneState, "")
	if err != nil {
		cancel()
		return nil, nil, err
	}
	for _, state := range states {
		if state.Key == "main" {
			continue
		}
		config, err := options.Session.GetRegister(ctx, RegisterLaneConfig, state.Key)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		if config == nil {
			cancel()
			return nil, nil, fmt.Errorf("lane has no configuration: %s", state.Key)
		}
		laneConfig, ok := config.Value.(LaneConfiguration)
		if !ok {
			cancel()
			return nil, nil, fmt.Errorf("lane configuration has invalid type: %s", state.Key)
		}
		lane, err := h.attachLane(ctx, state.Key, AgentHarnessOptions{Model: laneConfig.Model, ThinkingLevel: laneConfig.ThinkingLevel, ActiveToolNames: laneConfig.ActiveToolNames})
		if err != nil {
			cancel()
			return nil, nil, err
		}
		if err := restore(lane); err != nil {
			cancel()
			return nil, nil, err
		}
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
			return h.telemetry.StartSpan(ctx, SpanOptions{Name: actionSpanName(info.Kind)}, func(TelemetrySpan) error { return fn(ctx) })
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

func (h *Harness) operationSpan(ctx context.Context, name, lane, operationID string, fn func() error) error {
	if h.telemetry == nil {
		return fn()
	}
	return h.telemetry.StartSpan(ctx, SpanOptions{Name: name, Attributes: map[string]AttributeValue{"lane": lane, "operation.id": operationID}}, func(TelemetrySpan) error {
		return fn()
	})
}

func actionSpanName(kind string) string {
	switch kind {
	case "provider":
		return SpanAIRequest
	case "hook":
		return SpanHarnessHook
	case "wait":
		return SpanHarnessSleep
	case "tool":
		return SpanHarnessTool
	default:
		return SpanHarnessStep
	}
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
			resources, err := l.harness.GetResources(ctx)
			if err != nil {
				return err
			}
			output, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(operationID), HookName: HookBeforeRun, Event: map[string]JSONValue{"prompt": append([]AgentMessage(nil), messages...), "systemPrompt": capturedSystemPrompt, "resources": resources}})
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
	var result RunOutcome
	driveErr := l.harness.operationSpan(ctx, SpanHarnessRun, l.name, operationID, func() error {
		var driveErr error
		result, driveErr = l.harness.drive(ctx, l, accepted, acceptedState)
		return driveErr
	})
	if driveErr != nil {
		return Result[RunOutcome, error]{Err: driveErr}, nil
	}
	if result.Kind == "suspended" {
		l.setSuspension(SuspendedOperation{Lane: l.name, OperationID: operationID, Kind: OperationRun, Reason: result.Reason, StartedAt: accepted.StartedAt, Deferred: result.Deferred})
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
		if phase.Kind == PhaseCompaction {
			return h.driveRunCompaction(ctx, lane, operation, current)
		}
		if phase.Kind == PhaseDeferred {
			return h.driveDeferred(ctx, lane, operation, current)
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
			started, err := h.maybeStartThresholdCompaction(ctx, lane, operation, current)
			if err != nil {
				return RunOutcome{}, err
			}
			if started {
				continue
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
			generation := &Generation{Status: GenerationReady, NextAttempt: 1, Context: GenerationContext{StepID: operation.OperationID + ":step:1", TriggerEntryID: phase.Checkpoint.TriggerEntryID, Configuration: config, StreamOptions: settings.StreamOptions, RetryPolicy: settings.RetryPolicy, OverflowRecoveryUsed: phase.Checkpoint.Continuation.OverflowRecoveryUsed}}
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
		missingTools := missingActiveTools(h.toolsSnapshot(), generation.Context.Configuration.ActiveToolNames)
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
		pending.Run.Phase.Generation.IntendedOutputLimit = generation.Context.Configuration.Model.OutputLimit
		pending.Run.Phase.Generation.ContextWindow = generation.Context.Configuration.Model.ContextWindow
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
		message, err := h.runAssistantAttempt(ctx, lane, operation, pending, current.LeafID, fx)
		if err != nil {
			latest, reloadErr := h.currentOperation(ctx, lane)
			if reloadErr == nil && latest.State.Run != nil && latest.State.Run.Control.Status == ControlCancelRequested {
				return h.settleAborted(ctx, lane, operation, latest, *pending.Run.Phase.Generation, AgentMessage{Role: "assistant", Content: err.Error()})
			}
			if isOperationMissing(reloadErr) {
				return h.resolveExternalFinalization(ctx, lane, operation)
			}
			if overflow, reason := overflowError(err); overflow {
				return h.settleGenerationOverflow(ctx, lane, operation, current, *pending.Run.Phase.Generation, AgentMessage{Role: "assistant", Content: reason}, reason)
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
			if overflow, reason := overflowMessage(message, *pending.Run.Phase.Generation); overflow {
				return h.settleGenerationOverflow(ctx, lane, operation, current, *pending.Run.Phase.Generation, message, reason)
			}
			return h.settleGenerationErrorMessage(ctx, lane, operation, current, *pending.Run.Phase.Generation, message, fmt.Errorf("provider returned an error"))
		}
		if overflow, reason := overflowMessage(message, *pending.Run.Phase.Generation); overflow {
			return h.settleGenerationOverflow(ctx, lane, operation, current, *pending.Run.Phase.Generation, message, reason)
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

func (h *Harness) maybeStartThresholdCompaction(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation) (bool, error) {
	run := current.State.Run
	if run == nil || run.Phase.Checkpoint == nil || run.Phase.Checkpoint.Continuation.Kind != ContinuationNeedAssistant || !run.Settings.Compaction.Enabled {
		return false, nil
	}
	trigger := run.Phase.Checkpoint.TriggerEntryID
	if trigger == "" || run.Phase.Checkpoint.ThresholdCheckedTriggerEntryID == trigger || current.Configuration.Model.ContextWindow <= 0 {
		return false, nil
	}
	start := current.LeafID
	if start == nil {
		start = &trigger
	}
	entries, err := lane.view.FindEntriesOnBranch(ctx, BranchScan{Start: *start, Order: OldestFirst})
	if err != nil {
		return false, err
	}
	tokens := 0
	for _, entry := range entries {
		if entry.Message != nil && includeInContext(*entry.Message) {
			tokens += estimateMessageTokens(*entry.Message)
		}
	}
	if int64(tokens)+run.Settings.Compaction.ReserveTokens < current.Configuration.Model.ContextWindow {
		return false, nil
	}
	prep, hasPreparation, err := h.buildCompactionPreparation(ctx, lane, current.LeafID, run.Settings.Compaction)
	if err != nil {
		return false, err
	}
	next := current.State
	marked := *run.Phase.Checkpoint
	marked.ThresholdCheckedTriggerEntryID = trigger
	if !hasPreparation {
		next.Run.Phase.Checkpoint = &marked
	} else {
		taskID := "task:threshold:" + trigger
		next.Run.Phase = RunPhase{Kind: PhaseCompaction, Reason: "threshold", Structural: &StructuralDecision{TaskID: taskID, Status: "deciding"}, ResumeAfter: &marked}
	}
	writes := []Write{registerSet(RegisterOpState, operation.OperationID, next)}
	if hasPreparation {
		writes = append([]Write{registerSet(RegisterOpPreparation, operation.OperationID+":"+next.Run.Phase.Structural.TaskID, prep)}, writes...)
	}
	if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "threshold compaction"}, func(ctx context.Context) error {
		return h.line(lane.name).Do(ctx, func() error {
			valid, err := (&runtimeEffects{harness: h, lane: lane}).current(ctx, current)
			if err != nil || !valid {
				if err == nil {
					err = errOperationMissing
				}
				return err
			}
			_, err = h.session.Commit(ctx, Transaction{Writes: writes})
			return err
		})
	}); err != nil {
		return false, err
	}
	return true, nil
}

func estimateMessageTokens(message AgentMessage) int {
	text := fmt.Sprint(message.Content)
	if len(text) < 4 {
		return 1
	}
	return (len(text) + 3) / 4
}

func (h *Harness) driveDeferred(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation) (RunOutcome, error) {
	run := current.State.Run
	if run == nil || run.Phase.Deferred == nil {
		return RunOutcome{}, fmt.Errorf("deferred state is missing")
	}
	deferred := *run.Phase.Deferred
	if deferred.SourceEntryID == "" {
		return RunOutcome{}, fmt.Errorf("deferred state has no source entry")
	}
	entry, err := lane.view.GetEntry(ctx, deferred.SourceEntryID)
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

	poll := deferred.Poll
	if deferred.Status == DeferredSuspended {
		poll++
	} else if deferred.Status != DeferredEffectPending {
		return RunOutcome{}, fmt.Errorf("unsupported deferred status %q", deferred.Status)
	}
	if poll < 1 {
		poll = 1
	}
	requestOptions := cloneValue(deferred.StreamOptions).(AgentHarnessStreamOptions)
	requestOptions.Deferred = false
	fx := &runtimeEffects{harness: h, lane: lane}
	turnID := fmt.Sprintf("%s:poll:%d", deferred.StepID, poll)
	if err := h.runBeforeRequestStep(ctx, fx, operation, poll, "deferred", turnID, &requestOptions); err != nil {
		return h.finishFailure(ctx, lane, operation, current, err)
	}
	requestOptions.Deferred = false
	responseID, usageID := h.session.IDGenerator().Next(), h.session.IDGenerator().Next()
	pendingDeferred := deferred
	pendingDeferred.Status = DeferredEffectPending
	pendingDeferred.Poll = poll
	pendingDeferred.ResponseEntryID = responseID
	pendingDeferred.UsageID = usageID
	generation := deferredGeneration(pendingDeferred, responseID, usageID)
	pending := current.State
	pending.Run.Phase = RunPhase{Kind: PhaseDeferred, Deferred: &pendingDeferred}
	var transition *CurrentOperation
	if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "deferred effect pending"}, func(ctx context.Context) error {
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
		return RunOutcome{}, fmt.Errorf("deferred intent lost its compare-and-swap")
	}
	current = *transition

	response, err := h.runDeferredPoll(ctx, lane, operation, pendingDeferred, *handle, requestOptions, fx, turnID)
	if err != nil {
		latest, reloadErr := h.currentOperation(ctx, lane)
		if reloadErr == nil && latest.State.Run != nil && latest.State.Run.Control.Status == ControlCancelRequested {
			h.cancelDeferredSource(ctx, lane, pendingDeferred)
			return h.settleAborted(ctx, lane, operation, latest, generation, AgentMessage{Role: "assistant", Content: "deferred request aborted"})
		}
		if isOperationMissing(reloadErr) {
			return h.resolveExternalFinalization(ctx, lane, operation)
		}
		return h.settleDeferredError(ctx, lane, operation, current, generation, err)
	}
	latest, reloadErr := h.currentOperation(ctx, lane)
	if reloadErr == nil && latest.State.Run != nil && latest.State.Run.Control.Status == ControlCancelRequested {
		h.cancelDeferredSource(ctx, lane, pendingDeferred)
		message := AgentMessage{Role: "assistant", Content: "deferred request aborted"}
		if response.Message != nil {
			message = *response.Message
		}
		return h.settleAborted(ctx, lane, operation, latest, generation, message)
	}
	if isOperationMissing(reloadErr) {
		return h.resolveExternalFinalization(ctx, lane, operation)
	}

	if response.Kind == "pending" {
		if response.Handle == nil && response.Message != nil {
			response.Handle, _ = deferredHandle(*response.Message)
		}
		if response.Handle == nil || !reflect.DeepEqual(*response.Handle, *handle) {
			return h.settleDeferredError(ctx, lane, operation, current, generation, fmt.Errorf("deferred provider returned a mismatched handle"))
		}
		message := deferredResponseMessage(response, *handle, StopReasonDeferred)
		next := RunPhase{Kind: PhaseDeferred, Deferred: &Deferred{Status: DeferredSuspended, StepID: deferred.StepID, SourceEntryID: responseID, Poll: poll, Configuration: deferred.Configuration, StreamOptions: deferred.StreamOptions}}
		settled, err := h.settleGeneration(ctx, lane, operation, current, generation, message, next)
		if err != nil {
			return RunOutcome{}, err
		}
		return RunOutcome{Kind: "suspended", RunID: operation.OperationID, LeafID: settled.LeafID, FinalEntryID: settled.LeafID, FinalMessage: &message, Reason: "deferred", Deferred: response.Handle}, nil
	}
	if response.Kind == "error" {
		message := response.Error
		if message == "" {
			message = "deferred provider returned an error"
		}
		return h.settleDeferredError(ctx, lane, operation, current, generation, fmt.Errorf("%s", message))
	}
	if response.Kind != "ready" || response.Message == nil {
		return h.settleDeferredError(ctx, lane, operation, current, generation, fmt.Errorf("deferred provider returned invalid response"))
	}
	message := *response.Message
	message.Role = "assistant"
	if message.StopReason == "" {
		message.StopReason = StopReasonStop
	}
	if err := validateAssistantMessage(message); err != nil {
		return h.settleDeferredError(ctx, lane, operation, current, generation, err)
	}
	if message.StopReason == StopReasonAborted {
		return RunOutcome{}, h.lifecycle.Fault(fmt.Errorf("provider returned aborted while control is running"))
	}
	if message.StopReason == StopReasonError {
		return h.settleDeferredError(ctx, lane, operation, current, generation, fmt.Errorf("provider returned an error"))
	}
	if message.StopReason == StopReasonDeferred {
		return h.settleDeferredError(ctx, lane, operation, current, generation, fmt.Errorf("ready deferred response has deferred stop reason"))
	}
	if message.StopReason == StopReasonToolUse && len(message.ToolCalls) == 0 {
		return h.settleDeferredError(ctx, lane, operation, current, generation, fmt.Errorf("tool_use response has no tool calls"))
	}
	if len(message.ToolCalls) != 0 {
		calls := make([]ToolCall, len(message.ToolCalls))
		for i := range message.ToolCalls {
			calls[i] = ToolCall{Status: "planned", SourceIndex: i, ResultEntryID: h.session.IDGenerator().Next()}
		}
		next := RunPhase{Kind: PhaseTools, ToolBatch: &ToolBatch{AssistantEntryID: responseID, Configuration: deferred.Configuration, StepID: deferred.StepID, TurnID: turnID, Calls: calls}}
		settled, err := h.settleGeneration(ctx, lane, operation, current, generation, message, next)
		if err != nil {
			return RunOutcome{}, err
		}
		return h.driveTools(ctx, lane, operation, settled)
	}
	next := RunPhase{Kind: PhaseCheckpoint, Checkpoint: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationMayFinish, IncludeFinalAssistant: true}, TriggerEntryID: responseID}}
	settled, err := h.settleGeneration(ctx, lane, operation, current, generation, message, next)
	if err != nil {
		return RunOutcome{}, err
	}
	return h.drive(ctx, lane, operation, settled.State)
}

func deferredGeneration(deferred Deferred, responseID, usageID string) Generation {
	return Generation{Status: GenerationEffectPending, Context: GenerationContext{StepID: deferred.StepID, TriggerEntryID: deferred.SourceEntryID, Configuration: deferred.Configuration, StreamOptions: deferred.StreamOptions, RetryPolicy: NormalizedRetryPolicy{MaxAttempts: 1}}, Attempt: deferred.Poll, NextAttempt: deferred.Poll, ResponseEntryID: responseID, UsageID: usageID}
}

func deferredResponseMessage(response DeferredResponse, handle DeferredHandle, reason StopReason) AgentMessage {
	if response.Message != nil {
		message := *response.Message
		message.Role = "assistant"
		message.StopReason = reason
		if message.Metadata == nil {
			message.Metadata = make(map[string]JSONValue)
		}
		message.Metadata["deferred"] = cloneValue(handle)
		return message
	}
	content := JSONValue(handle)
	if response.Error != "" {
		content = response.Error
	}
	return AgentMessage{Role: "assistant", Content: content, StopReason: reason}
}

func (h *Harness) settleDeferredError(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, generation Generation, cause error) (RunOutcome, error) {
	if cause == nil {
		cause = fmt.Errorf("deferred request failed")
	}
	return h.settleGenerationErrorMessage(ctx, lane, operation, current, generation, AgentMessage{Role: "assistant", Content: cause.Error()}, terminalGenerationError{cause})
}

func (h *Harness) driveCompaction(ctx context.Context, lane *runtimeLane, operation Operation, state OperationState) (CompactionOutcome, error) {
	current, err := h.currentOperation(ctx, lane)
	if err != nil {
		return CompactionOutcome{}, err
	}
	for {
		compaction := current.State.Compaction
		if compaction == nil {
			return CompactionOutcome{}, fmt.Errorf("compaction state is missing")
		}
		if compaction.Control.Status == ControlCancelRequested {
			return h.finishCompaction(ctx, lane, operation, current, nil, "aborted", nil, nil)
		}
		prepRegister, err := h.session.GetRegister(ctx, RegisterOpPreparation, operation.OperationID+":"+compaction.Structural.TaskID)
		if err != nil || prepRegister == nil {
			if err == nil {
				err = fmt.Errorf("compaction preparation is missing")
			}
			return CompactionOutcome{}, err
		}
		prep, ok := prepRegister.Value.(DurableStructuralPreparation)
		if !ok || prep.Kind != EntryCompaction {
			return CompactionOutcome{}, fmt.Errorf("compaction preparation has invalid type")
		}
		structural := compaction.Structural
		if structural.Status == "deciding" {
			decision, err := h.runCompactionHook(ctx, lane, operation, prep, compaction.CustomInstructions)
			if err != nil {
				return h.finishCompaction(ctx, lane, operation, current, nil, "failed", &OperationError{Code: "compaction", Message: err.Error()}, nil)
			}
			if decision.decline {
				return h.finishCompaction(ctx, lane, operation, current, nil, "declined", nil, nil)
			}
			if decision.result != nil {
				return h.finishCompaction(ctx, lane, operation, current, decision.result, "completed", nil, nil)
			}
			settings := h.settings.Snapshot()
			resultID := h.session.IDGenerator().Next()
			generation := &SummaryGeneration{Status: GenerationReady, NextAttempt: 1, Context: SummaryContext{TaskID: structural.TaskID, ResultEntryID: resultID, Kind: EntryCompaction, Configuration: current.Configuration, StreamOptions: settings.StreamOptions, RetryPolicy: settings.RetryPolicy, Reason: "manual"}, UsageIDs: []string{}}
			next := current.State
			next.Run = nil
			next.Compaction.Structural.Status = "generating"
			next.Compaction.Structural.Generation = generation
			var transition *CurrentOperation
			fx := &runtimeEffects{harness: h, lane: lane}
			if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "compaction generating"}, func(ctx context.Context) error {
				var err error
				transition, err = fx.CommitTransition(ctx, current, next, h.telemetry, &current.ConfigurationSeq, nil)
				return err
			}); err != nil {
				if isOperationMissing(err) {
					return CompactionOutcome{}, errOperationMissing
				}
				return CompactionOutcome{}, err
			}
			if transition == nil {
				return CompactionOutcome{}, fmt.Errorf("compaction decision lost its compare-and-swap")
			}
			current = *transition
			continue
		}
		if structural.Status != "generating" || structural.Generation == nil {
			return CompactionOutcome{}, fmt.Errorf("unsupported compaction structural status %q", structural.Status)
		}
		generation := *structural.Generation
		if generation.Status == GenerationRetryWait {
			if delay := generation.NotBefore - time.Now().UnixMilli(); delay > 0 {
				if err := h.effect(ctx, ActionInfo{Kind: "wait", Description: "compaction retry wait"}, func(ctx context.Context) error {
					return (&runtimeEffects{harness: h, lane: lane}).Sleep(ctx, delay, h.telemetry)
				}); err != nil {
					return CompactionOutcome{}, err
				}
			}
			next := current.State
			next.Compaction.Structural.Generation.Status = GenerationReady
			var transition *CurrentOperation
			if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "compaction retry ready"}, func(ctx context.Context) error {
				var err error
				transition, err = (&runtimeEffects{harness: h, lane: lane}).CommitTransition(ctx, current, next, h.telemetry, nil, nil)
				return err
			}); err != nil {
				return CompactionOutcome{}, err
			}
			if transition == nil {
				return CompactionOutcome{}, fmt.Errorf("compaction retry transition lost its compare-and-swap")
			}
			current = *transition
			continue
		}
		if generation.Status == GenerationEffectPending {
			if generation.Attempt >= generation.Context.RetryPolicy.MaxAttempts {
				return h.finishCompaction(ctx, lane, operation, current, nil, "failed", &OperationError{Code: "compaction", Message: "recovered compaction request reached retry cap"}, nil)
			}
			next := current.State
			next.Compaction.Structural.Generation.Status = GenerationReady
			next.Compaction.Structural.Generation.NextAttempt = generation.Attempt + 1
			next.Compaction.Structural.Generation.Attempt = 0
			next.Compaction.Structural.Generation.Request = nil
			var transition *CurrentOperation
			if err := h.effect(ctx, ActionInfo{Kind: "recovery", Description: "recover compaction request"}, func(ctx context.Context) error {
				var err error
				transition, err = (&runtimeEffects{harness: h, lane: lane}).CommitTransition(ctx, current, next, h.telemetry, nil, nil)
				return err
			}); err != nil {
				return CompactionOutcome{}, err
			}
			if transition == nil {
				return CompactionOutcome{}, fmt.Errorf("compaction recovery transition lost its compare-and-swap")
			}
			current = *transition
			continue
		}
		if generation.Status != GenerationReady {
			return CompactionOutcome{}, fmt.Errorf("unsupported compaction generation status %q", generation.Status)
		}
		attempt := generation.NextAttempt
		if attempt == 0 {
			attempt = generation.Attempt + 1
		}
		options := generation.Context.StreamOptions
		options.Deferred = false
		fx := &runtimeEffects{harness: h, lane: lane}
		if err := h.runBeforeRequestStep(ctx, fx, operation, attempt, "compaction", fmt.Sprintf("%s:attempt:%d", generation.Context.TaskID, attempt), &options); err != nil {
			return h.finishCompaction(ctx, lane, operation, current, nil, "failed", &OperationError{Code: "compaction", Message: err.Error()}, nil)
		}
		options.Deferred = false
		usageID := h.session.IDGenerator().Next()
		pending := current.State
		pending.Compaction.Structural.Generation.Status = GenerationEffectPending
		pending.Compaction.Structural.Generation.Attempt = attempt
		pending.Compaction.Structural.Generation.NextAttempt = attempt
		pending.Compaction.Structural.Generation.Request = &SummaryRequest{Index: len(generation.UsageIDs), UsageID: usageID}
		var transition *CurrentOperation
		if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "compaction request pending"}, func(ctx context.Context) error {
			var err error
			transition, err = fx.CommitTransition(ctx, current, pending, h.telemetry, nil, nil)
			return err
		}); err != nil {
			return CompactionOutcome{}, err
		}
		if transition == nil {
			return CompactionOutcome{}, fmt.Errorf("compaction request lost its compare-and-swap")
		}
		current = *transition
		generation = *current.State.Compaction.Structural.Generation
		message, err := h.runSummaryAttempt(ctx, lane, operation, prep, generation, options, fx)
		if err != nil {
			latest, reloadErr := h.currentOperation(ctx, lane)
			if reloadErr == nil && latest.State.Compaction != nil && latest.State.Compaction.Control.Status == ControlCancelRequested {
				return h.finishCompaction(ctx, lane, operation, latest, nil, "aborted", nil, nil)
			}
			if isOperationMissing(reloadErr) {
				return CompactionOutcome{}, errOperationMissing
			}
			return h.compactionAttemptFailure(ctx, lane, operation, current, messageUsage(message), err)
		}
		latest, reloadErr := h.currentOperation(ctx, lane)
		if reloadErr == nil && latest.State.Compaction != nil && latest.State.Compaction.Control.Status == ControlCancelRequested {
			return h.finishCompaction(ctx, lane, operation, latest, nil, "aborted", nil, nil)
		}
		if isOperationMissing(reloadErr) {
			return CompactionOutcome{}, errOperationMissing
		}
		result := &CompactResult{Summary: summaryText(message), Usage: message.Usage}
		if prep.IsSplitTurn && generation.Request != nil && generation.Request.Index == 0 {
			next := current.State
			next.Compaction.Structural.Generation.Status = GenerationReady
			next.Compaction.Structural.Generation.Request = nil
			next.Compaction.Structural.Generation.UsageIDs = append(append([]string(nil), generation.UsageIDs...), generation.Request.UsageID)
			writes := []Write{{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: generation.Request.UsageID, Usage: usageValue(message.Usage)}}}, registerSet(RegisterOpState, operation.OperationID, next)}
			if err := h.effect(ctx, ActionInfo{Kind: "settlement", Description: "compaction request settled"}, func(ctx context.Context) error {
				return h.line(lane.name).Do(ctx, func() error {
					valid, err := (&runtimeEffects{harness: h, lane: lane}).current(ctx, current)
					if err != nil || !valid {
						if err == nil {
							err = errOperationMissing
						}
						return err
					}
					_, err = h.session.Commit(ctx, Transaction{Writes: writes})
					return err
				})
			}); err != nil {
				if isOperationMissing(err) {
					return CompactionOutcome{}, errOperationMissing
				}
				return CompactionOutcome{}, err
			}
			current, err = h.currentOperation(ctx, lane)
			if err != nil {
				return CompactionOutcome{}, err
			}
			continue
		}
		return h.finishCompaction(ctx, lane, operation, current, result, "completed", nil, generation.Request)
	}
}

type compactionDecision struct {
	decline bool
	result  *CompactResult
}

func (h *Harness) runCompactionHook(ctx context.Context, lane *runtimeLane, operation Operation, preparation DurableStructuralPreparation, customInstructions string) (compactionDecision, error) {
	return h.runCompactionHookReason(ctx, lane, operation, preparation, customInstructions, "manual")
}

func (h *Harness) runCompactionHookReason(ctx context.Context, lane *runtimeLane, operation Operation, preparation DurableStructuralPreparation, customInstructions, reason string) (compactionDecision, error) {
	var result JSONValue
	err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "before_compaction"}, func(ctx context.Context) error {
		var err error
		result, err = h.hooks.Run(ctx, HookInvocation{Name: HookBeforeCompaction, Lane: lane.name, RunID: operation.OperationID, Event: map[string]JSONValue{"reason": reason, "preparation": compactionPreparation(preparation), "customInstructions": customInstructions}})
		return err
	})
	if err != nil {
		return compactionDecision{}, err
	}
	values, _ := result.(map[string]JSONValue)
	if values == nil {
		return compactionDecision{}, nil
	}
	decision := compactionDecision{}
	if decline, ok := values["decline"].(bool); ok {
		decision.decline = decline
	}
	if supplied, ok := values["compaction"].(CompactResult); ok {
		decision.result = &supplied
	}
	if decision.decline && decision.result != nil {
		return compactionDecision{}, nil
	}
	return decision, nil
}

func compactionPreparation(preparation DurableStructuralPreparation) CompactionPreparation {
	return CompactionPreparation{MessagesToSummarize: cloneValue(preparation.MessagesToSummarize).([]AgentMessage), TurnPrefixMessages: cloneValue(preparation.TurnPrefixMessages).([]AgentMessage), RetainedTail: cloneValue(preparation.RetainedTail).([]AgentMessage), IsSplitTurn: preparation.IsSplitTurn, TokensBefore: preparation.TokensBefore, PreviousSummary: preparation.PreviousSummary, Settings: preparation.Settings}
}

func (h *Harness) runSummaryAttempt(ctx context.Context, lane *runtimeLane, operation Operation, preparation DurableStructuralPreparation, generation SummaryGeneration, options AgentHarnessStreamOptions, fx *runtimeEffects) (AgentMessage, error) {
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "before_payload"}, func(ctx context.Context) error {
		_, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(fmt.Sprintf("%s:summary:%d:before_payload", operation.OperationID, generation.Attempt)), HookName: HookBeforePayload, Event: map[string]JSONValue{"model": generation.Context.Configuration.Model, "payload": JSONValue("summary-request"), "step": "compaction"}})
		return err
	}); err != nil {
		return AgentMessage{}, err
	}
	messages := append([]Message(nil), preparation.MessagesToSummarize...)
	if preparation.Kind == EntryBranchSummary {
		messages = append([]Message(nil), preparation.Messages...)
	}
	var output EffectOutput
	err := h.effect(ctx, ActionInfo{Kind: "provider", Description: "compaction summary"}, func(ctx context.Context) error {
		var err error
		output, err = fx.Run(ctx, EffectPlan{Kind: EffectSummary, Key: EffectKey(fmt.Sprintf("%s:summary:%d", operation.OperationID, generation.Attempt)), Summary: &generation, Model: generation.Context.Configuration.Model, Messages: messages, StreamOptions: options})
		return err
	})
	if err != nil {
		return AgentMessage{}, err
	}
	if output.Message == nil {
		return AgentMessage{}, fmt.Errorf("summary provider returned no message")
	}
	message := *output.Message
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "after_response"}, func(ctx context.Context) error {
		result, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(fmt.Sprintf("%s:summary:%d:after_response", operation.OperationID, generation.Attempt)), HookName: HookAfterResponse, Event: map[string]JSONValue{"message": message, "step": "compaction"}})
		if values, ok := result.Result.(map[string]JSONValue); ok {
			if replacement, ok := values["message"].(AgentMessage); ok {
				message = replacement
			}
		}
		return err
	}); err != nil {
		return AgentMessage{}, err
	}
	if message.Role == "" {
		message.Role = "assistant"
	}
	if message.StopReason == "" {
		message.StopReason = StopReasonStop
	}
	if message.StopReason != StopReasonStop && message.StopReason != StopReasonLength {
		return AgentMessage{}, fmt.Errorf("summary provider returned stop reason %q", message.StopReason)
	}
	return message, nil
}

func summaryText(message AgentMessage) string {
	if text, ok := message.Content.(string); ok {
		return text
	}
	return fmt.Sprint(message.Content)
}

func messageUsage(message AgentMessage) *Usage {
	return message.Usage
}

func (h *Harness) compactionAttemptFailure(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, usage *Usage, cause error) (CompactionOutcome, error) {
	gen := current.State.Compaction.Structural.Generation
	if gen == nil || gen.Request == nil {
		return CompactionOutcome{}, fmt.Errorf("compaction request is missing")
	}
	if gen.Attempt < gen.Context.RetryPolicy.MaxAttempts {
		next := current.State
		next.Compaction.Structural.Generation.Status = GenerationRetryWait
		next.Compaction.Structural.Generation.NextAttempt = gen.Attempt + 1
		next.Compaction.Structural.Generation.Request = nil
		next.Compaction.Structural.Generation.UsageIDs = append(append([]string(nil), gen.UsageIDs...), gen.Request.UsageID)
		if gen.Context.RetryPolicy.BaseDelayMs > 0 {
			next.Compaction.Structural.Generation.NotBefore = retryNotBefore(time.Now().UnixMilli(), gen.Context.RetryPolicy.BaseDelayMs)
		}
		writes := []Write{{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: gen.Request.UsageID, Usage: usageValue(usage)}}}, registerSet(RegisterOpState, operation.OperationID, next)}
		if err := h.effect(ctx, ActionInfo{Kind: "settlement", Description: "compaction retry"}, func(ctx context.Context) error {
			return h.line(lane.name).Do(ctx, func() error { _, err := h.session.Commit(ctx, Transaction{Writes: writes}); return err })
		}); err != nil {
			return CompactionOutcome{}, err
		}
		current.State = next
		return h.driveCompaction(ctx, lane, operation, next)
	}
	return h.finishCompaction(ctx, lane, operation, current, nil, "failed", &OperationError{Code: "compaction", Message: cause.Error()}, &SummaryRequest{UsageID: gen.Request.UsageID})
}

func usageValue(usage *Usage) Usage {
	if usage == nil {
		return Usage{}
	}
	return *usage
}

func (h *Harness) finishCompaction(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, result *CompactResult, kind string, errorValue *OperationError, request *SummaryRequest) (CompactionOutcome, error) {
	leaf := current.LeafID
	var entry *Entry
	writes := make([]Write, 0, 12)
	if result != nil {
		if result.Summary == "" {
			return CompactionOutcome{}, fmt.Errorf("compaction result has no summary")
		}
		prepRegister, err := h.session.GetRegister(ctx, RegisterOpPreparation, current.Operation.OperationID+":"+current.State.Compaction.Structural.TaskID)
		if err != nil || prepRegister == nil {
			return CompactionOutcome{}, fmt.Errorf("compaction preparation is missing")
		}
		prep := prepRegister.Value.(DurableStructuralPreparation)
		id := ""
		if current.State.Compaction.Structural.Generation != nil {
			id = current.State.Compaction.Structural.Generation.Context.ResultEntryID
		}
		if id == "" {
			id = h.session.IDGenerator().Next()
		}
		built := &Entry{EntryBase: EntryBase{ID: id, ParentID: cloneStringPointer(current.LeafID), Type: EntryCompaction}, Summary: result.Summary, RetainedTail: cloneValue(prep.RetainedTail).([]AgentMessage), TokensBefore: prep.TokensBefore, Usage: result.Usage, FromHook: request == nil}
		entry = built
		writes = append(writes, Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: *built}}, registerSet(RegisterLaneLeaf, lane.name, stringPointer(id)))
		leaf = stringPointer(id)
		if request != nil {
			writes = append(writes, Write{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: request.UsageID, Usage: usageValue(result.Usage), EntryID: stringPointer(id)}}})
		} else if result.Usage != nil {
			writes = append(writes, Write{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: h.session.IDGenerator().Next(), Usage: *result.Usage, EntryID: stringPointer(id)}}})
		}
	} else if request != nil && request.UsageID != "" {
		writes = append(writes, Write{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: request.UsageID, Usage: Usage{}}}})
	}
	cleanup, err := h.operationCleanupWrites(ctx, operation.OperationID, current.State)
	if err != nil {
		return CompactionOutcome{}, err
	}
	writes = append(writes, cleanup...)
	writes = append(writes, registerSet(RegisterLaneLastResult, lane.name, LaneLastResult{OperationID: operation.OperationID, Kind: OperationCompaction, Outcome: kind, LeafID: leaf, Error: errorValue}))
	laneState := current.LaneState
	laneState.CurrentOperationID = nil
	writes = append(writes, registerSet(RegisterLaneState, lane.name, laneState))
	if err := h.effect(ctx, ActionInfo{Kind: "terminal", Description: "compaction terminal"}, func(ctx context.Context) error {
		return h.line(lane.name).Do(ctx, func() error {
			if register, err := h.session.GetRegister(ctx, RegisterLaneState, lane.name); err != nil {
				return err
			} else if register != nil {
				if latest, ok := register.Value.(LaneState); ok {
					latest.CurrentOperationID = nil
					writes[len(writes)-1] = registerSet(RegisterLaneState, lane.name, latest)
				}
			}
			valid, err := (&runtimeEffects{harness: h, lane: lane}).current(ctx, current)
			if err != nil || !valid {
				if err == nil {
					err = errOperationMissing
				}
				return err
			}
			_, err = h.session.Commit(ctx, Transaction{Writes: writes})
			return err
		})
	}); err != nil {
		if isOperationMissing(err) {
			return CompactionOutcome{}, errOperationMissing
		}
		return CompactionOutcome{}, err
	}
	lane.finish()
	outcome := CompactionOutcome{Kind: kind, LeafID: leaf, Entry: entry, Error: errorValue}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventCompactionEnd), Lane: lane.name, Payload: cloneValue(outcome)})
	return outcome, nil
}

func (h *Harness) runDeferredPoll(ctx context.Context, lane *runtimeLane, operation Operation, deferred Deferred, handle DeferredHandle, options AgentHarnessStreamOptions, fx *runtimeEffects, turnID string) (DeferredResponse, error) {
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "before_payload"}, func(ctx context.Context) error {
		_, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(fmt.Sprintf("%s:deferred:%d:before_payload", operation.OperationID, deferred.Poll)), HookName: HookBeforePayload, Event: map[string]JSONValue{"model": deferred.Configuration.Model, "payload": JSONValue("provider-request"), "turnId": turnID, "deferred": true}})
		return err
	}); err != nil {
		return DeferredResponse{}, terminalGenerationError{err}
	}
	var response DeferredResponse
	if err := h.effect(ctx, ActionInfo{Kind: "provider", Description: "deferred poll"}, func(ctx context.Context) error {
		output, err := fx.Run(ctx, EffectPlan{Kind: EffectDeferred, Key: EffectKey(fmt.Sprintf("%s:deferred:%d", operation.OperationID, deferred.Poll)), Deferred: &deferred, Handle: &handle, Model: deferred.Configuration.Model, StreamOptions: options})
		if output.Deferred != nil {
			response = *output.Deferred
		}
		return err
	}); err != nil {
		return DeferredResponse{}, err
	}
	if response.Kind == "" {
		return DeferredResponse{}, fmt.Errorf("deferred provider returned no response")
	}
	if response.Message == nil {
		message := deferredResponseMessage(response, handle, StopReason(response.Kind))
		response.Message = &message
	}
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "after_response"}, func(ctx context.Context) error {
		output, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(fmt.Sprintf("%s:deferred:%d:after_response", operation.OperationID, deferred.Poll)), HookName: HookAfterResponse, Event: map[string]JSONValue{"message": *response.Message, "turnId": turnID, "deferred": true}})
		if values, ok := output.Result.(map[string]JSONValue); ok {
			if replacement, ok := values["message"].(AgentMessage); ok {
				response.Message = &replacement
			}
		}
		return err
	}); err != nil {
		return DeferredResponse{}, terminalGenerationError{err}
	}
	return response, nil
}

func (h *Harness) driveRunCompaction(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation) (RunOutcome, error) {
	run := current.State.Run
	if run == nil || run.Phase.Structural == nil {
		return RunOutcome{}, fmt.Errorf("run compaction state is missing")
	}
	structural := run.Phase.Structural
	prepRegister, err := h.session.GetRegister(ctx, RegisterOpPreparation, operation.OperationID+":"+structural.TaskID)
	if err != nil || prepRegister == nil {
		if err == nil {
			err = fmt.Errorf("run compaction preparation is missing")
		}
		return RunOutcome{}, err
	}
	prep, ok := prepRegister.Value.(DurableStructuralPreparation)
	if !ok || prep.Kind != EntryCompaction {
		return RunOutcome{}, fmt.Errorf("run compaction preparation has invalid type")
	}
	if run.Control.Status == ControlCancelRequested {
		return h.finishOutcome(ctx, lane, operation, current, RunOutcome{Kind: "aborted", RunID: operation.OperationID, LeafID: current.LeafID, FinalEntryID: run.LatestAssistantEntryID, Reason: "cancelled"})
	}
	if structural.Status == "deciding" {
		decision, err := h.runCompactionHookReason(ctx, lane, operation, prep, operation.Intent.CustomInstructions, string(run.Phase.Reason))
		if err != nil {
			return h.settleRunCompactionFailure(ctx, lane, operation, current, err)
		}
		if decision.decline {
			if run.Phase.Reason == "threshold" {
				return h.resumeRunAfterCompaction(ctx, lane, operation, current)
			}
			return h.settleRunCompactionFailure(ctx, lane, operation, current, fmt.Errorf("overflow compaction was declined"))
		}
		if decision.result != nil {
			return h.settleRunCompactionResult(ctx, lane, operation, current, prep, *decision.result, nil)
		}
		settings := h.settings.Snapshot()
		resultID := h.session.IDGenerator().Next()
		next := current.State
		next.Run.Phase.Structural.Status = "generating"
		next.Run.Phase.Structural.Generation = &SummaryGeneration{Status: GenerationReady, NextAttempt: 1, Context: SummaryContext{TaskID: structural.TaskID, ResultEntryID: resultID, Kind: EntryCompaction, Configuration: current.Configuration, StreamOptions: settings.StreamOptions, RetryPolicy: settings.RetryPolicy, Reason: run.Phase.Reason}, UsageIDs: []string{}}
		var transition *CurrentOperation
		fx := &runtimeEffects{harness: h, lane: lane}
		if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "run compaction generating"}, func(ctx context.Context) error {
			var err error
			transition, err = fx.CommitTransition(ctx, current, next, h.telemetry, &current.ConfigurationSeq, nil)
			return err
		}); err != nil {
			return RunOutcome{}, err
		}
		if transition == nil {
			return RunOutcome{}, fmt.Errorf("run compaction decision lost its compare-and-swap")
		}
		return h.driveRunCompaction(ctx, lane, operation, *transition)
	}
	if structural.Status != "generating" || structural.Generation == nil {
		return RunOutcome{}, fmt.Errorf("unsupported run compaction status %q", structural.Status)
	}
	generation := *structural.Generation
	if generation.Status == GenerationEffectPending {
		if generation.Attempt >= generation.Context.RetryPolicy.MaxAttempts {
			return h.settleRunCompactionFailure(ctx, lane, operation, current, fmt.Errorf("recovered overflow compaction reached retry cap"))
		}
		next := current.State
		next.Run.Phase.Structural.Generation.Status = GenerationReady
		next.Run.Phase.Structural.Generation.NextAttempt = generation.Attempt + 1
		next.Run.Phase.Structural.Generation.Attempt = 0
		next.Run.Phase.Structural.Generation.Request = nil
		if err := h.effect(ctx, ActionInfo{Kind: "recovery", Description: "recover run compaction"}, func(ctx context.Context) error {
			return h.line(lane.name).Do(ctx, func() error {
				_, err := h.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpState, operation.OperationID, next)}})
				return err
			})
		}); err != nil {
			return RunOutcome{}, err
		}
		return h.driveRunCompaction(ctx, lane, operation, nextCurrent(current, next))
	}
	if generation.Status != GenerationReady {
		return RunOutcome{}, fmt.Errorf("unsupported run compaction generation status %q", generation.Status)
	}
	attempt := generation.NextAttempt
	if attempt == 0 {
		attempt = generation.Attempt + 1
	}
	options := generation.Context.StreamOptions
	options.Deferred = false
	fx := &runtimeEffects{harness: h, lane: lane}
	if err := h.runBeforeRequestStep(ctx, fx, operation, attempt, "compaction", fmt.Sprintf("%s:attempt:%d", generation.Context.TaskID, attempt), &options); err != nil {
		return h.settleRunCompactionFailure(ctx, lane, operation, current, err)
	}
	options.Deferred = false
	usageID := h.session.IDGenerator().Next()
	next := current.State
	next.Run.Phase.Structural.Generation.Status = GenerationEffectPending
	next.Run.Phase.Structural.Generation.Attempt = attempt
	next.Run.Phase.Structural.Generation.NextAttempt = attempt
	next.Run.Phase.Structural.Generation.Request = &SummaryRequest{Index: len(generation.UsageIDs), UsageID: usageID}
	var transition *CurrentOperation
	if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "run compaction request pending"}, func(ctx context.Context) error {
		var err error
		transition, err = fx.CommitTransition(ctx, current, next, h.telemetry, nil, nil)
		return err
	}); err != nil {
		return RunOutcome{}, err
	}
	if transition == nil {
		return RunOutcome{}, fmt.Errorf("run compaction request lost its compare-and-swap")
	}
	current = *transition
	generation = *current.State.Run.Phase.Structural.Generation
	message, err := h.runSummaryAttempt(ctx, lane, operation, prep, generation, options, fx)
	if err != nil {
		latest, reloadErr := h.currentOperation(ctx, lane)
		if reloadErr == nil && latest.State.Run != nil && latest.State.Run.Control.Status == ControlCancelRequested {
			return h.finishOutcome(ctx, lane, operation, latest, RunOutcome{Kind: "aborted", RunID: operation.OperationID, LeafID: latest.LeafID, FinalEntryID: latest.State.Run.LatestAssistantEntryID, Reason: "cancelled"})
		}
		if isOperationMissing(reloadErr) {
			return h.resolveExternalFinalization(ctx, lane, operation)
		}
		return h.settleRunCompactionAttemptFailure(ctx, lane, operation, current, generation, err)
	}
	latest, reloadErr := h.currentOperation(ctx, lane)
	if reloadErr == nil && latest.State.Run != nil && latest.State.Run.Control.Status == ControlCancelRequested {
		return h.finishOutcome(ctx, lane, operation, latest, RunOutcome{Kind: "aborted", RunID: operation.OperationID, LeafID: latest.LeafID, FinalEntryID: latest.State.Run.LatestAssistantEntryID, Reason: "cancelled"})
	}
	if isOperationMissing(reloadErr) {
		return h.resolveExternalFinalization(ctx, lane, operation)
	}
	return h.settleRunCompactionResult(ctx, lane, operation, current, prep, CompactResult{Summary: summaryText(message), Usage: message.Usage}, generation.Request)
}

func (h *Harness) settleRunCompactionAttemptFailure(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, generation SummaryGeneration, cause error) (RunOutcome, error) {
	if cause == nil {
		cause = fmt.Errorf("compaction failed")
	}
	if generation.Request == nil {
		return h.settleRunCompactionFailure(ctx, lane, operation, current, cause)
	}
	next := current.State
	if generation.Attempt < generation.Context.RetryPolicy.MaxAttempts {
		next.Run.Phase.Structural.Generation.Status = GenerationRetryWait
		next.Run.Phase.Structural.Generation.NextAttempt = generation.Attempt + 1
		next.Run.Phase.Structural.Generation.NotBefore = retryNotBefore(time.Now().UnixMilli(), retryDelay(generation.Context.RetryPolicy, generation.Context.StreamOptions, generation.Attempt))
		next.Run.Phase.Structural.Generation.Request = nil
		next.Run.Phase.Structural.Generation.UsageIDs = append(append([]string(nil), generation.UsageIDs...), generation.Request.UsageID)
	} else {
		next.Run.Phase = RunPhase{Kind: PhaseFailureDrain, Error: &OperationError{Code: "compaction", Message: cause.Error()}, Provenance: &FailureProvenance{Kind: "structural", TaskID: current.State.Run.Phase.Structural.TaskID}}
	}
	writes := []Write{
		{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: generation.Request.UsageID, Usage: Usage{}}}},
		registerSet(RegisterOpState, operation.OperationID, next),
	}
	if err := h.effect(ctx, ActionInfo{Kind: "settlement", Description: "run compaction failure"}, func(ctx context.Context) error {
		return h.line(lane.name).Do(ctx, func() error {
			valid, err := (&runtimeEffects{harness: h, lane: lane}).current(ctx, current)
			if err != nil || !valid {
				if err == nil {
					err = errOperationMissing
				}
				return err
			}
			_, err = h.session.Commit(ctx, Transaction{Writes: writes})
			return err
		})
	}); err != nil {
		return RunOutcome{}, err
	}
	current.State = next
	return h.drive(ctx, lane, operation, next)
}

func nextCurrent(current CurrentOperation, state OperationState) CurrentOperation {
	current.State = state
	return current
}

func (h *Harness) resumeRunAfterCompaction(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation) (RunOutcome, error) {
	resume := current.State.Run.Phase.ResumeAfter
	if resume == nil {
		return h.settleRunCompactionFailure(ctx, lane, operation, current, fmt.Errorf("compaction has no resume checkpoint"))
	}
	next := current.State
	next.Run.Phase = RunPhase{Kind: PhaseCheckpoint, Checkpoint: resume}
	if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "restore checkpoint after compaction"}, func(ctx context.Context) error {
		return h.line(lane.name).Do(ctx, func() error {
			_, err := h.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpState, operation.OperationID, next)}})
			return err
		})
	}); err != nil {
		return RunOutcome{}, err
	}
	return h.drive(ctx, lane, operation, next)
}

func (h *Harness) settleRunCompactionResult(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, prep DurableStructuralPreparation, result CompactResult, request *SummaryRequest) (RunOutcome, error) {
	if result.Summary == "" {
		return h.settleRunCompactionFailure(ctx, lane, operation, current, fmt.Errorf("compaction result has no summary"))
	}
	id := current.State.Run.Phase.Structural.Generation.Context.ResultEntryID
	entry := Entry{EntryBase: EntryBase{ID: id, ParentID: cloneStringPointer(current.LeafID), Type: EntryCompaction}, Summary: result.Summary, RetainedTail: cloneValue(prep.RetainedTail).([]AgentMessage), TokensBefore: prep.TokensBefore, Usage: result.Usage, FromHook: request == nil}
	next := current.State
	next.Run.Phase = RunPhase{Kind: PhaseCheckpoint, Checkpoint: current.State.Run.Phase.ResumeAfter}
	writes := []Write{{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}}, registerSet(RegisterLaneLeaf, lane.name, stringPointer(id))}
	if request != nil {
		writes = append(writes, Write{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: request.UsageID, Usage: usageValue(result.Usage), EntryID: stringPointer(id)}}})
	} else if result.Usage != nil {
		writes = append(writes, Write{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: h.session.IDGenerator().Next(), Usage: *result.Usage, EntryID: stringPointer(id)}}})
	}
	writes = append(writes, registerSet(RegisterOpState, operation.OperationID, next))
	if err := h.effect(ctx, ActionInfo{Kind: "settlement", Description: "run compaction result"}, func(ctx context.Context) error {
		return h.line(lane.name).Do(ctx, func() error {
			valid, err := (&runtimeEffects{harness: h, lane: lane}).current(ctx, current)
			if err != nil || !valid {
				if err == nil {
					err = errOperationMissing
				}
				return err
			}
			_, err = h.session.Commit(ctx, Transaction{Writes: writes})
			return err
		})
	}); err != nil {
		return RunOutcome{}, err
	}
	current.State = next
	current.LeafID = stringPointer(id)
	h.events.Emit(ctx, HarnessEvent{Type: string(EventEntryAdded), Lane: lane.name, Payload: cloneValue(entry)})
	h.events.Emit(ctx, HarnessEvent{Type: string(EventUsage), Lane: lane.name, Payload: cloneValue(usageValue(result.Usage))})
	return h.drive(ctx, lane, operation, next)
}

func (h *Harness) settleRunCompactionFailure(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, cause error) (RunOutcome, error) {
	if cause == nil {
		cause = fmt.Errorf("compaction failed")
	}
	next := current.State
	next.Run.Phase = RunPhase{Kind: PhaseFailureDrain, Error: &OperationError{Code: "compaction", Message: cause.Error()}, Provenance: &FailureProvenance{Kind: "structural", TaskID: current.State.Run.Phase.Structural.TaskID}}
	if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "compaction failure drain"}, func(ctx context.Context) error {
		return h.line(lane.name).Do(ctx, func() error {
			_, err := h.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpState, operation.OperationID, next)}})
			return err
		})
	}); err != nil {
		return RunOutcome{}, err
	}
	return h.drive(ctx, lane, operation, next)
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
		deferred := *phase.Deferred
		h.cancelDeferredSource(ctx, lane, deferred)
		if deferred.Status == DeferredEffectPending {
			generation := deferredGeneration(deferred, deferred.ResponseEntryID, deferred.UsageID)
			return h.settleAborted(ctx, lane, operation, current, generation, AgentMessage{Role: "assistant", Content: "deferred request aborted"})
		}
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
	return h.runBeforeRequestStep(ctx, fx, operation, attempt, "assistant", "", options)
}

func (h *Harness) runBeforeRequestStep(ctx context.Context, fx *runtimeEffects, operation Operation, attempt int64, step, turnID string, options *AgentHarnessStreamOptions) error {
	key := fmt.Sprintf("%s:before_request:%d", operation.OperationID, attempt)
	if step != "assistant" {
		key = fmt.Sprintf("%s:%s:before_request:%d", operation.OperationID, step, attempt)
	}
	return h.effect(ctx, ActionInfo{Kind: "hook", Description: "before_request"}, func(ctx context.Context) error {
		event := map[string]JSONValue{"step": step, "attempt": attempt, "streamOptions": *options}
		if turnID != "" {
			event["turnId"] = turnID
		}
		output, err := fx.Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(key), HookName: HookBeforeRequest, Event: event})
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
	h.configurationMu.RLock()
	defer h.configurationMu.RUnlock()
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

func (h *Harness) toolsSnapshot() []AgentHarnessTool {
	h.configurationMu.RLock()
	defer h.configurationMu.RUnlock()
	return append([]AgentHarnessTool(nil), h.tools...)
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

func (h *Harness) runAssistantAttempt(ctx context.Context, lane *runtimeLane, operation Operation, pending OperationState, leaf *string, fx *runtimeEffects) (AgentMessage, error) {
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
		if leaf == nil {
			return fmt.Errorf("assistant context has no leaf")
		}
		entries, err := lane.view.FindEntriesOnBranch(ctx, BranchScan{Start: *leaf, Order: NewestFirst})
		if err != nil {
			return err
		}
		messages, err := ProjectContext(ctx, entries, h.entryProjectors)
		if err != nil {
			return err
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
	entry := Entry{EntryBase: EntryBase{ID: responseID, ParentID: cloneStringPointer(current.LeafID), Type: EntryMessage}, Message: messageCopy(message)}
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
	case EffectDeferred:
		if plan.Handle == nil {
			return EffectOutput{}, fmt.Errorf("deferred effect has no handle")
		}
		effectCtx, stop := e.lane.cancellableEffectContext(ctx)
		defer stop()
		if _, err := e.harness.models.Resolve(effectCtx, plan.Model.Provider, plan.Model.ModelID); err != nil {
			return EffectOutput{}, err
		}
		response, err := e.harness.models.FetchDeferred(effectCtx, plan.Model, *plan.Handle, plan.StreamOptions)
		return EffectOutput{Kind: "deferred", Key: plan.Key, Deferred: &response}, err
	case EffectSummary:
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
			return EffectOutput{}, fmt.Errorf("summary provider returned a nil stream")
		}
		var message *AgentMessage
		for event := range stream {
			if event.Message != nil {
				copy := *event.Message
				message = &copy
			}
		}
		if message == nil {
			return EffectOutput{}, fmt.Errorf("summary provider returned no message")
		}
		return EffectOutput{Kind: "summary", Key: plan.Key, Message: message}, nil
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
	for _, tool := range e.harness.toolsSnapshot() {
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

func (l *runtimeLane) Skill(ctx context.Context, name, additional string) (Result[RunOutcome, error], error) {
	resources, err := l.harness.GetResources(ctx)
	if err != nil {
		return Result[RunOutcome, error]{Err: err}, nil
	}
	for _, skill := range resources.Skills {
		if skill.Name == name {
			text := skill.Content
			if additional != "" {
				text += "\n\n" + additional
			}
			return l.Prompt(ctx, PromptInput{Text: text})
		}
	}
	return Err[RunOutcome, error](&UnknownSkill{TaggedError: TaggedError{Message: "unknown skill"}, Name: name}), nil
}
func (l *runtimeLane) PromptFromTemplate(ctx context.Context, name string, args []string) (Result[RunOutcome, error], error) {
	resources, err := l.harness.GetResources(ctx)
	if err != nil {
		return Result[RunOutcome, error]{Err: err}, nil
	}
	for _, template := range resources.PromptTemplates {
		if template.Name == name {
			text := template.Content
			for i, arg := range args {
				text = strings.ReplaceAll(text, fmt.Sprintf("{{%d}}", i), arg)
			}
			return l.Prompt(ctx, PromptInput{Text: text})
		}
	}
	return Err[RunOutcome, error](&UnknownTemplate{TaggedError: TaggedError{Message: "unknown prompt template"}, Name: name}), nil
}
func (l *runtimeLane) Compact(ctx context.Context, customInstructions string) (Result[CompactionOutcome, error], error) {
	if err := l.harness.lifecycle.Err(); err != nil {
		return Result[CompactionOutcome, error]{Err: err}, nil
	}
	var operation Operation
	var state OperationState
	var accepted bool
	err := l.harness.line(l.name).Do(ctx, func() error {
		laneStateRegister, err := l.harness.session.GetRegister(ctx, RegisterLaneState, l.name)
		if err != nil || laneStateRegister == nil {
			return err
		}
		laneState, ok := laneStateRegister.Value.(LaneState)
		if !ok {
			return fmt.Errorf("lane state has invalid type")
		}
		if laneState.CurrentOperationID != nil {
			kind := OperationRun
			if register, err := l.harness.session.GetRegister(ctx, RegisterOpMeta, *laneState.CurrentOperationID); err == nil && register != nil {
				if operation, ok := register.Value.(Operation); ok {
					kind = operation.Intent.Kind
				}
			}
			return &LaneBusy{TaggedError: TaggedError{Message: "lane is busy"}, Lane: l.name, OperationID: *laneState.CurrentOperationID, OperationKind: kind}
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
		if leaf == nil {
			return &NothingToCompact{TaggedError: TaggedError{Message: "nothing to compact"}, Lane: l.name}
		}
		entries, err := l.view.FindEntriesOnBranch(ctx, BranchScan{Start: *leaf, Order: OldestFirst})
		if err != nil {
			return err
		}
		messages := make([]AgentMessage, 0, len(entries))
		previousSummary := ""
		for _, entry := range entries {
			if entry.Type == EntryCompaction {
				messages = nil
				previousSummary = entry.Summary
				continue
			}
			if entry.Message != nil && includeInContext(*entry.Message) {
				messages = append(messages, *messageCopy(*entry.Message))
			}
		}
		if len(messages) < 2 {
			return &NothingToCompact{TaggedError: TaggedError{Message: "nothing to compact"}, Lane: l.name}
		}
		retained := []AgentMessage{messages[len(messages)-1]}
		toSummarize := append([]AgentMessage(nil), messages[:len(messages)-1]...)
		settings, err := l.harness.GetCompactionSettings(ctx)
		if err != nil {
			return err
		}
		configRegister, err := l.harness.session.GetRegister(ctx, RegisterLaneConfig, l.name)
		if err != nil || configRegister == nil {
			return err
		}
		config, ok := configRegister.Value.(LaneConfiguration)
		if !ok {
			return fmt.Errorf("lane configuration has invalid type")
		}
		operationID := l.harness.session.IDGenerator().Next()
		taskID := "task:1"
		prep := DurableStructuralPreparation{Kind: EntryCompaction, MessagesToSummarize: toSummarize, RetainedTail: retained, TokensBefore: int64(len(messages)), PreviousSummary: previousSummary, Settings: settings}
		operation = Operation{OperationID: operationID, Lane: l.name, SourceLeafID: cloneStringPointer(leaf), StartedAt: time.Now().UnixMilli(), Intent: OperationIntent{Kind: OperationCompaction, CustomInstructions: customInstructions}}
		state = OperationState{Kind: OperationCompaction, Compaction: &CompactionState{Kind: OperationCompaction, Control: Control{Status: ControlRunning}, CustomInstructions: customInstructions, Structural: StructuralDecision{TaskID: taskID, Status: "deciding"}}}
		laneState.CurrentOperationID = &operationID
		_, err = l.harness.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpMeta, operationID, operation), registerSet(RegisterOpPreparation, operationID+":"+taskID, prep), registerSet(RegisterOpState, operationID, state), registerSet(RegisterLaneState, l.name, laneState)}})
		accepted = err == nil
		_ = config
		return err
	})
	if err != nil {
		return Result[CompactionOutcome, error]{Err: err}, nil
	}
	if !accepted {
		return Result[CompactionOutcome, error]{Err: fmt.Errorf("compaction was not accepted")}, nil
	}
	l.begin()
	l.harness.events.Emit(ctx, HarnessEvent{Type: string(EventCompactionStart), Lane: l.name, Payload: map[string]JSONValue{"runId": operation.OperationID}})
	var outcome CompactionOutcome
	err = l.harness.operationSpan(ctx, SpanHarnessCompaction, l.name, operation.OperationID, func() error {
		var driveErr error
		outcome, driveErr = l.harness.driveCompaction(ctx, l, operation, state)
		return driveErr
	})
	if err != nil {
		return Result[CompactionOutcome, error]{Err: err}, nil
	}
	return Ok[CompactionOutcome, error](outcome), nil
}
func (l *runtimeLane) NavigateTree(ctx context.Context, targetID *string, options NavigateOptions) (Result[NavigationOutcome, error], error) {
	if err := l.harness.lifecycle.Err(); err != nil {
		return Result[NavigationOutcome, error]{Err: err}, nil
	}
	var operation Operation
	var state OperationState
	var accepted bool
	err := l.harness.line(l.name).Do(ctx, func() error {
		laneRegister, err := l.harness.session.GetRegister(ctx, RegisterLaneState, l.name)
		if err != nil || laneRegister == nil {
			return err
		}
		laneState, ok := laneRegister.Value.(LaneState)
		if !ok {
			return fmt.Errorf("lane state has invalid type")
		}
		if laneState.CurrentOperationID != nil {
			kind := OperationRun
			if meta, metaErr := l.harness.session.GetRegister(ctx, RegisterOpMeta, *laneState.CurrentOperationID); metaErr == nil && meta != nil {
				if operation, metaOK := meta.Value.(Operation); metaOK {
					kind = operation.Intent.Kind
				}
			}
			return &LaneBusy{TaggedError: TaggedError{Message: "lane is busy"}, Lane: l.name, OperationID: *laneState.CurrentOperationID, OperationKind: kind}
		}
		leafRegister, err := l.harness.session.GetRegister(ctx, RegisterLaneLeaf, l.name)
		if err != nil {
			return err
		}
		var source *string
		if leafRegister != nil {
			source, ok = leafRegister.Value.(*string)
			if !ok {
				return fmt.Errorf("lane leaf has invalid type")
			}
		}
		if targetID != nil && source != nil && *targetID == *source {
			return &InvalidNavigation{TaggedError: TaggedError{Message: "target is current leaf"}, Lane: l.name, Reason: "target is current leaf"}
		}
		var target *Entry
		if targetID != nil {
			target, err = l.view.GetEntry(ctx, *targetID)
			if err != nil {
				return err
			}
			if target == nil {
				return &UnknownTarget{TaggedError: TaggedError{Message: "unknown navigation target"}, TargetID: *targetID}
			}
			if target.ParentID == nil {
				return &InvalidNavigation{TaggedError: TaggedError{Message: "cannot navigate to root entry"}, Lane: l.name, Reason: "target is root"}
			}
		} else if options.Summarize {
			return &InvalidNavigation{TaggedError: TaggedError{Message: "summarized navigation requires a target"}, Lane: l.name, Reason: "null target cannot be summarized"}
		}
		if options.Label != "" && (targetID == nil || target.ParentID == nil) {
			return &InvalidNavigation{TaggedError: TaggedError{Message: "cannot label root"}, Lane: l.name, Reason: "label on root target"}
		}
		if options.Summarize && source == nil {
			return &InvalidNavigation{TaggedError: TaggedError{Message: "cannot summarize from root"}, Lane: l.name, Reason: "summarize from root"}
		}
		configRegister, err := l.harness.session.GetRegister(ctx, RegisterLaneConfig, l.name)
		if err != nil || configRegister == nil {
			return err
		}
		config, ok := configRegister.Value.(LaneConfiguration)
		if !ok {
			return fmt.Errorf("lane configuration has invalid type")
		}
		if options.Summarize {
			if missing := l.missingIdentities(ctx, config); missing != nil {
				return missing
			}
		}
		operationID := l.harness.session.IDGenerator().Next()
		taskID := "task:1"
		operation = Operation{OperationID: operationID, Lane: l.name, SourceLeafID: cloneStringPointer(source), StartedAt: time.Now().UnixMilli(), Intent: OperationIntent{Kind: OperationNavigation, TargetID: cloneStringPointer(targetID), Summarize: options.Summarize, Label: options.Label, CustomInstructions: options.CustomInstructions}}
		state = OperationState{Kind: OperationNavigation, Navigation: &NavigationState{Kind: OperationNavigation, Control: Control{Status: ControlRunning}, TargetID: cloneStringPointer(targetID), Label: options.Label, CustomInstructions: options.CustomInstructions, Summarize: options.Summarize, Phase: NavigationPhase{Kind: "ready_to_commit"}}}
		writes := []Write{}
		if options.Summarize {
			entries, err := l.view.FindEntriesOnBranch(ctx, BranchScan{Start: *source, Order: NewestFirst})
			if err != nil {
				return err
			}
			messages, err := ProjectContext(ctx, entries, l.harness.entryProjectors)
			if err != nil {
				return err
			}
			if len(messages) == 0 {
				return &InvalidNavigation{TaggedError: TaggedError{Message: "nothing to summarize"}, Lane: l.name, Reason: "empty source branch"}
			}
			prep := DurableStructuralPreparation{Kind: EntryBranchSummary, Messages: messages, TotalTokens: int64(len(messages))}
			state.Navigation.Phase = NavigationPhase{Kind: "summary", Structural: &StructuralDecision{TaskID: taskID, Status: "deciding"}}
			writes = append(writes, registerSet(RegisterOpPreparation, operationID+":"+taskID, prep))
		}
		laneState.CurrentOperationID = &operationID
		writes = append(writes, registerSet(RegisterOpMeta, operationID, operation), registerSet(RegisterOpState, operationID, state), registerSet(RegisterLaneState, l.name, laneState))
		_, err = l.harness.session.Commit(ctx, Transaction{Writes: writes})
		accepted = err == nil
		return err
	})
	if err != nil {
		return Result[NavigationOutcome, error]{Err: err}, nil
	}
	if !accepted {
		return Result[NavigationOutcome, error]{Err: fmt.Errorf("navigation was not accepted")}, nil
	}
	l.begin()
	l.harness.events.Emit(ctx, HarnessEvent{Type: string(EventNavigationStart), Lane: l.name, Payload: map[string]JSONValue{"runId": operation.OperationID}})
	var outcome NavigationOutcome
	err = l.harness.operationSpan(ctx, SpanHarnessNavigation, l.name, operation.OperationID, func() error {
		var driveErr error
		outcome, driveErr = l.harness.driveNavigation(ctx, l, operation, state)
		return driveErr
	})
	if err != nil {
		return Result[NavigationOutcome, error]{Err: err}, nil
	}
	return Ok[NavigationOutcome, error](outcome), nil
}

func (h *Harness) driveNavigation(ctx context.Context, lane *runtimeLane, operation Operation, state OperationState) (NavigationOutcome, error) {
	current, err := h.currentOperation(ctx, lane)
	if err != nil {
		return NavigationOutcome{}, err
	}
	for {
		navigation := current.State.Navigation
		if navigation == nil {
			return NavigationOutcome{}, fmt.Errorf("navigation state is missing")
		}
		if navigation.Control.Status == ControlCancelRequested {
			return h.finishNavigation(ctx, lane, operation, current, nil, "aborted", nil)
		}
		if navigation.Phase.Kind == "ready_to_commit" {
			return h.finishNavigation(ctx, lane, operation, current, nil, "completed", nil)
		}
		if navigation.Phase.Structural == nil || navigation.Phase.Kind != "summary" {
			return NavigationOutcome{}, fmt.Errorf("unsupported navigation phase %q", navigation.Phase.Kind)
		}
		register, err := h.session.GetRegister(ctx, RegisterOpPreparation, operation.OperationID+":"+navigation.Phase.Structural.TaskID)
		if err != nil || register == nil {
			if err == nil {
				err = fmt.Errorf("navigation preparation is missing")
			}
			return NavigationOutcome{}, err
		}
		prep, ok := register.Value.(DurableStructuralPreparation)
		if !ok || prep.Kind != EntryBranchSummary {
			return NavigationOutcome{}, fmt.Errorf("navigation preparation has invalid type")
		}
		structural := navigation.Phase.Structural
		if structural.Status == "deciding" {
			decision, err := h.runNavigationHook(ctx, lane, operation, prep)
			if err != nil {
				return h.finishNavigation(ctx, lane, operation, current, nil, "failed", &OperationError{Code: "navigation", Message: err.Error()})
			}
			if decision.decline {
				return h.finishNavigation(ctx, lane, operation, current, nil, "declined", nil)
			}
			if decision.result != nil {
				return h.finishNavigation(ctx, lane, operation, current, decision.result, "completed", nil)
			}
			settings := h.settings.Snapshot()
			resultID := h.session.IDGenerator().Next()
			next := current.State
			next.Navigation.Phase.Structural.Status = "generating"
			next.Navigation.Phase.Structural.Generation = &SummaryGeneration{Status: GenerationReady, NextAttempt: 1, Context: SummaryContext{TaskID: structural.TaskID, ResultEntryID: resultID, Kind: EntryBranchSummary, Configuration: current.Configuration, StreamOptions: settings.StreamOptions, RetryPolicy: settings.RetryPolicy, Reason: "navigation"}, UsageIDs: []string{}}
			var transition *CurrentOperation
			fx := &runtimeEffects{harness: h, lane: lane}
			if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "navigation summary generating"}, func(ctx context.Context) error {
				var err error
				transition, err = fx.CommitTransition(ctx, current, next, h.telemetry, &current.ConfigurationSeq, nil)
				return err
			}); err != nil {
				return NavigationOutcome{}, err
			}
			if transition == nil {
				return NavigationOutcome{}, fmt.Errorf("navigation decision lost its compare-and-swap")
			}
			current = *transition
			continue
		}
		if structural.Status != "generating" || structural.Generation == nil {
			return NavigationOutcome{}, fmt.Errorf("unsupported navigation structural status %q", structural.Status)
		}
		generation := *structural.Generation
		if generation.Status == GenerationRetryWait {
			if delay := generation.NotBefore - time.Now().UnixMilli(); delay > 0 {
				if err := h.effect(ctx, ActionInfo{Kind: "wait", Description: "navigation summary retry wait"}, func(ctx context.Context) error {
					return (&runtimeEffects{harness: h, lane: lane}).Sleep(ctx, delay, h.telemetry)
				}); err != nil {
					return NavigationOutcome{}, err
				}
			}
			next := current.State
			next.Navigation.Phase.Structural.Generation.Status = GenerationReady
			if err := h.commitState(ctx, lane, operation.OperationID, next); err != nil {
				return NavigationOutcome{}, err
			}
			current, err = h.currentOperation(ctx, lane)
			if err != nil {
				return NavigationOutcome{}, err
			}
			continue
		}
		if generation.Status == GenerationEffectPending {
			next := current.State
			next.Navigation.Phase.Structural.Generation.Status = GenerationReady
			next.Navigation.Phase.Structural.Generation.NextAttempt = generation.Attempt + 1
			next.Navigation.Phase.Structural.Generation.Attempt = 0
			next.Navigation.Phase.Structural.Generation.Request = nil
			if err := h.commitState(ctx, lane, operation.OperationID, next); err != nil {
				return NavigationOutcome{}, err
			}
			current, err = h.currentOperation(ctx, lane)
			if err != nil {
				return NavigationOutcome{}, err
			}
			continue
		}
		if generation.Status != GenerationReady {
			return NavigationOutcome{}, fmt.Errorf("unsupported navigation generation status %q", generation.Status)
		}
		attempt := generation.NextAttempt
		if attempt == 0 {
			attempt = generation.Attempt + 1
		}
		options := generation.Context.StreamOptions
		options.Deferred = false
		fx := &runtimeEffects{harness: h, lane: lane}
		if err := h.runBeforeRequestStep(ctx, fx, operation, attempt, "navigation", fmt.Sprintf("%s:attempt:%d", generation.Context.TaskID, attempt), &options); err != nil {
			return h.finishNavigation(ctx, lane, operation, current, nil, "failed", &OperationError{Code: "navigation", Message: err.Error()})
		}
		usageID := h.session.IDGenerator().Next()
		next := current.State
		next.Navigation.Phase.Structural.Generation.Status = GenerationEffectPending
		next.Navigation.Phase.Structural.Generation.Attempt = attempt
		next.Navigation.Phase.Structural.Generation.NextAttempt = attempt
		next.Navigation.Phase.Structural.Generation.Request = &SummaryRequest{Index: len(generation.UsageIDs), UsageID: usageID}
		var transition *CurrentOperation
		if err := h.effect(ctx, ActionInfo{Kind: "transition", Description: "navigation summary request pending"}, func(ctx context.Context) error {
			var err error
			transition, err = fx.CommitTransition(ctx, current, next, h.telemetry, nil, nil)
			return err
		}); err != nil {
			return NavigationOutcome{}, err
		}
		if transition == nil {
			return NavigationOutcome{}, fmt.Errorf("navigation summary request lost its compare-and-swap")
		}
		current = *transition
		generation = *current.State.Navigation.Phase.Structural.Generation
		message, err := h.runSummaryAttempt(ctx, lane, operation, prep, generation, options, fx)
		if err != nil {
			if generation.Attempt < generation.Context.RetryPolicy.MaxAttempts {
				next := current.State
				next.Navigation.Phase.Structural.Generation.Status = GenerationRetryWait
				next.Navigation.Phase.Structural.Generation.NextAttempt = generation.Attempt + 1
				next.Navigation.Phase.Structural.Generation.NotBefore = retryNotBefore(time.Now().UnixMilli(), retryDelay(generation.Context.RetryPolicy, generation.Context.StreamOptions, generation.Attempt))
				next.Navigation.Phase.Structural.Generation.Request = nil
				next.Navigation.Phase.Structural.Generation.UsageIDs = append(append([]string(nil), generation.UsageIDs...), generation.Request.UsageID)
				if err := h.line(lane.name).Do(ctx, func() error {
					_, err := h.session.Commit(ctx, Transaction{Writes: []Write{{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: generation.Request.UsageID, Usage: Usage{}}}}, registerSet(RegisterOpState, operation.OperationID, next)}})
					return err
				}); err != nil {
					return NavigationOutcome{}, err
				}
				current, err = h.currentOperation(ctx, lane)
				if err != nil {
					return NavigationOutcome{}, err
				}
				continue
			}
			return h.finishNavigation(ctx, lane, operation, current, nil, "failed", &OperationError{Code: "navigation", Message: err.Error()})
		}
		return h.finishNavigation(ctx, lane, operation, current, &BranchSummaryResult{Summary: summaryText(message), Usage: message.Usage}, "completed", nil)
	}
}

type navigationDecision struct {
	decline bool
	result  *BranchSummaryResult
}

func (h *Harness) runNavigationHook(ctx context.Context, lane *runtimeLane, operation Operation, preparation DurableStructuralPreparation) (navigationDecision, error) {
	var result JSONValue
	if err := h.effect(ctx, ActionInfo{Kind: "hook", Description: "before_navigation"}, func(ctx context.Context) error {
		var err error
		result, err = h.hooks.Run(ctx, HookInvocation{Name: HookBeforeNavigation, Lane: lane.name, RunID: operation.OperationID, Event: map[string]JSONValue{"targetId": stringValue(operation.Intent.TargetID), "preparation": BranchPreparation{Messages: cloneValue(preparation.Messages).([]AgentMessage), TotalTokens: preparation.TotalTokens}, "customInstructions": operation.Intent.CustomInstructions}})
		return err
	}); err != nil {
		return navigationDecision{}, err
	}
	values, _ := result.(map[string]JSONValue)
	if values == nil {
		return navigationDecision{}, nil
	}
	decision := navigationDecision{}
	decision.decline, _ = values["decline"].(bool)
	if summary, ok := values["summary"].(BranchSummaryResult); ok {
		decision.result = &summary
	}
	if decision.decline && decision.result != nil {
		return navigationDecision{}, nil
	}
	return decision, nil
}

func (h *Harness) finishNavigation(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, result *BranchSummaryResult, kind string, errorValue *OperationError) (NavigationOutcome, error) {
	if current.State.Navigation == nil {
		return NavigationOutcome{}, fmt.Errorf("navigation state is missing")
	}
	leaf := cloneStringPointer(current.State.Navigation.TargetID)
	var summaryEntry *Entry
	writes := make([]Write, 0, 12)
	if result != nil {
		if result.Summary == "" {
			return NavigationOutcome{}, fmt.Errorf("navigation summary has no text")
		}
		generation := current.State.Navigation.Phase.Structural.Generation
		id := h.session.IDGenerator().Next()
		if generation != nil && generation.Context.ResultEntryID != "" {
			id = generation.Context.ResultEntryID
		}
		entry := Entry{EntryBase: EntryBase{ID: id, ParentID: cloneStringPointer(current.State.Navigation.TargetID), Type: EntryBranchSummary}, Summary: result.Summary, Usage: result.Usage, FromHook: current.State.Navigation.Phase.Structural.Generation == nil, FromID: stringValue(operation.SourceLeafID)}
		summaryEntry = &entry
		writes = append(writes, registerSet(RegisterLaneLeaf, lane.name, cloneStringPointer(current.State.Navigation.TargetID)), Write{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}}, registerSet(RegisterLaneLeaf, lane.name, stringPointer(id)))
		leaf = stringPointer(id)
		if generation != nil && generation.Request != nil {
			writes = append(writes, Write{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: generation.Request.UsageID, Usage: usageValue(result.Usage), EntryID: stringPointer(id)}}})
		} else if result.Usage != nil {
			writes = append(writes, Write{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: h.session.IDGenerator().Next(), Usage: *result.Usage, EntryID: stringPointer(id)}}})
		}
	} else {
		writes = append(writes, registerSet(RegisterLaneLeaf, lane.name, cloneStringPointer(leaf)))
	}
	if current.State.Navigation.Label != "" && current.State.Navigation.TargetID != nil {
		writes = append(writes, registerSet(RegisterFactLabel, *current.State.Navigation.TargetID, current.State.Navigation.Label))
	}
	cleanup, err := h.operationCleanupWrites(ctx, operation.OperationID, current.State)
	if err != nil {
		return NavigationOutcome{}, err
	}
	writes = append(writes, cleanup...)
	writes = append(writes, registerSet(RegisterLaneLastResult, lane.name, LaneLastResult{OperationID: operation.OperationID, Kind: OperationNavigation, Outcome: kind, LeafID: leaf, Error: errorValue}))
	laneStateIndex := len(writes)
	writes = append(writes, registerSet(RegisterLaneState, lane.name, LaneState{}))
	if err := h.effect(ctx, ActionInfo{Kind: "terminal", Description: "navigation terminal"}, func(ctx context.Context) error {
		return h.line(lane.name).Do(ctx, func() error {
			latest, err := h.session.GetRegister(ctx, RegisterLaneState, lane.name)
			if err != nil || latest == nil {
				return err
			}
			value, ok := latest.Value.(LaneState)
			if !ok {
				return fmt.Errorf("lane state has invalid type")
			}
			value.CurrentOperationID = nil
			writes[laneStateIndex] = registerSet(RegisterLaneState, lane.name, value)
			valid, err := (&runtimeEffects{harness: h, lane: lane}).current(ctx, current)
			if err != nil || !valid {
				if err == nil {
					err = errOperationMissing
				}
				return err
			}
			_, err = h.session.Commit(ctx, Transaction{Writes: writes})
			return err
		})
	}); err != nil {
		return NavigationOutcome{}, err
	}
	lane.finish()
	outcome := NavigationOutcome{Kind: kind, OldLeafID: cloneStringPointer(operation.SourceLeafID), NewLeafID: leaf, LeafID: leaf, SummaryEntry: summaryEntry, Error: errorValue}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventNavigationEnd), Lane: lane.name, Payload: cloneValue(outcome)})
	return outcome, nil
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
	wasRecovery := l.hasCrashSuspension()
	l.harness.events.Emit(ctx, HarnessEvent{Type: string(EventRunResume), Lane: l.name, Recovery: wasRecovery, Payload: map[string]JSONValue{"runId": current.Operation.OperationID}})
	if l.harness.hooks.Has(HookBeforeResume) {
		if err := l.harness.effect(ctx, ActionInfo{Kind: "hook", Description: "before_resume"}, func(ctx context.Context) error {
			_, err := (&runtimeEffects{harness: l.harness, lane: l}).Run(ctx, EffectPlan{Kind: EffectHook, Key: EffectKey(current.Operation.OperationID), HookName: HookBeforeResume, Event: map[string]JSONValue{"kind": current.Operation.Intent.Kind, "runId": current.Operation.OperationID, "lane": l.name, "sourceLeafId": current.Operation.SourceLeafID, "targetId": current.Operation.Intent.TargetID, "summarize": current.Operation.Intent.Summarize, "label": current.Operation.Intent.Label, "customInstructions": current.Operation.Intent.CustomInstructions, "resumeData": current.Operation.Intent.ResumeData}})
			return err
		}); err != nil {
			return Result[ResumeOutcome, error]{Err: err}, nil
		}
	}
	if current.Operation.Intent.Kind == OperationCompaction && current.State.Compaction != nil {
		if missing := l.missingIdentities(ctx, current.Configuration); missing != nil {
			return Result[ResumeOutcome, error]{Err: missing}, nil
		}
		if err := l.harness.line(l.name).Do(ctx, func() error {
			l.begin()
			l.clearSuspension()
			return nil
		}); err != nil {
			return Result[ResumeOutcome, error]{Err: err}, nil
		}
		outcome, err := l.harness.driveCompaction(ctx, l, current.Operation, current.State)
		if err != nil {
			return Result[ResumeOutcome, error]{Err: err}, nil
		}
		return Ok[ResumeOutcome, error](ResumeOutcome{Operation: OperationCompaction, RunID: current.Operation.OperationID, Compaction: &outcome}), nil
	}
	if current.Operation.Intent.Kind == OperationNavigation && current.State.Navigation != nil {
		if current.State.Navigation.Summarize {
			if missing := l.missingIdentities(ctx, current.Configuration); missing != nil {
				return Result[ResumeOutcome, error]{Err: missing}, nil
			}
		}
		if err := l.harness.line(l.name).Do(ctx, func() error { l.begin(); l.clearSuspension(); return nil }); err != nil {
			return Result[ResumeOutcome, error]{Err: err}, nil
		}
		outcome, err := l.harness.driveNavigation(ctx, l, current.Operation, current.State)
		if err != nil {
			return Result[ResumeOutcome, error]{Err: err}, nil
		}
		return Ok[ResumeOutcome, error](ResumeOutcome{Operation: OperationNavigation, RunID: current.Operation.OperationID, Navigation: &outcome}), nil
	}
	if current.Operation.Intent.Kind != OperationRun || current.State.Run == nil {
		return Result[ResumeOutcome, error]{Err: fmt.Errorf("unsupported resume operation %q", current.Operation.Intent.Kind)}, nil
	}
	if current.State.Run.Phase.Kind == PhaseAssistant && current.State.Run.Phase.Generation != nil && current.State.Run.Phase.Generation.Status != GenerationEffectPending {
		if missing := l.missingIdentities(ctx, current.Configuration); missing != nil {
			return Result[ResumeOutcome, error]{Err: missing}, nil
		}
	}
	if current.State.Run.Phase.Kind == PhaseDeferred && current.State.Run.Phase.Deferred != nil {
		if missing := l.missingIdentities(ctx, current.State.Run.Phase.Deferred.Configuration); missing != nil {
			return Result[ResumeOutcome, error]{Err: missing}, nil
		}
	}
	if err := l.harness.line(l.name).Do(ctx, func() error {
		l.begin()
		l.clearSuspension()
		return nil
	}); err != nil {
		return Result[ResumeOutcome, error]{Err: err}, nil
	}
	outcome, err := l.harness.drive(ctx, l, current.Operation, current.State)
	if err != nil {
		return Result[ResumeOutcome, error]{Err: err}, nil
	}
	if outcome.Kind == "suspended" && outcome.Reason == "missing_identities" {
		missing := outcome.Missing
		if missing == nil {
			missing = &MissingIdentitySuspension{Reason: outcome.Reason}
		}
		l.setSuspension(SuspendedOperation{Lane: l.name, OperationID: current.Operation.OperationID, Kind: OperationRun, Reason: outcome.Reason, StartedAt: current.Operation.StartedAt, MissingTools: missing.Tools, MissingModels: missing.Models})
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
	missing.Tools = missingActiveTools(l.harness.toolsSnapshot(), config.ActiveToolNames)
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
			if current.State.Compaction != nil {
				compaction := current.State.Compaction
				if compaction.Control.Status == ControlRunning {
					compaction.Control.Status = ControlCancelRequested
					compaction.Control.RequestedAt = time.Now().UnixMilli()
					if _, err := l.harness.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpState, current.Operation.OperationID, current.State)}}); err != nil {
						return err
					}
					shouldSignal = true
				}
				outcome.RunID = current.Operation.OperationID
				return nil
			}
			if current.State.Navigation != nil {
				navigation := current.State.Navigation
				if navigation.Control.Status == ControlRunning {
					navigation.Control.Status = ControlCancelRequested
					navigation.Control.RequestedAt = time.Now().UnixMilli()
					if _, err := l.harness.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterOpState, current.Operation.OperationID, current.State)}}); err != nil {
						return err
					}
					shouldSignal = true
				}
				outcome.RunID = current.Operation.OperationID
				return nil
			}
			return &NoActiveOperation{TaggedError: TaggedError{Message: "operation is not a run"}, Lane: l.name}
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
func (l *runtimeLane) RecordUsage(ctx context.Context, usage Usage, entryID *string, details JSONValue) (Result[RecordUsageOutcome, error], error) {
	if err := l.harness.lifecycle.Err(); err != nil {
		return Result[RecordUsageOutcome, error]{Err: err}, nil
	}
	id := l.harness.session.IDGenerator().Next()
	if err := l.harness.line(l.name).Do(ctx, func() error {
		_, err := l.harness.session.Commit(ctx, Transaction{Writes: []Write{{Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: id, Usage: usage, EntryID: cloneStringPointer(entryID), Adjustment: true, Details: cloneValue(details)}}}}})
		return err
	}); err != nil {
		return Result[RecordUsageOutcome, error]{Err: err}, nil
	}
	l.harness.events.Emit(ctx, HarnessEvent{Type: string(EventUsage), Lane: l.name, Payload: usage})
	return Ok[RecordUsageOutcome, error](RecordUsageOutcome{UsageID: id}), nil
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

func (l *runtimeLane) setSuspension(value SuspendedOperation) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.suspension = &value
}

func (l *runtimeLane) clearSuspension() {
	l.mu.Lock()
	l.suspension = nil
	l.mu.Unlock()
}

func (l *runtimeLane) hasCrashSuspension() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.suspension != nil && l.suspension.Reason == "crash"
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
	handle := l.watchSnapshot(LaneSnapshot{})
	snapshot, err := l.snapshot(ctx)
	if err != nil {
		handle.Unsubscribe()
		return WatchHandle[LaneSnapshot]{}, err
	}
	handle.Snapshot = snapshot
	return handle, nil
}
func (l *runtimeLane) snapshot(ctx context.Context) (LaneSnapshot, error) {
	leaf, err := l.GetLeafID(ctx)
	if err != nil {
		return LaneSnapshot{}, err
	}
	entries, err := l.view.FindEntriesOnBranch(ctx, BranchScan{Order: NewestFirst, StopAtType: entryTypePointer(EntryCompaction)})
	if err != nil {
		return LaneSnapshot{}, err
	}
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	queues, err := l.harness.queueSnapshot(ctx, l)
	if err != nil {
		return LaneSnapshot{}, err
	}
	current, currentErr := l.harness.currentOperation(ctx, l)
	if currentErr != nil && !isOperationMissing(currentErr) {
		return LaneSnapshot{}, currentErr
	}
	var operation *OperationSnapshot
	var pending []PendingEntrySnapshot
	if current.Operation.OperationID != "" {
		operation = l.harness.operationSnapshot(ctx, l, current)
		if current.State.Run != nil {
			for _, id := range current.State.Run.Inbox.Writes {
				if item, ok := l.harness.pendingSnapshot(ctx, id); ok {
					pending = append(pending, item)
				}
			}
		}
	}
	return LaneSnapshot{Lane: l.name, Transcript: entries, LeafID: leaf, Operation: operation, Queues: queues, PendingWrites: pending, Faulted: l.harness.lifecycle.Err() != nil}, nil
}

func entryTypePointer(value EntryType) *EntryType { return &value }

func (l *runtimeLane) watchSnapshot(snapshot LaneSnapshot) WatchHandle[LaneSnapshot] {
	var mu sync.Mutex
	buffer := make([]HarnessEvent, 0)
	var listener func(HarnessEvent)
	started, draining, stopped := false, false, false
	accept := func(event HarnessEvent) bool {
		return event.Lane == "" || event.Lane == l.name || event.Type == string(EventUsage)
	}
	unsub := l.harness.events.On("*", func(ctx context.Context, event HarnessEvent) {
		if !accept(event) {
			return
		}
		mu.Lock()
		if stopped || !started || draining {
			buffer = append(buffer, event)
			mu.Unlock()
			return
		}
		current := listener
		mu.Unlock()
		if current != nil {
			current(event)
		}
	})
	return WatchHandle[LaneSnapshot]{Snapshot: snapshot, Start: func(next func(HarnessEvent)) {
		mu.Lock()
		if started || stopped {
			mu.Unlock()
			return
		}
		started, draining, listener = true, true, next
		mu.Unlock()
		for {
			mu.Lock()
			if len(buffer) == 0 {
				draining = false
				mu.Unlock()
				return
			}
			event := buffer[0]
			buffer = buffer[1:]
			mu.Unlock()
			if next != nil {
				next(event)
			}
		}
	}, Unsubscribe: func() {
		mu.Lock()
		stopped = true
		buffer = nil
		mu.Unlock()
		unsub()
	}}
}

func (h *Harness) pendingSnapshot(ctx context.Context, id string) (PendingEntrySnapshot, bool) {
	register, err := h.session.GetRegister(ctx, RegisterPendingEntry, id)
	if err != nil || register == nil {
		return PendingEntrySnapshot{}, false
	}
	pending, ok := register.Value.(PendingEntry)
	if !ok {
		return PendingEntrySnapshot{}, false
	}
	item := PendingEntrySnapshot{EntryID: id, Type: pending.Type, CustomType: pending.CustomType}
	if pending.Type == EntryMessage {
		if message, ok := pending.Payload.(AgentMessage); ok {
			item.Message = messageCopy(message)
		}
	} else {
		item.Data = cloneValue(pending.Payload)
	}
	return item, true
}

func (h *Harness) operationSnapshot(ctx context.Context, lane *runtimeLane, current CurrentOperation) *OperationSnapshot {
	operation := &OperationSnapshot{ID: current.Operation.OperationID, Kind: current.Operation.Intent.Kind, Status: "running", StartedAt: current.Operation.StartedAt, RunningTools: []RunningTool{}}
	lane.mu.Lock()
	if lane.suspension != nil {
		suspended := cloneValue(*lane.suspension).(SuspendedOperation)
		operation.Status = "suspended"
		operation.Suspended = &suspended
	}
	lane.mu.Unlock()
	if current.State.Run != nil {
		if current.State.Run.Control.Status == ControlCancelRequested {
			operation.Status = "aborting"
		}
		phase := current.State.Run.Phase
		if phase.Kind == PhaseDeferred && operation.Suspended == nil {
			operation.Status = "suspended"
			operation.Suspended = &SuspendedOperation{Lane: lane.name, OperationID: current.Operation.OperationID, Kind: OperationRun, Reason: "deferred", StartedAt: current.Operation.StartedAt}
		}
		if phase.Generation != nil && phase.Generation.Status == GenerationRetryWait {
			operation.Retry = &RetrySnapshot{Attempt: phase.Generation.Attempt, MaxAttempts: phase.Generation.Context.RetryPolicy.MaxAttempts, NextAttemptAt: phase.Generation.NotBefore}
		}
		if phase.ToolBatch != nil {
			for _, call := range phase.ToolBatch.Calls {
				if call.Status != "effect_pending" {
					continue
				}
				item := RunningTool{ToolName: "", Args: map[string]JSONValue{}}
				if entry, err := lane.view.GetEntry(ctx, phase.ToolBatch.AssistantEntryID); err == nil && entry != nil && entry.Message != nil && call.SourceIndex < len(entry.Message.ToolCalls) {
					item.ToolCallID, item.ToolName, item.Args = entry.Message.ToolCalls[call.SourceIndex].ID, entry.Message.ToolCalls[call.SourceIndex].Name, entry.Message.ToolCalls[call.SourceIndex].Arguments
				}
				operation.RunningTools = append(operation.RunningTools, item)
			}
		}
	}
	return operation
}

func (h *Harness) Lane(ctx context.Context, name string) (AgentLane, error) {
	lane, err := h.lane(name)
	return lane, err
}
func (h *Harness) CreateLane(ctx context.Context, name string, at *string) (Result[AgentLane, error], error) {
	if err := h.lifecycle.Err(); err != nil {
		return Result[AgentLane, error]{Err: err}, nil
	}
	if name == "" || name == "main" {
		return Err[AgentLane, error](&InvalidLane{TaggedError: TaggedError{Message: "invalid lane name"}, Lane: name, Reason: "reserved or empty"}), nil
	}
	h.laneCreationMu.Lock()
	defer h.laneCreationMu.Unlock()
	if existing, err := h.session.GetRegister(ctx, RegisterLaneState, name); err != nil {
		return Result[AgentLane, error]{Err: err}, nil
	} else if existing != nil {
		return Err[AgentLane, error](&LaneExists{TaggedError: TaggedError{Message: "lane already exists"}, Lane: name}), nil
	}
	main, err := h.lane("main")
	if err != nil {
		return Result[AgentLane, error]{Err: err}, nil
	}
	config, err := main.configuration(ctx)
	if err != nil {
		return Result[AgentLane, error]{Err: err}, nil
	}
	if at != nil {
		entry, err := h.session.GetEntry(ctx, *at)
		if err != nil {
			return Result[AgentLane, error]{Err: err}, nil
		}
		if entry == nil {
			return Err[AgentLane, error](&UnknownTarget{TaggedError: TaggedError{Message: "unknown lane anchor"}, TargetID: *at}), nil
		}
	}
	if _, err := h.session.Commit(ctx, Transaction{Writes: []Write{registerSet(RegisterLaneConfig, name, config), registerSet(RegisterLaneLeaf, name, cloneStringPointer(at)), registerSet(RegisterLaneState, name, LaneState{PendingNextRun: []string{}})}}); err != nil {
		return Result[AgentLane, error]{Err: err}, nil
	}
	lane, err := h.attachLane(ctx, name, AgentHarnessOptions{Model: config.Model, ThinkingLevel: config.ThinkingLevel, ActiveToolNames: config.ActiveToolNames})
	if err != nil {
		return Result[AgentLane, error]{Err: err}, nil
	}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventLaneCreated), Lane: name, Payload: map[string]JSONValue{"name": name, "leafId": cloneStringPointer(at)}})
	return Ok[AgentLane, error](lane), nil
}
func (h *Harness) Lanes(ctx context.Context) ([]LaneInfo, error) {
	var result []LaneInfo
	h.lanes.Range(func(_, value any) bool {
		lane := value.(*runtimeLane)
		leaf, _ := lane.GetLeafID(ctx)
		info := LaneInfo{Name: lane.name, LeafID: leaf}
		if current, err := h.currentOperation(ctx, lane); err == nil {
			snapshot := h.operationSnapshot(ctx, lane, current)
			info.Operation = &OperationInfo{ID: snapshot.ID, Kind: snapshot.Kind, Status: snapshot.Status}
			info.Suspended = snapshot.Suspended
		}
		result = append(result, info)
		return true
	})
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}
func (h *Harness) GetTools(context.Context) ([]AgentHarnessTool, error) {
	return h.toolsSnapshot(), nil
}
func (h *Harness) SetTools(ctx context.Context, tools []AgentHarnessTool) error {
	if err := h.lifecycle.Err(); err != nil {
		return err
	}
	h.configurationMu.Lock()
	h.tools = append([]AgentHarnessTool(nil), tools...)
	h.configurationMu.Unlock()
	return nil
}
func (h *Harness) GetResources(context.Context) (Resources, error) {
	h.configurationMu.RLock()
	defer h.configurationMu.RUnlock()
	return cloneValue(h.resources).(Resources), nil
}
func (h *Harness) SetResources(ctx context.Context, resources Resources) error {
	if err := h.lifecycle.Err(); err != nil {
		return err
	}
	h.configurationMu.Lock()
	h.resources = cloneValue(resources).(Resources)
	h.configurationMu.Unlock()
	return nil
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
	entry := Entry{EntryBase: EntryBase{ID: generation.ResponseEntryID, ParentID: cloneStringPointer(current.LeafID), Type: EntryMessage}, Message: messageCopy(message)}
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

func overflowError(err error) (bool, string) {
	if err == nil {
		return false, ""
	}
	text := strings.ToLower(err.Error())
	for _, marker := range []string{"context window", "context length", "maximum context", "too many tokens", "token limit", "prompt is too long"} {
		if strings.Contains(text, marker) {
			return true, err.Error()
		}
	}
	return false, ""
}

func overflowMessage(message AgentMessage, generation Generation) (bool, string) {
	if message.StopReason == StopReasonError {
		return overflowError(fmt.Errorf("%v", message.Content))
	}
	if message.StopReason == StopReasonLength && len(message.ToolCalls) == 0 && generation.IntendedOutputLimit > 0 && message.Usage != nil && message.Usage.Output < generation.IntendedOutputLimit {
		return true, fmt.Sprintf("context window overflow after output limit %d", generation.IntendedOutputLimit)
	}
	return false, ""
}

func (h *Harness) buildCompactionPreparation(ctx context.Context, lane *runtimeLane, leaf *string, settings CompactionSettings) (DurableStructuralPreparation, bool, error) {
	if leaf == nil {
		return DurableStructuralPreparation{}, false, nil
	}
	entries, err := lane.view.FindEntriesOnBranch(ctx, BranchScan{Start: *leaf, Order: OldestFirst})
	if err != nil {
		return DurableStructuralPreparation{}, false, err
	}
	messages := make([]AgentMessage, 0, len(entries))
	previousSummary := ""
	for _, entry := range entries {
		if entry.Type == EntryCompaction {
			messages = nil
			previousSummary = entry.Summary
			continue
		}
		if entry.Message != nil && includeInContext(*entry.Message) {
			messages = append(messages, *messageCopy(*entry.Message))
		}
	}
	if len(messages) < 2 {
		return DurableStructuralPreparation{}, false, nil
	}
	return DurableStructuralPreparation{Kind: EntryCompaction, MessagesToSummarize: append([]AgentMessage(nil), messages[:len(messages)-1]...), RetainedTail: []AgentMessage{messages[len(messages)-1]}, TokensBefore: int64(len(messages)), PreviousSummary: previousSummary, Settings: settings}, true, nil
}

func (h *Harness) settleGenerationOverflow(ctx context.Context, lane *runtimeLane, operation Operation, current CurrentOperation, generation Generation, message AgentMessage, reason string) (RunOutcome, error) {
	message.Role = "assistant"
	message.StopReason = StopReasonError
	settings := current.State.Run.Settings.Compaction
	prep, hasPreparation, err := h.buildCompactionPreparation(ctx, lane, current.LeafID, settings)
	if err != nil {
		return RunOutcome{}, err
	}
	responseID, usageID := generation.ResponseEntryID, generation.UsageID
	entry := Entry{EntryBase: EntryBase{ID: responseID, ParentID: cloneStringPointer(current.LeafID), Type: EntryMessage}, Message: messageCopy(message)}
	next := current.State
	phase := RunPhase{Kind: PhaseFailureDrain, Error: &OperationError{Code: "overflow", Message: reason}, Provenance: &FailureProvenance{Kind: "response", EntryID: responseID}}
	if hasPreparation && generation.Context.OverflowRecoveryUsed {
		hasPreparation = false
	}
	if hasPreparation {
		taskID := "task:overflow:" + responseID
		phase = RunPhase{Kind: PhaseCompaction, Reason: "overflow", Structural: &StructuralDecision{TaskID: taskID, Status: "deciding"}, ResumeAfter: &CheckpointPhase{Continuation: Continuation{Kind: ContinuationNeedAssistant, OverflowRecoveryUsed: true}, TriggerEntryID: generation.Context.TriggerEntryID}}
	}
	next.Run.Phase = phase
	next.Run.LatestAssistantEntryID = &responseID
	writes := []Write{{Kind: WriteEntry, Entry: &EntryWrite{Entry: entry}}, {Kind: WriteUsage, Usage: &UsageWrite{Row: UsageRow{ID: usageID, Usage: usageValue(message.Usage), EntryID: &responseID}}}, registerSet(RegisterLaneLeaf, lane.name, &responseID)}
	if hasPreparation {
		writes = append(writes, registerSet(RegisterOpPreparation, operation.OperationID+":"+phase.Structural.TaskID, prep))
	}
	writes = append(writes, registerSet(RegisterOpState, operation.OperationID, next))
	var commit CommitResult
	if err := h.effect(ctx, ActionInfo{Kind: "settlement", Description: "overflow response"}, func(ctx context.Context) error {
		return h.line(lane.name).Do(ctx, func() error {
			valid, err := (&runtimeEffects{harness: h, lane: lane}).current(ctx, current)
			if err != nil || !valid {
				if err == nil {
					err = errOperationMissing
				}
				return err
			}
			commit, err = h.session.Commit(ctx, Transaction{Writes: writes})
			return err
		})
	}); err != nil {
		if isOperationMissing(err) {
			return h.resolveExternalFinalization(ctx, lane, operation)
		}
		return RunOutcome{}, err
	}
	current.State = next
	current.LeafID = &responseID
	if len(commit.Seqs) != 0 {
		current.OperationStateSeq = commit.Seqs[len(commit.Seqs)-1]
	}
	h.events.Emit(ctx, HarnessEvent{Type: string(EventEntryAdded), Lane: lane.name, Payload: cloneValue(entry)})
	h.events.Emit(ctx, HarnessEvent{Type: string(EventUsage), Lane: lane.name, Payload: cloneValue(usageValue(message.Usage))})
	if hasPreparation {
		return h.drive(ctx, lane, operation, next)
	}
	return h.driveFailureDrain(ctx, lane, operation, current)
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
func (h *Harness) WatchSession(ctx context.Context) (WatchHandle[SessionSnapshot], error) {
	handle := h.watchSessionSnapshot(SessionSnapshot{})
	lanes, err := h.Lanes(ctx)
	if err != nil {
		handle.Unsubscribe()
		return WatchHandle[SessionSnapshot]{}, err
	}
	handle.Snapshot = SessionSnapshot{Lanes: lanes, Faulted: h.lifecycle.Err() != nil}
	return handle, nil
}

func (h *Harness) watchSessionSnapshot(snapshot SessionSnapshot) WatchHandle[SessionSnapshot] {
	var mu sync.Mutex
	buffer := make([]HarnessEvent, 0)
	var listener func(HarnessEvent)
	started, draining, stopped := false, false, false
	unsub := h.events.On("*", func(ctx context.Context, event HarnessEvent) {
		mu.Lock()
		if stopped || !started || draining {
			buffer = append(buffer, event)
			mu.Unlock()
			return
		}
		current := listener
		mu.Unlock()
		if current != nil {
			current(event)
		}
	})
	return WatchHandle[SessionSnapshot]{Snapshot: snapshot, Start: func(next func(HarnessEvent)) {
		mu.Lock()
		if started || stopped {
			mu.Unlock()
			return
		}
		started, draining, listener = true, true, next
		mu.Unlock()
		for {
			mu.Lock()
			if len(buffer) == 0 {
				draining = false
				mu.Unlock()
				return
			}
			event := buffer[0]
			buffer = buffer[1:]
			mu.Unlock()
			if next != nil {
				next(event)
			}
		}
	}, Unsubscribe: func() {
		mu.Lock()
		stopped = true
		buffer = nil
		mu.Unlock()
		unsub()
	}}
}

var _ AgentHarness = (*Harness)(nil)
