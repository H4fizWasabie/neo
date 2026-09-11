package harness

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type CurrentOperation struct {
	Operation         Operation
	State             OperationState
	OperationStateSeq int64
	LaneState         LaneState
	LaneStateSeq      int64
	LeafID            *string
	Configuration     LaneConfiguration
	ConfigurationSeq  int64
}

type RestoreResult struct {
	Lane    string
	Idle    bool
	Current *CurrentOperation
}

func Restore(ctx context.Context, session Session, lane string) (RestoreResult, error) {
	if err := contextError(ctx); err != nil {
		return RestoreResult{}, err
	}
	if lane == "" {
		return RestoreResult{}, sessionError(SessionInvalidLane, fmt.Errorf("lane name is required"))
	}
	stateRegister, err := session.GetRegister(ctx, RegisterLaneState, lane)
	if err != nil {
		return RestoreResult{}, err
	}
	if stateRegister == nil {
		return RestoreResult{}, sessionError(SessionInvalidLane, fmt.Errorf("lane does not exist: %s", lane))
	}
	state, ok := stateRegister.Value.(LaneState)
	if !ok {
		return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("lane state has invalid type"))
	}
	laneStateSeq := stateRegister.Seq
	leafRegister, err := session.GetRegister(ctx, RegisterLaneLeaf, lane)
	if err != nil {
		return RestoreResult{}, err
	}
	var leaf *string
	if leafRegister != nil {
		var valid bool
		leaf, valid = leafRegister.Value.(*string)
		if !valid {
			return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("lane leaf has invalid type"))
		}
	}
	entryIDs := make([]string, 0, 1)
	if leaf != nil {
		entryIDs = append(entryIDs, *leaf)
	}
	if len(entryIDs) > 0 {
		entries, err := session.GetEntries(ctx, entryIDs)
		if err != nil {
			return RestoreResult{}, err
		}
		for _, id := range entryIDs {
			if _, ok := entries[id]; !ok {
				return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("lane references missing entry %s", id))
			}
		}
	}
	for _, id := range state.PendingNextRun {
		register, err := session.GetRegister(ctx, RegisterPendingEntry, id)
		if err != nil {
			return RestoreResult{}, err
		}
		if register == nil {
			return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("pending next-run item %s has no payload register", id))
		}
		if _, ok := register.Value.(PendingEntry); !ok {
			return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("pending next-run item %s has invalid payload", id))
		}
	}
	if state.CurrentOperationID == nil {
		return RestoreResult{Lane: lane, Idle: true}, nil
	}
	opID := *state.CurrentOperationID
	metaRegister, err := session.GetRegister(ctx, RegisterOpMeta, opID)
	if err != nil {
		return RestoreResult{}, err
	}
	stateRegister, err = session.GetRegister(ctx, RegisterOpState, opID)
	if err != nil {
		return RestoreResult{}, err
	}
	meta, metaOK := valueAs[Operation](metaRegister)
	opState, stateOK := valueAs[OperationState](stateRegister)
	if !metaOK || !stateOK || meta.OperationID != opID || meta.Lane != lane {
		return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("operation %s has invalid ownership registers", opID))
	}
	if err := validateOperationState(*opState); err != nil {
		return RestoreResult{}, err
	}
	entryIDs = operationEntryIDs(*meta, *opState)
	entries, err := session.GetEntries(ctx, entryIDs)
	if err != nil {
		return RestoreResult{}, err
	}
	for _, id := range entryIDs {
		if _, ok := entries[id]; !ok {
			return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("operation %s references missing entry %s", opID, id))
		}
	}
	var config LaneConfiguration
	var configSeq int64
	if register, err := session.GetRegister(ctx, RegisterLaneConfig, lane); err != nil {
		return RestoreResult{}, err
	} else if register != nil {
		var configOK bool
		config, configOK = register.Value.(LaneConfiguration)
		if !configOK {
			return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("lane configuration has invalid type"))
		}
		configSeq = register.Seq
	}
	return RestoreResult{Lane: lane, Current: &CurrentOperation{Operation: *meta, State: *opState, OperationStateSeq: stateRegister.Seq, LaneState: state, LaneStateSeq: laneStateSeq, LeafID: cloneStringPointer(leaf), Configuration: config, ConfigurationSeq: configSeq}}, nil
}

func operationEntryIDs(operation Operation, state OperationState) []string {
	seen := make(map[string]struct{})
	ids := make([]string, 0, 8)
	add := func(id *string) {
		if id != nil && *id != "" {
			if _, ok := seen[*id]; !ok {
				seen[*id] = struct{}{}
				ids = append(ids, *id)
			}
		}
	}
	for _, id := range operation.Intent.PromptEntryIDs {
		value := id
		add(&value)
	}
	add(operation.SourceLeafID)
	add(operation.Intent.TargetID)
	if state.Run != nil {
		add(runPhaseEntryID(state.Run.Phase))
		add(state.Run.LatestAssistantEntryID)
	}
	if state.Compaction != nil && state.Compaction.Structural.Generation != nil {
		add(&state.Compaction.Structural.Generation.Context.ResultEntryID)
	}
	if state.Navigation != nil && state.Navigation.TargetID != nil {
		add(state.Navigation.TargetID)
	}
	return ids
}

func runPhaseEntryID(phase RunPhase) *string {
	switch phase.Kind {
	case PhaseCheckpoint:
		if phase.Checkpoint != nil {
			return stringPointer(phase.Checkpoint.TriggerEntryID)
		}
	case PhaseTools:
		if phase.ToolBatch != nil {
			return stringPointer(phase.ToolBatch.AssistantEntryID)
		}
	case PhaseDeferred:
		if phase.Deferred != nil {
			return stringPointer(phase.Deferred.SourceEntryID)
		}
	case PhaseFailureDrain:
		if phase.Provenance != nil && phase.Provenance.EntryID != "" {
			return stringPointer(phase.Provenance.EntryID)
		}
	}
	return nil
}

func valueAs[T any](register *Register) (*T, bool) {
	if register == nil {
		return nil, false
	}
	value, ok := register.Value.(T)
	if !ok {
		return nil, false
	}
	return &value, true
}

func validateOperationState(state OperationState) error {
	count := 0
	if state.Run != nil {
		count++
	}
	if state.Compaction != nil {
		count++
	}
	if state.Navigation != nil {
		count++
	}
	if count != 1 {
		return sessionError(SessionInvalidEntry, fmt.Errorf("operation state must contain exactly one kind"))
	}
	switch state.Kind {
	case OperationRun:
		if state.Run == nil || state.Run.Kind != OperationRun {
			return sessionError(SessionInvalidEntry, fmt.Errorf("run operation state is incompatible"))
		}
	case OperationCompaction:
		if state.Compaction == nil || state.Compaction.Kind != OperationCompaction {
			return sessionError(SessionInvalidEntry, fmt.Errorf("compaction operation state is incompatible"))
		}
	case OperationNavigation:
		if state.Navigation == nil || state.Navigation.Kind != OperationNavigation {
			return sessionError(SessionInvalidEntry, fmt.Errorf("navigation operation state is incompatible"))
		}
	default:
		return sessionError(SessionInvalidEntry, fmt.Errorf("unknown operation kind %q", state.Kind))
	}
	return nil
}

type RuntimeSnapshot struct {
	SettingsRevision int64
	StreamOptions    AgentHarnessStreamOptions
	RetryPolicy      NormalizedRetryPolicy
}

type SettingsSnapshot struct {
	RuntimeSnapshot
	mu sync.RWMutex
}

func NewSettingsSnapshot(stream AgentHarnessStreamOptions, retry NormalizedRetryPolicy) *SettingsSnapshot {
	return &SettingsSnapshot{RuntimeSnapshot: RuntimeSnapshot{StreamOptions: cloneValue(stream).(AgentHarnessStreamOptions), RetryPolicy: retry}}
}

func (s *SettingsSnapshot) Snapshot() RuntimeSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return RuntimeSnapshot{SettingsRevision: s.SettingsRevision, StreamOptions: cloneValue(s.StreamOptions).(AgentHarnessStreamOptions), RetryPolicy: s.RetryPolicy}
}

func (s *SettingsSnapshot) Update(stream AgentHarnessStreamOptions, retry NormalizedRetryPolicy) RuntimeSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SettingsRevision++
	s.StreamOptions = cloneValue(stream).(AgentHarnessStreamOptions)
	s.RetryPolicy = retry
	return RuntimeSnapshot{SettingsRevision: s.SettingsRevision, StreamOptions: cloneValue(s.StreamOptions).(AgentHarnessStreamOptions), RetryPolicy: s.RetryPolicy}
}

type ScheduledAction struct {
	Info ActionInfo
	Run  func(context.Context) error
}

type ManualScheduler struct {
	manual bool
	mu     sync.Mutex
	queue  []scheduledAction
	closed bool
}

type scheduledAction struct {
	action ScheduledAction
	done   chan error
}

func NewManualScheduler(manual bool) *ManualScheduler { return &ManualScheduler{manual: manual} }

func (s *ManualScheduler) Enqueue(ctx context.Context, action ScheduledAction) (<-chan error, error) {
	if action.Run == nil {
		return nil, fmt.Errorf("scheduled action is missing a function")
	}
	done := make(chan error, 1)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("scheduler is closed")
	}
	if s.manual {
		s.queue = append(s.queue, scheduledAction{action: action, done: done})
		s.mu.Unlock()
		return done, nil
	}
	s.mu.Unlock()
	go func() { done <- action.Run(ctx) }()
	return done, nil
}

func (s *ManualScheduler) Peek() *ActionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return nil
	}
	info := s.queue[0].action.Info
	return &info
}

func (s *ManualScheduler) Execute(ctx context.Context) (*ActionInfo, error) {
	s.mu.Lock()
	if len(s.queue) == 0 {
		s.mu.Unlock()
		return nil, nil
	}
	item := s.queue[0]
	s.queue = s.queue[1:]
	s.mu.Unlock()
	err := item.action.Run(ctx)
	item.done <- err
	return &item.action.Info, nil
}

func (s *ManualScheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for _, item := range s.queue {
		item.done <- fmt.Errorf("scheduler is closed")
	}
	s.queue = nil
}

type EventBus struct {
	mu        sync.RWMutex
	nextID    int
	listeners map[string]map[int]func(context.Context, HarnessEvent)
}

func NewEventBus() *EventBus {
	return &EventBus{listeners: make(map[string]map[int]func(context.Context, HarnessEvent))}
}

func (b *EventBus) On(eventType string, listener func(context.Context, HarnessEvent)) func() {
	if listener == nil {
		return func() {}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	if b.listeners[eventType] == nil {
		b.listeners[eventType] = make(map[int]func(context.Context, HarnessEvent))
	}
	b.listeners[eventType][b.nextID] = listener
	id := b.nextID
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.listeners[eventType], id)
	}
}

func (b *EventBus) Emit(ctx context.Context, event HarnessEvent) {
	b.mu.RLock()
	listeners := make([]func(context.Context, HarnessEvent), 0)
	for _, eventType := range []string{event.Type, "*"} {
		ids := make([]int, 0, len(b.listeners[eventType]))
		for id := range b.listeners[eventType] {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		for _, id := range ids {
			listeners = append(listeners, b.listeners[eventType][id])
		}
	}
	b.mu.RUnlock()
	for _, listener := range listeners {
		listener(ctx, event)
	}
}

type HookRunner struct {
	mu       sync.RWMutex
	handlers map[HookName]map[string]HookHandler
}

func NewHookRunner() *HookRunner {
	return &HookRunner{handlers: make(map[HookName]map[string]HookHandler)}
}

func (r *HookRunner) On(name HookName, handler HookHandler, id string) (func(), error) {
	if handler == nil || id == "" {
		return nil, fmt.Errorf("hook handler and id are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handlers[name] == nil {
		r.handlers[name] = make(map[string]HookHandler)
	}
	if _, exists := r.handlers[name][id]; exists {
		return nil, fmt.Errorf("hook already registered: %s", id)
	}
	r.handlers[name][id] = handler
	return func() { r.mu.Lock(); delete(r.handlers[name], id); r.mu.Unlock() }, nil
}

func (r *HookRunner) Run(ctx context.Context, invocation HookInvocation) ([]JSONValue, error) {
	r.mu.RLock()
	handlers := make(map[string]HookHandler, len(r.handlers[invocation.Name]))
	for id, handler := range r.handlers[invocation.Name] {
		handlers[id] = handler
	}
	r.mu.RUnlock()
	ids := make([]string, 0, len(handlers))
	for id := range handlers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	results := make([]JSONValue, 0, len(ids))
	for _, id := range ids {
		result, err := handlers[id](ctx, invocation)
		if err != nil {
			return results, fmt.Errorf("hook %s: %w", id, err)
		}
		results = append(results, result)
	}
	return results, nil
}

type EffectPlan struct {
	Kind string
	Key  string
}

type EffectOutput struct {
	Kind  string
	Key   string
	Value JSONValue
}

type Effects interface {
	Run(context.Context, EffectPlan) (EffectOutput, error)
	Sleep(context.Context, int64, TelemetryContext) error
}
