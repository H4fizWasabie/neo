package harness

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type EffectKey string

type EffectKind string

const (
	EffectAssistant      EffectKind = "assistant"
	EffectSummary        EffectKind = "summary"
	EffectTool           EffectKind = "tool"
	EffectDeferred       EffectKind = "deferred"
	EffectCancelDeferred EffectKind = "cancel_deferred"
	EffectHook           EffectKind = "hook"
)

type LiveEffect struct {
	Plan    EffectPlan
	Promise <-chan EffectOutput
}

type DriveState struct {
	DeferredPollsRemaining int
	Running                map[EffectKey]LiveEffect
	ToolBatches            map[string]any
	DeferredCancellations  map[string]struct{}
}

func NewDriveState(deferredPolls int) DriveState {
	if deferredPolls != 1 {
		deferredPolls = 0
	}
	return DriveState{DeferredPollsRemaining: deferredPolls, Running: make(map[EffectKey]LiveEffect), ToolBatches: make(map[string]any), DeferredCancellations: make(map[string]struct{})}
}

type SummaryAttemptOutcome struct {
	Kind   string
	Result any
	Error  *OperationError
}

type EffectPlan struct {
	Kind             EffectKind
	Key              EffectKey
	TelemetryContext TelemetryContext
	Generation       *Generation
	Summary          *SummaryGeneration
	AssistantEntryID string
	SourceIndex      int
	ArgsKey          string
	Deferred         *Deferred
	SourceEntryID    string
	Handle           *DeferredHandle
	HookName         HookName
	Event            JSONValue
	StreamOptions    AgentHarnessStreamOptions
	Model            Model
	Messages         []Message
}

type EffectOutput struct {
	Kind       string
	Key        EffectKey
	Message    *AgentMessage
	Summary    *SummaryAttemptOutcome
	ToolResult *AgentToolResult
	IsError    bool
	Result     JSONValue
}

type SettlementOutput struct {
	Kind       string
	Key        EffectKey
	Message    *AgentMessage
	Summary    *SummaryAttemptOutcome
	ToolResult *AgentToolResult
	IsError    bool
	Terminate  bool
}

type SummaryRequestPlan struct {
	TaskID           string
	Attempt          int
	RequestIndex     int
	UsageID          string
	Configuration    LaneConfiguration
	Messages         []AgentMessage
	TelemetryContext TelemetryContext
}

type SummaryRequestOutput struct {
	Kind    string
	Message *AgentMessage
}

type SettlementResult struct {
	Current             CurrentOperation
	Dispatch            *EffectPlan
	Suspend             OperationResult
	ConsumeDeferredPoll bool
}

type PlannerInputs struct {
	Running                map[EffectKey]EffectPlan
	DeferredPollsRemaining int
	DeferredCancellations  map[string]struct{}
	Loaded                 map[string]any
	Runtime                RuntimeSnapshot
	Context                []AgentMessage
	Now                    int64
}

type ActionKind string

const (
	ActionTransition  ActionKind = "transition"
	ActionDispatch    ActionKind = "dispatch"
	ActionAwaitEffect ActionKind = "await_effect"
	ActionWait        ActionKind = "wait"
	ActionSuspend     ActionKind = "suspend"
	ActionFinish      ActionKind = "finish"
)

type Action struct {
	Kind                     ActionKind
	Next                     *OperationState
	TelemetryContext         TelemetryContext
	ExpectedConfigurationSeq int64
	ExpectedSettingsRevision int64
	Intent                   *OperationState
	Effect                   *EffectPlan
	ConsumeDeferredPoll      bool
	Key                      EffectKey
	Until                    int64
	Result                   OperationResult
}

type Effects interface {
	CommitTransition(context.Context, CurrentOperation, OperationState, TelemetryContext, *int64, *int64) (*CurrentOperation, error)
	CommitEffectSettlement(context.Context, CurrentOperation, EffectPlan, SettlementOutput, TelemetryContext) (SettlementResult, error)
	CommitTerminal(context.Context, CurrentOperation, OperationResult) (*CurrentOperation, error)
	FinalizeTool(context.Context, EffectPlan, EffectOutput) (SettlementOutput, error)
	RunSummaryRequest(context.Context, SummaryRequestPlan) (SummaryRequestOutput, error)
	SettleSummaryRequest(context.Context, CurrentOperation, SummaryRequestPlan, *AgentMessage, TelemetryContext) (CurrentOperation, error)
	Run(context.Context, EffectPlan) (EffectOutput, error)
	Sleep(context.Context, int64, TelemetryContext) error
}

type OperationResult interface{ operationResult() }

func (RunOutcome) operationResult()        {}
func (CompactionOutcome) operationResult() {}
func (NavigationOutcome) operationResult() {}

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
	configRegister, err := session.GetRegister(ctx, RegisterLaneConfig, lane)
	if err != nil {
		return RestoreResult{}, err
	}
	if configRegister == nil {
		return RestoreResult{}, sessionError(SessionInvalidLane, fmt.Errorf("lane has no configuration: %s", lane))
	}
	config, ok := configRegister.Value.(LaneConfiguration)
	if !ok {
		return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("lane configuration has invalid type"))
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
	if err := validateLaneState(state); err != nil {
		return RestoreResult{}, err
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
	if !metaOK || !stateOK || meta.OperationID != opID || meta.Lane != lane || meta.Intent.Kind == "" {
		return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("operation %s has invalid ownership registers", opID))
	}
	if meta.Intent.Kind != opState.Kind {
		return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("operation %s intent and state kinds differ", opID))
	}
	if err := validateOperationState(*opState); err != nil {
		return RestoreResult{}, err
	}
	entryIDs, optionalEntryIDs := operationEntryInventory(*meta, *opState)
	entries, err := session.GetEntries(ctx, entryIDs)
	if err != nil {
		return RestoreResult{}, err
	}
	for _, id := range entryIDs {
		if _, ok := entries[id]; !ok {
			return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("operation %s references missing entry %s", opID, id))
		}
	}
	for _, id := range optionalEntryIDs {
		if _, ok := entries[id]; ok {
			continue
		}
		optional, err := session.GetEntries(ctx, []string{id})
		if err != nil {
			return RestoreResult{}, err
		}
		if entry, exists := optional[id]; exists && !reservedEntryMatches(*opState, id, entry) {
			return RestoreResult{}, sessionError(SessionInvalidEntry, fmt.Errorf("operation %s has invalid reserved entry %s", opID, id))
		}
	}
	if err := validateOperationRegisters(ctx, session, *opState, opID); err != nil {
		return RestoreResult{}, err
	}
	return RestoreResult{Lane: lane, Current: &CurrentOperation{Operation: *meta, State: *opState, OperationStateSeq: stateRegister.Seq, LaneState: state, LaneStateSeq: laneStateSeq, LeafID: cloneStringPointer(leaf), Configuration: config, ConfigurationSeq: configRegister.Seq}}, nil
}

func operationEntryIDs(operation Operation, state OperationState) []string {
	ids, _ := operationEntryInventory(operation, state)
	return ids
}

func operationEntryInventory(operation Operation, state OperationState) ([]string, []string) {
	seen := make(map[string]struct{})
	ids := make([]string, 0, 8)
	optional := make([]string, 0, 4)
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
		if state.Run.Phase.Generation != nil && state.Run.Phase.Generation.ResponseEntryID != "" {
			optional = append(optional, state.Run.Phase.Generation.ResponseEntryID)
		}
		if state.Run.Phase.ToolBatch != nil {
			for _, call := range state.Run.Phase.ToolBatch.Calls {
				if call.Status == "completed" && call.ResultEntryID != "" {
					value := call.ResultEntryID
					add(&value)
				}
			}
		}
	}
	if state.Compaction != nil && state.Compaction.Structural.Generation != nil {
		id := state.Compaction.Structural.Generation.Context.ResultEntryID
		if id != "" {
			optional = append(optional, id)
		}
	}
	if state.Navigation != nil && state.Navigation.TargetID != nil {
		add(state.Navigation.TargetID)
	}
	return ids, optional
}

func validateLaneState(state LaneState) error {
	seen := make(map[string]struct{}, len(state.PendingNextRun))
	for _, id := range state.PendingNextRun {
		if id == "" {
			return sessionError(SessionInvalidEntry, fmt.Errorf("lane has an empty pending next-run id"))
		}
		if _, exists := seen[id]; exists {
			return sessionError(SessionInvalidEntry, fmt.Errorf("lane repeats pending next-run id %s", id))
		}
		seen[id] = struct{}{}
	}
	return nil
}

func reservedEntryMatches(state OperationState, id string, entry Entry) bool {
	if state.Run != nil && state.Run.Phase.Generation != nil && state.Run.Phase.Generation.ResponseEntryID == id {
		return entry.Type == EntryMessage
	}
	if state.Run != nil && state.Run.Phase.ToolBatch != nil {
		for _, call := range state.Run.Phase.ToolBatch.Calls {
			if call.ResultEntryID == id {
				return entry.Type == EntryMessage
			}
		}
	}
	return true
}

func operationRegisterKeys(state OperationState, operationID string) []struct {
	namespace RegisterNamespace
	key       string
} {
	keys := make([]struct {
		namespace RegisterNamespace
		key       string
	}, 0)
	addPending := func(ids []string) {
		for _, id := range ids {
			if id != "" {
				keys = append(keys, struct {
					namespace RegisterNamespace
					key       string
				}{RegisterPendingEntry, id})
			}
		}
	}
	if state.Run != nil {
		addPending(state.Run.Inbox.Steer)
		addPending(state.Run.Inbox.FollowUp)
		addPending(state.Run.Inbox.Writes)
		addPending(state.Run.Control.DrainedSteer)
		addPending(state.Run.Control.DrainedFollowUp)
		if state.Run.Phase.Kind == PhaseTools && state.Run.Phase.ToolBatch != nil {
			stepID := state.Run.Phase.ToolBatch.StepID
			if stepID == "" {
				stepID = state.Run.Phase.ToolBatch.TurnID
			}
			for _, call := range state.Run.Phase.ToolBatch.Calls {
				if call.Status == "effect_pending" {
					keys = append(keys, struct {
						namespace RegisterNamespace
						key       string
					}{RegisterOpToolArgs, fmt.Sprintf("%s:%s:%d", operationID, stepID, call.SourceIndex)})
				}
			}
		}
		if state.Run.Phase.Structural != nil && state.Run.Phase.Structural.TaskID != "" {
			keys = append(keys, struct {
				namespace RegisterNamespace
				key       string
			}{RegisterOpPreparation, fmt.Sprintf("%s:%s", operationID, state.Run.Phase.Structural.TaskID)})
		}
	}
	if state.Compaction != nil && state.Compaction.Structural.TaskID != "" {
		keys = append(keys, struct {
			namespace RegisterNamespace
			key       string
		}{RegisterOpPreparation, fmt.Sprintf("%s:%s", operationID, state.Compaction.Structural.TaskID)})
	}
	if state.Navigation != nil && state.Navigation.Phase.Structural != nil && state.Navigation.Phase.Structural.TaskID != "" {
		keys = append(keys, struct {
			namespace RegisterNamespace
			key       string
		}{RegisterOpPreparation, fmt.Sprintf("%s:%s", operationID, state.Navigation.Phase.Structural.TaskID)})
	}
	return keys
}

func validateOperationRegisters(ctx context.Context, session Session, state OperationState, operationID string) error {
	for _, key := range operationRegisterKeys(state, operationID) {
		register, err := session.GetRegister(ctx, key.namespace, key.key)
		if err != nil {
			return err
		}
		if register == nil {
			return sessionError(SessionInvalidEntry, fmt.Errorf("operation references missing %s register %s", key.namespace, key.key))
		}
		if key.namespace == RegisterPendingEntry {
			if _, ok := register.Value.(PendingEntry); !ok {
				return sessionError(SessionInvalidEntry, fmt.Errorf("pending entry register %s has invalid payload", key.key))
			}
		}
		if key.namespace == RegisterOpPreparation {
			if _, ok := register.Value.(DurableStructuralPreparation); !ok {
				return sessionError(SessionInvalidEntry, fmt.Errorf("preparation register %s has invalid payload", key.key))
			}
		}
	}
	return nil
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
		if err := validateRunPhase(state.Run.Phase); err != nil {
			return err
		}
		if state.Run.Phase.ToolBatch != nil {
			if err := validateToolBatch(*state.Run.Phase.ToolBatch); err != nil {
				return err
			}
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

func validateRunPhase(phase RunPhase) error {
	valid := false
	switch phase.Kind {
	case PhaseCheckpoint:
		valid = phase.Checkpoint != nil
	case PhaseAssistant:
		valid = phase.Generation != nil
	case PhaseTools:
		valid = phase.ToolBatch != nil
	case PhaseCompaction:
		valid = phase.Structural != nil
	case PhaseDeferred:
		valid = phase.Deferred != nil
	case PhaseFailureDrain:
		valid = phase.Error != nil || phase.Provenance != nil
	default:
		return sessionError(SessionInvalidEntry, fmt.Errorf("unknown run phase %q", phase.Kind))
	}
	if !valid {
		return sessionError(SessionInvalidEntry, fmt.Errorf("run phase %q has no matching payload", phase.Kind))
	}
	return nil
}

func validateToolBatch(batch ToolBatch) error {
	seenIndexes := make(map[int]struct{}, len(batch.Calls))
	seenResults := make(map[string]struct{}, len(batch.Calls))
	for _, call := range batch.Calls {
		if call.SourceIndex < 0 || call.SourceIndex >= len(batch.Calls) {
			return sessionError(SessionInvalidEntry, fmt.Errorf("tool source index %d is out of range", call.SourceIndex))
		}
		if _, exists := seenIndexes[call.SourceIndex]; exists {
			return sessionError(SessionInvalidEntry, fmt.Errorf("tool source index %d is repeated", call.SourceIndex))
		}
		seenIndexes[call.SourceIndex] = struct{}{}
		if call.ResultEntryID == "" {
			return sessionError(SessionInvalidEntry, fmt.Errorf("tool source index %d has no result id", call.SourceIndex))
		}
		if _, exists := seenResults[call.ResultEntryID]; exists {
			return sessionError(SessionInvalidEntry, fmt.Errorf("tool result id %s is repeated", call.ResultEntryID))
		}
		seenResults[call.ResultEntryID] = struct{}{}
		switch call.Status {
		case "planned", "effect_pending", "completed", "interrupted":
		default:
			return sessionError(SessionInvalidEntry, fmt.Errorf("unknown tool status %q", call.Status))
		}
	}
	for index := range batch.Calls {
		if _, exists := seenIndexes[index]; !exists {
			return sessionError(SessionInvalidEntry, fmt.Errorf("tool source index %d is missing", index))
		}
	}
	return nil
}

type RuntimeSnapshot struct {
	SettingsRevision int64
	StreamOptions    AgentHarnessStreamOptions
	RetryPolicy      NormalizedRetryPolicy
}

type SettingsSnapshot struct {
	mu       sync.RWMutex
	revision int64
	stream   AgentHarnessStreamOptions
	retry    NormalizedRetryPolicy
}

type RegisterToken struct {
	Namespace RegisterNamespace
	Key       string
	Seq       int64
	Exists    bool
}

type LaneMutationLine struct{ mu sync.Mutex }

func (l *LaneMutationLine) Do(ctx context.Context, fn func() error) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := contextError(ctx); err != nil {
		return err
	}
	return fn()
}

func (l *LaneMutationLine) Commit(ctx context.Context, session Session, expected []RegisterToken, tx Transaction) (bool, error) {
	var committed bool
	err := l.Do(ctx, func() error {
		for _, token := range expected {
			register, err := session.GetRegister(ctx, token.Namespace, token.Key)
			if err != nil {
				return err
			}
			if register == nil {
				if token.Exists {
					return nil
				}
				continue
			}
			if !token.Exists || register.Seq != token.Seq {
				return nil
			}
		}
		if _, err := session.Commit(ctx, tx); err != nil {
			return err
		}
		committed = true
		return nil
	})
	return committed, err
}

type RuntimeLifecycle struct {
	mu     sync.Mutex
	closed bool
	fault  error
}

type IdentityResolver struct {
	Models Models
	Tools  map[string]AgentHarnessTool
}

func (r IdentityResolver) ResolveModel(ctx context.Context, provider, modelID string) (Model, error) {
	if r.Models == nil {
		return Model{}, &MissingIdentities{TaggedError: TaggedError{Message: "model registry is unavailable"}, Models: []string{provider + "/" + modelID}}
	}
	return r.Models.Resolve(ctx, provider, modelID)
}

func (r IdentityResolver) ResolveTool(name string) (AgentHarnessTool, error) {
	tool, ok := r.Tools[name]
	if !ok || tool == nil {
		return nil, &MissingIdentities{TaggedError: TaggedError{Message: "tool is unavailable"}, Tools: []string{name}}
	}
	return tool, nil
}

func (r IdentityResolver) ResolveEffect(ctx context.Context, plan EffectPlan) error {
	switch plan.Kind {
	case EffectAssistant, EffectSummary, EffectDeferred:
		if plan.Generation != nil {
			_, err := r.ResolveModel(ctx, plan.Generation.Context.Configuration.Model.Provider, plan.Generation.Context.Configuration.Model.ModelID)
			return err
		}
		if plan.Summary != nil {
			_, err := r.ResolveModel(ctx, plan.Summary.Context.Configuration.Model.Provider, plan.Summary.Context.Configuration.Model.ModelID)
			return err
		}
		if plan.Deferred != nil {
			_, err := r.ResolveModel(ctx, plan.Deferred.Configuration.Model.Provider, plan.Deferred.Configuration.Model.ModelID)
			return err
		}
	case EffectTool:
		if plan.Event == nil {
			return fmt.Errorf("tool effect has no durable tool identity")
		}
		if name, ok := plan.Event.(string); ok {
			_, err := r.ResolveTool(name)
			return err
		}
	}
	return nil
}

func (l *RuntimeLifecycle) Fault(err error) error {
	if err == nil {
		err = fmt.Errorf("harness fault")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fault == nil {
		l.fault = &HarnessFault{Message: err.Error(), Cause: err}
	}
	return l.fault
}

func (l *RuntimeLifecycle) Close() {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
}

func (l *RuntimeLifecycle) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fault != nil {
		return l.fault
	}
	if l.closed {
		return &HarnessClosed{Message: "harness is closed"}
	}
	return nil
}

func NewSettingsSnapshot(stream AgentHarnessStreamOptions, retry NormalizedRetryPolicy) *SettingsSnapshot {
	return &SettingsSnapshot{stream: cloneValue(stream).(AgentHarnessStreamOptions), retry: retry}
}

func (s *SettingsSnapshot) Snapshot() RuntimeSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return RuntimeSnapshot{SettingsRevision: s.revision, StreamOptions: cloneValue(s.stream).(AgentHarnessStreamOptions), RetryPolicy: s.retry}
}

func (s *SettingsSnapshot) Update(stream AgentHarnessStreamOptions, retry NormalizedRetryPolicy) RuntimeSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revision++
	s.stream = cloneValue(stream).(AgentHarnessStreamOptions)
	s.retry = retry
	return RuntimeSnapshot{SettingsRevision: s.revision, StreamOptions: cloneValue(s.stream).(AgentHarnessStreamOptions), RetryPolicy: s.retry}
}

type ScheduledAction struct {
	Info ActionInfo
	Run  func(context.Context) error
}

type ManualScheduler struct {
	manual bool
	mu     sync.Mutex
	queue  []scheduledAction
	active int
	wake   chan struct{}
	closed bool
}

type scheduledAction struct {
	action ScheduledAction
	done   chan error
}

func NewManualScheduler(manual bool) *ManualScheduler {
	return &ManualScheduler{manual: manual, wake: make(chan struct{}, 1)}
}

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
		s.signal()
		s.mu.Unlock()
		return done, nil
	}
	s.active++
	s.mu.Unlock()
	go func() {
		err := action.Run(ctx)
		s.mu.Lock()
		s.active--
		s.signal()
		s.mu.Unlock()
		done <- err
	}()
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
	s.mu.Lock()
	s.active++
	s.signal()
	s.mu.Unlock()
	go func() {
		err := item.action.Run(ctx)
		s.mu.Lock()
		s.active--
		s.signal()
		s.mu.Unlock()
		item.done <- err
	}()
	return &item.action.Info, nil
}

func (s *ManualScheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *ManualScheduler) RunToCompletion(ctx context.Context) error {
	for {
		if _, err := s.Execute(ctx); err != nil {
			return err
		}
		s.mu.Lock()
		idle := len(s.queue) == 0 && s.active == 0
		closed := s.closed
		s.mu.Unlock()
		if idle {
			if closed {
				return fmt.Errorf("scheduler is closed")
			}
			return nil
		}
		if s.Peek() == nil {
			select {
			case <-s.wake:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (s *ManualScheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for _, item := range s.queue {
		item.done <- fmt.Errorf("scheduler is closed")
	}
	s.queue = nil
	s.signal()
}

type EventBus struct {
	mu        sync.RWMutex
	nextID    int
	listeners map[string]map[int]func(context.Context, HarnessEvent)
	buffer    []HarnessEvent
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
	// Keep a copy for late observers and tests of event order. Listeners still
	// receive the event synchronously, so publication never depends on a worker.
	b.mu.Lock()
	b.buffer = append(b.buffer, cloneEvent(event))
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
	b.mu.Unlock()
	for _, listener := range listeners {
		func() {
			defer func() { _ = recover() }()
			listener(ctx, cloneEvent(event))
		}()
	}
}

func (b *EventBus) Drain() []HarnessEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := append([]HarnessEvent(nil), b.buffer...)
	b.buffer = nil
	return out
}

func cloneEvent(event HarnessEvent) HarnessEvent {
	return HarnessEvent{Type: event.Type, Lane: event.Lane, Recovery: event.Recovery, Payload: cloneValue(event.Payload)}
}

type HookRunner struct {
	mu       sync.RWMutex
	handlers map[HookName][]hookRegistration
}

type hookRegistration struct {
	id      string
	handler HookHandler
}

func NewHookRunner() *HookRunner {
	return &HookRunner{handlers: make(map[HookName][]hookRegistration)}
}

func (r *HookRunner) Has(name HookName) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers[name]) != 0
}

func (r *HookRunner) On(name HookName, handler HookHandler, id string) (func(), error) {
	if handler == nil || id == "" {
		return nil, fmt.Errorf("hook handler and id are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, registration := range r.handlers[name] {
		if registration.id == id {
			return nil, fmt.Errorf("hook already registered: %s", id)
		}
	}
	r.handlers[name] = append(r.handlers[name], hookRegistration{id: id, handler: handler})
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		registrations := r.handlers[name]
		for i, registration := range registrations {
			if registration.id == id {
				r.handlers[name] = append(registrations[:i], registrations[i+1:]...)
				return
			}
		}
	}, nil
}

func (r *HookRunner) Run(ctx context.Context, invocation HookInvocation) (JSONValue, error) {
	r.mu.RLock()
	handlers := append([]hookRegistration(nil), r.handlers[invocation.Name]...)
	r.mu.RUnlock()
	var aggregate JSONValue
	for _, registration := range handlers {
		invocation.Prior = aggregate
		result, err := registration.handler(ctx, invocation)
		if err != nil {
			if invocation.Name == HookBeforeTool {
				return map[string]JSONValue{"block": map[string]JSONValue{"reason": "hook failed", "handler": registration.id}}, nil
			}
			continue
		}
		if result == nil {
			continue
		}
		aggregate = mergeHookValue(aggregate, result)
	}
	return aggregate, nil
}

func mergeHookValue(previous, next JSONValue) JSONValue {
	previousMap, previousOK := previous.(map[string]JSONValue)
	nextMap, nextOK := next.(map[string]JSONValue)
	if !previousOK || !nextOK {
		return next
	}
	merged := make(map[string]JSONValue, len(previousMap)+len(nextMap))
	for key, value := range previousMap {
		merged[key] = value
	}
	for key, value := range nextMap {
		if key == "messages" {
			if prior, ok := merged[key].([]AgentMessage); ok {
				if current, ok := value.([]AgentMessage); ok {
					merged[key] = append(append([]AgentMessage(nil), prior...), current...)
					continue
				}
			}
		}
		merged[key] = value
	}
	return merged
}
