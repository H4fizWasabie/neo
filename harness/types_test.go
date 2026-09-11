package harness

import "testing"

func TestTypeSurfaceConstructsStorageAndOperationValues(t *testing.T) {
	leaf := "01900000-0000-7000-8000-000000000000"
	state := OperationState{
		Kind: OperationRun,
		Run: &RunState{
			Kind:    OperationRun,
			Control: Control{Status: ControlRunning},
			Phase: RunPhase{
				Kind: PhaseCheckpoint,
				Checkpoint: &CheckpointPhase{
					Continuation:   Continuation{Kind: ContinuationNeedAssistant},
					TriggerEntryID: leaf,
				},
			},
			Inbox: Inbox{},
		},
	}

	tx := Transaction{Writes: []Write{
		{Kind: WriteEntry, Entry: &EntryWrite{Entry: Entry{
			EntryBase: EntryBase{ID: leaf, Type: EntryMessage},
			Message:   &AgentMessage{Role: "user", Content: "hello"},
		}}},
		{Kind: WriteRegister, Register: &RegisterWrite{
			Operation: RegisterSet,
			Namespace: RegisterOpState,
			Key:       "op-1",
			Value:     state,
		}},
	}}

	if state.Run == nil || state.Run.Phase.Checkpoint == nil {
		t.Fatal("operation state lost its run/checkpoint payload")
	}
	if len(tx.Writes) != 2 || tx.Writes[1].Register.Namespace != RegisterOpState {
		t.Fatal("transaction type surface lost its ordered writes")
	}
}
