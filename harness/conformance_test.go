package harness

import (
	"context"
	"encoding/json"
	"testing"
)

func TestInstrumentedStorageRecordsOnlySuccessfulCommitOrder(t *testing.T) {
	ctx := context.Background()
	inner := NewMemoryStorage(MemoryStorageOptions{})
	storage := NewInstrumentedStorage(inner)
	generator := NewUUIDv7Generator()
	first := generator.Next()
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{entryWrite(first, nil, "one")}}); err != nil {
		t.Fatal(err)
	}
	bad := generator.Next()
	if _, err := storage.Commit(ctx, Transaction{Writes: []Write{entryWrite(bad, stringPointer("missing"), "bad")}}); err == nil {
		t.Fatal("invalid commit succeeded")
	}
	observed := storage.Commits()
	if len(observed) != 1 || len(observed[0].Transaction.Writes) != 1 || observed[0].Result.Seqs[0] != 1 {
		t.Fatalf("unexpected observations: %+v", observed)
	}
}

func TestSessionCodecRoundTripsPersistedEntryAndTypedRegisters(t *testing.T) {
	codec := NewSessionCodec(SessionCodecOptions{})
	generator := NewUUIDv7Generator()
	parent := generator.Next()
	entry := Entry{EntryBase: EntryBase{ID: generator.Next(), ParentID: &parent, Seq: 9, Timestamp: 1700000000000, Type: EntryMessage}, Message: &AgentMessage{Role: "assistant", Content: "done", StopReason: StopReasonStop}}
	encoded, err := codec.EncodeEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.DecodeEntry(encoded)
	if err != nil || decoded.Seq != entry.Seq || decoded.Message.Content != "done" {
		t.Fatalf("entry codec failed: %v %+v", err, decoded)
	}
	register := Register{Namespace: RegisterLaneConfig, Key: "main", Seq: 4, Value: LaneConfiguration{Model: Model{Provider: "p", ModelID: "m"}, ThinkingLevel: ThinkingMedium, ActiveToolNames: []string{}}}
	encoded, err = codec.EncodeRegister(register)
	if err != nil {
		t.Fatal(err)
	}
	decodedRegister, err := codec.DecodeRegister(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decodedRegister.Value.(LaneConfiguration); !ok {
		t.Fatalf("register lost typed value: %#v", decodedRegister.Value)
	}
	if _, err := json.Marshal(decodedRegister.Value); err != nil {
		t.Fatal(err)
	}
}
