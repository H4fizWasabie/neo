package harness

import "context"

// telemetrySession keeps storage instrumentation at the session seam so all
// three backends expose the same transaction telemetry contract.
type telemetrySession struct {
	Session
	telemetry TelemetryContext
}

func (s *telemetrySession) View(lane string) SessionTree {
	return &telemetryTree{SessionTree: s.Session.View(lane), telemetry: s.telemetry}
}

func (s *telemetrySession) Commit(ctx context.Context, tx Transaction) (CommitResult, error) {
	if s.telemetry == nil || len(tx.Writes) == 0 {
		return s.Session.Commit(ctx, tx)
	}
	attributes := map[string]AttributeValue{
		"theoses.session.item_count": len(tx.Writes),
		"theoses.session.item_kinds": writeKinds(tx),
	}
	var result CommitResult
	var commitErr error
	called := false
	spanErr := safeTelemetry(s.telemetry, ctx, SpanOptions{Name: SpanSessionWrite, Attributes: attributes}, func(span TelemetrySpan) error {
		called = true
		result, commitErr = s.Session.Commit(ctx, tx)
		if commitErr != nil {
			span.SetStatus(SpanError, commitErr.Error())
			return commitErr
		}
		last := int64(0)
		if len(result.Seqs) != 0 {
			last = result.Seqs[len(result.Seqs)-1]
		}
		span.SetAttributes(map[string]AttributeValue{
			"theoses.session.first_seq": result.FirstSeq,
			"theoses.session.last_seq":  last,
		})
		return nil
	})
	if called {
		return result, commitErr
	}
	if spanErr != nil {
		return s.Session.Commit(ctx, tx)
	}
	return result, commitErr
}

func writeKinds(tx Transaction) []string {
	kinds := make([]string, len(tx.Writes))
	for i, write := range tx.Writes {
		kinds[i] = string(write.Kind)
	}
	return kinds
}

type telemetryTree struct {
	SessionTree
	telemetry TelemetryContext
}

func (t *telemetryTree) AppendMessage(ctx context.Context, message AgentMessage) (string, error) {
	var id string
	err := t.write(ctx, "entry", func() error {
		var err error
		id, err = t.SessionTree.AppendMessage(ctx, message)
		return err
	}, func() int64 {
		entry, err := t.SessionTree.GetEntry(ctx, id)
		if err != nil || entry == nil {
			return 0
		}
		return entry.Seq
	})
	return id, err
}

func (t *telemetryTree) AppendCustomEntry(ctx context.Context, customType string, data JSONValue) (string, error) {
	var id string
	err := t.write(ctx, "entry", func() error {
		var err error
		id, err = t.SessionTree.AppendCustomEntry(ctx, customType, data)
		return err
	}, func() int64 {
		entry, err := t.SessionTree.GetEntry(ctx, id)
		if err != nil || entry == nil {
			return 0
		}
		return entry.Seq
	})
	return id, err
}

func (t *telemetryTree) SetName(ctx context.Context, name *string) error {
	return t.write(ctx, "register", func() error { return t.SessionTree.SetName(ctx, name) }, func() int64 {
		return t.registerSequence(ctx, RegisterFactName, "")
	})
}

func (t *telemetryTree) SetLabel(ctx context.Context, id string, label *string) error {
	return t.write(ctx, "register", func() error { return t.SessionTree.SetLabel(ctx, id, label) }, func() int64 {
		return t.registerSequence(ctx, RegisterFactLabel, id)
	})
}

func (t *telemetryTree) SetCustomFact(ctx context.Context, key string, value JSONValue) error {
	return t.write(ctx, "register", func() error { return t.SessionTree.SetCustomFact(ctx, key, value) }, func() int64 {
		return t.registerSequence(ctx, RegisterFactCustom, key)
	})
}

func (t *telemetryTree) DeleteCustomFact(ctx context.Context, key string) error {
	return t.write(ctx, "register", func() error { return t.SessionTree.DeleteCustomFact(ctx, key) }, func() int64 { return 0 })
}

func (t *telemetryTree) registerSequence(ctx context.Context, namespace RegisterNamespace, key string) int64 {
	tree, ok := t.SessionTree.(*memoryTree)
	if !ok {
		return 0
	}
	register, _ := tree.storage.GetRegister(ctx, namespace, key)
	if register == nil {
		return 0
	}
	return register.Seq
}

func (t *telemetryTree) write(ctx context.Context, kind string, fn func() error, sequence func() int64) error {
	if t.telemetry == nil {
		return fn()
	}
	called := false
	var operationErr error
	spanErr := safeTelemetry(t.telemetry, ctx, SpanOptions{Name: SpanSessionWrite, Attributes: map[string]AttributeValue{"theoses.session.item_count": 1, "theoses.session.item_kinds": []string{kind}}}, func(span TelemetrySpan) error {
		called = true
		operationErr = fn()
		if operationErr != nil {
			span.SetStatus(SpanError, operationErr.Error())
			return operationErr
		}
		seq := sequence()
		span.SetAttributes(map[string]AttributeValue{"theoses.session.first_seq": seq, "theoses.session.last_seq": seq})
		return nil
	})
	if called {
		return operationErr
	}
	if spanErr != nil {
		return fn()
	}
	return operationErr
}
