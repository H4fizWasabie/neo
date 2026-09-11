package harness

import "context"

// telemetrySession keeps storage instrumentation at the session seam so all
// three backends expose the same transaction telemetry contract.
type telemetrySession struct {
	Session
	telemetry TelemetryContext
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
