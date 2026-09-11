package harness

import "context"

// ProjectContext applies the branch projection rules before provider-specific
// transformation. The input is the newest-first branch scan from Storage.
func ProjectContext(ctx context.Context, entries []Entry, projectors map[string]EntryProjector) ([]AgentMessage, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return []AgentMessage{}, nil
	}
	end := len(entries)
	for i, entry := range entries {
		if entry.Type == EntryCompaction {
			end = i + 1
			break
		}
	}
	selected := append([]Entry(nil), entries[:end]...)
	result := make([]AgentMessage, 0, len(selected))
	for i := len(selected) - 1; i >= 0; i-- {
		entry := selected[i]
		switch entry.Type {
		case EntryMessage:
			if entry.Message != nil && includeInContext(*entry.Message) {
				result = append(result, cloneValue(*entry.Message).(AgentMessage))
			}
		case EntryCompaction:
			result = append(result, AgentMessage{Role: "assistant", Content: entry.Summary})
			for _, message := range entry.RetainedTail {
				if includeInContext(message) {
					result = append(result, cloneValue(message).(AgentMessage))
				}
			}
		case EntryBranchSummary:
			result = append(result, AgentMessage{Role: "assistant", Content: entry.Summary})
		case EntryCustom:
			projector := projectors[entry.CustomType]
			if projector == nil {
				continue
			}
			messages, err := projector(ctx, cloneEntry(entry))
			if err != nil {
				return nil, err
			}
			result = append(result, messages...)
		}
	}
	return result, nil
}

func includeInContext(message AgentMessage) bool {
	return message.Role != "assistant" || message.StopReason != StopReasonError && message.StopReason != StopReasonAborted && message.StopReason != StopReasonDeferred
}
