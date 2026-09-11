package harness

import "context"

type SearchQuery struct {
	Text  string `json:"text"`
	Limit int    `json:"limit,omitempty"`
}

type SessionSearchHit struct {
	SessionID string            `json:"sessionId"`
	Score     float64           `json:"score,omitempty"`
	Top       *SessionSearchTop `json:"top,omitempty"`
}

type SessionSearchTop struct {
	EntryID   string `json:"entryId"`
	Snippet   string `json:"snippet,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

type EntrySearchHit struct {
	SessionID string  `json:"sessionId"`
	EntryID   string  `json:"entryId"`
	Timestamp int64   `json:"timestamp"`
	Snippet   string  `json:"snippet,omitempty"`
	Score     float64 `json:"score,omitempty"`
}

type SessionSearchService interface {
	SearchSessions(context.Context, SearchQuery) ([]SessionSearchHit, error)
	Sync(context.Context) error
	Notify(string)
	Remove(context.Context, string) error
	Close(context.Context) error
}

type EntrySearchService interface {
	SearchEntries(context.Context, SearchQuery) ([]EntrySearchHit, error)
}
