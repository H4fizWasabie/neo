// Package harness contains Neo's durable agent-harness contracts and the
// reference Memory session backend. Storage owns immutable entries, mutable
// registers, and the append-only usage ledger; later runtime slices build on
// these seams without adding a second state model.
package harness
