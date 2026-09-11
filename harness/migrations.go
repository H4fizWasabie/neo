package harness

import "context"

// The v1 to v2 mapping is intentionally an identity mapping. Keeping it as a
// named total step makes future state-machine migrations explicit and chained.
func migrateMemoryStorage(ctx context.Context, _ *memoryData, version int) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if version == 0 {
		version = 1
	}
	for version < CurrentStorageVersion {
		switch version {
		case 1:
			version++
		default:
			return &SessionError{Code: SessionStorageFailure, Msg: "no migration registered"}
		}
	}
	return nil
}
