package harness

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// UUIDv7Generator mints time-sortable UUIDs. The random tail is intentionally
// fresh for follower ids; only the 48-bit time prefix is inherited.
type UUIDv7Generator struct {
	clock func() time.Time
}

func NewUUIDv7Generator() *UUIDv7Generator {
	return &UUIDv7Generator{clock: time.Now}
}

func (g *UUIDv7Generator) Next(timestampMs ...int64) string {
	ms := g.clock().UnixMilli()
	if len(timestampMs) != 0 {
		ms = timestampMs[0]
	}
	if ms < 0 || ms > (1<<48)-1 {
		return ""
	}
	var b [16]byte
	if _, err := rand.Read(b[6:]); err != nil {
		return ""
	}
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]))
}

func isUUIDv7(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return false
	}
	var b [16]byte
	if _, err := hex.Decode(b[:], []byte(id[:8]+id[9:13]+id[14:18]+id[19:23]+id[24:])); err != nil {
		return false
	}
	return b[6]>>4 == 7 && b[8]&0xc0 == 0x80
}
