// Package idgen generates short, time-ordered, unique job IDs: a millisecond
// timestamp prefix (so IDs sort roughly by creation order, which is pleasant
// in logs and DB indexes) followed by random bytes (so concurrent callers in
// the same millisecond never collide).
package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func New() string {
	var b [10]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("idgen: crypto/rand unavailable: %v", err))
	}
	return fmt.Sprintf("%013x%s", time.Now().UnixMilli(), hex.EncodeToString(b[:]))
}
