package hierarchydeletion

import (
	"crypto/sha256"
	"encoding/hex"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"
)

type HierarchyDeletionOperation struct {
	Tombstone         HierarchyDeletionTombstone
	RootTaskID        string
	MarkerLocator     idempotencyrecord.IdempotencyLocator
	Owner             taskjournal.TaskOwner
	TombstoneRevision int64
	Fence             HierarchyDeletionCleanupFence
	FenceRevision     int64
	Intent            HierarchyDeletionIntent
	IntentRevision    int64
	PlanCursor        int64
	SucceededCount    int64
	FailedCount       int64
	UpdatedAt         time.Time
}

func HierarchyDeletionPlanDigest(actions [][]byte) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("gp-deletion-plan-v1\x00"))
	writeUint64(hash, uint64(len(actions)))
	for _, action := range actions {
		writeUint32(hash, uint32(len(action)))
		_, _ = hash.Write(action)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

type byteWriter interface{ Write([]byte) (int, error) }

func writeUint64(writer byteWriter, value uint64) {
	buffer := []byte{byte(value >> 56), byte(value >> 48), byte(value >> 40), byte(value >> 32),
		byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}
	_, _ = writer.Write(buffer)
}

func writeUint32(writer byteWriter, value uint32) {
	buffer := []byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}
	_, _ = writer.Write(buffer)
}
