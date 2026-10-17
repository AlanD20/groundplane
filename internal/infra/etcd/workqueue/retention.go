package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareCollection must commit before operation-owned pruning begins. A
// concurrent Retry compares the same Task revision, so exactly one can proceed.
func PrepareCollection(task TaskSnapshot, at time.Time) (Change, error) {
	next, err := workexecution.BeginCollection(task.Task, at)
	if err != nil {
		return Change{}, err
	}
	return taskChange(task, next)
}

const retentionPrefix = root + "retention/"
const retentionTimeLayout = "20060102T150405.000000000Z"

func retentionKey(taskID string, until time.Time) string {
	return retentionPrefix + until.UTC().Format(retentionTimeLayout) + "/" + taskID
}

// Completion, Retry and collection use the same Task CAS. Reopening a finished
// Task removes its expiry entry in that write, not through eventual cleanup.
func updateRetentionIndex(change *Change, before, after workexecution.Task) {
	previous, next := "", ""
	if before.RetainUntil != nil {
		previous = retentionKey(before.ID, *before.RetainUntil)
	}
	if after.RetainUntil != nil {
		next = retentionKey(after.ID, *after.RetainUntil)
	}
	if previous == next {
		return
	}
	if previous != "" {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: previous})
	}
	if next != "" {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: next, Value: []byte(after.ID)})
	}
}
