package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// GuardReplayRetention keeps submission replay while its Task still exists.
// Task collection first proves operation input and recovery ownership retired;
// marker collection follows independently after its own retention window. This
// ordering also prevents Retry reopening a Task whose original replay is gone.
// There is no fallback to the superseded Task journal storage.
func (repository *Repository) GuardReplayRetention(ctx context.Context,
	input idempotency.PruneGuardInput,
) (idempotency.PruneGuardResult, error) {
	if input.MarkerKind != idempotency.IdempotencyMarkerTask {
		return idempotency.PruneGuardResult{}, nil
	}
	if ids.Validate(ids.KindTask, input.TaskID) != nil || input.Revision <= 0 {
		return idempotency.PruneGuardResult{}, invalid("replay retention requires a Task and collection revision")
	}
	read, err := repository.read(ctx, []string{TaskKey(input.TaskID)}, input.Revision)
	if err != nil {
		return idempotency.PruneGuardResult{}, err
	}
	if read.Values[0] != nil {
		task, err := DecodeTask(read.Values[0].Value)
		if err != nil || task.ID != input.TaskID {
			return idempotency.PruneGuardResult{}, corrupt()
		}
		return idempotency.PruneGuardResult{Retain: true}, nil
	}
	return idempotency.PruneGuardResult{Conditions: []keyvalue.Condition{{Key: TaskKey(input.TaskID)}}}, nil
}
