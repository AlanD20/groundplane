package etcd

import idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"

func prepareEnvironmentDesiredTaskIdentities(
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
	runtime *blueprintTaskPair,
) (TaskRecord, TaskRecord, error) {
	task = cloneTaskRecord(task)
	publicTask := task
	if runtime == nil {
		if task.IdempotencyKey == "" {
			task.IdempotencyKey = marker.Locator.Key
		}
		task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
		publicTask = task
	} else {
		publicTask = cloneTaskRecord(runtime.parent)
		if publicTask.IdempotencyKey == "" {
			publicTask.IdempotencyKey = marker.Locator.Key
		}
		publicTask.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
		if task.IdempotencyKey == "" {
			task.IdempotencyKey = runtime.childMarker.Locator.Key
		}
		task.idempotencyMarker = cloneIdempotencyLocator(&runtime.childMarker.Locator)
	}
	if err := ValidateTaskRecord(task); err != nil {
		return TaskRecord{}, TaskRecord{}, err
	}
	if err := ValidateTaskRecord(publicTask); err != nil {
		return TaskRecord{}, TaskRecord{}, err
	}
	return task, publicTask, nil
}
