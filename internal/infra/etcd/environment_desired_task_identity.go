package etcd

import idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"

func prepareEnvironmentDesiredTaskIdentities(
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (TaskRecord, TaskRecord, error) {
	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := ValidateTaskRecord(task); err != nil {
		return TaskRecord{}, TaskRecord{}, err
	}
	return task, task, nil
}
