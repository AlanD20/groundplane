package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func prepareTaskRetentionIndex(task TaskRecord) (string, []byte, error) {
	if validateTaskRecord(task) != nil || !isTerminalTaskStatus(task.Status) || task.RetainUntil == nil {
		return "", nil, errs.New(errs.KindInternal, "terminal Task retention metadata is invalid")
	}
	key := taskRetentionIndexKey(task.ID, *task.RetainUntil)
	taskID, retainUntil, err := parseTaskRetentionIndexKey(key)
	if err != nil || taskID != task.ID || !retainUntil.Equal(*task.RetainUntil) {
		return "", nil, errs.New(errs.KindInternal, "terminal Task retention key is invalid")
	}
	value, err := encodeTaskReference(task.ID)
	if err != nil {
		return "", nil, err
	}
	return key, value, nil
}

func (repository *TaskRepository) validateTaskRetentionReplay(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) error {
	if task.RetainUntil == nil || ids.Validate(ids.KindTask, task.ID) != nil {
		return errs.New(errs.KindInternal, "terminal Task retention replay is invalid")
	}
	key := taskRetentionIndexKey(task.ID, *task.RetainUntil)
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "terminal Task retention index is missing")
	}
	taskID, err := decodeTaskReference(result.Values[0].Value)
	if err != nil || taskID != task.ID {
		return errs.New(errs.KindInternal, "terminal Task retention index is corrupt")
	}
	return nil
}

func validTaskPruneTime(value time.Time) bool {
	return validateTimestamp("Task prune time", value) == nil
}
