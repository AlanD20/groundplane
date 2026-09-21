package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// PruneExpiredTasks removes at most one complete expired Task journal. A
// previously checkpointed intent is always resumed before another Task starts.
func (repository *TaskRepository) PruneExpiredTasks(
	ctx context.Context,
	now time.Time,
) (int, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return 0, err
	}
	if repository == nil || repository.store == nil {
		return 0, errs.New(errs.KindInternal, "task repository is not initialized")
	}
	if !validTaskPruneTime(now) {
		return 0, errs.New(errs.KindValidationFailed, "task prune time must be UTC")
	}
	for attempt := 0; attempt < maximumTaskPruneCASAttempts; attempt++ {
		intent, found, err := repository.nextTaskPruneIntent(ctx)
		if err != nil {
			return 0, err
		}
		if !found {
			intent, found, err = repository.beginTaskPrune(ctx, now)
			if err != nil {
				if taskPruneConflict(err) {
					continue
				}
				return 0, err
			}
			if !found {
				return 0, nil
			}
		}
		if err := repository.drainTaskPruneIntent(ctx, intent); err != nil {
			if taskPruneConflict(err) {
				continue
			}
			return 0, err
		}
		return 1, nil
	}
	return 0, errs.New(errs.KindStateConflict, "task prune state kept changing")
}

func (repository *TaskRepository) nextTaskPruneIntent(
	ctx context.Context,
) (etcdstore.Versioned[taskjournal.PruneIntent], bool, error) {
	page, err := repository.store.Range(
		ctx,
		etcdstore.RangeRequest{Prefix: taskjournal.TaskPruneIntentPrefix, Limit: 1},
	)
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, false, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > 1 {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, false, taskjournal.CorruptPruneIntent()
	}
	if len(page.Values) == 0 {
		return etcdstore.Versioned[taskjournal.PruneIntent]{ReadRevision: page.ReadRevision}, false, nil
	}
	entry := page.Values[0]
	defer clear(entry.Value)
	intent, err := taskjournal.DecodePruneIntent(entry.Value)
	if err != nil || entry.Key != taskjournal.TaskPruneIntentKey(intent.TaskID) || entry.ModRevision <= 0 {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, false, taskjournal.CorruptPruneIntent()
	}
	return etcdstore.Versioned[taskjournal.PruneIntent]{
		Record: intent, Revision: entry.ModRevision, ReadRevision: page.ReadRevision,
	}, true, nil
}

func (repository *TaskRepository) drainTaskPruneIntent(
	ctx context.Context,
	current etcdstore.Versioned[taskjournal.PruneIntent],
) error {
	var err error
	for !current.Record.BackupCheckpointCursorsComplete {
		current, err = repository.pruneTaskBackupCheckpointBatch(ctx, current, true)
		if err != nil {
			return err
		}
	}
	for !current.Record.BackupCheckpointDeduplicationsComplete {
		current, err = repository.pruneTaskBackupCheckpointBatch(ctx, current, false)
		if err != nil {
			return err
		}
	}
	if !current.Record.TaskPrimaryDeleted {
		current, err = repository.deleteTaskPrunePrimary(ctx, current)
		if err != nil {
			return err
		}
	}
	for current.Record.RemainingEvents > 0 {
		current, err = repository.pruneTaskSubordinateBatch(ctx, current, true)
		if err != nil {
			return err
		}
	}
	if err := repository.verifyTaskPrunePrefixEmpty(ctx, current.Record.TaskID, true); err != nil {
		return err
	}
	for current.Record.RemainingDeduplications > 0 {
		current, err = repository.pruneTaskSubordinateBatch(ctx, current, false)
		if err != nil {
			return err
		}
	}
	if err := repository.verifyTaskPrunePrefixEmpty(ctx, current.Record.TaskID, false); err != nil {
		return err
	}
	return repository.finishTaskPruneIntent(ctx, current)
}
