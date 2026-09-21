package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) validateComponentTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			environmentchanges.ComponentTaskIntentKey(task.ID),
			environmentchanges.ComponentTaskActiveEnvironmentKey(task.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if result == nil || len(result.Values) != 2 {
		return errs.New(errs.KindInternal, "Component candidate replay read is incomplete")
	}
	if result.Values[0] == nil {
		return nil
	}
	intent, err := environmentchanges.DecodeComponentTaskIntent(result.Values[0].Value)
	if err != nil {
		return err
	}
	if err := validateComponentTaskOwner(task, intent); err != nil {
		return err
	}
	if intent.Status != terminalStatus || intent.TerminalAt == nil || task.FinishedAt == nil ||
		!intent.TerminalAt.Equal(*task.FinishedAt) {
		return errs.New(errs.KindStateConflict, "Component candidate does not match terminal Task")
	}
	if result.Values[1] != nil {
		activeTaskID := string(result.Values[1].Value)
		if activeTaskID == task.ID || ids.Validate(ids.KindTask, activeTaskID) != nil {
			return errs.New(errs.KindStateConflict, "terminal Component candidate remains active")
		}
	}
	return nil
}
