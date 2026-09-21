package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const controllerUpdateHistoryPrefix = "/v1/indexes/tasks/controller-updates/"

func isNativeControllerUpdate(task TaskRecord) bool {
	return task.Executor == taskjournal.TaskExecutorController && task.Type == taskjournal.TaskUpdate && task.Target == "controller" &&
		task.Owner == taskjournal.PlatformTaskOwner() && task.Params[TaskResourceKindParam] == TaskResourceController
}

// taskJournalIndexKeys includes the closed native history index in ordinary
// publication, primary verification and retention cleanup. No second Task
// record or host summary is independently written after acceptance.
func taskJournalIndexKeys(task TaskRecord) ([]string, error) {
	keys, err := taskOwnerIndexKeys(task.Owner, task.ID)
	if err != nil {
		return nil, err
	}
	if isNativeControllerUpdate(task) {
		keys = append(keys, controllerUpdateHistoryPrefix+task.ID)
	}
	return keys, nil
}

func (repository *TaskRepository) LatestControllerUpdate(ctx context.Context) (etcdstore.Versioned[TaskRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, false, err
	}
	index, err := repository.store.Range(
		ctx,
		etcdstore.RangeRequest{Prefix: controllerUpdateHistoryPrefix, Limit: 1, Descending: true},
	)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, false, err
	}
	if index == nil {
		return etcdstore.Versioned[TaskRecord]{}, false, corruptControllerUpdateHistory()
	}
	if len(index.Values) == 0 {
		return etcdstore.Versioned[TaskRecord]{}, false, nil
	}
	if len(index.Values) != 1 || index.ReadRevision <= 0 {
		return etcdstore.Versioned[TaskRecord]{}, false, corruptControllerUpdateHistory()
	}
	entry := index.Values[0]
	id := strings.TrimPrefix(entry.Key, controllerUpdateHistoryPrefix)
	if !strings.HasPrefix(entry.Key, controllerUpdateHistoryPrefix) || ids.Validate(ids.KindTask, id) != nil ||
		string(entry.Value) != id {
		return etcdstore.Versioned[TaskRecord]{}, false, corruptControllerUpdateHistory()
	}
	primary, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{taskjournal.TaskStorageKey(id)}, Revision: index.ReadRevision},
	)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, false, err
	}
	if primary == nil || primary.ReadRevision != index.ReadRevision || len(primary.Values) != 1 ||
		primary.Values[0] == nil ||
		primary.Values[0].Key != taskjournal.TaskStorageKey(id) {
		return etcdstore.Versioned[TaskRecord]{}, false, corruptControllerUpdateHistory()
	}
	task, err := decodeTaskRecord(primary.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, false, err
	}
	if task.ID != id || !isNativeControllerUpdate(task) {
		return etcdstore.Versioned[TaskRecord]{}, false, corruptControllerUpdateHistory()
	}
	result := etcdstore.Versioned[TaskRecord]{
		Record:       task,
		Revision:     primary.Values[0].ModRevision,
		ReadRevision: index.ReadRevision,
	}
	if _, err := repository.verifyTaskOwnerPage(ctx, etcdstore.Page[TaskRecord]{Items: []etcdstore.Versioned[TaskRecord]{result}, Revision: index.ReadRevision}, nil); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, false, err
	}
	return result, true, nil
}

func corruptControllerUpdateHistory() error {
	return errs.New(errs.KindInternal, "native Controller update history is corrupt")
}
