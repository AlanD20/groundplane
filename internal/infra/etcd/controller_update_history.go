package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const controllerUpdateHistoryPrefix = "/v1/indexes/tasks/controller-updates/"

func isNativeControllerUpdate(task TaskRecord) bool {
	return task.Executor == TaskExecutorController && task.Type == TaskUpdate && task.Target == "controller" &&
		task.Owner == PlatformTaskOwner() && task.Params[TaskResourceKindParam] == TaskResourceController
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

func (repository *TaskRepository) LatestControllerUpdate(ctx context.Context) (Versioned[TaskRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[TaskRecord]{}, false, err
	}
	index, err := repository.store.Range(
		ctx,
		etcdstore.RangeRequest{Prefix: controllerUpdateHistoryPrefix, Limit: 1, Descending: true},
	)
	if err != nil {
		return Versioned[TaskRecord]{}, false, err
	}
	if index == nil {
		return Versioned[TaskRecord]{}, false, corruptControllerUpdateHistory()
	}
	if len(index.Values) == 0 {
		return Versioned[TaskRecord]{}, false, nil
	}
	if len(index.Values) != 1 || index.ReadRevision <= 0 {
		return Versioned[TaskRecord]{}, false, corruptControllerUpdateHistory()
	}
	entry := index.Values[0]
	id := strings.TrimPrefix(entry.Key, controllerUpdateHistoryPrefix)
	if !strings.HasPrefix(entry.Key, controllerUpdateHistoryPrefix) || ids.Validate(ids.KindTask, id) != nil ||
		string(entry.Value) != id {
		return Versioned[TaskRecord]{}, false, corruptControllerUpdateHistory()
	}
	primary, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{taskKey(id)}, Revision: index.ReadRevision},
	)
	if err != nil {
		return Versioned[TaskRecord]{}, false, err
	}
	if primary == nil || primary.ReadRevision != index.ReadRevision || len(primary.Values) != 1 ||
		primary.Values[0] == nil ||
		primary.Values[0].Key != taskKey(id) {
		return Versioned[TaskRecord]{}, false, corruptControllerUpdateHistory()
	}
	task, err := decodeTaskRecord(primary.Values[0].Value)
	if err != nil {
		return Versioned[TaskRecord]{}, false, err
	}
	if task.ID != id || !isNativeControllerUpdate(task) {
		return Versioned[TaskRecord]{}, false, corruptControllerUpdateHistory()
	}
	result := Versioned[TaskRecord]{
		Record:       task,
		Revision:     primary.Values[0].ModRevision,
		ReadRevision: index.ReadRevision,
	}
	if _, err := repository.verifyTaskOwnerPage(ctx, Page[TaskRecord]{Items: []Versioned[TaskRecord]{result}, Revision: index.ReadRevision}, nil); err != nil {
		return Versioned[TaskRecord]{}, false, err
	}
	return result, true, nil
}

func corruptControllerUpdateHistory() error {
	return errs.New(errs.KindInternal, "native Controller update history is corrupt")
}
