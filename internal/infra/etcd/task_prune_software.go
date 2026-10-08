package etcd

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activation "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	preparation "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Child history remains execution proof until its software parent settles.
// The stable child key encodes its parent; no scan of every activation is needed.
func (repository *TaskRepository) softwareChildPruneAuthority(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (bool, []etcdstore.Condition, error) {
	if task.Type != taskjournal.TaskUpdate || task.Executor != taskjournal.TaskExecutorController {
		return false, nil, nil
	}
	resource := task.Params[taskjournal.TaskResourceKindParam]
	if resource != taskjournal.TaskResourceController && resource != taskjournal.TaskResourceAgent {
		return false, nil, nil
	}
	parentID := strings.TrimSuffix(strings.TrimPrefix(task.IdempotencyKey, "software-"), "-"+resource)
	if task.IdempotencyKey != "software-"+parentID+"-"+resource || ids.Validate(ids.KindTask, parentID) != nil {
		return false, nil, nil
	}
	key := taskjournal.TaskStorageKey(parentID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return false, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return false, nil, errs.New(errs.KindInternal, "software parent prune evidence is missing")
	}
	defer etcdstore.ClearValues(read.Values)
	condition := etcdstore.Condition{Key: key}
	if read.Values[0] == nil {
		return false, []etcdstore.Condition{condition}, nil
	}
	parent, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil || parent.ID != parentID ||
		parent.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceSoftware ||
		parent.Executor != taskjournal.TaskExecutorSoftware {
		return false, nil, errs.New(errs.KindInternal, "software parent prune authority is inconsistent")
	}
	condition.ModRevision = read.Values[0].ModRevision
	return !taskjournal.IsTerminalTaskStatus(parent.Status), []etcdstore.Condition{condition}, nil
}

func (repository *TaskRepository) prepareSoftwareProjectionPrune(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	var key string
	switch task.Params[taskjournal.TaskResourceKindParam] {
	case taskjournal.TaskResourceSoftwarePreparation:
		key = preparation.Key(task.ID)
	case taskjournal.TaskResourceSoftware:
		key = activation.Key(task.ID)
	default:
		return nil, nil, nil
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, nil, errs.New(errs.KindInternal, "software prune projection is missing")
	}
	defer etcdstore.ClearValues(read.Values)
	if task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceSoftwarePreparation {
		record, err := preparation.Decode(read.Values[0].Value)
		if err != nil || record.TaskID != task.ID || record.OperationID != task.OperationID ||
			record.InputSHA256 != task.PlanHash {
			return nil, nil, errs.New(errs.KindInternal, "software preparation prune authority differs")
		}
	} else {
		record, err := activation.Decode(read.Values[0].Value)
		if err != nil || record.TaskID != task.ID || record.OperationID != task.OperationID || record.InputSHA256 != task.PlanHash || record.Progress.Phase != activation.PhaseCompleted && record.Progress.Phase != activation.PhaseFailed {
			return nil, nil, errs.New(errs.KindInternal, "software activation prune authority differs")
		}
	}
	conditions := []etcdstore.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: key}}
	return conditions, mutations, nil
}
