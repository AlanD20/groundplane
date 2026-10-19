package agentregistration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareWorkUnavailable joins credential revocation, so deleting an Agent
// cannot leave a dispatchable executor. It deliberately retains every slot.
func (repository *Repository) prepareWorkUnavailable(
	ctx context.Context,
	agentID string,
	revision int64,
) (workqueue.Change, error) {
	binding, err := hostbindings.Read(ctx, repository.store, agentID, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := workqueue.ExecutorKey(workexecution.Identity{Executor: workexecution.Agent, HostID: binding.Binding.HostID})
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: binding.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != binding.ReadRevision || len(read.Values) != 1 {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Agent executor read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{Conditions: append(binding.Conditions, keyvalue.Condition{Key: key})}, nil
	}
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Agent executor revision is invalid")
	}
	record, err := workqueue.DecodeExecutor(value.Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if record.Identity.Executor != workexecution.Agent || record.Identity.HostID != binding.Binding.HostID ||
		record.Identity.AgentID != agentID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Agent does not own the registered host executor")
	}
	change, err := workqueue.PrepareAvailability(
		workqueue.ExecutorSnapshot{Record: record, Revision: value.ModRevision},
		record.Identity,
		false,
		record.Capacity,
		record.UnavailableOperations,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, binding.Conditions...)
	return change, nil
}
