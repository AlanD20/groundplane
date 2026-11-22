package agentregistration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// MaintenanceExecution fixes the credential-owned host binding and the
// independently versioned executor process to one storage revision.
type MaintenanceExecution struct {
	Binding           MaintenanceBinding
	Identity          workexecution.Identity
	ProcessGeneration [16]byte
	ExecutorRevision  int64
}

func (repository *Repository) MaintenanceExecution(
	ctx context.Context,
	agentID string,
	credentialGeneration uint64,
) (MaintenanceExecution, error) {
	binding, err := repository.MaintenanceBinding(ctx, agentID, credentialGeneration)
	if err != nil {
		return MaintenanceExecution{}, err
	}
	key := workqueue.ExecutorKey(workexecution.Identity{
		Executor: workexecution.Agent, HostID: binding.HostID, AgentID: agentID, Generation: 1,
	})
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{key}, Revision: binding.ReadRevision,
	})
	if err != nil {
		return MaintenanceExecution{}, err
	}
	if read == nil || read.ReadRevision != binding.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return MaintenanceExecution{}, errs.New(
			errs.KindStateConflict,
			"Agent maintenance executor is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0].Key != key || read.Values[0].ModRevision <= 0 ||
		read.Values[0].ModRevision > read.ReadRevision {
		return MaintenanceExecution{}, errs.New(
			errs.KindStateConflict,
			"Agent maintenance executor revision is invalid",
		)
	}
	executor, err := workqueue.DecodeExecutor(read.Values[0].Value)
	if err != nil {
		return MaintenanceExecution{}, err
	}
	if executor.Identity.Executor != workexecution.Agent || executor.Identity.HostID != binding.HostID ||
		executor.Identity.AgentID != agentID || executor.ProcessGeneration == [16]byte{} {
		return MaintenanceExecution{}, errs.New(
			errs.KindStateConflict,
			"Agent maintenance executor differs from its credential-owned host",
		)
	}
	return MaintenanceExecution{Binding: binding, Identity: executor.Identity,
		ProcessGeneration: executor.ProcessGeneration, ExecutorRevision: read.Values[0].ModRevision}, nil
}
