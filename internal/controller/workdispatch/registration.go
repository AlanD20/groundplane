package workdispatch

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/controllerownership"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RegisterController is called once under the Controller's exclusive process
// ownership barrier. It leaves dispatch disabled. Neither a successful CAS nor
// a newer generation proves that an external operation from the old process has
// stopped; reconciliation must account for its retained slots and claims.
// On an uncertain transaction response, fail startup rather than blindly create
// another generation. This method does not adopt an identity by reading it back.
func (runtime *Runtime) RegisterController(ctx context.Context, ownership *controllerownership.Lease, capacity uint16) (workexecution.Identity, error) {
	if err := ownership.Check(); err != nil {
		return workexecution.Identity{}, err
	}
	placement := workexecution.Placement{Executor: workexecution.Controller}
	previous, err := runtime.queue.ReadExecutor(ctx, placement)
	if err != nil {
		return workexecution.Identity{}, err
	}
	operations := make([]workexecution.Operation, 0, len(runtime.controller.owners))
	for operation := range runtime.controller.owners {
		if runtime.scheduler.owners[admissionKey{operation, workexecution.Controller}] == nil {
			return workexecution.Identity{}, errs.New(errs.KindInternal, "controller operation has no scheduling owner")
		}
		operations = append(operations, operation)
	}
	for _, operation := range runtime.scheduler.decisions {
		if runtime.controller.owners[operation] != nil {
			return workexecution.Identity{}, errs.New(errs.KindInternal, "Controller decision also has an execution owner")
		}
		operations = append(operations, operation)
	}
	slices.Sort(operations)
	change, record, err := workqueue.PrepareRegistration(previous, workqueue.ExecutorRecord{
		Identity: workexecution.Identity{Executor: workexecution.Controller}, Capacity: capacity, Operations: operations,
	})
	if err != nil {
		return workexecution.Identity{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	result, err := runtime.queue.Commit(ctx, change)
	if err != nil {
		return workexecution.Identity{}, err
	}
	if !result.Succeeded {
		return workexecution.Identity{}, errs.New(errs.KindStateConflict, "controller executor registration changed during startup")
	}
	return record.Identity, nil
}
