package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) PrepareUnstartedRunnerCreation(ctx context.Context,
	input keyvalue.Versioned[runnerwork.Input], item workexecution.Item,
) (workqueue.Change, error) {
	if input.Revision <= 0 || input.Record.MatchWork(item) != nil ||
		input.Record.Operation != runnerallocation.RuntimeOperationCreate ||
		item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone ||
		(item.Phase != workexecution.Ready && item.Phase != workexecution.Waiting) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner creation has execution effects to resolve")
	}
	current, err := repository.GetRunner(ctx, input.Record.Desired.ID)
	if err != nil {
		return workqueue.Change{}, err
	}
	record := current.Record
	if record.CreateTaskID != item.TaskID || record.ProvisioningState != runners.RunnerProvisioningProvisioning ||
		record.ContainerID != "" || record.Allocation != input.Record.Plan.Allocation || !runnerCreationDesiredEqual(record.Desired, input.Record.Desired) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner creation no longer owns this allocation")
	}
	record.ProvisioningState = runners.RunnerProvisioningFailed
	value, err := runners.EncodeRunnerLifecycleRecord(record.RunnerLifecycleRecord)
	if err != nil {
		return workqueue.Change{}, err
	}
	id := record.Desired.ID
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: runnerwork.InputKey(item.ID), ModRevision: input.Revision},
		{Key: runners.RunnerKey(id), ModRevision: current.Revision},
		{Key: runners.RunnerLifecycleKey(id), ModRevision: record.LifecycleRevision},
		{Key: runners.RunnerRuntimeOwnershipKey(id)}, {Key: runnerwork.AppliedKey(id)},
		{Key: deletions.TombstoneKey(string(deletions.DeletionTargetRunner), id)},
	}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: runners.RunnerLifecycleKey(id), Value: value}}}, nil
}
