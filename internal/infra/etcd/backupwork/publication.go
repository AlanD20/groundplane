package backupwork

import (
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// NewItem keeps ordered sources in one operation. This preserves fail-fast
// capture and source-local resume without manufacturing a Task per source.
func NewItem(
	task workexecution.Task,
	workID, hostID string,
	input *agentpb.BackupWorkInput,
) (workexecution.Item, error) {
	if task.Validate() != nil || input == nil || task.ExpectedItems != 1 ||
		task.Scope.ProjectID != input.ProjectId || task.Scope.EnvironmentID != input.EnvironmentId {
		return workexecution.Item{}, conflict("Backup Task scope differs from its captured resources")
	}
	_, raw, reference, err := executionplan.CaptureBackupWorkInput(task.Intent.ID, input)
	defer clear(raw)
	if err != nil {
		return workexecution.Item{}, err
	}
	if task.Intent != reference || !actionMatches(task.Action, input.Operation) {
		return workexecution.Item{}, conflict("Backup Task action differs from its captured input")
	}
	item := workexecution.Item{ID: workID, TaskID: task.ID, Operation: operation(input.Operation),
		TargetID: task.TargetID, TargetName: task.TargetName, Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: hostID},
		Input: &reference, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline,
		Claims: claims(input, hostID)}
	if err := item.Validate(); err != nil {
		return workexecution.Item{}, err
	}
	return item, nil
}

// PrepareCapturePublication creates native execution records and captured input.
// The run publisher must join its frozen policy/source/credential comparisons,
// run record, exclusion locks and secret pins before committing this change.
func PrepareCapturePublication(task workexecution.Task, item workexecution.Item,
	input *agentpb.BackupWorkInput, run backupruntime.BackupRunRecord,
	marker idempotency.IdempotencyMarker,
) (workqueue.Change, error) {
	if input == nil || input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE ||
		run.TaskID != task.ID || run.State != backupruntime.BackupRunQueued ||
		run.EnvironmentID != input.EnvironmentId || task.TargetID != input.EnvironmentId ||
		!run.CreatedAt.Equal(task.CreatedAt) || !task.Deadline.Equal(run.CreatedAt.Add(6*time.Hour)) ||
		len(run.Sources) != len(input.Steps) {
		return workqueue.Change{}, conflict("Backup capture publication differs from its frozen run")
	}
	switch run.Initiator {
	case backupruntime.BackupRunInitiatorOperator:
		if task.Actor != workexecution.OperatorActor {
			return workqueue.Change{}, conflict("Manual Backup requires an operator action")
		}
	case backupruntime.BackupRunInitiatorSchedule:
		if task.Actor != workexecution.SystemActor {
			return workqueue.Change{}, conflict("Scheduled Backup requires a system action")
		}
	default:
		return workqueue.Change{}, conflict("Backup initiator is invalid")
	}
	scope := &agentpb.BackupPlanScope{ProjectId: input.ProjectId, Project: input.Project,
		EnvironmentId: input.EnvironmentId, Environment: input.Environment, Services: input.Services}
	for index, source := range run.Sources {
		if source.Ordinal != uint32(index) {
			return workqueue.Change{}, conflict("Backup capture source order changed")
		}
		if err := backupplanning.ValidateBackupRunStepAuthority(run, source, input.Steps[index], scope); err != nil {
			return workqueue.Change{}, err
		}
	}
	return PrepareExecutionPublication(task, item, input, marker)
}

// PrepareExecutionPublication initializes one captured Backup, Restore or prune
// operation. Its caller must atomically join the operation's resource admission,
// exclusions and credential pins. This function does not commit independently.
func PrepareExecutionPublication(task workexecution.Task, item workexecution.Item,
	input *agentpb.BackupWorkInput, marker idempotency.IdempotencyMarker,
) (workqueue.Change, error) {
	expected, err := NewItem(task, item.ID, item.Placement.HostID, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	if item.TaskID != expected.TaskID || item.Operation != expected.Operation || item.TargetID != expected.TargetID ||
		item.Placement != expected.Placement || item.Input == nil || *item.Input != *expected.Input ||
		!slices.Equal(item.Claims, expected.Claims) || len(item.Dependencies) != 0 ||
		item.Phase != workexecution.Ready || !item.Deadline.Equal(expected.Deadline) ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Outcome != "" ||
		item.FirstFailure != nil || item.Result != nil || item.CleanupResult != nil || item.Wait != nil || item.ForwardRevoked ||
		item.Cleanup != workexecution.CleanupNone || !item.CreatedAt.Equal(expected.CreatedAt) ||
		!item.UpdatedAt.Equal(expected.UpdatedAt) {
		return workqueue.Change{}, conflict("Backup work item differs from its initial admission")
	}
	change, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return workqueue.Change{}, err
	}
	captured, err := PrepareInput(item, input)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, captured.Conditions...)
	change.Mutations = append(change.Mutations, captured.Mutations...)
	progress, err := prepareInitialProgress(item, input)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, progress.Conditions...)
	change.Mutations = append(change.Mutations, progress.Mutations...)
	return change, nil
}

func claims(input *agentpb.BackupWorkInput, hostID string) []workexecution.Claim {
	result := []workexecution.Claim{{ScopeID: input.EnvironmentId}, {ScopeID: input.EnvironmentId, HostID: hostID}}
	for _, service := range input.Services {
		result = append(result, workexecution.Claim{ScopeID: service.ServiceId},
			workexecution.Claim{ScopeID: service.ServiceId, HostID: hostID})
	}
	return result
}

func actionMatches(action workexecution.Action, operation agentpb.BackupWorkOperation) bool {
	switch operation {
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE:
		return action == workexecution.ActionBackup
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE:
		return action == workexecution.ActionRestore
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE:
		return action == workexecution.ActionBackupPrune
	default:
		return false
	}
}
