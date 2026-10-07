package backup

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RetryRestoreTask creates a new execution from the original selected Point.
// Selection may resolve current records only when they still equal the sealed
// source authority; BindRetry fences both source and current admission state.
func (service *RestoreService) RetryRestoreTask(ctx context.Context,
	sourceTaskID, retryTaskID string, marker idempotency.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	if service == nil || service.runtime == nil {
		return etcd.IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Restore retry is not configured")
	}
	source, err := service.runtime.PrepareRestoreRetrySource(ctx, sourceTaskID, retryTaskID, marker.CreatedAt)
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	request := apiTypes.RestoreRequest{SourceID: source.Restore.Point.SourceID,
		RecoveryPointID: source.Restore.Point.ID}
	prepared, err := service.prepareRestore(ctx, source.Restore.EnvironmentID, retryTaskID,
		source.Restore.OperationID, request, marker.CreatedAt, false)
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	defer prepared.Publication.Clear()
	if err := prepared.Publication.BindRetry(source); err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	prepared.Scope.TaskAttempt = source.Plan.BackupScope.TaskAttempt + 1
	task := etcd.TaskRecord{ID: retryTaskID, OperationID: source.Task.OperationID,
		RetryOf: sourceTaskID, Owner: source.Task.Owner,
		Actor: taskjournal.TaskActorOperator, Executor: taskjournal.TaskExecutorAgent,
		PlanID: ids.New(ids.KindPlan), Type: taskjournal.TaskRestore,
		Target: source.Restore.EnvironmentID,
		Steps: []taskjournal.TaskStepRecord{
			{ID: prepared.Authority.StepId, Kind: taskjournal.TaskStepOperation},
		},
		TimeoutSeconds: backupRunTaskTimeoutSeconds, Status: taskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: marker.CreatedAt, UpdatedAt: marker.CreatedAt}
	sealed, err := prepared.buildPlan(task)
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)
	task.Steps = taskjournal.CaptureStepDescriptions(task.Steps, sealed.Steps)
	return prepared.Publication.Publish(ctx, task, sealed, marker)
}
