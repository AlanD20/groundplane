package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// workPublication joins the captured resource transaction with native execution
// admission. There is no separately committed queue record and no generic plan.
func (plan backupRunPublicationPlan) workPublication(task workexecution.Task, item workexecution.Item,
	input *agentpb.BackupWorkInput, marker idempotency.IdempotencyMarker, initiation TaskInitiation,
) (*idempotencyMutationPlan, error) {
	if task.Scope.TenantID != initiation.owner.TenantID || task.Scope.ProjectID != initiation.owner.ProjectID ||
		task.Scope.EnvironmentID != initiation.owner.EnvironmentID ||
		(task.Actor == workexecution.OperatorActor && initiation.actor != taskjournal.TaskActorOperator) ||
		(task.Actor == workexecution.SystemActor && initiation.actor != taskjournal.TaskActorSystem) {
		return nil, errs.New(errs.KindValidationFailed, "Backup publication initiating scope changed")
	}
	if marker.Kind != idempotency.IdempotencyMarkerTask || marker.State != idempotency.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != plan.record.EnvironmentID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return nil, errs.New(errs.KindValidationFailed, "Backup publication request identity changed")
	}
	change, err := backupwork.PrepareCapturePublication(task, item, input, plan.record, marker)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	conditions, mutations, err := plan.composeTransaction(change.Conditions, change.Mutations)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	classify := idempotencyPlanClassifier(func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "Backup sources or policy changed before admission; submit a new action")
	})
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return nil, err
	}
	publication, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations, classify)
	if err != nil {
		return nil, err
	}
	if err := publication.enforceTransactionBounds(backupruntime.ValidateBackupRuntimeTransactionBounds); err != nil {
		return nil, err
	}
	return publication, nil
}
