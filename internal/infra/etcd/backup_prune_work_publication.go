package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (publication *PreparedBackupPrunePublication) PublishWork(ctx context.Context,
	task workexecution.Task, item workexecution.Item, input *agentpb.BackupWorkInput, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if publication == nil || publication.state == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Prune publication is not prepared")
	}
	state := publication.state
	state.mu.Lock()
	if state.repository == nil {
		state.mu.Unlock()
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Prune publication was consumed")
	}
	repository, plan, scope, owner, dispatch, hostID := state.repository, state.plan, state.scope, state.owner, state.dispatch, state.hostID
	state.repository, state.plan, state.scope = nil, backupPruneTransactionPlan{}, nil
	state.mu.Unlock()
	defer plan.clear()
	if input == nil || scope == nil || hostID == "" || item.Placement.HostID != hostID ||
		task.Actor != workexecution.SystemActor || task.ID != dispatch.TaskID || task.TargetID != dispatch.EnvironmentID ||
		task.Scope.TenantID != owner.TenantID || task.Scope.ProjectID != owner.ProjectID || task.Scope.EnvironmentID != owner.EnvironmentID ||
		!task.CreatedAt.Equal(dispatch.CreatedAt) || !task.Deadline.Equal(dispatch.CreatedAt.Add(30*time.Minute)) ||
		input.ProjectId != scope.ProjectId || input.EnvironmentId != scope.EnvironmentId ||
		!proto.Equal(input.Project, scope.Project) || !proto.Equal(input.Environment, scope.Environment) ||
		len(input.Services) != 0 || len(input.Artifacts) != 0 ||
		marker.Kind != idempotency.IdempotencyMarkerTask || marker.State != idempotency.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != dispatch.EnvironmentID || !marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Prune captured publication changed")
	}
	if err := backupplanning.ValidateBackupPruneWorkInput(dispatch, plan.evidence, input); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	change, err := backupwork.PrepareExecutionPublication(task, item, input, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	conditions, mutations, err := plan.composeTransaction(change.Conditions, change.Mutations)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	initiation, err := newTaskInitiation(owner, taskjournal.TaskActorSystem)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	classify := idempotencyPlanClassifier(func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "Prune retained object ownership changed before admission")
	})
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	mutation, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := mutation.enforceTransactionBounds(backupruntime.ValidateBackupRuntimeTransactionBounds); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	receipts, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return receipts.Apply(ctx, marker, mutation)
}
