package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type restoreWorkPublication struct {
	hostID     string
	owner      taskjournal.TaskOwner
	restore    backupruntime.BackupRestoreRecord
	scope      *agentpb.BackupPlanScope
	authority  *agentpb.BackupStepAuthority
	artifacts  []*agentpb.ComposeArtifact
	conditions []keyvalue.Condition
	mutations  []keyvalue.Mutation
}

func (repository *BackupRuntimeRepository) publishRestoreWork(ctx context.Context,
	captured restoreWorkPublication, task workexecution.Task, item workexecution.Item,
	input *agentpb.BackupWorkInput, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	defer keyvalue.ClearMutationValues(captured.mutations)
	invalid := func() (IdempotencyTransactionResult, error) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Restore captured publication changed",
		)
	}
	if input == nil || captured.scope == nil || captured.hostID == "" || item.Placement.HostID != captured.hostID ||
		task.Scope.TenantID != captured.owner.TenantID || task.Scope.ProjectID != captured.owner.ProjectID ||
		task.Scope.EnvironmentID != captured.owner.EnvironmentID || input.ProjectId != captured.scope.ProjectId ||
		input.EnvironmentId != captured.scope.EnvironmentId || !proto.Equal(input.Project, captured.scope.Project) ||
		!proto.Equal(
			input.Environment,
			captured.scope.Environment,
		) || len(input.Services) != len(captured.scope.Services) ||
		len(
			input.Steps,
		) != 1 || !proto.Equal(input.Steps[0], captured.authority) || len(input.Artifacts) != len(captured.artifacts) {
		return invalid()
	}
	for index, fact := range captured.scope.Services {
		if !proto.Equal(fact, input.Services[index]) {
			return invalid()
		}
	}
	for index, artifact := range captured.artifacts {
		if !proto.Equal(artifact, input.Artifacts[index]) {
			return invalid()
		}
	}
	if marker.Kind != idempotency.IdempotencyMarkerTask || marker.State != idempotency.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != captured.restore.EnvironmentID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return invalid()
	}
	change, err := backupwork.PrepareRestorePublication(task, item, input, captured.restore, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	conditions := append(change.Conditions, captured.conditions...)
	mutations := append(keyvalue.CloneMutations(change.Mutations), keyvalue.CloneMutations(captured.mutations)...)
	defer keyvalue.ClearMutationValues(mutations)
	initiation, err := newTaskInitiation(captured.owner, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	classify := idempotencyPlanClassifier(func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(
			errs.KindStateConflict,
			"Restore source or destination changed before admission; submit a new action",
		)
	})
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := plan.enforceTransactionBounds(backupruntime.ValidateBackupRuntimeTransactionBounds); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	receipts, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return receipts.Apply(ctx, marker, plan)
}
