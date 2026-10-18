package etcd

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secretremoval"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishSecretRemoval atomically hides one Secret and publishes the
// Controller Task that owns finalization. Metadata, indexes, and ciphertext
// remain durable until the Task reaches a terminal state.
func (repository *SecretRepository) PublishSecretRemoval(
	ctx context.Context,
	owner secretrecord.Owner,
	current etcdstore.Versioned[secretrecord.Record],
	input secretremoval.Input,
	task workexecution.Task,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := secretrecord.ValidateSecretOwnership(ctx, owner, current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := secretrecord.ValidateSecretVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	secretID := current.Record.Secret.ID
	ref, err := input.Reference()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	scope := workexecution.Scope{}
	if owner.Project != nil {
		scope = workexecution.Scope{TenantID: owner.Project.Record.TenantID, ProjectID: owner.Project.Record.ID}
	}
	if task.Validate() != nil || task.Action != workexecution.ActionRemove || task.Actor != workexecution.OperatorActor ||
		task.TargetID != secretID || task.TargetName != current.Record.Secret.Key || task.Scope != scope || task.ExpectedItems != 1 ||
		input.TaskID != task.ID || input.SecretID != secretID || input.SecretRevision != current.Revision ||
		input.ProjectID != scope.ProjectID || input.TenantID != scope.TenantID || task.Intent != ref || !input.CreatedAt.Equal(task.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Secret removal differs from its captured input")
	}
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetSecret, TargetID: secretID, TargetRevision: current.Revision,
		TaskID: task.ID, Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt}
	expectedScope, expectedScopeID := secretIdempotencyScope(owner)
	wantReplayTarget := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetSecret,
		ID:   secretID,
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.Method != http.MethodDelete || marker.Locator.Route != "/secrets/{id}" ||
		!idempotencyrecord.ValidTaskResponse(marker.Response, task.ID) ||
		marker.Locator.ScopeKind != expectedScope ||
		marker.Locator.ScopeID != expectedScopeID ||
		marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Secret deletion marker does not match its Task",
		)
	}

	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	dependencies, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			secretrecord.SecretOwnerKey(current.Record.Secret),
			secretrecord.SecretScopedKey(current.Record.Secret),
			secretrecord.ValueKey(secretID),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if dependencies != nil {
		defer etcdstore.ClearValues(dependencies.Values)
	}
	if dependencies == nil || dependencies.ReadRevision != current.ReadRevision || len(dependencies.Values) != 3 || dependencies.Values[0] == nil ||
		dependencies.Values[1] == nil || dependencies.Values[2] == nil || dependencies.Values[2].ModRevision != input.ValueRevision ||
		string(
			dependencies.Values[0].Value,
		) != secretID || string(dependencies.Values[1].Value) != secretID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Secret deletion indexes or encrypted value are corrupt",
		)
	}
	encrypted, err := secretrecord.DecodeEncryptedValue(dependencies.Values[2].Value)
	if err != nil || encrypted.SecretID != secretID {
		clear(encrypted.Ciphertext)
		return IdempotencyTransactionResult{}, secretrecord.CorruptRecord()
	}
	clear(encrypted.Ciphertext)

	item := workexecution.Item{ID: input.WorkItemID, TaskID: task.ID, Operation: workexecution.SecretRemoval,
		TargetID: secretID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: secretID}}, Input: &ref, Phase: workexecution.Ready,
		Cleanup: workexecution.CleanupNone, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline}
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearMutationValues(work.Mutations)
	captureConditions, captureWrites, err := secretremoval.PreparePublication(input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearMutationValues(captureWrites)
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
	conditions := []etcdstore.Condition{
		{Key: secretrecord.RecordKey(secretID), ModRevision: current.Revision},
		{
			Key:         secretrecord.SecretOwnerKey(current.Record.Secret),
			ModRevision: dependencies.Values[0].ModRevision,
		},
		{
			Key:         secretrecord.SecretScopedKey(current.Record.Secret),
			ModRevision: dependencies.Values[1].ModRevision,
		},
		{Key: secretrecord.ValueKey(secretID), ModRevision: dependencies.Values[2].ModRevision},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetSecret), secretID)},
	}
	if owner.Project != nil {
		conditions = append(
			conditions,
			etcdstore.Condition{
				Key:         hierarchyrecord.ProjectKey(owner.Project.Record.ID),
				ModRevision: owner.Project.Revision,
			},
			etcdstore.Condition{
				Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), owner.Project.Record.ID),
			},
		)
		if owner.Project.Record.TenantID != "" {
			conditions = append(conditions, etcdstore.Condition{
				Key: deletionrecord.TombstoneKey(
					string(deletionrecord.DeletionTargetTenant),
					owner.Project.Record.TenantID,
				),
			})
		}
	}
	references, err := secretremoval.PrepareReferenceAbsence(ctx, repository.store, secretID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions = append(conditions, references...)
	mutations := []etcdstore.Mutation{
		{
			Type: etcdstore.MutationPut, Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetSecret), secretID),
			Value: tombstoneValue,
		},
	}
	var initiation TaskInitiation
	if owner.Project == nil {
		initiation, err = newPlatformTaskInitiation(taskjournal.TaskActorOperator)
	} else {
		taskTenant, tenantErr := loadTaskInitiationTenant(ctx, repository.store, *owner.Project)
		if tenantErr != nil {
			return IdempotencyTransactionResult{}, tenantErr
		}
		initiation, err = newProjectTaskInitiation(taskTenant, *owner.Project, taskjournal.TaskActorOperator)
	}
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions = append(conditions, work.Conditions...)
	conditions = append(conditions, captureConditions...)
	mutations = append(mutations, work.Mutations...)
	mutations = append(mutations, captureWrites...)
	classify := idempotencyPlanClassifier(func(_ int64, _ []*etcdstore.KeyValue) error {
		return errs.New(errs.KindStateConflict, "Secret removal references or ownership changed; refresh before removing")
	})
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newIdempotencyMutationPlanForMarker(idempotencyrecord.IdempotencyMarkerTask, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func secretIdempotencyScope(owner secretrecord.Owner) (idempotencyrecord.IdempotencyScopeKind, string) {
	if owner.Project == nil {
		return idempotencyrecord.IdempotencyScopePlatform, "-"
	}
	return idempotencyrecord.IdempotencyScopeProject, owner.Project.Record.ID
}
