package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginSecretDeletionWithTask atomically hides one Secret and publishes the
// Controller Task that owns finalization. Metadata, indexes, and ciphertext
// remain durable until the Task reaches a terminal state.
func (repository *SecretRepository) BeginSecretDeletionWithTask(
	ctx context.Context,
	owner secretrecord.Owner,
	current etcdstore.Versioned[secretrecord.Record],
	tombstone deletionrecord.DeletionTombstoneRecord,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := secretrecord.ValidateSecretOwnership(ctx, owner, current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := secretrecord.ValidateSecretVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := deletionrecord.ValidateDeletionTombstone(tombstone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	secretID := current.Record.Secret.ID
	if tombstone.TargetKind != deletionrecord.DeletionTargetSecret || tombstone.TargetID != secretID ||
		tombstone.TargetRevision != current.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != deletionrecord.DeletionPhaseFinalizing || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(
			tombstone.CreatedAt,
		) || task.Executor != taskjournal.TaskExecutorController ||
		task.Type != taskjournal.TaskRemove || task.Target != secretID || task.Status != taskjournal.TaskStatusPending ||
		len(task.Params) != 1 || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceSecret {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Secret deletion Task and tombstone do not match",
		)
	}
	expectedScope, expectedScopeID := secretIdempotencyScope(owner)
	wantReplayTarget := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetSecret, ID: secretID}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != expectedScope ||
		marker.Locator.ScopeID != expectedScopeID || marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Secret deletion marker does not match its Task",
		)
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
	if dependencies == nil || len(dependencies.Values) != 3 || dependencies.Values[0] == nil ||
		dependencies.Values[1] == nil || dependencies.Values[2] == nil ||
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

	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := ValidateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
	taskValue, err := EncodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)

	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
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
	conditions = append(conditions, etcdstore.Condition{
		Key: componentSecretReferencePrefix(secretID), Prefix: true,
	})
	conditions = append(conditions, secretScriptAbsenceConditions(secretID)...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
			Value: reference,
		},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
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
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classifySecretDeletionStartConflict(owner, current, task.OperationID),
	)
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

func classifySecretDeletionStartConflict(
	owner secretrecord.Owner,
	current etcdstore.Versioned[secretrecord.Record],
	operationID string,
) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		guardCount := len(secretScriptAbsenceConditions(current.Record.Secret.ID))
		expected := 10 + guardCount
		if owner.Project != nil {
			expected += 2
			if owner.Project.Record.TenantID != "" {
				expected++
			}
		}
		if len(values) != expected {
			return errs.New(errs.KindInternal, "Secret deletion compare evidence is incomplete")
		}
		guardPosition := len(values) - guardCount
		if err := classifySecretScriptReferences(current.Record.Secret.ID, values[guardPosition:]); err != nil {
			return err
		}
		referencePosition := guardPosition - 1
		if values[referencePosition] != nil {
			return errs.New(errs.KindResourceInUse, "Secret is referenced by a Component")
		}
		if values[2] != nil {
			activeTaskID, err := idempotencyrecord.DecodeTaskReference(values[2].Value)
			if err != nil {
				return err
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				operationID,
				activeTaskID,
			)
		}
		for _, index := range []int{0, 1, 3} {
			if values[index] != nil {
				return errs.New(
					errs.KindInternal,
					"Secret deletion collided with durable Task state",
				)
			}
		}
		if values[4] == nil {
			return errs.New(errs.KindSecretNotFound, "Secret was not found")
		}
		if values[4].ModRevision != current.Revision {
			return recordcodec.StateConflict("secret", current.Record.Secret.ID)
		}
		for _, index := range []int{5, 6} {
			if values[index] == nil || string(values[index].Value) != current.Record.Secret.ID {
				return errs.New(errs.KindInternal, "Secret deletion index changed or is corrupt")
			}
		}
		if values[7] == nil {
			return errs.New(errs.KindInternal, "Secret encrypted value disappeared during deletion")
		}
		if values[8] != nil {
			return errs.New(errs.KindResourceInUse, "Secret deletion is already in progress")
		}
		position := 9
		if owner.Project != nil {
			if values[position] == nil {
				return errs.New(errs.KindProjectNotFound, "project was not found")
			}
			if values[position].ModRevision != owner.Project.Revision {
				return recordcodec.StateConflict("project", owner.Project.Record.ID)
			}
			position++
			for ; position < referencePosition; position++ {
				if values[position] != nil {
					return errs.New(errs.KindResourceInUse, "Secret owner deletion is in progress")
				}
			}
		}
		return errs.New(errs.KindStateConflict, "Secret deletion state changed")
	}
}
