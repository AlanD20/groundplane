package etcd

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishBackupKeyRotation atomically appends the real Task and pending
// idempotency marker to the prepared rotation transaction.
func (repository *BackupPolicyRepository) PublishBackupKeyRotation(
	ctx context.Context,
	prepared PreparedBackupKeyRotation,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	publication := prepared.Publication
	if publication == nil || publication.state == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"backup key rotation publication is not prepared",
		)
	}
	publication.state.mu.Lock()
	belongsToRepository := publication.state.repository == repository
	publication.state.mu.Unlock()
	if !belongsToRepository {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backup key rotation publication repository is invalid",
		)
	}
	return publication.publish(ctx, task, marker)
}

func (publication *PreparedBackupKeyRotationPublication) publish(
	ctx context.Context, task TaskRecord, marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if publication == nil || publication.state == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"backup key rotation publication is not prepared",
		)
	}
	publication.state.mu.Lock()
	if publication.state.consumed || publication.state.repository == nil {
		publication.state.mu.Unlock()
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation publication was already consumed",
		)
	}
	publication.state.consumed = true
	repository, plan := publication.state.repository, publication.state.plan
	publication.state.repository = nil
	publication.state.plan = backupKeyRotationPublicationPlan{}
	publication.state.mu.Unlock()
	defer plan.clear()
	if task.Type != taskjournal.TaskRotate || task.Executor != taskjournal.TaskExecutorController || task.Status != taskjournal.TaskStatusPending ||
		task.Target != plan.record.EnvironmentID || task.ID != plan.record.TaskID || task.OperationID != plan.record.OperationID ||
		task.Owner.EnvironmentID != plan.record.EnvironmentID || task.CreatedAt.UTC() != plan.record.CreatedAt ||
		marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backup key rotation Task publication is invalid",
		)
	}
	if err := ValidateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newTaskInitiation(task.Owner, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	task = cloneTaskRecord(task)
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
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
	taskConditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID)}, {Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{
			Key: taskjournal.TaskActiveOperationKey(task.OperationID),
		}, {Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
	}
	conditions := append(taskConditions, plan.conditions...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
			Value: reference,
		},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
	}
	for _, mutation := range plan.mutations {
		copyOf := mutation
		copyOf.Value = append([]byte(nil), mutation.Value...)
		mutations = append(mutations, copyOf)
	}
	classify := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "backup key rotation publication evidence is incomplete")
		}
		return errs.New(errs.KindStateConflict, "backup key rotation publication state changed")
	}
	mutationPlan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations, classify)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	return (&IdempotencyRepository{store: repository.store}).Apply(ctx, marker, mutationPlan)
}

func (plan *backupKeyRotationPublicationPlan) clear() {
	if plan == nil {
		return
	}
	etcdstore.ClearMutationValues(plan.mutations)
	clear(plan.record.NextEncryptedIdentity)
	plan.mutations = nil
}
