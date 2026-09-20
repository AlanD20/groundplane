package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	corebackup "github.com/AlanD20/groundplane/internal/core/backup"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backupKeyRotationTimeoutSeconds = corebackup.KeyRotationTimeoutSeconds

// BackupKeyRotationInput contains only freshly allocated operation identity.
// The repository derives all hierarchy and key evidence at one MVCC revision.
type BackupKeyRotationInput struct {
	EnvironmentID string
	TaskID        string
	OperationID   string
	PlanID        string
	CreatedAt     time.Time
}

// PreparedBackupKeyRotation is one-shot authority for publishing a rotation.
// Its wrapped identity is safe to retain durably; no plaintext identity is
// present in this value.
type PreparedBackupKeyRotation struct {
	Owner       TaskOwner
	Publication *PreparedBackupKeyRotationPublication
}

// Clear abandons unused prepared authority and erases its wrapped ciphertext.
func (prepared *PreparedBackupKeyRotation) Clear() {
	if prepared == nil {
		return
	}
	prepared.Publication.Clear()
	prepared.Publication = nil
	prepared.Owner = TaskOwner{}
}

type backupKeyRotationPublicationPlan struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	record     BackupKeyRotationRecord
}

type preparedBackupKeyRotationState struct {
	mu         sync.Mutex
	repository *BackupPolicyRepository
	plan       backupKeyRotationPublicationPlan
	consumed   bool
}

// PreparedBackupKeyRotationPublication is one-shot publication authority.
type PreparedBackupKeyRotationPublication struct {
	state *preparedBackupKeyRotationState
}

// Clear abandons a prepared publication and erases its wrapped ciphertext buffer.
func (publication *PreparedBackupKeyRotationPublication) Clear() {
	if publication == nil || publication.state == nil {
		return
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	publication.state.plan.clear()
	publication.state.repository = nil
	publication.state.consumed = true
}

// PrepareBackupKeyRotation captures current key revisions and acquires no
// lock. Publish is the single transaction that creates the task, rotation
// authority, and Environment lock.
func (repository *BackupPolicyRepository) PrepareBackupKeyRotation(
	ctx context.Context,
	input BackupKeyRotationInput,
	material BackupPolicyInitialKeyMaterial,
) (PreparedBackupKeyRotation, error) {
	if repository == nil || repository.store == nil {
		return PreparedBackupKeyRotation{}, errs.New(errs.KindInternal, "backup policy repository is not configured")
	}
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return PreparedBackupKeyRotation{}, err
	}
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, input.TaskID) != nil ||
		ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		ids.Validate(ids.KindPlan, input.PlanID) != nil || !backuppolicy.ValidUTCInstant(input.CreatedAt) {
		return PreparedBackupKeyRotation{}, errs.New(errs.KindValidationFailed, "backup key rotation input is invalid")
	}
	if backuppolicy.ValidateBackupKeyRecord(backuppolicy.BackupKeyRecord{
		EnvironmentID: input.EnvironmentID, Recipient: material.Recipient, KeyEra: 1,
		CreatedAt: input.CreatedAt, RotatedAt: input.CreatedAt,
	}) != nil || len(material.Ciphertext) == 0 || len(material.Ciphertext) > backuppolicy.MaximumKeyCiphertextLen {
		return PreparedBackupKeyRotation{}, errs.New(
			errs.KindValidationFailed,
			"backup key rotation material is invalid",
		)
	}
	anchor, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		hierarchyrecord.EnvironmentKey(input.EnvironmentID), backuppolicy.BackupKeyKey(input.EnvironmentID), backuppolicy.BackupKeyValueKey(input.EnvironmentID),
	}})
	if err != nil {
		return PreparedBackupKeyRotation{}, err
	}
	if anchor == nil || len(anchor.Values) != 3 {
		return PreparedBackupKeyRotation{}, errs.New(errs.KindInternal, "backup key rotation evidence is incomplete")
	}
	defer clearKeyValues(anchor.Values)
	if anchor.Values[0] == nil || anchor.Values[1] == nil || anchor.Values[2] == nil {
		return PreparedBackupKeyRotation{}, errs.New(errs.KindStateConflict, "backup encryption key is not configured")
	}
	environment, err := hierarchyrecord.DecodeEnvironment(anchor.Values[0].Value)
	if err != nil || environment.ID != input.EnvironmentID {
		return PreparedBackupKeyRotation{}, errs.New(errs.KindInternal, "backup key rotation Environment is corrupt")
	}
	current, err := backuppolicy.DecodeBackupKeyRecord(anchor.Values[1].Value)
	if err != nil {
		return PreparedBackupKeyRotation{}, corruptBackupKey()
	}
	currentValue, err := backuppolicy.DecodeBackupKeyEncryptedValue(anchor.Values[2].Value)
	if err != nil {
		return PreparedBackupKeyRotation{}, corruptBackupKey()
	}
	defer clear(currentValue.Ciphertext)
	if current.EnvironmentID != input.EnvironmentID || currentValue.EnvironmentID != input.EnvironmentID ||
		current.KeyEra != currentValue.KeyEra {
		return PreparedBackupKeyRotation{}, corruptBackupKey()
	}
	fence, err := loadOrdinaryEnvironmentMutationFence(ctx, repository.store, input.EnvironmentID, anchor.ReadRevision)
	if err != nil {
		return PreparedBackupKeyRotation{}, err
	}
	projectRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{hierarchyrecord.ProjectKey(environment.ProjectID)}, Revision: anchor.ReadRevision,
	})
	if err != nil || projectRead == nil || len(projectRead.Values) != 1 || projectRead.Values[0] == nil {
		return PreparedBackupKeyRotation{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation Project is unavailable",
		)
	}
	defer clearKeyValues(projectRead.Values)
	project, err := hierarchyrecord.DecodeProject(projectRead.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID {
		return PreparedBackupKeyRotation{}, errs.New(errs.KindInternal, "backup key rotation Project is corrupt")
	}
	owner, err := EnvironmentTaskOwner(project, environment)
	if err != nil {
		return PreparedBackupKeyRotation{}, err
	}
	indexKey, err := backupKeyRotationEnvironmentIndexKey(input.EnvironmentID, input.TaskID)
	if err != nil {
		return PreparedBackupKeyRotation{}, err
	}
	record := BackupKeyRotationRecord{
		TaskID: input.TaskID, OperationID: input.OperationID, EnvironmentID: input.EnvironmentID,
		ExpectedCurrentRecordRevision: anchor.Values[1].ModRevision,
		ExpectedCurrentValueRevision:  anchor.Values[2].ModRevision,
		CurrentKeyEra:                 current.KeyEra, NextKeyEra: current.KeyEra + 1,
		NextRecipient: material.Recipient, NextEncryptedIdentity: append([]byte(nil), material.Ciphertext...),
		State: BackupKeyRotationPrepared, CreatedAt: input.CreatedAt.UTC(), UpdatedAt: input.CreatedAt.UTC(),
	}
	rotationValue, err := encodeBackupKeyRotationRecord(record)
	if err != nil {
		clear(record.NextEncryptedIdentity)
		return PreparedBackupKeyRotation{}, err
	}
	lockValue, err := encodeBackupOperationLockRecord(BackupOperationLockRecord{
		EnvironmentID: input.EnvironmentID, OperationID: input.OperationID, TaskID: input.TaskID,
		Kind: BackupOperationRotation, CreatedAt: input.CreatedAt.UTC(), UpdatedAt: input.CreatedAt.UTC(),
	})
	if err != nil {
		clear(rotationValue)
		clear(record.NextEncryptedIdentity)
		return PreparedBackupKeyRotation{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: backupKeyRotationKey(input.TaskID)}, {Key: indexKey},
		{Key: backuppolicy.BackupKeyKey(input.EnvironmentID), ModRevision: anchor.Values[1].ModRevision},
		{Key: backuppolicy.BackupKeyValueKey(input.EnvironmentID), ModRevision: anchor.Values[2].ModRevision},
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: backupKeyRotationKey(input.TaskID), Value: rotationValue},
		{Type: etcdstore.MutationPut, Key: indexKey, Value: []byte(input.TaskID)},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentOperationLockKey(input.EnvironmentID), Value: lockValue},
	}
	epoch, err := fence.epochRewriteMutation()
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		clear(record.NextEncryptedIdentity)
		return PreparedBackupKeyRotation{}, err
	}
	mutations = append(mutations, epoch)
	return PreparedBackupKeyRotation{
		Owner: owner,
		Publication: &PreparedBackupKeyRotationPublication{state: &preparedBackupKeyRotationState{
			repository: repository,
			plan:       backupKeyRotationPublicationPlan{conditions: conditions, mutations: mutations, record: record},
		}},
	}, nil
}

// PublishBackupKeyRotation atomically appends the real Task and pending
// idempotency marker to the prepared rotation transaction.
func (repository *BackupPolicyRepository) PublishBackupKeyRotation(
	ctx context.Context,
	prepared PreparedBackupKeyRotation,
	task TaskRecord,
	marker IdempotencyMarker,
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
	ctx context.Context, task TaskRecord, marker IdempotencyMarker,
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
	if task.Type != TaskRotate || task.Executor != TaskExecutorController || task.Status != TaskStatusPending ||
		task.Target != plan.record.EnvironmentID || task.ID != plan.record.TaskID || task.OperationID != plan.record.OperationID ||
		task.Owner.EnvironmentID != plan.record.EnvironmentID || task.CreatedAt.UTC() != plan.record.CreatedAt ||
		marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending || marker.TaskID != task.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backup key rotation Task publication is invalid",
		)
	}
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newTaskInitiation(task.Owner, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	task = cloneTaskRecord(task)
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	taskConditions := []etcdstore.Condition{
		{Key: taskKey(task.ID)}, {Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)}, {Key: taskQueueKey(task.Executor, task.ID)},
	}
	conditions := append(taskConditions, plan.conditions...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
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
		clearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	return (&IdempotencyRepository{store: repository.store}).Apply(ctx, marker, mutationPlan)
}

func (plan *backupKeyRotationPublicationPlan) clear() {
	if plan == nil {
		return
	}
	clearBackupRuntimeMutations(plan.mutations)
	clear(plan.record.NextEncryptedIdentity)
	plan.mutations = nil
}

// ApplyBackupKeyRotation swaps the current public key and wrapped identity.
// The operation lock remains part of the same transaction and is released
// only after the applied rotation authority is durable.
func (repository *BackupPolicyRepository) ApplyBackupKeyRotation(ctx context.Context, taskID string) error {
	if repository == nil || repository.store == nil {
		return errs.New(errs.KindInternal, "backup policy repository is not configured")
	}
	tasks, err := newTaskRepository(repository.store)
	if err != nil {
		return err
	}
	task, err := tasks.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if task.Record.Type != TaskRotate || task.Record.Executor != TaskExecutorController ||
		task.Record.Status != TaskStatusRunning {
		return errs.New(errs.KindStateConflict, "backup key rotation Task is not running")
	}
	change, err := tasks.prepareBackupKeyRotationTaskAcknowledgement(
		ctx, task.Record, TaskStatusCompleted, task.Record.UpdatedAt, task.ReadRevision,
	)
	change.clear()
	return err
}

type backupKeyRotationTaskChange struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func (change *backupKeyRotationTaskChange) clear() {
	if change == nil {
		return
	}
	clearMutationValues(change.mutations)
	change.conditions = nil
	change.mutations = nil
}

func (repository *TaskRepository) prepareBackupKeyRotationTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	status TaskStatus,
	terminalAt time.Time,
	readRevision int64,
) (backupKeyRotationTaskChange, error) {
	if task.Type != TaskRotate {
		return backupKeyRotationTaskChange{}, nil
	}
	if task.Executor != TaskExecutorController || task.Target != task.Owner.EnvironmentID ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil || !isTerminalTaskStatus(status) ||
		!backuppolicy.ValidUTCInstant(terminalAt) || readRevision <= 0 {
		return backupKeyRotationTaskChange{}, errs.New(
			errs.KindInternal,
			"backup key rotation Task acknowledgement is invalid",
		)
	}
	keys := []string{
		backupKeyRotationKey(task.ID),
		backuppolicy.BackupKeyKey(task.Target),
		backuppolicy.BackupKeyValueKey(task.Target),
		hierarchyrecord.EnvironmentOperationLockKey(task.Target),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return backupKeyRotationTaskChange{}, err
	}
	if read == nil || read.ReadRevision != readRevision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil || read.Values[3] == nil {
		return backupKeyRotationTaskChange{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation acknowledgement authority changed",
		)
	}
	defer clearKeyValues(read.Values)
	rotation, err := decodeBackupKeyRotationRecord(read.Values[0].Value)
	if err != nil {
		return backupKeyRotationTaskChange{}, corruptBackupKey()
	}
	defer clear(rotation.NextEncryptedIdentity)
	current, err := backuppolicy.DecodeBackupKeyRecord(read.Values[1].Value)
	if err != nil {
		return backupKeyRotationTaskChange{}, corruptBackupKey()
	}
	currentValue, err := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[2].Value)
	if err != nil {
		return backupKeyRotationTaskChange{}, corruptBackupKey()
	}
	defer clear(currentValue.Ciphertext)
	lock, err := decodeBackupOperationLockRecord(read.Values[3].Value)
	if err != nil || rotation.State != BackupKeyRotationPrepared ||
		rotation.TaskID != task.ID || rotation.OperationID != task.OperationID ||
		rotation.EnvironmentID != task.Target || current.EnvironmentID != task.Target ||
		currentValue.EnvironmentID != task.Target || current.KeyEra != rotation.CurrentKeyEra ||
		currentValue.KeyEra != rotation.CurrentKeyEra ||
		read.Values[1].ModRevision != rotation.ExpectedCurrentRecordRevision ||
		read.Values[2].ModRevision != rotation.ExpectedCurrentValueRevision ||
		lock.TaskID != task.ID || lock.OperationID != task.OperationID ||
		lock.Kind != BackupOperationRotation {
		return backupKeyRotationTaskChange{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation acknowledgement authority changed",
		)
	}
	fence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		task.Target,
		readRevision,
		environmentMutationFenceOwner{
			Kind: BackupOperationRotation, OperationID: task.OperationID, TaskID: task.ID,
		},
	)
	if err != nil {
		return backupKeyRotationTaskChange{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: backupKeyRotationKey(task.ID), ModRevision: read.Values[0].ModRevision},
		{Key: backuppolicy.BackupKeyKey(task.Target), ModRevision: read.Values[1].ModRevision},
		{Key: backuppolicy.BackupKeyValueKey(task.Target), ModRevision: read.Values[2].ModRevision},
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations := make([]etcdstore.Mutation, 0, 5)
	if status == TaskStatusCompleted {
		nextRecord := backuppolicy.BackupKeyRecord{
			EnvironmentID: task.Target,
			Recipient:     rotation.NextRecipient,
			KeyEra:        rotation.NextKeyEra,
			CreatedAt:     current.CreatedAt,
			RotatedAt:     terminalAt.UTC(),
		}
		recordValue, encodeErr := backuppolicy.EncodeBackupKeyRecord(nextRecord)
		if encodeErr != nil {
			return backupKeyRotationTaskChange{}, encodeErr
		}
		nextValue := backuppolicy.BackupKeyEncryptedValue{
			EnvironmentID: task.Target,
			KeyEra:        rotation.NextKeyEra,
			Ciphertext:    append([]byte(nil), rotation.NextEncryptedIdentity...),
		}
		valueValue, encodeErr := backuppolicy.EncodeBackupKeyEncryptedValue(nextValue)
		clear(nextValue.Ciphertext)
		if encodeErr != nil {
			clear(recordValue)
			return backupKeyRotationTaskChange{}, encodeErr
		}
		rotation.State = BackupKeyRotationApplied
		rotation.UpdatedAt = terminalAt.UTC()
		clear(rotation.NextEncryptedIdentity)
		rotation.NextEncryptedIdentity = nil
		rotationValue, encodeErr := encodeBackupKeyRotationRecord(rotation)
		if encodeErr != nil {
			clear(recordValue)
			clear(valueValue)
			return backupKeyRotationTaskChange{}, encodeErr
		}
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: backuppolicy.BackupKeyKey(task.Target), Value: recordValue},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: backuppolicy.BackupKeyValueKey(task.Target), Value: valueValue},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: backupKeyRotationKey(task.ID), Value: rotationValue},
		)
	}
	mutations = append(
		mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentOperationLockKey(task.Target)},
	)
	epoch, err := fence.epochRewriteMutation()
	if err != nil {
		clearMutationValues(mutations)
		return backupKeyRotationTaskChange{}, err
	}
	mutations = append(mutations, epoch)
	return backupKeyRotationTaskChange{conditions: conditions, mutations: mutations}, nil
}

func (repository *TaskRepository) validateBackupKeyRotationTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	status TaskStatus,
	readRevision int64,
) error {
	if task.Type != TaskRotate {
		return nil
	}
	keys := []string{
		backupKeyRotationKey(task.ID),
		backuppolicy.BackupKeyKey(task.Target),
		backuppolicy.BackupKeyValueKey(task.Target),
		hierarchyrecord.EnvironmentOperationLockKey(task.Target),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != readRevision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil || read.Values[3] != nil {
		return errs.New(errs.KindStateConflict, "backup key rotation terminal replay changed")
	}
	defer clearKeyValues(read.Values)
	rotation, err := decodeBackupKeyRotationRecord(read.Values[0].Value)
	if err != nil {
		return corruptBackupKey()
	}
	defer clear(rotation.NextEncryptedIdentity)
	current, err := backuppolicy.DecodeBackupKeyRecord(read.Values[1].Value)
	if err != nil {
		return corruptBackupKey()
	}
	currentValue, err := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[2].Value)
	if err != nil {
		return corruptBackupKey()
	}
	defer clear(currentValue.Ciphertext)
	if rotation.TaskID != task.ID || rotation.OperationID != task.OperationID ||
		rotation.EnvironmentID != task.Target || current.EnvironmentID != task.Target ||
		currentValue.EnvironmentID != task.Target || current.KeyEra != currentValue.KeyEra {
		return errs.New(errs.KindStateConflict, "backup key rotation terminal replay changed")
	}
	if status == TaskStatusCompleted {
		if rotation.State != BackupKeyRotationApplied || len(rotation.NextEncryptedIdentity) != 0 ||
			current.KeyEra != rotation.NextKeyEra || current.Recipient != rotation.NextRecipient {
			return errs.New(errs.KindStateConflict, "backup key rotation completion replay changed")
		}
		return nil
	}
	if rotation.State != BackupKeyRotationPrepared || current.KeyEra != rotation.CurrentKeyEra ||
		read.Values[1].ModRevision != rotation.ExpectedCurrentRecordRevision ||
		read.Values[2].ModRevision != rotation.ExpectedCurrentValueRevision {
		return errs.New(errs.KindStateConflict, "backup key rotation failed-attempt replay changed")
	}
	return nil
}

func (repository *TaskRepository) retryBackupKeyRotationTask(
	ctx context.Context,
	source etcdstore.Versioned[TaskRecord],
	retryTaskID string,
	actor TaskActor,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if actor != TaskActorOperator {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backup key rotation retry actor must be operator",
		)
	}
	retry, err := cloneRetryTask(source.Record, retryTaskID, actor, marker.CreatedAt)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending || marker.TaskID != retry.ID ||
		!marker.CreatedAt.Equal(retry.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backup key rotation retry marker does not match its Task",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	retry.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		backupKeyRotationKey(source.Record.ID),
		backupKeyRotationEnvironmentIndexKeyForRetry(source.Record.Owner.EnvironmentID, source.Record.ID),
		backupKeyRotationKey(retry.ID),
		backupKeyRotationEnvironmentIndexKeyForRetry(source.Record.Owner.EnvironmentID, retry.ID),
		backuppolicy.BackupKeyKey(source.Record.Owner.EnvironmentID), backuppolicy.BackupKeyValueKey(source.Record.Owner.EnvironmentID),
		hierarchyrecord.EnvironmentOperationLockKey(source.Record.Owner.EnvironmentID),
	}, Revision: source.ReadRevision})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if read == nil || read.ReadRevision != source.ReadRevision || len(read.Values) != 7 || read.Values[0] == nil ||
		read.Values[1] == nil ||
		read.Values[2] != nil ||
		read.Values[3] != nil ||
		read.Values[6] != nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation retry authority changed",
		)
	}
	defer clearKeyValues(read.Values)
	rotation, err := decodeBackupKeyRotationRecord(read.Values[0].Value)
	if err != nil || rotation.TaskID != source.Record.ID || rotation.State != BackupKeyRotationPrepared {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindTaskNotRetryable,
			"backup key rotation authority is not retryable",
		)
	}
	current, err := backuppolicy.DecodeBackupKeyRecord(read.Values[4].Value)
	if err != nil {
		return IdempotencyTransactionResult{}, corruptBackupKey()
	}
	currentValue, err := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[5].Value)
	if err != nil {
		return IdempotencyTransactionResult{}, corruptBackupKey()
	}
	defer clear(currentValue.Ciphertext)
	if current.KeyEra != rotation.CurrentKeyEra ||
		read.Values[4].ModRevision != rotation.ExpectedCurrentRecordRevision ||
		read.Values[5].ModRevision != rotation.ExpectedCurrentValueRevision {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"backup key changed before rotation retry",
		)
	}
	fence, err := loadOrdinaryEnvironmentMutationFence(
		ctx,
		repository.store,
		source.Record.Owner.EnvironmentID,
		source.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	newIndex := backupKeyRotationEnvironmentIndexKeyForRetry(source.Record.Owner.EnvironmentID, retry.ID)
	rotation.TaskID = retry.ID
	rotation.UpdatedAt = retry.CreatedAt
	rotationValue, err := encodeBackupKeyRotationRecord(rotation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	lockValue, err := encodeBackupOperationLockRecord(BackupOperationLockRecord{
		EnvironmentID: source.Record.Owner.EnvironmentID, OperationID: retry.OperationID, TaskID: retry.ID,
		Kind: BackupOperationRotation, CreatedAt: rotation.CreatedAt, UpdatedAt: retry.CreatedAt,
	})
	if err != nil {
		clear(rotationValue)
		return IdempotencyTransactionResult{}, err
	}
	taskValue, err := encodeTaskRecord(retry)
	if err != nil {
		clear(rotationValue)
		clear(lockValue)
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(retry.ID)
	if err != nil {
		clear(rotationValue)
		clear(lockValue)
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := []etcdstore.Condition{
		{Key: taskKey(retry.ID)}, {Key: taskOperationIndexKey(retry.OperationID, retry.ID)},
		{Key: taskActiveOperationKey(retry.OperationID)}, {Key: taskQueueKey(retry.Executor, retry.ID)},
		{Key: backupKeyRotationKey(source.Record.ID), ModRevision: read.Values[0].ModRevision},
		{
			Key: backupKeyRotationEnvironmentIndexKeyForRetry(
				source.Record.Owner.EnvironmentID,
				source.Record.ID,
			),
			ModRevision: read.Values[1].ModRevision,
		},
		{Key: backupKeyRotationKey(retry.ID)}, {Key: newIndex},
		{Key: backuppolicy.BackupKeyKey(source.Record.Owner.EnvironmentID), ModRevision: read.Values[4].ModRevision},
		{Key: backuppolicy.BackupKeyValueKey(source.Record.Owner.EnvironmentID), ModRevision: read.Values[5].ModRevision},
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(retry.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(retry.OperationID, retry.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(retry.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(retry.Executor, retry.ID), Value: reference},
		{Type: etcdstore.MutationDelete, Key: backupKeyRotationKey(source.Record.ID)},
		{
			Type: etcdstore.MutationDelete,
			Key:  backupKeyRotationEnvironmentIndexKeyForRetry(source.Record.Owner.EnvironmentID, source.Record.ID),
		},
		{Type: etcdstore.MutationPut, Key: backupKeyRotationKey(retry.ID), Value: rotationValue},
		{Type: etcdstore.MutationPut, Key: newIndex, Value: []byte(retry.ID)},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentOperationLockKey(source.Record.Owner.EnvironmentID), Value: lockValue},
	}
	epoch, err := fence.epochRewriteMutation()
	if err != nil {
		clearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	mutations = append(mutations, epoch)
	classify := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "backup key rotation retry evidence is incomplete")
		}
		return errs.New(errs.KindStateConflict, "backup key rotation retry state changed")
	}
	initiation, err := newInheritedTaskInitiation(source, actor)
	if err != nil {
		clearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(retry, initiation, conditions, mutations, classify)
	if err != nil {
		clearMutationValues(mutations)
		return IdempotencyTransactionResult{}, err
	}
	result, err := (&IdempotencyRepository{store: repository.store}).Apply(ctx, marker, plan)
	clear(rotationValue)
	clear(lockValue)
	return result, err
}

func backupKeyRotationEnvironmentIndexKeyForRetry(environmentID, taskID string) string {
	key, _ := backupKeyRotationEnvironmentIndexKey(environmentID, taskID)
	return key
}
