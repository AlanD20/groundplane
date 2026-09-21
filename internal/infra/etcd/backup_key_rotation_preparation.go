package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
	defer etcdstore.ClearValues(anchor.Values)
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
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, input.EnvironmentID, anchor.ReadRevision)
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
	defer etcdstore.ClearValues(projectRead.Values)
	project, err := hierarchyrecord.DecodeProject(projectRead.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID {
		return PreparedBackupKeyRotation{}, errs.New(errs.KindInternal, "backup key rotation Project is corrupt")
	}
	owner, err := taskjournal.EnvironmentTaskOwner(project, environment)
	if err != nil {
		return PreparedBackupKeyRotation{}, err
	}
	indexKey, err := backupruntime.BackupKeyRotationEnvironmentIndexKey(input.EnvironmentID, input.TaskID)
	if err != nil {
		return PreparedBackupKeyRotation{}, err
	}
	record := backupruntime.BackupKeyRotationRecord{
		TaskID: input.TaskID, OperationID: input.OperationID, EnvironmentID: input.EnvironmentID,
		ExpectedCurrentRecordRevision: anchor.Values[1].ModRevision,
		ExpectedCurrentValueRevision:  anchor.Values[2].ModRevision,
		CurrentKeyEra:                 current.KeyEra, NextKeyEra: current.KeyEra + 1,
		NextRecipient: material.Recipient, NextEncryptedIdentity: append([]byte(nil), material.Ciphertext...),
		State: backupruntime.BackupKeyRotationPrepared, CreatedAt: input.CreatedAt.UTC(), UpdatedAt: input.CreatedAt.UTC(),
	}
	rotationValue, err := backupruntime.EncodeBackupKeyRotationRecord(record)
	if err != nil {
		clear(record.NextEncryptedIdentity)
		return PreparedBackupKeyRotation{}, err
	}
	lockValue, err := backupruntime.EncodeBackupOperationLockRecord(backupruntime.BackupOperationLockRecord{
		EnvironmentID: input.EnvironmentID, OperationID: input.OperationID, TaskID: input.TaskID,
		Kind: backupruntime.BackupOperationRotation, CreatedAt: input.CreatedAt.UTC(), UpdatedAt: input.CreatedAt.UTC(),
	})
	if err != nil {
		clear(rotationValue)
		clear(record.NextEncryptedIdentity)
		return PreparedBackupKeyRotation{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: backupruntime.BackupKeyRotationKey(input.TaskID)}, {Key: indexKey},
		{Key: backuppolicy.BackupKeyKey(input.EnvironmentID), ModRevision: anchor.Values[1].ModRevision},
		{Key: backuppolicy.BackupKeyValueKey(input.EnvironmentID), ModRevision: anchor.Values[2].ModRevision},
	}
	conditions = append(conditions, fence.TransactionConditions()...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: backupruntime.BackupKeyRotationKey(input.TaskID), Value: rotationValue},
		{Type: etcdstore.MutationPut, Key: indexKey, Value: []byte(input.TaskID)},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentOperationLockKey(input.EnvironmentID), Value: lockValue},
	}
	epoch, err := fence.EpochRewriteMutation()
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
