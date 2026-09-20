package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type secretTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareSecretTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (secretTaskChange, error) {
	applies, err := taskOwnsSecretRemoval(source)
	if err != nil || !applies {
		return secretTaskChange{}, err
	}
	if retry.Type != source.Type || retry.Target != source.Target ||
		retry.Params[TaskResourceKindParam] != TaskResourceSecret {
		return secretTaskChange{}, errs.New(errs.KindInternal, "Secret retry changed its durable target")
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			secretrecord.RecordKey(source.Target),
			deletionTombstoneKey(string(DeletionTargetSecret), source.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return secretTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[0] == nil || stored.Values[1] != nil {
		return secretTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Secret is not available for deletion retry",
		)
	}
	record, err := secretrecord.DecodeRecord(stored.Values[0].Value)
	if err != nil || record.Secret.ID != source.Target {
		return secretTaskChange{}, secretrecord.CorruptRecord()
	}
	fences, err := prepareSecretScriptAbsence(ctx, repository.store, source.Target)
	if err != nil {
		return secretTaskChange{}, err
	}
	dependencies, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			secretOwnerKey(record.Secret), secretScopedKey(record.Secret), secretrecord.ValueKey(record.Secret.ID),
		},
		Revision: revision,
	})
	if err != nil {
		return secretTaskChange{}, err
	}
	if dependencies == nil || len(dependencies.Values) != 3 || dependencies.Values[0] == nil ||
		dependencies.Values[1] == nil || dependencies.Values[2] == nil ||
		string(dependencies.Values[0].Value) != record.Secret.ID ||
		string(dependencies.Values[1].Value) != record.Secret.ID {
		return secretTaskChange{}, errs.New(errs.KindInternal, "Secret retry dependencies are corrupt")
	}
	change := secretTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: secretrecord.RecordKey(record.Secret.ID), ModRevision: stored.Values[0].ModRevision},
			{Key: secretOwnerKey(record.Secret), ModRevision: dependencies.Values[0].ModRevision},
			{Key: secretScopedKey(record.Secret), ModRevision: dependencies.Values[1].ModRevision},
			{Key: secretrecord.ValueKey(record.Secret.ID), ModRevision: dependencies.Values[2].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetSecret), record.Secret.ID)},
		},
	}
	change.conditions = append(change.conditions, fences...)
	if record.Secret.Scope == core.SecretScopeProject {
		parents, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{
				projectKey(record.Secret.ProjectID),
				deletionTombstoneKey(string(DeletionTargetProject), record.Secret.ProjectID),
			},
			Revision: revision,
		})
		if err != nil {
			return secretTaskChange{}, err
		}
		if parents == nil || len(parents.Values) != 2 || parents.Values[0] == nil || parents.Values[1] != nil {
			return secretTaskChange{}, errs.New(errs.KindResourceInUse, "Secret owner is unavailable")
		}
		project, err := decodeProject(parents.Values[0].Value)
		if err != nil || project.ID != record.Secret.ProjectID {
			return secretTaskChange{}, corruptRecord()
		}
		change.conditions = append(change.conditions,
			etcdstore.Condition{Key: projectKey(project.ID), ModRevision: parents.Values[0].ModRevision},
			etcdstore.Condition{Key: deletionTombstoneKey(string(DeletionTargetProject), project.ID)},
		)
		if project.TenantID != "" {
			tenantFence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
				Keys:     []string{deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID)},
				Revision: revision,
			})
			if err != nil {
				return secretTaskChange{}, err
			}
			if tenantFence == nil || len(tenantFence.Values) != 1 || tenantFence.Values[0] != nil {
				return secretTaskChange{}, errs.New(errs.KindResourceInUse, "Secret owner is unavailable")
			}
			change.conditions = append(change.conditions, etcdstore.Condition{
				Key: deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID),
			})
		}
	}
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetSecret, TargetID: record.Secret.ID,
		TargetRevision: stored.Values[0].ModRevision, TaskID: retry.ID,
		Phase: DeletionPhaseFinalizing, CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	value, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return secretTaskChange{}, err
	}
	change.values = append(change.values, value)
	change.mutations = append(change.mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: deletionTombstoneKey(string(DeletionTargetSecret), record.Secret.ID), Value: value,
	})
	return change, nil
}

func (repository *TaskRepository) prepareSecretTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) (secretTaskChange, error) {
	applies, err := taskOwnsSecretRemoval(task)
	if err != nil || !applies {
		return secretTaskChange{}, err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			secretrecord.RecordKey(task.Target),
			deletionTombstoneKey(string(DeletionTargetSecret), task.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return secretTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[0] == nil || stored.Values[1] == nil {
		return secretTaskChange{}, errs.New(errs.KindInternal, "Secret deletion state is inconsistent")
	}
	record, err := secretrecord.DecodeRecord(stored.Values[0].Value)
	if err != nil || record.Secret.ID != task.Target {
		return secretTaskChange{}, secretrecord.CorruptRecord()
	}
	tombstone, err := decodeDeletionTombstone(stored.Values[1].Value)
	if err != nil || tombstone.TargetKind != DeletionTargetSecret || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != stored.Values[0].ModRevision || tombstone.TaskID != task.ID ||
		tombstone.Phase != DeletionPhaseFinalizing {
		return secretTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Secret deletion tombstone does not match its Task",
		)
	}
	dependencies, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			secretOwnerKey(record.Secret), secretScopedKey(record.Secret), secretrecord.ValueKey(record.Secret.ID),
		},
		Revision: revision,
	})
	if err != nil {
		return secretTaskChange{}, err
	}
	if dependencies == nil || len(dependencies.Values) != 3 || dependencies.Values[0] == nil ||
		dependencies.Values[1] == nil || dependencies.Values[2] == nil ||
		string(dependencies.Values[0].Value) != record.Secret.ID ||
		string(dependencies.Values[1].Value) != record.Secret.ID {
		return secretTaskChange{}, errs.New(errs.KindInternal, "Secret deletion dependencies are corrupt")
	}
	encrypted, err := secretrecord.DecodeEncryptedValue(dependencies.Values[2].Value)
	if err != nil || encrypted.SecretID != task.Target {
		clear(encrypted.Ciphertext)
		return secretTaskChange{}, secretrecord.CorruptRecord()
	}
	clear(encrypted.Ciphertext)
	change := secretTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: secretrecord.RecordKey(task.Target), ModRevision: stored.Values[0].ModRevision},
			{
				Key:         deletionTombstoneKey(string(DeletionTargetSecret), task.Target),
				ModRevision: stored.Values[1].ModRevision,
			},
			{Key: secretOwnerKey(record.Secret), ModRevision: dependencies.Values[0].ModRevision},
			{Key: secretScopedKey(record.Secret), ModRevision: dependencies.Values[1].ModRevision},
			{Key: secretrecord.ValueKey(task.Target), ModRevision: dependencies.Values[2].ModRevision},
		},
		mutations: []etcdstore.Mutation{{
			Type: etcdstore.MutationDelete, Key: deletionTombstoneKey(string(DeletionTargetSecret), task.Target),
		}},
	}
	if terminalStatus == TaskStatusCompleted {
		fences, err := prepareSecretScriptAbsence(ctx, repository.store, task.Target)
		if err != nil {
			return secretTaskChange{}, err
		}
		change.conditions = append(change.conditions, fences...)
		change.mutations = append(change.mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: secretOwnerKey(record.Secret)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: secretScopedKey(record.Secret)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: secretrecord.ValueKey(task.Target)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: secretrecord.RecordKey(task.Target)},
		)
	}
	return change, nil
}

func (repository *TaskRepository) validateSecretTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) error {
	applies, err := taskOwnsSecretRemoval(task)
	if err != nil || !applies {
		return err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			secretrecord.RecordKey(task.Target),
			deletionTombstoneKey(string(DeletionTargetSecret), task.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[1] != nil {
		return errs.New(errs.KindStateConflict, "Secret deletion terminal state does not match its Task")
	}
	if terminalStatus == TaskStatusCompleted {
		if stored.Values[0] != nil {
			return errs.New(errs.KindStateConflict, "completed Secret deletion retained its target")
		}
		return nil
	}
	if stored.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "failed Secret deletion lost its target")
	}
	record, err := secretrecord.DecodeRecord(stored.Values[0].Value)
	if err != nil || record.Secret.ID != task.Target {
		return errs.New(errs.KindStateConflict, "failed Secret deletion retained another target")
	}
	return nil
}

func taskOwnsSecretRemoval(task TaskRecord) (bool, error) {
	if task.Executor != TaskExecutorController || task.Params[TaskResourceKindParam] != TaskResourceSecret {
		return false, nil
	}
	if task.Type != TaskRemove || len(task.Params) != 1 || ids.Validate(ids.KindSecret, task.Target) != nil {
		return false, errs.New(errs.KindInternal, "Secret deletion Task has invalid durable input")
	}
	return true, nil
}

func clearSecretTaskChange(change secretTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
