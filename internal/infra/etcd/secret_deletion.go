package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secretmutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secretremoval"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareSecretRemovalAuthority captures guarded native deletion authority for
// promotion of an already accepted Task. It does not publish a new Task.
func (repository *SecretRepository) PrepareSecretRemovalAuthority(
	ctx context.Context,
	owner secretrecord.Owner,
	current etcdstore.Versioned[secretrecord.Record],
	input secretremoval.Input,
	task workexecution.Task,
) (workqueue.Change, error) {
	if err := secretrecord.ValidateSecretOwnership(ctx, owner, current.Record); err != nil {
		return workqueue.Change{}, err
	}
	if err := secretrecord.ValidateSecretVersion(current); err != nil {
		return workqueue.Change{}, err
	}
	secretID := current.Record.Secret.ID
	ref, err := input.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	scope := workexecution.Scope{}
	if owner.Project != nil {
		scope = workexecution.Scope{TenantID: owner.Project.Record.TenantID, ProjectID: owner.Project.Record.ID}
	}
	if task.Validate() != nil || task.Action != workexecution.ActionRemove ||
		task.Actor != workexecution.OperatorActor ||
		task.TargetID != secretID ||
		task.TargetName != current.Record.Secret.Key ||
		task.Scope != scope ||
		task.ExpectedItems != 1 ||
		input.TaskID != task.ID ||
		input.SecretID != secretID ||
		input.SecretRevision != current.Revision ||
		input.ProjectID != scope.ProjectID ||
		input.TenantID != scope.TenantID ||
		task.Intent != ref ||
		input.CreatedAt.Before(task.CreatedAt) || !input.CreatedAt.Before(task.Deadline) {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Secret removal differs from its captured input",
		)
	}
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetSecret, TargetID: secretID, TargetRevision: current.Revision,
		TaskID: task.ID, Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt}
	dependencies, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			secretrecord.SecretOwnerKey(current.Record.Secret),
			secretrecord.SecretScopedKey(current.Record.Secret),
			secretrecord.ValueKey(secretID),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	if dependencies != nil {
		defer etcdstore.ClearValues(dependencies.Values)
	}
	if dependencies == nil || dependencies.ReadRevision != current.ReadRevision || len(dependencies.Values) != 3 ||
		dependencies.Values[0] == nil ||
		dependencies.Values[1] == nil ||
		dependencies.Values[2] == nil ||
		dependencies.Values[2].ModRevision != input.ValueRevision ||
		string(
			dependencies.Values[0].Value,
		) != secretID ||
		string(dependencies.Values[1].Value) != secretID {
		return workqueue.Change{}, errs.New(
			errs.KindInternal,
			"Secret deletion indexes or encrypted value are corrupt",
		)
	}
	encrypted, err := secretrecord.DecodeStoredValue(dependencies.Values[2].Value)
	if err != nil || encrypted.SecretID != secretID {
		clear(encrypted.Ciphertext)
		return workqueue.Change{}, secretrecord.CorruptRecord()
	}
	clear(encrypted.Ciphertext)

	captureConditions, captureWrites, err := secretremoval.PreparePublication(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	keep := false
	defer func() {
		if !keep {
			etcdstore.ClearMutationValues(captureWrites)
		}
	}()
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer func() {
		if !keep {
			clear(tombstoneValue)
		}
	}()
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
	references, err := secretmutations.PrepareSecretRemovalAbsence(ctx, repository.store, secretID)
	if err != nil {
		return workqueue.Change{}, err
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
			return workqueue.Change{}, tenantErr
		}
		initiation, err = newProjectTaskInitiation(taskTenant, *owner.Project, taskjournal.TaskActorOperator)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions = append(conditions, captureConditions...)
	mutations = append(mutations, captureWrites...)
	classify := idempotencyPlanClassifier(func(_ int64, _ []*etcdstore.KeyValue) error {
		return errs.New(
			errs.KindStateConflict,
			"Secret removal references or ownership changed; refresh before removing",
		)
	})
	conditions, _, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return workqueue.Change{}, err
	}
	keep = true
	return workqueue.Change{Conditions: conditions, Mutations: mutations}, nil
}
