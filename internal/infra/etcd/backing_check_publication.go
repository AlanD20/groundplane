package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateBackingCheck publishes exact captured input, native work and all cleanup
// references together. Image-removal and hierarchy admission share this commit.
func (repository *AttachRepository) CreateBackingCheck(ctx context.Context, source BackingCheckSource,
	input backingchecks.Record, task TaskRecord, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateBackingCheckPublication(ctx, source, input, task, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	tenant, err := loadConnectorTaskInitiationTenantAtRevision(
		ctx,
		repository.store,
		source.project,
		source.fence.ReadRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(
		tenant,
		source.project,
		source.environment,
		taskjournal.TaskActorOperator,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateTaskInitiation(task, initiation, true); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	taskValue, err := EncodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotency.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	keys := []string{taskjournal.TaskStorageKey(task.ID), taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
		taskjournal.TaskActiveOperationKey(task.OperationID), taskjournal.TaskQueueKey(task.Executor, task.ID)}
	conditions := slices.Clone(source.conditions)
	mutations := make([]keyvalue.Mutation, 0, 20)
	for index, key := range keys {
		conditions = append(conditions, keyvalue.Condition{Key: key})
		value := reference
		if index == 0 {
			value = taskValue
		}
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	}
	inputConditions, inputMutations, err := backingchecks.PreparePublication(input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(inputMutations)
	conditions = append(conditions, inputConditions...)
	mutations = append(mutations, inputMutations...)
	epoch, err := source.fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epoch.Value)
	backingEpoch, err := source.backingFence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(backingEpoch.Value)
	mutations = append(mutations, epoch, backingEpoch)
	ancestry, err := hierarchydeletion.BindMutationEpochs(
		ctx,
		repository.store,
		source.fence.ReadRevision(),
		hierarchydeletion.MutationScope{
			TenantID:  source.project.Record.TenantID,
			ProjectID: source.project.Record.ID,
		},
		nil,
		nil,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer ancestry.Clear()
	backingAncestry, err := hierarchydeletion.BindMutationEpochs(ctx, repository.store, source.fence.ReadRevision(),
		hierarchydeletion.MutationScope{ProjectID: source.backingProject.Record.ID}, nil, nil)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer backingAncestry.Clear()
	conditions = append(conditions, ancestry.Conditions()...)
	conditions = append(conditions, backingAncestry.Conditions()...)
	mutations = append(mutations, ancestry.Mutations()...)
	mutations = append(mutations, backingAncestry.Mutations()...)
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations,
		func(_ int64, values []*keyvalue.KeyValue) error {
			if len(values) != len(conditions) {
				return errs.New(errs.KindInternal, "access check publication evidence is incomplete")
			}
			for index, condition := range conditions {
				if keyvalue.RevisionOf(values[index]) != condition.ModRevision {
					return errs.New(
						errs.KindStateConflict,
						"Attach, credentials, runtime or operation changed before the check was accepted; refresh and submit a new check",
					)
				}
			}
			return nil
		})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	// Apply runs this after adding the marker and Task indexes. Count the
	// image fence and failed-comparison reads too, not just successful writes.
	if err := plan.enforceTransactionBounds(func(conditions []keyvalue.Condition, mutations []keyvalue.Mutation) error {
		compares := len(keyvalue.ImageSelectionConditions(ctx, conditions))
		if compares+len(mutations) > keyvalue.MaximumOperations || 2*compares+len(mutations) > 128 {
			return errs.New(errs.KindValidationFailed, "access check exceeds the atomic publication limit")
		}
		return nil
	}); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotencyRepository, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotencyRepository.Apply(ctx, marker, plan)
}

func validateBackingCheckPublication(ctx context.Context, source BackingCheckSource, input backingchecks.Record,
	task TaskRecord, marker idempotency.IdempotencyMarker,
) error {
	_, imageCaptured := imagefence.Revision(ctx)
	if !imageCaptured || source.fence.ReadRevision() <= 0 ||
		source.fence.EnvironmentID() != source.environment.Record.ID ||
		source.backingFence.EnvironmentID() != source.facts.Consumer.Record.BackingEnvironmentID ||
		input.Validate() != nil ||
		input.Phase != backingchecks.Ready ||
		!backingCheckMatchesTask(input, task) ||
		task.Status != taskjournal.TaskStatusPending ||
		task.RetryOf != "" ||
		task.Owner.TenantID != source.project.Record.TenantID ||
		task.Owner.ProjectID != source.project.Record.ID ||
		task.Owner.EnvironmentID != source.environment.Record.ID ||
		task.TargetName != source.facts.Consumer.Record.Name ||
		marker.Kind != idempotency.IdempotencyMarkerTask ||
		marker.State != idempotency.IdempotencyMarkerPending ||
		marker.TaskID != task.ID ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != task.Owner.EnvironmentID ||
		marker.Locator.Key != task.IdempotencyKey ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) ||
		!input.CreatedAt.Equal(task.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "access check publication differs from captured authority")
	}
	expected, err := source.Configuration(input.Source.Parameters)
	if err != nil {
		return err
	}
	expected.CredentialDigest = input.Source.CredentialDigest
	rebuilt, err := backingchecks.NewRecord(input.Owner, expected, input.EncryptedInput, input.CreatedAt)
	if err != nil {
		return err
	}
	defer rebuilt.Clear()
	if rebuilt.ConfigurationSHA256 != input.ConfigurationSHA256 {
		return errs.New(errs.KindValidationFailed, "access check source does not match its captured host selection")
	}
	return idempotency.ValidateIdempotencyMarker(marker)
}
