package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateBackingCheck publishes exact captured input, native work and all cleanup
// references together. Image-removal and hierarchy admission share this commit.
func (repository *AttachRepository) CreateBackingCheck(ctx context.Context, source BackingCheckSource,
	input backingchecks.Record, task workexecution.Task, marker idempotency.IdempotencyMarker,
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
	item := workexecution.Item{ID: input.Owner.WorkItemID, TaskID: task.ID, Operation: workexecution.BackingCheck,
		TargetID: task.TargetID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: task.TargetID}}, Input: &task.Intent,
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline}
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	conditions := append(work.Conditions, source.conditions...)
	mutations := work.Mutations
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
	publicationConditions := conditions
	conditions, classify, err := prepareTaskInitiationFences(initiation, conditions,
		func(_ int64, values []*keyvalue.KeyValue) error {
			if len(values) != len(publicationConditions) {
				return errs.New(errs.KindInternal, "access check publication evidence is incomplete")
			}
			for index, condition := range publicationConditions {
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
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations, classify)
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
	task workexecution.Task, marker idempotency.IdempotencyMarker,
) error {
	_, imageCaptured := imagefence.Revision(ctx)
	if !imageCaptured || source.fence.ReadRevision() <= 0 ||
		source.fence.EnvironmentID() != source.environment.Record.ID ||
		source.backingFence.EnvironmentID() != source.facts.Consumer.Record.BackingEnvironmentID ||
		input.Validate() != nil ||
		input.Phase != backingchecks.Ready ||
		task.Validate() != nil || task.Action != workexecution.ActionCheck || task.Actor != workexecution.OperatorActor ||
		task.ExpectedItems != 1 || task.PublicationComplete || task.Progress.Total() != 0 ||
		input.Owner.TaskID != task.ID || input.Owner.PlanID != task.Intent.ID ||
		input.Source.Parameters.AttachID != task.TargetID ||
		task.Scope.TenantID != source.project.Record.TenantID ||
		task.Scope.ProjectID != source.project.Record.ID ||
		task.Scope.EnvironmentID != source.environment.Record.ID ||
		task.TargetName != source.facts.Consumer.Record.Name ||
		marker.Kind != idempotency.IdempotencyMarkerTask ||
		marker.State != idempotency.IdempotencyMarkerPending ||
		marker.TaskID != task.ID ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != task.Scope.EnvironmentID ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) ||
		!input.CreatedAt.Equal(task.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "access check publication differs from captured authority")
	}
	digest, err := backingchecks.PlanHash(input)
	if err != nil || digest != task.Intent.SHA256 {
		return errs.New(errs.KindValidationFailed, "access check captured input digest differs from its work")
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
