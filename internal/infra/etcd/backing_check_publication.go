package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareBackingCheck returns exact capture authority and cleanup references
// for atomic promotion of the accepted Task.
func (repository *AttachRepository) PrepareBackingCheck(ctx context.Context, source BackingCheckSource,
	input backingchecks.Record, task workexecution.Task,
) (workexecution.Item, workqueue.Change, error) {
	if err := validateCapturedBackingCheck(ctx, source, input, task); err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	tenant, err := loadConnectorTaskInitiationTenantAtRevision(
		ctx,
		repository.store,
		source.project,
		source.fence.ReadRevision(),
	)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(
		tenant,
		source.project,
		source.environment,
		taskjournal.TaskActorOperator,
	)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	item := workexecution.Item{ID: input.Owner.WorkItemID, TaskID: task.ID, Operation: workexecution.BackingCheck,
		TargetID: task.TargetID, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: task.TargetID}}, Input: &task.Intent,
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: task.Deadline}
	conditions := append([]keyvalue.Condition(nil), source.conditions...)
	var mutations []keyvalue.Mutation
	keep := false
	defer func() {
		if !keep {
			keyvalue.ClearMutationValues(mutations)
		}
	}()
	inputConditions, inputMutations, err := backingchecks.PreparePublication(input)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	conditions = append(conditions, inputConditions...)
	mutations = append(mutations, inputMutations...)
	epoch, err := source.fence.EpochRewriteMutation()
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	backingEpoch, err := source.backingFence.EpochRewriteMutation()
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
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
		return workexecution.Item{}, workqueue.Change{}, err
	}
	backingAncestry, err := hierarchydeletion.BindMutationEpochs(ctx, repository.store, source.fence.ReadRevision(),
		hierarchydeletion.MutationScope{ProjectID: source.backingProject.Record.ID}, nil, nil)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	conditions = append(conditions, ancestry.Conditions()...)
	conditions = append(conditions, backingAncestry.Conditions()...)
	mutations = append(mutations, ancestry.Mutations()...)
	mutations = append(mutations, backingAncestry.Mutations()...)
	joined, err := workqueue.JoinChanges(
		workqueue.Change{Conditions: conditions, Mutations: mutations},
		workqueue.Change{Conditions: initiation.fences},
	)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	keep = true
	return item, joined, nil
}

func validateCapturedBackingCheck(ctx context.Context, source BackingCheckSource, input backingchecks.Record,
	task workexecution.Task,
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
		input.CreatedAt.Before(task.CreatedAt) || input.CreatedAt.After(task.UpdatedAt) {
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
	return nil
}
