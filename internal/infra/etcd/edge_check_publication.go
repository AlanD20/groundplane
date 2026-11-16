package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareEdgeCheck returns exact source and helper authority for atomic
// promotion of an accepted Route or standalone Tunnel check.
func (repository *RouteRepository) PrepareEdgeCheck(ctx context.Context, source EdgeCheckSource,
	input edgechecks.Record, task workexecution.Task,
) (workexecution.Item, workqueue.Change, error) {
	if err := validateCapturedEdgeCheck(ctx, source, input, task); err != nil {
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
	item := workexecution.Item{ID: input.Owner.WorkItemID, TaskID: task.ID, Operation: workexecution.EdgeCheck,
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
	inputConditions, inputMutations, err := edgechecks.PreparePublication(input)
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	conditions, mutations = append(conditions, inputConditions...), append(mutations, inputMutations...)
	epoch, err := source.fence.EpochRewriteMutation()
	if err != nil {
		return workexecution.Item{}, workqueue.Change{}, err
	}
	mutations = append(mutations, epoch)
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
	conditions, mutations = append(conditions, ancestry.Conditions()...), append(mutations, ancestry.Mutations()...)
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

func validateCapturedEdgeCheck(ctx context.Context, source EdgeCheckSource, input edgechecks.Record,
	task workexecution.Task,
) error {
	_, imageCaptured := imagefence.Revision(ctx)
	if !imageCaptured || source.fence.ReadRevision() <= 0 ||
		source.fence.EnvironmentID() != source.environment.Record.ID ||
		input.Validate() != nil ||
		input.Phase != edgechecks.Ready ||
		task.Validate() != nil || task.Actor != workexecution.OperatorActor || task.Action != workexecution.ActionCheck ||
		task.ExpectedItems != 1 || input.Owner.TaskID != task.ID || input.Owner.PlanID != task.Intent.ID ||
		input.Source.Request.TargetID() != task.TargetID || input.Owner.EnvironmentID != task.Scope.EnvironmentID ||
		task.Scope.TenantID != source.project.Record.TenantID ||
		task.Scope.ProjectID != source.project.Record.ID ||
		task.Scope.EnvironmentID != source.environment.Record.ID ||
		input.CreatedAt.Before(task.CreatedAt) || input.CreatedAt.After(task.UpdatedAt) {
		return errs.New(errs.KindValidationFailed, "Tunnel check publication differs from captured authority")
	}
	digest, err := edgechecks.PlanHash(input)
	if err != nil || digest != task.Intent.SHA256 {
		return errs.New(errs.KindValidationFailed, "Tunnel check input digest differs from its Task")
	}
	configuration, err := source.Configuration()
	if err != nil {
		return err
	}
	rebuilt, err := edgechecks.NewRecord(input.Owner, configuration, input.CreatedAt)
	if err != nil {
		return err
	}
	if rebuilt.ConfigurationSHA256 != input.ConfigurationSHA256 {
		return errs.New(errs.KindValidationFailed, "Tunnel check source differs from its repository and host capture")
	}
	return nil
}
