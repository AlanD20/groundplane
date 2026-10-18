package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateEdgeCheck commits source fences, native Task/idempotency, latest result
// selection and exact helper/resource ownership together. Concurrent image or
// hierarchy deletion cannot slip between host selection and publication.
func (repository *RouteRepository) CreateEdgeCheck(ctx context.Context, source EdgeCheckSource,
	input edgechecks.Record, task workexecution.Task, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateEdgeCheckPublication(ctx, source, input, task, marker); err != nil {
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
	item := workexecution.Item{ID: input.Owner.WorkItemID, TaskID: task.ID, Operation: workexecution.EdgeCheck,
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
	inputConditions, inputMutations, err := edgechecks.PreparePublication(input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(inputMutations)
	conditions, mutations = append(conditions, inputConditions...), append(mutations, inputMutations...)
	epoch, err := source.fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epoch.Value)
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
		return IdempotencyTransactionResult{}, err
	}
	defer ancestry.Clear()
	conditions, mutations = append(conditions, ancestry.Conditions()...), append(mutations, ancestry.Mutations()...)
	publicationConditions := conditions
	conditions, classify, err := prepareTaskInitiationFences(initiation, conditions,
		func(_ int64, values []*keyvalue.KeyValue) error {
			if len(values) != len(publicationConditions) {
				return errs.New(errs.KindInternal, "Tunnel check publication evidence is incomplete")
			}
			for index, condition := range publicationConditions {
				if keyvalue.RevisionOf(values[index]) != condition.ModRevision {
					return errs.New(
						errs.KindStateConflict,
						"Route, Tunnel, Agent or another check changed before acceptance; refresh and submit a new check",
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
	if err := plan.enforceTransactionBounds(func(conditions []keyvalue.Condition, mutations []keyvalue.Mutation) error {
		compares := len(keyvalue.ImageSelectionConditions(ctx, conditions))
		if compares+len(mutations) > keyvalue.MaximumOperations || 2*compares+len(mutations) > 128 {
			return errs.New(errs.KindValidationFailed, "Tunnel check exceeds the atomic publication limit")
		}
		return nil
	}); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	markers, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return markers.Apply(ctx, marker, plan)
}

func validateEdgeCheckPublication(ctx context.Context, source EdgeCheckSource, input edgechecks.Record,
	task workexecution.Task, marker idempotency.IdempotencyMarker,
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
		marker.Kind != idempotency.IdempotencyMarkerTask ||
		marker.State != idempotency.IdempotencyMarkerPending ||
		marker.TaskID != task.ID ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != task.Scope.EnvironmentID ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) ||
		!input.CreatedAt.Equal(task.CreatedAt) {
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
	return idempotency.ValidateIdempotencyMarker(marker)
}
