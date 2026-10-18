package etcd

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *ServiceRepository) PublishServiceLifecycle(ctx context.Context,
	tenant *keyvalue.Versioned[hierarchy.TenantRecord], project keyvalue.Versioned[hierarchy.ProjectRecord],
	environment keyvalue.Versioned[hierarchy.EnvironmentRecord], current keyvalue.Versioned[services.ServiceRecord],
	input servicelifecyclework.Input, envelope *taskconfiguration.BackingHookEncryptedInputs,
	deadline time.Time, marker idempotency.IdempotencyMarker,
) (_ IdempotencyTransactionResult, resultErr error) {
	if err := validateServiceLifecycleHierarchy(tenant, project, environment, current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if input.Scope != (workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID}) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Service lifecycle scope differs from its Environment")
	}
	route := "/services/{id}/" + string(input.Action)
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetService, ID: input.ServiceID}
	if idempotency.ValidateIdempotencyMarker(marker) != nil || marker.Kind != idempotency.IdempotencyMarkerTask ||
		marker.State != idempotency.IdempotencyMarkerPending || marker.TaskID != input.TaskID ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment || marker.Locator.ScopeID != environment.Record.ID ||
		marker.Locator.Method != http.MethodPost || marker.Locator.Route != route || marker.ReplayTarget == nil || *marker.ReplayTarget != target ||
		!idempotency.ValidTaskResponse(marker.Response, input.TaskID) || !marker.CreatedAt.Equal(input.CreatedAt) || !marker.UpdatedAt.Equal(input.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Service lifecycle replay marker differs from its action")
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, environment.Record.ID, current.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	inputs, err := servicelifecyclework.NewRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	publication, err := inputs.PrepareInput(ctx, input, envelope)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(publication.Change.Mutations)
	attempted := false
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if attempted {
			if resultErr != nil {
				resultErr = errors.Join(resultErr, publication.ReconcilePreparation(cleanup))
			}
			return
		}
		resultErr = errors.Join(resultErr, publication.Abandon(cleanup))
	}()
	_, work, err := servicelifecyclework.PrepareAdmission(publication.Input, current, deadline, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epoch.Value)
	conditions := append(work.Conditions, publication.Change.Conditions...)
	conditions = append(conditions, fence.TransactionConditions()...)
	conditions = append(conditions,
		keyvalue.Condition{Key: hierarchy.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		keyvalue.Condition{Key: hierarchy.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		keyvalue.Condition{Key: deletions.TombstoneKey("service", input.ServiceID)},
	)
	mutations := append(work.Mutations, publication.Change.Mutations...)
	mutations = append(mutations, epoch)
	initiation, err := newEnvironmentTaskInitiation(tenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	classify := idempotencyPlanClassifier(func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "Service runtime or Environment changed before lifecycle publication; retry the action")
	})
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	receipts, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	attempted = true
	result, err := receipts.Apply(ctx, marker, plan)
	if err == nil && result.kind != idempotencyTransactionApplied {
		attempted = false // Conflict/replay is an observed non-publication.
	}
	return result, err
}
