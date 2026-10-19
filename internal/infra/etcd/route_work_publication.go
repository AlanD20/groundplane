package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routepersistence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type RouteWorkPublication struct {
	Project               keyvalue.Versioned[hierarchy.ProjectRecord]
	Environment           keyvalue.Versioned[hierarchy.EnvironmentRecord]
	Target                keyvalue.Versioned[services.ServiceRecord]
	Current               *keyvalue.Versioned[routes.Record]
	Claim                 blueprints.EnvironmentBlueprintStageClaim
	Projection            projectionrecord.EnvironmentComposeProjection
	EpochRevision         int64
	Task                  workexecution.Task
	Item                  workexecution.Item
	InputID               string
	Input                 *agentpb.ComponentWorkInput
	Capture               componentwork.Capture
	Secrets               tasksecretpins.Prepared
	Marker                idempotency.IdempotencyMarker
	PreparationConditions []keyvalue.Condition
	InactiveRouterKinds   []core.ComponentKind
}

// PublishRouteWork joins desired state and the native router operation. Removal
// publishes intent now, but keeps mutable Route metadata until host completion.
func (repository *HierarchyRepository) PublishRouteWork(
	ctx context.Context,
	request RouteWorkPublication,
) (IdempotencyTransactionResult, error) {
	project, environment, claim := request.Project, request.Environment, request.Claim
	task, item, route := request.Task, request.Item, request.Capture.Route
	if ctx == nil || repository == nil || route == nil ||
		(item.Placement.Executor == workexecution.Agent) != (request.Input != nil) ||
		task.Validate() != nil || item.Validate() != nil || blueprints.ValidateEnvironmentBlueprintStageClaim(claim) != nil ||
		task.Scope.ProjectID != project.Record.ID || task.Scope.EnvironmentID != environment.Record.ID ||
		claim.EnvironmentID != environment.Record.ID || claim.TaskID != task.ID || claim.RevisionID != task.ID ||
		claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation || request.EpochRevision <= 0 ||
		request.Projection.EnvironmentID != environment.Record.ID || request.Projection.RevisionID != task.ID ||
		request.Projection.RenderGeneration != claim.RenderGeneration || task.TargetID != route.Record.Desired.ID ||
		request.Marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		request.Marker.Locator.ScopeID != environment.Record.ID ||
		(route.Remove && (request.Current == nil || request.Current.Record != route.Record)) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Route publication authority is inconsistent",
		)
	}
	if err := routepersistence.ValidateRouteHierarchy(ctx, environment, project, request.Target, route.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, request.Marker); err != nil ||
		found {
		return existing, err
	}
	fence, err := repository.loadEnvironmentBlueprintMutationFence(ctx, project, environment)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if epoch, found := fence.EpochRevision(); !found || epoch != request.EpochRevision {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Route Environment changed before publication",
		)
	}
	publication, err := repository.prepareEnvironmentBlueprintPublication(ctx, claim,
		blueprints.EnvironmentDesiredRevisionIdentity{EnvironmentID: environment.Record.ID, RevisionID: task.ID},
		request.Projection, desiredPublicationOwner{TaskID: task.ID, EnvironmentID: environment.Record.ID,
			RevisionID: task.ID, RenderGeneration: claim.RenderGeneration}, request.Marker, claim.BaselineHeadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)
	identities, err := prepareEnvironmentDesiredIdentityPublication(ctx, repository.store, request.Projection,
		claim.SourceKind, claim.BaselineHeadRevision, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer identities.clear()
	definitions := routepersistence.NewRepository(repository.store)
	conditions, writes, err := definitions.PrepareDefinition(
		ctx,
		environment,
		project,
		request.Target,
		request.Current,
		route.Record,
		claim.BaselineHeadRevision,
		fence.ReadRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(writes)
	if item.Placement.Executor == workexecution.Controller && route.Remove {
		retirementConditions, retirement, err := routepersistence.PrepareRetirement(ctx, repository.store,
			environment.Record.ID, route.Record.Desired.ID, fence.ReadRevision())
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer keyvalue.ClearMutationValues(retirement)
		conditions = append(conditions, retirementConditions...)
		writes = retirement
	}
	check, err := edgechecks.RequireRouteIdle(ctx, repository.store, route.Record.Desired.ID, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	guards := append(fence.TransactionConditions(), request.PreparationConditions...)
	guards = append(guards, identities.conditions...)
	guards = append(guards, conditions...)
	guards = append(
		guards,
		check,
		keyvalue.Condition{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		keyvalue.Condition{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintRootKey(environment.Record.ID, task.ID),
			ModRevision: publication.rootRevision,
		},
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintHeadKey(environment.Record.ID),
			ModRevision: claim.BaselineHeadRevision,
		},
	)
	change, err := repository.prepareRouteWorkExecution(ctx, request, guards, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	for _, mutations := range [][]keyvalue.Mutation{identities.mutations, writes} {
		for _, mutation := range mutations {
			mutation.Value = append([]byte(nil), mutation.Value...)
			change.Mutations = append(change.Mutations, mutation)
		}
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	reference, err := idempotency.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	change.Mutations = append(
		change.Mutations,
		epoch,
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   blueprints.EnvironmentBlueprintHeadKey(environment.Record.ID),
			Value: reference,
		},
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   publication.descriptorKey,
			Value: append([]byte(nil), publication.publishedDescriptor...),
		},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: publication.locatorKey},
	)
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	result, err := queue.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if result.Succeeded {
		return IdempotencyTransactionResult{kind: idempotencyTransactionApplied, revision: result.Revision}, nil
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, request.Marker); err != nil ||
		found {
		return existing, err
	}
	return IdempotencyTransactionResult{}, errs.New(
		errs.KindStateConflict,
		"Route publication changed before atomic acceptance",
	)
}
