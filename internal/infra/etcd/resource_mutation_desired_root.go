package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareMutationDesiredRoot advances only sealed desired-state authority. The
// resource publisher must join identity, policy, impact, replay and native-work
// changes before committing. No generic execution plan or old Task is created.
func (repository *HierarchyRepository) prepareMutationDesiredRoot(ctx context.Context,
	claim blueprints.EnvironmentBlueprintStageClaim, projection projectionrecord.EnvironmentComposeProjection,
	task workexecution.Task, marker idempotency.IdempotencyMarker, expectedHeadRevision int64,
) (workqueue.Change, error) {
	if err := task.Validate(); err != nil {
		return workqueue.Change{}, err
	}
	if err := idempotency.ValidateIdempotencyMarker(marker); err != nil {
		return workqueue.Change{}, err
	}
	if claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation || expectedHeadRevision < 0 ||
		projection.EnvironmentID != claim.EnvironmentID || projection.RevisionID != claim.RevisionID ||
		task.ID != claim.TaskID || task.Scope.EnvironmentID != claim.EnvironmentID ||
		task.ExpectedItems != 1 || task.PublicationComplete || task.FinishedAt != nil || task.Stop != nil ||
		(task.Action != workexecution.ActionCreate && task.Action != workexecution.ActionUpdate && task.Action != workexecution.ActionRemove) ||
		task.Intent.SHA256 != claim.Intent.CiphertextDigest || !task.CreatedAt.Equal(claim.CreatedAt) ||
		marker.TaskID != task.ID || marker.Kind != idempotency.IdempotencyMarkerTask ||
		marker.State != idempotency.IdempotencyMarkerPending ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment || marker.Locator.ScopeID != claim.EnvironmentID ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(task.CreatedAt) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "resource desired publication differs from its captured action")
	}
	revision := blueprints.EnvironmentDesiredRevisionIdentity{EnvironmentID: claim.EnvironmentID, RevisionID: claim.RevisionID}
	publication, err := repository.prepareEnvironmentBlueprintPublication(ctx, claim, revision, projection,
		desiredPublicationOwner{TaskID: task.ID, EnvironmentID: task.Scope.EnvironmentID,
			RevisionID: projection.RevisionID, RenderGeneration: uint64(projection.RenderGeneration)}, marker, expectedHeadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	reference, err := idempotency.EncodeTaskReference(revision.RevisionID)
	if err != nil {
		clear(publication.publishedDescriptor)
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: blueprints.EnvironmentBlueprintRootKey(revision.EnvironmentID, revision.RevisionID), ModRevision: publication.rootRevision},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: blueprints.EnvironmentBlueprintHeadKey(revision.EnvironmentID), ModRevision: expectedHeadRevision},
	}, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: keyvalue.MutationDelete, Key: publication.locatorKey},
		{Type: keyvalue.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(revision.EnvironmentID), Value: reference},
	}}, nil
}
