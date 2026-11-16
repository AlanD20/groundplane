package desiredauthoring

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareAccepted stages a metadata-only edit for an already accepted Task.
// The original public replay remains unchanged. Its deterministic revision can
// resume identical inert staging after interruption; publication still joins
// the Task promotion and current desired-head comparison in one transaction.
func PrepareAccepted(ctx context.Context, store Store, task workexecution.Task, taskRevision int64,
	marker idempotency.IdempotencyMarker, at time.Time,
	mutate func(*core.BlueprintDesiredInput, *environmentprojection.EnvironmentComposeProjection) error,
) (Publication, error) {
	if taskRevision <= 0 || task.AcceptedIntent == nil || marker.TaskID != task.ID ||
		marker.Kind != idempotency.IdempotencyMarkerTask ||
		marker.State != idempotency.IdempotencyMarkerPending ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
		at.Before(task.CreatedAt) {
		return Publication{}, errs.New(errs.KindValidationFailed, "desired edit requires its accepted Task")
	}
	snapshot, err := loadSnapshot(ctx, store, task.Scope.EnvironmentID)
	if err != nil {
		return Publication{}, err
	}
	if snapshot.projection == nil {
		return Publication{}, errs.New(
			errs.KindStateConflict,
			"This edit requires the Environment runtime configuration",
		)
	}
	snapshot.input.RevisionID, snapshot.owned.RevisionID = task.ID, task.ID
	snapshot.projection.RevisionID = task.ID
	if err := mutate(&snapshot.input.Input, snapshot.projection); err != nil {
		return Publication{}, err
	}
	// A repeated compilation must reproduce the original staged descriptor,
	// including its capture clock, rather than move its cleanup deadline.
	descriptorKey := blueprints.EnvironmentBlueprintDescriptorKeyByID(strings.TrimPrefix(task.ID, "task_"))
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{descriptorKey}})
	if err != nil {
		return Publication{}, err
	}
	if read == nil || len(read.Values) != 1 {
		return Publication{}, errs.New(errs.KindInternal, "desired staging read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil {
		previous, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(read.Values[0].Value)
		if err != nil {
			return Publication{}, err
		}
		if previous.State != blueprints.EnvironmentBlueprintStageMetadata || previous.Claim.TaskID != task.ID ||
			previous.Claim.EnvironmentID != task.Scope.EnvironmentID {
			return Publication{}, errs.New(errs.KindStateConflict, "desired staging belongs to a different publication")
		}
		at = previous.UpdatedAt
	}
	publication, err := snapshot.prepareAcceptedStage(ctx, store, marker, at)
	if err != nil {
		return Publication{}, err
	}
	publication.Conditions = append(
		publication.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: taskRevision},
	)
	return publication, nil
}

func (snapshot metadataSnapshot) prepareAcceptedStage(
	ctx context.Context,
	store Store,
	marker idempotency.IdempotencyMarker,
	at time.Time,
) (Publication, error) {
	digest, err := blueprints.EnvironmentBlueprintDependencyDigest(*snapshot.projection)
	if err != nil {
		return Publication{}, err
	}
	publication, err := PrepareValues(
		snapshot.input,
		snapshot.projection,
		snapshot.owned,
		marker,
		snapshot.baseRevisionID,
		snapshot.headRevision,
		snapshot.environmentRevision,
		digest,
	)
	if err != nil {
		return Publication{}, err
	}
	descriptor, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(publication.Mutations[0].Value)
	if err != nil {
		keyvalue.ClearMutationValues(publication.Mutations)
		return Publication{}, err
	}
	descriptor.Claim.TaskLocator = &marker.Locator
	descriptor.UpdatedAt = at
	value, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		keyvalue.ClearMutationValues(publication.Mutations)
		return Publication{}, err
	}
	clear(publication.Mutations[0].Value)
	publication.Mutations[0].Value = value
	staged, err := stage(ctx, store, publication)
	if err != nil {
		return Publication{}, err
	}
	if snapshot.runtimeCondition != nil {
		staged.Conditions = append(staged.Conditions, *snapshot.runtimeCondition)
	}
	return staged, nil
}
