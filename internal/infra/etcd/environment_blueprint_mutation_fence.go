package etcd

import (
	"context"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func classifyEnvironmentBlueprintBaseConflict(
	values []*etcdstore.KeyValue,
	fileCount int,
	expectedHeadRevision int64,
	operationID string,
) error {
	if values[2] != nil {
		activeTaskID, err := idempotencyrecord.DecodeTaskReference(values[2].Value)
		if err != nil {
			return err
		}
		return errs.Newf(
			errs.KindStateConflict,
			"operation %s already has active task %s",
			operationID,
			activeTaskID,
		)
	}
	for _, index := range []int{0, 1, 3} {
		if values[index] != nil {
			return errs.New(errs.KindInternal, "Blueprint apply collided with durable Task state")
		}
	}
	for index := 4; index < 5+fileCount; index++ {
		if values[index] != nil {
			return errs.New(
				errs.KindInternal,
				"Blueprint apply collided with immutable revision state",
			)
		}
	}
	headIndex := 5 + fileCount
	if (expectedHeadRevision == 0 && values[headIndex] != nil) ||
		(expectedHeadRevision > 0 && (values[headIndex] == nil || values[headIndex].ModRevision != expectedHeadRevision)) {
		return errs.New(errs.KindStateConflict, "Environment desired state changed")
	}
	projectionIndex := headIndex + 1
	if (expectedHeadRevision == 0 && values[projectionIndex] != nil) ||
		(expectedHeadRevision > 0 &&
			(values[projectionIndex] == nil || values[projectionIndex].ModRevision != expectedHeadRevision)) {
		return errs.New(errs.KindStateConflict, "Environment Compose projection changed")
	}
	return nil
}

func (repository *HierarchyRepository) loadEnvironmentBlueprintMutationFence(
	ctx context.Context,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
) (environmentfence.Evidence, error) {
	keys := []string{hierarchyrecord.EnvironmentKey(environment.Record.ID), hierarchyrecord.ProjectKey(project.Record.ID)}
	anchor, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return environmentfence.Evidence{}, err
	}
	if anchor == nil || anchor.ReadRevision <= 0 || len(anchor.Values) != len(keys) ||
		anchor.Values[0] == nil || anchor.Values[1] == nil {
		return environmentfence.Evidence{}, errs.New(
			errs.KindStateConflict,
			"Environment Blueprint hierarchy is unavailable",
		)
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0].ModRevision != environment.Revision ||
		anchor.Values[1].ModRevision != project.Revision {
		return environmentfence.Evidence{}, errs.New(
			errs.KindStateConflict,
			"Environment Blueprint hierarchy changed",
		)
	}
	return environmentfence.LoadOrdinary(
		ctx, repository.store, environment.Record.ID, anchor.ReadRevision,
	)
}

func (repository *HierarchyRepository) getEnvironmentBlueprintProjectionAtRevision(
	ctx context.Context,
	environmentID string,
	readRevision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	return repository.getEnvironmentComposeProjectionAtRevision(ctx, environmentID, readRevision)
}
