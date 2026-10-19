package etcd

import (
	"context"

	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyRepository) loadEnvironmentBlueprintMutationFence(
	ctx context.Context,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
) (environmentfence.Evidence, error) {
	keys := []string{
		hierarchyrecord.EnvironmentKey(environment.Record.ID),
		hierarchyrecord.ProjectKey(project.Record.ID),
	}
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
	return blueprints.ReadProjectionAtRevision(ctx, repository.store, environmentID, readRevision)
}
