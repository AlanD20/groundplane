package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *HierarchyRepository) GetEnvironmentDesiredInput(
	ctx context.Context, environmentID string,
) (keyvalue.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error) {
	return blueprints.ReadCurrentDesiredInput(ctx, repository.store, environmentID, 0)
}

func (repository *HierarchyRepository) GetEnvironmentDesiredInputRevision(
	ctx context.Context, environmentID, revisionID string,
) (keyvalue.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error) {
	return blueprints.ReadDesiredInputRevision(ctx, repository.store, environmentID, revisionID, 0)
}

func (repository *HierarchyRepository) GetEnvironmentOwnedIdentities(
	ctx context.Context, environmentID string,
) (keyvalue.Versioned[projectionrecord.EnvironmentOwnedIdentities], bool, error) {
	return blueprints.ReadCurrentOwnedIdentities(ctx, repository.store, environmentID, 0)
}

func (repository *HierarchyRepository) GetEnvironmentOwnedIdentitiesRevision(
	ctx context.Context, environmentID, revisionID string,
) (keyvalue.Versioned[projectionrecord.EnvironmentOwnedIdentities], bool, error) {
	return blueprints.ReadOwnedIdentitiesRevision(ctx, repository.store, environmentID, revisionID, 0)
}
