package environmentqueries

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GetEnvironmentZoneRemovalAuthorities reads both authorities required to
// fence a Zone removal. Each returned revision belongs to its own etcd key.
func (repository *ProjectionReader) GetEnvironmentZoneRemovalAuthorities(
	ctx context.Context,
	environmentID string,
) (environmentchanges.EnvironmentZoneRemovalAuthorities, bool, error) {
	desired, found, err := repository.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil || !found {
		return environmentchanges.EnvironmentZoneRemovalAuthorities{}, found, err
	}
	applied, found, err := repository.GetEnvironmentAppliedComposeProjection(ctx, environmentID)
	if err != nil {
		return environmentchanges.EnvironmentZoneRemovalAuthorities{}, false, err
	}
	if !found {
		return environmentchanges.EnvironmentZoneRemovalAuthorities{}, false, errs.New(
			errs.KindStateConflict,
			"Environment applied projection is missing",
		)
	}
	return environmentchanges.EnvironmentZoneRemovalAuthorities{Desired: desired, Applied: applied}, true, nil
}

func (repository *ProjectionReader) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	return blueprints.ReadCurrentProjection(ctx, repository.store, environmentID, 0)
}

// GetEnvironmentComposeProjectionRevision resolves one immutable published
// revision directly. Task execution uses this method and never substitutes the
// Environment's newer current head as render input.
func (repository *ProjectionReader) GetEnvironmentComposeProjectionRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindTask, revisionID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	return blueprints.ReadEffectiveProjectionRevision(ctx, repository.store, environmentID, revisionID, 0)
}
