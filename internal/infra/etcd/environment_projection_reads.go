package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GetEnvironmentZoneRemovalAuthorities reads both authorities required to
// fence a Zone removal. Each returned revision belongs to its own etcd key.
func (repository *HierarchyRepository) GetEnvironmentZoneRemovalAuthorities(
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

func (repository *HierarchyRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	head, err := repository.store.Get(ctx, blueprints.EnvironmentBlueprintHeadKey(environmentID))
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if head == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, errs.New(
			errs.KindInternal,
			"Environment desired head read is empty",
		)
	}
	if head.Entry == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{ReadRevision: head.ReadRevision}, false, nil
	}
	revisionID, err := idempotencyrecord.DecodeTaskReference(head.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	root, err := repository.store.Get(ctx, blueprints.EnvironmentBlueprintRootKey(environmentID, revisionID))
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if root == nil || root.Entry == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(root.Entry.Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	stream, readRevision, err := blueprints.ReadStream(ctx, repository.store, seal, "projection")
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	defer clear(stream)
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(stream)
	if err != nil || projection.EnvironmentID != environmentID || projection.RevisionID != revisionID {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: projection, Revision: head.Entry.ModRevision, ReadRevision: readRevision,
	}, true, nil
}

// GetEnvironmentComposeProjectionRevision resolves one immutable published
// revision directly. Task execution uses this method and never substitutes the
// Environment's newer current head as render input.
func (repository *HierarchyRepository) GetEnvironmentComposeProjectionRevision(
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
	root, err := repository.store.Get(ctx, blueprints.EnvironmentBlueprintRootKey(environmentID, revisionID))
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if root == nil || root.Entry == nil {
		readRevision := int64(0)
		if root != nil {
			readRevision = root.ReadRevision
		}
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{ReadRevision: readRevision}, false, nil
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(root.Entry.Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	stream, readRevision, err := blueprints.ReadStream(ctx, repository.store, seal, "projection")
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	defer clear(stream)
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(stream)
	if err != nil || projection.EnvironmentID != environmentID || projection.RevisionID != revisionID {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: projection, Revision: root.Entry.ModRevision, ReadRevision: readRevision,
	}, true, nil
}
