package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// GetEnvironmentBlueprintHead returns the current immutable revision pointer.
// A missing head is normal before an Environment's first successful apply.
func (repository *HierarchyRepository) GetEnvironmentBlueprintHead(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[blueprints.EnvironmentBlueprintHead], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintHead]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintHead]{}, false, err
	}
	result, err := repository.store.Get(ctx, blueprints.EnvironmentBlueprintHeadKey(environmentID))
	if err != nil {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintHead]{}, false, err
	}
	if result.Entry == nil {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintHead]{ReadRevision: result.ReadRevision}, false, nil
	}
	revisionID, err := idempotencyrecord.DecodeTaskReference(result.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintHead]{}, false, err
	}
	return etcdstore.Versioned[blueprints.EnvironmentBlueprintHead]{
		Record:   blueprints.EnvironmentBlueprintHead{EnvironmentID: environmentID, RevisionID: revisionID},
		Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

// GetEnvironmentBlueprintRevision reconstructs verified file bytes at the
// manifest's pinned MVCC revision. Missing immutable state is reported as not
// found; mismatched bytes or metadata are durable corruption.
func (repository *HierarchyRepository) GetEnvironmentBlueprintRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindTask, revisionID); err != nil {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision]{}, false, err
	}
	rootResult, err := repository.store.Get(ctx, blueprints.EnvironmentBlueprintRootKey(environmentID, revisionID))
	if err != nil {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision]{}, false, err
	}
	if rootResult.Entry == nil {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision]{
			ReadRevision: rootResult.ReadRevision,
		}, false, nil
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(rootResult.Entry.Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision]{}, false, err
	}
	if seal.SourceKind == blueprints.EnvironmentBlueprintSourceMutation {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision]{ReadRevision: rootResult.ReadRevision}, false, nil
	}
	stream, readRevision, err := blueprints.ReadStream(ctx, repository.store, seal, "audit")
	if err != nil {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision]{}, false, err
	}
	defer clear(stream)
	revision, err := blueprints.DecodeEnvironmentBlueprintAuditStream(stream)
	if err != nil || revision.EnvironmentID != environmentID || revision.RevisionID != revisionID {
		return etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision]{}, false, blueprints.CorruptEnvironmentBlueprint()
	}
	return etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision]{
		Record: revision, Revision: rootResult.Entry.ModRevision,
		ReadRevision: readRevision,
	}, true, nil
}
