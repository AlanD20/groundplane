package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// GetEnvironmentBlueprintHead returns the current immutable revision pointer.
// A missing head is normal before an Environment's first successful apply.
func (repository *HierarchyRepository) GetEnvironmentBlueprintHead(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[EnvironmentBlueprintHead], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	result, err := repository.store.Get(ctx, environmentBlueprintHeadKey(environmentID))
	if err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	if result.Entry == nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{ReadRevision: result.ReadRevision}, false, nil
	}
	revisionID, err := idempotencyrecord.DecodeTaskReference(result.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	return etcdstore.Versioned[EnvironmentBlueprintHead]{
		Record:   EnvironmentBlueprintHead{EnvironmentID: environmentID, RevisionID: revisionID},
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
) (etcdstore.Versioned[EnvironmentBlueprintRevision], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindTask, revisionID); err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	rootResult, err := repository.store.Get(ctx, environmentBlueprintRootKey(environmentID, revisionID))
	if err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if rootResult.Entry == nil {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{
			ReadRevision: rootResult.ReadRevision,
		}, false, nil
	}
	seal, err := decodeEnvironmentBlueprintSeal(rootResult.Entry.Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if seal.SourceKind == EnvironmentBlueprintSourceMutation {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{ReadRevision: rootResult.ReadRevision}, false, nil
	}
	stream, readRevision, err := repository.readEnvironmentBlueprintStream(ctx, seal, "audit")
	if err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	defer clear(stream)
	revision, err := decodeEnvironmentBlueprintAuditStream(stream)
	if err != nil || revision.EnvironmentID != environmentID || revision.RevisionID != revisionID {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
	}
	return etcdstore.Versioned[EnvironmentBlueprintRevision]{
		Record: revision, Revision: rootResult.Entry.ModRevision,
		ReadRevision: readRevision,
	}, true, nil
}
