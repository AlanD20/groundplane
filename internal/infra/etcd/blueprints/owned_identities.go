package blueprints

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadCurrentOwnedIdentities follows one desired head, even when its effective
// runtime projection has not been rendered yet.
func ReadCurrentOwnedIdentities(
	ctx context.Context, store blueprintSnapshotReader, environmentID string, revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities], bool, error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, false, err
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{EnvironmentBlueprintHeadKey(environmentID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, false, err
	}
	if result == nil || len(result.Values) != 1 {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, false,
			errs.New(errs.KindInternal, "Environment identity head read is incomplete")
	}
	defer etcdstore.ClearValues(result.Values)
	if result.Values[0] == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{
			ReadRevision: result.ReadRevision,
		}, false, nil
	}
	revisionID, err := idempotencyrecord.DecodeTaskReference(result.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, false, err
	}
	selected, found, err := ReadOwnedIdentitiesRevision(ctx, store, environmentID, revisionID, result.ReadRevision)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, false, err
	}
	if !found {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, false,
			errs.New(errs.KindInternal, "Environment desired identity record is missing")
	}
	selected.Revision = result.Values[0].ModRevision
	return selected, true, nil
}

func ReadOwnedIdentitiesRevision(
	ctx context.Context, store blueprintSnapshotReader, environmentID, revisionID string, revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities], bool, error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindTask, revisionID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, false, err
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			EnvironmentBlueprintRootKey(environmentID, revisionID),
			EnvironmentBlueprintOwnedIdentitiesKey(environmentID, revisionID),
		}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, false, err
	}
	if result == nil || len(result.Values) != 2 {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, false,
			errs.New(errs.KindInternal, "Environment identity revision read is incomplete")
	}
	defer etcdstore.ClearValues(result.Values)
	if result.Values[0] == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{
			ReadRevision: result.ReadRevision,
		}, false, nil
	}
	if result.Values[1] == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, false,
			errs.New(errs.KindInternal, "Environment desired identity record is missing")
	}
	seal, err := DecodeEnvironmentBlueprintSeal(result.Values[0].Value)
	identities, identityErr := projectionrecord.DecodeEnvironmentOwnedIdentities(result.Values[1].Value)
	if err != nil || identityErr != nil || seal.EnvironmentID != environmentID ||
		seal.RevisionID != revisionID || identities.EnvironmentID != environmentID ||
		identities.RevisionID != revisionID || identities.RenderGeneration != seal.RenderGeneration {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, false,
			errs.New(errs.KindInternal, "Environment desired identity record is corrupt")
	}
	return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{
		Record: identities, Revision: result.Values[1].ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
