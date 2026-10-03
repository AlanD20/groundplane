package blueprints

import (
	"context"
	"crypto/sha256"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintSnapshotReader interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

func ReadStream(
	ctx context.Context,
	store blueprintSnapshotReader,
	seal EnvironmentBlueprintSeal,
	family string,
) ([]byte, int64, error) {
	count, familyID := seal.AuditChunks, EnvironmentBlueprintChunkAudit
	if family == "projection" {
		count, familyID = seal.ProjectionChunks, EnvironmentBlueprintChunkProjection
	} else if family != "audit" {
		return nil, 0, errs.New(errs.KindInternal, "Blueprint stream family is invalid")
	}
	keys := make([]string, int(count))
	for index := range keys {
		keys[index] = EnvironmentBlueprintChunkKeyFor(seal.EnvironmentID, seal.RevisionID, familyID, uint32(index))
	}
	return ReadStreamAtRevision(ctx, store, seal, family, keys, 0)
}

func ReadStreamAtRevision(
	ctx context.Context,
	store blueprintSnapshotReader,
	seal EnvironmentBlueprintSeal,
	family string,
	keys []string,
	revision int64,
) ([]byte, int64, error) {
	length, digest, familyID := seal.AuditBytes, seal.AuditSHA256, EnvironmentBlueprintChunkAudit
	if family == "projection" {
		length, digest, familyID = seal.ProjectionBytes, seal.ProjectionSHA256, EnvironmentBlueprintChunkProjection
	} else if family != "audit" {
		return nil, 0, errs.New(errs.KindInternal, "Blueprint stream family is invalid")
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, 0, err
	}
	if result == nil || len(result.Values) != len(keys) {
		return nil, 0, CorruptEnvironmentBlueprintStage()
	}
	defer etcdstore.ClearValues(result.Values)
	stream := make([]byte, 0, int(length))
	for index, entry := range result.Values {
		if entry == nil || entry.Key != keys[index] {
			clear(stream)
			return nil, 0, CorruptEnvironmentBlueprintStage()
		}
		chunk, err := DecodeEnvironmentBlueprintChunk(entry.Value)
		if err != nil || chunk.Family != familyID || chunk.Sequence != uint32(index) {
			clear(chunk.Data)
			clear(stream)
			return nil, 0, CorruptEnvironmentBlueprintStage()
		}
		stream = append(stream, chunk.Data...)
		clear(chunk.Data)
	}
	computed := sha256.Sum256(stream)
	if uint64(len(stream)) != length || computed != digest {
		clear(stream)
		return nil, 0, CorruptEnvironmentBlueprintStage()
	}
	return stream, result.ReadRevision, nil
}

func ReadProjectionAtRevision(
	ctx context.Context,
	store blueprintSnapshotReader,
	environmentID string,
	revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	headResult, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{EnvironmentBlueprintHeadKey(environmentID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if headResult == nil || len(headResult.Values) != 1 {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	if headResult.Values[0] == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
			ReadRevision: headResult.ReadRevision,
		}, false, nil
	}
	revisionID, err := idempotencyrecord.DecodeTaskReference(headResult.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	projection, found, err := ReadEffectiveProjectionRevisionAt(
		ctx, store, environmentID, revisionID, headResult.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if !found {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	projection.Revision = headResult.Values[0].ModRevision
	return projection, true, nil
}

// ReadEffectiveProjectionRevisionAt reads the strict runtime projection for
// one exact desired revision. Missing derived runtime is a pending state, never
// evidence that no desired revision exists.
func ReadEffectiveProjectionRevisionAt(
	ctx context.Context,
	store blueprintSnapshotReader,
	environmentID string,
	revisionID string,
	revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	state, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			EnvironmentBlueprintRootKey(environmentID, revisionID),
			EnvironmentBlueprintEffectiveProjectionKey(environmentID, revisionID),
		},
		Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if state == nil || len(state.Values) != 2 {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	if state.Values[0] == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
			ReadRevision: state.ReadRevision,
		}, false, nil
	}
	seal, err := DecodeEnvironmentBlueprintSeal(state.Values[0].Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	if state.Values[1] == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, errs.New(
			errs.KindStateConflict,
			"Environment effective projection is pending",
		)
	}
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(state.Values[1].Value)
	if err != nil || projection.EnvironmentID != environmentID || projection.RevisionID != revisionID ||
		projection.RenderGeneration != seal.RenderGeneration || projection.BackingRuntime != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: projection, Revision: state.Values[0].ModRevision, ReadRevision: state.ReadRevision,
	}, true, nil
}

func ReadDesiredInputAtRevision(
	ctx context.Context,
	store blueprintSnapshotReader,
	environmentID string,
	revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error) {
	headResult, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{EnvironmentBlueprintHeadKey(environmentID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, err
	}
	if headResult == nil || len(headResult.Values) != 1 {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, projectionrecord.CorruptEnvironmentDesiredInput()
	}
	if headResult.Values[0] == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{
			ReadRevision: headResult.ReadRevision,
		}, false, nil
	}
	revisionID, err := idempotencyrecord.DecodeTaskReference(headResult.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, projectionrecord.CorruptEnvironmentDesiredInput()
	}
	input, found, err := ReadDesiredInputRevisionAt(
		ctx, store, environmentID, revisionID, headResult.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, err
	}
	if !found {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, projectionrecord.CorruptEnvironmentDesiredInput()
	}
	input.Revision = headResult.Values[0].ModRevision
	return input, true, nil
}

// ReadDesiredInputRevisionAt reads one exact immutable desired revision. It
// never follows a newer head and is therefore safe for queued work and retry.
func ReadDesiredInputRevisionAt(
	ctx context.Context,
	store blueprintSnapshotReader,
	environmentID string,
	revisionID string,
	revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error) {
	rootResult, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{EnvironmentBlueprintRootKey(environmentID, revisionID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, err
	}
	if rootResult == nil || len(rootResult.Values) != 1 {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, projectionrecord.CorruptEnvironmentDesiredInput()
	}
	if rootResult.Values[0] == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{
			ReadRevision: rootResult.ReadRevision,
		}, false, nil
	}
	seal, err := DecodeEnvironmentBlueprintSeal(rootResult.Values[0].Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, projectionrecord.CorruptEnvironmentDesiredInput()
	}
	keys := make([]string, int(seal.ProjectionChunks))
	for index := range keys {
		keys[index] = EnvironmentBlueprintChunkKeyFor(
			environmentID, revisionID, EnvironmentBlueprintChunkProjection, uint32(index),
		)
	}
	stream, readRevision, err := ReadStreamAtRevision(ctx, store, seal, "projection", keys, rootResult.ReadRevision)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, err
	}
	defer clear(stream)
	input, err := projectionrecord.DecodeEnvironmentDesiredInputStorage(stream)
	if err != nil || input.EnvironmentID != environmentID || input.RevisionID != revisionID ||
		input.RenderGeneration != seal.RenderGeneration {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, projectionrecord.CorruptEnvironmentDesiredInput()
	}
	return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{
		Record: input, Revision: rootResult.Values[0].ModRevision, ReadRevision: readRevision,
	}, true, nil
}
