// Package desiredauthoring publishes direct authored changes alongside their
// resource transaction. Metadata revisions preserve the exact rendered artifact;
// only a runtime-rendering action advances its generation.
package desiredauthoring

import (
	"context"
	"crypto/sha256"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Store interface {
	Get(context.Context, string) (*keyvalue.GetResult, error)
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Publication struct {
	Conditions []keyvalue.Condition
	Mutations  []keyvalue.Mutation
}

// Prepare reads all authority at one fixed revision. Its head comparison must
// be committed atomically with the direct resource mutation and replay marker.
func Prepare(ctx context.Context, store Store, environmentID string, marker idempotency.IdempotencyMarker,
	mutate func(*core.BlueprintDesiredInput, *environmentprojection.EnvironmentComposeProjection) error,
) (Publication, error) {
	current, found, err := blueprints.ReadCurrentDesiredInput(ctx, store, environmentID, 0)
	if err != nil {
		return Publication{}, err
	}
	if current.ReadRevision <= 0 {
		return Publication{}, errs.New(errs.KindInternal, "desired authoring snapshot is unavailable")
	}
	state, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{hierarchy.EnvironmentKey(environmentID)}, Revision: current.ReadRevision})
	if err != nil {
		return Publication{}, err
	}
	if state == nil || len(state.Values) != 1 || state.Values[0] == nil {
		return Publication{}, errs.New(errs.KindEnvironmentNotFound, "Environment does not exist")
	}
	environment, err := hierarchy.DecodeEnvironment(state.Values[0].Value)
	if err != nil {
		return Publication{}, err
	}
	if environment.ProvisioningState != hierarchy.EnvironmentProvisioningReady {
		return Publication{}, errs.New(errs.KindResourceInUse, "Environment is not ready for desired authoring")
	}
	revisionID := ids.New(ids.KindTask)
	var projection environmentprojection.EnvironmentComposeProjection
	var owned environmentprojection.EnvironmentOwnedIdentities
	if found {
		previous, hasProjection, readErr := blueprints.ReadCurrentProjection(ctx, store, environmentID, current.ReadRevision)
		if readErr != nil {
			return Publication{}, readErr
		}
		identities, hasOwned, readErr := blueprints.ReadCurrentOwnedIdentities(ctx, store, environmentID, current.ReadRevision)
		if readErr != nil {
			return Publication{}, readErr
		}
		if !hasProjection || !hasOwned || previous.Record.RevisionID != current.Record.RevisionID || identities.Record.RevisionID != current.Record.RevisionID {
			return Publication{}, errs.New(errs.KindStateConflict, "desired authoring requires a complete current runtime projection")
		}
		projection = environmentprojection.CloneEnvironmentComposeProjection(previous.Record)
		owned = environmentprojection.CloneEnvironmentOwnedIdentities(identities.Record)
	} else {
		projection, err = EmptyProjection(environment, revisionID)
		if err != nil {
			return Publication{}, err
		}
		owned, err = environmentprojection.OwnedIdentitiesFromProjection(projection)
		if err != nil {
			return Publication{}, err
		}
		current.Record = environmentprojection.EnvironmentDesiredInput{EnvironmentID: environmentID, RenderGeneration: 1,
			Input: core.BlueprintDesiredInput{NormalizedCompose: append([]byte(nil), projection.NormalizedCompose...), NetworkPool: environment.NetworkPool}}
	}
	input := environmentprojection.CloneEnvironmentDesiredInput(current.Record)
	baseRevisionID := input.RevisionID
	input.RevisionID, projection.RevisionID, owned.RevisionID = revisionID, revisionID, revisionID
	if err := mutate(&input.Input, &projection); err != nil {
		return Publication{}, err
	}
	publication, err := PrepareValues(input, projection, owned, marker, baseRevisionID, current.Revision, state.Values[0].ModRevision)
	if err != nil {
		return Publication{}, err
	}
	return stage(ctx, store, publication)
}

func PrepareValues(input environmentprojection.EnvironmentDesiredInput, projection environmentprojection.EnvironmentComposeProjection,
	owned environmentprojection.EnvironmentOwnedIdentities, marker idempotency.IdempotencyMarker,
	baseRevisionID string, headRevision, environmentRevision int64,
) (Publication, error) {
	claim := blueprints.EnvironmentBlueprintStageClaim{DescriptorID: strings.TrimPrefix(input.RevisionID, "task_"),
		EnvironmentID: input.EnvironmentID, RevisionID: input.RevisionID, TaskID: input.RevisionID,
		Locator: marker.Locator, Intent: marker.Intent, BaselineHeadRevision: headRevision,
		SourceKind: blueprints.EnvironmentBlueprintSourceMutation, RenderGeneration: input.RenderGeneration,
		ProjectionSchema: blueprints.EnvironmentDesiredInputSchema, CreatedAt: marker.CreatedAt}
	digest, err := blueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		return Publication{}, err
	}
	streams, err := blueprints.BuildEnvironmentBlueprintStreams(blueprints.EnvironmentBlueprintStageRequest{Claim: claim,
		Mutation:     &blueprints.EnvironmentDesiredMutationAudit{Configuration: &blueprints.EnvironmentConfigurationMutationAudit{BaseRevisionID: baseRevisionID}},
		DesiredInput: input, DependencyDigest: digest})
	if err != nil {
		return Publication{}, err
	}
	defer clear(streams.Audit)
	defer clear(streams.Projection)
	descriptor := streams.Descriptor
	descriptor.State = blueprints.EnvironmentBlueprintStagePublished
	descriptor.NextAuditChunk, descriptor.NextProjectionChunk = descriptor.AuditChunks, descriptor.ProjectionChunks
	publication := Publication{Conditions: []keyvalue.Condition{
		{Key: blueprints.EnvironmentBlueprintHeadKey(input.EnvironmentID), ModRevision: headRevision},
		{Key: hierarchy.EnvironmentKey(input.EnvironmentID), ModRevision: environmentRevision},
		{Key: volumeremovalrecord.EnvironmentLockKey(input.EnvironmentID)},
	}}
	add := func(key string, value []byte, err error) error {
		if err != nil {
			return err
		}
		publication.Conditions = append(publication.Conditions, keyvalue.Condition{Key: key})
		publication.Mutations = append(publication.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
		return nil
	}
	descriptorValue, err := encodeDescriptor(descriptor)
	if err = add(blueprints.EnvironmentBlueprintDescriptorKeyByID(claim.DescriptorID), descriptorValue, err); err != nil {
		return Publication{}, err
	}
	for family, stream := range map[uint8][]byte{blueprints.EnvironmentBlueprintChunkAudit: streams.Audit, blueprints.EnvironmentBlueprintChunkProjection: streams.Projection} {
		for index := uint32(0); index < blueprints.ChunkCount32(len(stream)); index++ {
			from := int(index) * blueprints.EnvironmentBlueprintChunkBytes
			to := min(from+blueprints.EnvironmentBlueprintChunkBytes, len(stream))
			data := stream[from:to]
			value, encodeErr := blueprints.EncodeEnvironmentBlueprintChunk(blueprints.EnvironmentBlueprintChunk{Family: family,
				Sequence: index, LogicalOffset: uint64(from), LogicalLength: uint32(len(data)), Digest: sha256.Sum256(data), Data: data})
			if err = add(blueprints.EnvironmentBlueprintChunkKeyFor(input.EnvironmentID, input.RevisionID, family, index), value, encodeErr); err != nil {
				keyvalue.ClearMutationValues(publication.Mutations)
				return Publication{}, err
			}
		}
	}
	seal, err := blueprints.EncodeEnvironmentBlueprintSeal(blueprints.EnvironmentBlueprintSealFromDescriptor(descriptor))
	if err = add(blueprints.EnvironmentBlueprintRootKey(input.EnvironmentID, input.RevisionID), seal, err); err != nil {
		return Publication{}, err
	}
	effective, err := environmentprojection.EncodePreparedEnvironmentComposeProjectionStorage(projection)
	if err = add(blueprints.EnvironmentBlueprintEffectiveProjectionKey(input.EnvironmentID, input.RevisionID), effective, err); err != nil {
		return Publication{}, err
	}
	identity, err := environmentprojection.EncodeEnvironmentOwnedIdentities(owned)
	if err = add(blueprints.EnvironmentBlueprintOwnedIdentitiesKey(input.EnvironmentID, input.RevisionID), identity, err); err != nil {
		return Publication{}, err
	}
	head, err := idempotency.EncodeTaskReference(input.RevisionID)
	if err != nil {
		return Publication{}, err
	}
	publication.Mutations = append(publication.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: blueprints.EnvironmentBlueprintHeadKey(input.EnvironmentID), Value: head})
	return publication, nil
}

func encodeDescriptor(value blueprints.EnvironmentBlueprintStageDescriptor) ([]byte, error) {
	return blueprints.EncodeEnvironmentBlueprintStageDescriptor(value)
}
