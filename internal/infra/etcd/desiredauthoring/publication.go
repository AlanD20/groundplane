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
	snapshot, err := loadSnapshot(ctx, store, environmentID)
	if err != nil {
		return Publication{}, err
	}
	if snapshot.projection == nil {
		return Publication{}, errs.New(
			errs.KindStateConflict,
			"This edit requires the Environment runtime configuration",
		)
	}
	if err := mutate(&snapshot.input.Input, snapshot.projection); err != nil {
		return Publication{}, err
	}
	return snapshot.stage(ctx, store, marker)
}

type metadataSnapshot struct {
	input               environmentprojection.EnvironmentDesiredInput
	projection          *environmentprojection.EnvironmentComposeProjection
	owned               environmentprojection.EnvironmentOwnedIdentities
	environment         hierarchy.EnvironmentRecord
	baseRevisionID      string
	headRevision        int64
	environmentRevision int64
	readRevision        int64
	dependencyDigest    [sha256.Size]byte
	runtimeCondition    *keyvalue.Condition
}

func loadSnapshot(ctx context.Context, store Store, environmentID string) (metadataSnapshot, error) {
	current, found, err := blueprints.ReadCurrentDesiredInput(ctx, store, environmentID, 0)
	if err != nil {
		return metadataSnapshot{}, err
	}
	if current.ReadRevision <= 0 {
		return metadataSnapshot{}, errs.New(errs.KindInternal, "desired authoring snapshot is unavailable")
	}
	keys := []string{hierarchy.EnvironmentKey(environmentID)}
	if found {
		keys = append(keys, blueprints.EnvironmentBlueprintRootKey(environmentID, current.Record.RevisionID))
		keys = append(
			keys,
			blueprints.EnvironmentBlueprintEffectiveProjectionKey(environmentID, current.Record.RevisionID),
		)
	}
	state, err := store.GetMany(
		ctx,
		keyvalue.GetManyRequest{
			Keys:     keys,
			Revision: current.ReadRevision,
		},
	)
	if err != nil {
		return metadataSnapshot{}, err
	}
	if state == nil || state.ReadRevision != current.ReadRevision || len(state.Values) != len(keys) ||
		state.Values[0] == nil {
		return metadataSnapshot{}, errs.New(errs.KindEnvironmentNotFound, "Environment does not exist")
	}
	environment, err := hierarchy.DecodeEnvironment(state.Values[0].Value)
	if err != nil {
		return metadataSnapshot{}, err
	}
	if environment.ProvisioningState != hierarchy.EnvironmentProvisioningReady {
		return metadataSnapshot{}, errs.New(errs.KindResourceInUse, "Environment is not ready for desired authoring")
	}
	revisionID := ids.New(ids.KindTask)
	var projection *environmentprojection.EnvironmentComposeProjection
	var owned environmentprojection.EnvironmentOwnedIdentities
	var dependencyDigest [sha256.Size]byte
	var runtimeCondition *keyvalue.Condition
	if found {
		if state.Values[1] == nil {
			return metadataSnapshot{}, errs.New(errs.KindInternal, "Desired authoring seal is unavailable")
		}
		seal, err := blueprints.DecodeEnvironmentBlueprintSeal(state.Values[1].Value)
		if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != current.Record.RevisionID ||
			seal.RenderGeneration != current.Record.RenderGeneration {
			return metadataSnapshot{}, errs.New(errs.KindInternal, "Desired authoring seal is inconsistent")
		}
		dependencyDigest = seal.DependencyDigest
		previous, hasProjection, readErr := blueprints.ReadAvailableRuntimeRevision(
			ctx,
			store,
			environmentID,
			current.Record.RevisionID,
			current.ReadRevision,
		)
		if readErr != nil {
			return metadataSnapshot{}, readErr
		}
		identities, hasOwned, readErr := blueprints.ReadCurrentOwnedIdentities(
			ctx,
			store,
			environmentID,
			current.ReadRevision,
		)
		if readErr != nil {
			return metadataSnapshot{}, readErr
		}
		if !hasOwned || hasProjection && previous.Record.RevisionID != current.Record.RevisionID ||
			identities.Record.RevisionID != current.Record.RevisionID {
			return metadataSnapshot{}, errs.New(
				errs.KindStateConflict,
				"desired authoring identities do not match the current revision",
			)
		}
		if hasProjection {
			copy := environmentprojection.CloneEnvironmentComposeProjection(previous.Record)
			projection = &copy
		}
		runtimeCondition = &keyvalue.Condition{
			Key: blueprints.EnvironmentBlueprintEffectiveProjectionKey(environmentID, current.Record.RevisionID),
		}
		if state.Values[2] != nil {
			runtimeCondition.ModRevision = state.Values[2].ModRevision
		}
		owned = environmentprojection.CloneEnvironmentOwnedIdentities(identities.Record)
	} else {
		empty, err := EmptyProjection(environment, revisionID)
		if err != nil {
			return metadataSnapshot{}, err
		}
		projection = &empty
		owned, err = environmentprojection.OwnedIdentitiesFromProjection(empty)
		if err != nil {
			return metadataSnapshot{}, err
		}
		current.Record = environmentprojection.EnvironmentDesiredInput{EnvironmentID: environmentID, RenderGeneration: 1,
			Input: core.BlueprintDesiredInput{NormalizedCompose: append([]byte(nil), projection.NormalizedCompose...), NetworkPool: environment.NetworkPool}}
	}
	input := environmentprojection.CloneEnvironmentDesiredInput(current.Record)
	baseRevisionID := input.RevisionID
	input.RevisionID, owned.RevisionID = revisionID, revisionID
	if projection != nil {
		projection.RevisionID = revisionID
	}
	return metadataSnapshot{
		input: input, projection: projection, owned: owned, environment: environment,
		baseRevisionID: baseRevisionID, headRevision: current.Revision,
		environmentRevision: state.Values[0].ModRevision, readRevision: current.ReadRevision,
		dependencyDigest: dependencyDigest, runtimeCondition: runtimeCondition,
	}, nil
}

func (snapshot metadataSnapshot) stage(
	ctx context.Context,
	store Store,
	marker idempotency.IdempotencyMarker,
) (Publication, error) {
	digest := snapshot.dependencyDigest
	if snapshot.projection != nil {
		var err error
		digest, err = blueprints.EnvironmentBlueprintDependencyDigest(*snapshot.projection)
		if err != nil {
			return Publication{}, err
		}
	}
	publication, err := PrepareValues(
		snapshot.input,
		snapshot.projection,
		snapshot.owned,
		marker,
		snapshot.baseRevisionID,
		snapshot.headRevision,
		snapshot.environmentRevision,
		digest,
	)
	if err != nil {
		return Publication{}, err
	}
	staged, err := stage(ctx, store, publication)
	if err != nil {
		return Publication{}, err
	}
	if snapshot.runtimeCondition != nil {
		staged.Conditions = append(staged.Conditions, *snapshot.runtimeCondition)
	}
	return staged, nil
}

func PrepareValues(
	input environmentprojection.EnvironmentDesiredInput,
	projection *environmentprojection.EnvironmentComposeProjection,
	owned environmentprojection.EnvironmentOwnedIdentities,
	marker idempotency.IdempotencyMarker,
	baseRevisionID string,
	headRevision, environmentRevision int64,
	dependencyDigest [sha256.Size]byte,
) (Publication, error) {
	claim := blueprints.EnvironmentBlueprintStageClaim{DescriptorID: strings.TrimPrefix(input.RevisionID, "task_"),
		EnvironmentID: input.EnvironmentID, RevisionID: input.RevisionID, TaskID: input.RevisionID,
		Locator: marker.Locator, Intent: marker.Intent, BaselineHeadRevision: headRevision,
		SourceKind: blueprints.EnvironmentBlueprintSourceMutation, RenderGeneration: input.RenderGeneration,
		ProjectionSchema: blueprints.EnvironmentDesiredInputSchema, CreatedAt: marker.CreatedAt}
	streams, err := blueprints.BuildEnvironmentBlueprintStreams(
		blueprints.EnvironmentBlueprintStageRequest{Claim: claim,
			Mutation: &blueprints.EnvironmentDesiredMutationAudit{
				Configuration: &blueprints.EnvironmentConfigurationMutationAudit{BaseRevisionID: baseRevisionID},
			},
			DesiredInput: input, DependencyDigest: dependencyDigest},
	)
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
		publication.Mutations = append(
			publication.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value},
		)
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
			value, encodeErr := blueprints.EncodeEnvironmentBlueprintChunk(
				blueprints.EnvironmentBlueprintChunk{Family: family,
					Sequence: index, LogicalOffset: uint64(from), LogicalLength: uint32(len(data)), Digest: sha256.Sum256(data), Data: data},
			)
			if err = add(blueprints.EnvironmentBlueprintChunkKeyFor(input.EnvironmentID, input.RevisionID, family, index), value, encodeErr); err != nil {
				keyvalue.ClearMutationValues(publication.Mutations)
				return Publication{}, err
			}
		}
	}
	seal, err := blueprints.EncodeEnvironmentBlueprintSeal(
		blueprints.EnvironmentBlueprintSealFromDescriptor(descriptor),
	)
	if err = add(blueprints.EnvironmentBlueprintRootKey(input.EnvironmentID, input.RevisionID), seal, err); err != nil {
		return Publication{}, err
	}
	if projection != nil {
		effective, err := environmentprojection.EncodePreparedEnvironmentComposeProjectionStorage(*projection)
		if err = add(blueprints.EnvironmentBlueprintEffectiveProjectionKey(input.EnvironmentID, input.RevisionID), effective, err); err != nil {
			return Publication{}, err
		}
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
