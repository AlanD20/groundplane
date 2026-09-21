package etcd

import (
	"context"
	"crypto/sha256"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type routeHeadPublication struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func prepareRouteHeadPublication(
	ctx context.Context,
	store hierarchyStore,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
	current *projectionrecord.EnvironmentComposeProjection,
	candidate projectionrecord.EnvironmentComposeProjection,
	audit blueprints.EnvironmentDesiredMutationAudit,
	expectedHeadRevision int64,
) (routeHeadPublication, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return routeHeadPublication{}, err
	}
	if candidate.EnvironmentID == "" || candidate.RevisionID != task.ID || expectedHeadRevision < 0 {
		return routeHeadPublication{}, errs.New(errs.KindValidationFailed, "Route desired candidate head is invalid")
	}
	if current != nil &&
		(current.EnvironmentID != candidate.EnvironmentID || current.RenderGeneration >= candidate.RenderGeneration) {
		return routeHeadPublication{}, errs.New(errs.KindStateConflict, "Route desired candidate head does not advance")
	}
	if err := projectionrecord.ValidateEnvironmentComposeProjection(candidate); err != nil {
		return routeHeadPublication{}, err
	}
	if err := blueprints.ValidateEnvironmentDesiredMutationAudit(audit); err != nil {
		return routeHeadPublication{}, err
	}
	if marker.Intent.EnvelopeVersion == 0 || task.ID == "" {
		return routeHeadPublication{}, errs.New(
			errs.KindValidationFailed,
			"Route desired candidate audit identity is invalid",
		)
	}
	baseRevisionID := ""
	if current != nil {
		baseRevisionID = current.RevisionID
	}
	if audit.Route == nil || audit.Route.BaseRevisionID != baseRevisionID {
		return routeHeadPublication{}, errs.New(
			errs.KindValidationFailed,
			"Route desired candidate audit base is invalid",
		)
	}
	digest, err := blueprints.EnvironmentBlueprintDependencyDigest(candidate)
	if err != nil {
		return routeHeadPublication{}, err
	}
	claim := blueprints.EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(task.ID, string(ids.KindTask)+"_"),
		EnvironmentID: candidate.EnvironmentID, RevisionID: task.ID, TaskID: task.ID,
		Locator: marker.Locator, Intent: marker.Intent,
		BaselineHeadRevision: expectedHeadRevision, SourceKind: blueprints.EnvironmentBlueprintSourceMutation,
		RenderGeneration: candidate.RenderGeneration, ProjectionSchema: blueprints.EnvironmentDesiredProjectionSchema,
		CreatedAt: task.CreatedAt,
	}
	if err := blueprints.ValidateEnvironmentBlueprintStageClaim(claim); err != nil {
		return routeHeadPublication{}, err
	}
	streams, err := blueprints.BuildEnvironmentBlueprintStreams(blueprints.EnvironmentBlueprintStageRequest{
		Claim: claim, Mutation: &audit, Projection: candidate, DependencyDigest: digest,
	})
	if err != nil {
		return routeHeadPublication{}, err
	}
	defer clear(streams.Audit)
	defer clear(streams.Projection)
	descriptor := streams.Descriptor
	publishHead := audit.Route.Action != blueprints.EnvironmentRouteMutationRemove
	if publishHead {
		previous := projectionrecord.EnvironmentComposeProjection{}
		if current != nil {
			previous = *current
		}
		if err := validateEnvironmentComposeProjectionAdvance(previous, current != nil, candidate); err != nil {
			return routeHeadPublication{}, err
		}
	}
	if publishHead {
		descriptor.State = blueprints.EnvironmentBlueprintStagePublished
	} else {
		descriptor.State = blueprints.EnvironmentBlueprintStageSealed
	}
	descriptor.NextAuditChunk = descriptor.AuditChunks
	descriptor.NextProjectionChunk = descriptor.ProjectionChunks
	descriptorValue, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		return routeHeadPublication{}, err
	}
	publication := routeHeadPublication{
		conditions: []etcdstore.Condition{
			{Key: blueprints.EnvironmentBlueprintDescriptorKeyByID(claim.DescriptorID)},
			{Key: blueprints.EnvironmentBlueprintRootKey(claim.EnvironmentID, claim.RevisionID)},
			{Key: blueprints.EnvironmentBlueprintHeadKey(claim.EnvironmentID), ModRevision: expectedHeadRevision},
			{Key: removalrecord.EnvironmentLockKey(claim.EnvironmentID)},
		},
		mutations: []etcdstore.Mutation{{
			Type: etcdstore.MutationPut, Key: blueprints.EnvironmentBlueprintDescriptorKeyByID(claim.DescriptorID), Value: descriptorValue,
		}},
	}
	publication.values = append(publication.values, descriptorValue)
	for family, stream := range map[uint8][]byte{
		blueprints.EnvironmentBlueprintChunkAudit:      streams.Audit,
		blueprints.EnvironmentBlueprintChunkProjection: streams.Projection,
	} {
		chunks := blueprints.ChunkCount32(len(stream))
		for index := uint32(0); index < chunks; index++ {
			from := int(index) * blueprints.EnvironmentBlueprintChunkBytes
			to := from + blueprints.EnvironmentBlueprintChunkBytes
			if to > len(stream) {
				to = len(stream)
			}
			data := stream[from:to]
			value, encodeErr := blueprints.EncodeEnvironmentBlueprintChunk(blueprints.EnvironmentBlueprintChunk{
				Family: family, Sequence: index, LogicalOffset: uint64(from),
				LogicalLength: uint32(len(data)), Digest: sha256.Sum256(data), Data: data,
			})
			if encodeErr != nil {
				clearRouteHeadPublication(publication)
				return routeHeadPublication{}, encodeErr
			}
			key := blueprints.EnvironmentBlueprintChunkKeyFor(claim.EnvironmentID, claim.RevisionID, family, index)
			publication.conditions = append(publication.conditions, etcdstore.Condition{Key: key})
			publication.mutations = append(publication.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: value})
			publication.values = append(publication.values, value)
		}
	}
	seal := blueprints.EnvironmentBlueprintSealFromDescriptor(descriptor)
	rootValue, err := blueprints.EncodeEnvironmentBlueprintSeal(seal)
	if err != nil {
		clearRouteHeadPublication(publication)
		return routeHeadPublication{}, err
	}
	publication.values = append(publication.values, rootValue)
	publication.mutations = append(publication.mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: blueprints.EnvironmentBlueprintRootKey(claim.EnvironmentID, claim.RevisionID), Value: rootValue,
	})
	if publishHead {
		reference, referenceErr := idempotencyrecord.EncodeTaskReference(task.ID)
		if referenceErr != nil {
			clearRouteHeadPublication(publication)
			return routeHeadPublication{}, referenceErr
		}
		publication.values = append(publication.values, reference)
		publication.mutations = append(publication.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(claim.EnvironmentID), Value: reference,
		})
	}
	_ = store
	return publication, nil
}

func prepareRouteHeadPromotion(
	ctx context.Context,
	store hierarchyStore,
	intent environmentchanges.RouteRemovalIntent,
	revision int64,
) (routeHeadPublication, error) {
	lock, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{removalrecord.EnvironmentLockKey(intent.EnvironmentID)}, Revision: revision,
	})
	if err != nil {
		return routeHeadPublication{}, err
	}
	if lock == nil || len(lock.Values) != 1 {
		return routeHeadPublication{}, errs.New(errs.KindInternal, "Route removal lock evidence is incomplete")
	}
	if lock.Values[0] != nil {
		return routeHeadPublication{}, errs.New(errs.KindStateConflict, "environment Volume removal is in progress")
	}
	return prepareRouteHeadCandidate(ctx, store, intent, revision)
}

func prepareRouteHeadCandidate(
	ctx context.Context,
	store hierarchyStore,
	intent environmentchanges.RouteRemovalIntent,
	revision int64,
) (routeHeadPublication, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return routeHeadPublication{}, err
	}
	if intent.CurrentProjection == nil || intent.CandidateProjection == nil {
		return routeHeadPublication{}, errs.New(errs.KindStateConflict, "Route desired candidate is unavailable")
	}
	candidate := *intent.CandidateProjection
	descriptorKey := blueprints.EnvironmentBlueprintDescriptorKeyByID(
		strings.TrimPrefix(candidate.RevisionID, string(ids.KindTask)+"_"),
	)
	rootKey := blueprints.EnvironmentBlueprintRootKey(intent.EnvironmentID, candidate.RevisionID)
	headKey := blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID)
	state, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{descriptorKey, rootKey, headKey}, Revision: revision,
	})
	if err != nil {
		return routeHeadPublication{}, err
	}
	if state == nil || len(state.Values) != 3 || state.Values[0] == nil || state.Values[1] == nil ||
		state.Values[2] == nil || state.Values[2].ModRevision != intent.CurrentProjectionRevision {
		return routeHeadPublication{}, errs.New(errs.KindStateConflict, "Route desired staging evidence is unavailable")
	}
	descriptor, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(state.Values[0].Value)
	if err != nil {
		return routeHeadPublication{}, err
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(state.Values[1].Value)
	if err != nil {
		return routeHeadPublication{}, err
	}
	currentRevisionID, err := idempotencyrecord.DecodeTaskReference(state.Values[2].Value)
	if err != nil {
		return routeHeadPublication{}, err
	}
	digest, err := blueprints.EnvironmentBlueprintDependencyDigest(candidate)
	if err != nil {
		return routeHeadPublication{}, err
	}
	audit := blueprints.EnvironmentDesiredMutationAudit{Route: &blueprints.EnvironmentRouteMutationAudit{
		Action: blueprints.EnvironmentRouteMutationRemove, BaseRevisionID: intent.CurrentProjection.RevisionID,
		RouteID: intent.RouteID,
	}}
	streams, err := blueprints.BuildEnvironmentBlueprintStreams(blueprints.EnvironmentBlueprintStageRequest{
		Claim: descriptor.Claim, Mutation: &audit, Projection: candidate, DependencyDigest: digest,
	})
	if err != nil {
		return routeHeadPublication{}, err
	}
	defer clear(streams.Audit)
	defer clear(streams.Projection)
	if descriptor.State != blueprints.EnvironmentBlueprintStageSealed ||
		descriptor.Claim.EnvironmentID != intent.EnvironmentID ||
		descriptor.Claim.RevisionID != candidate.RevisionID ||
		descriptor.Claim.TaskID != candidate.RevisionID ||
		descriptor.Claim.BaselineHeadRevision != intent.CurrentProjectionRevision ||
		descriptor.Claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
		descriptor.Claim.RenderGeneration != candidate.RenderGeneration ||
		!blueprints.SameEnvironmentBlueprintStageStreams(descriptor, streams.Descriptor) ||
		seal != blueprints.EnvironmentBlueprintSealFromDescriptor(
			descriptor,
		) || currentRevisionID != intent.CurrentProjection.RevisionID {
		return routeHeadPublication{}, errs.New(errs.KindStateConflict, "Route desired staging evidence changed")
	}
	published := descriptor
	published.State = blueprints.EnvironmentBlueprintStagePublished
	published.UpdatedAt = blueprints.NextBlueprintProgressTime(descriptor.UpdatedAt)
	descriptorValue, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(published)
	if err != nil {
		return routeHeadPublication{}, err
	}
	reference, err := idempotencyrecord.EncodeTaskReference(candidate.RevisionID)
	if err != nil {
		clear(descriptorValue)
		return routeHeadPublication{}, err
	}
	return routeHeadPublication{
		conditions: []etcdstore.Condition{
			{Key: descriptorKey, ModRevision: state.Values[0].ModRevision},
			{Key: rootKey, ModRevision: state.Values[1].ModRevision},
			{Key: headKey, ModRevision: state.Values[2].ModRevision},
			{Key: removalrecord.EnvironmentLockKey(intent.EnvironmentID)},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: descriptorKey, Value: descriptorValue},
			{Type: etcdstore.MutationPut, Key: headKey, Value: reference},
		},
		values: [][]byte{descriptorValue, reference},
	}, nil
}

func validateCompletedRouteHeadReplay(
	ctx context.Context,
	store hierarchyStore,
	task TaskRecord,
	revision int64,
) error {
	environmentID := task.Params[taskjournal.TaskRouteEnvironmentParam]
	if task.Type != taskjournal.TaskRemove || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceRoute ||
		recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil {
		return nil
	}
	candidateRevisionID := task.ID
	if task.RetryOf != "" {
		read, err := store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{environmentchanges.RouteRemovalIntentKey(task.RetryOf)}, Revision: revision,
		})
		if err != nil {
			return err
		}
		if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
			return errs.New(errs.KindStateConflict, "Route removal retry candidate is unavailable")
		}
		intent, err := environmentchanges.DecodeRouteRemovalIntent(read.Values[0].Value)
		if err != nil || intent.CandidateProjection == nil || intent.EnvironmentID != environmentID ||
			intent.RouteID != task.Target {
			return errs.New(errs.KindStateConflict, "Route removal retry candidate changed")
		}
		candidateRevisionID = intent.CandidateProjection.RevisionID
	}
	descriptorKey := blueprints.EnvironmentBlueprintDescriptorKeyByID(
		strings.TrimPrefix(candidateRevisionID, string(ids.KindTask)+"_"),
	)
	keys := []string{
		descriptorKey,
		blueprints.EnvironmentBlueprintRootKey(environmentID, candidateRevisionID),
		blueprints.EnvironmentBlueprintHeadKey(environmentID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRoute), task.Target),
		componentTaskActiveEnvironmentKey(environmentID),
		routerecord.ObservationKey(task.Target),
	}
	state, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != len(keys) || state.Values[0] == nil ||
		state.Values[1] == nil || state.Values[2] == nil ||
		state.Values[3] != nil || state.Values[4] != nil || state.Values[5] != nil {
		return errs.New(errs.KindStateConflict, "Route removal completed replay state is incomplete")
	}
	descriptor, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(state.Values[0].Value)
	if err != nil {
		return err
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(state.Values[1].Value)
	if err != nil {
		return err
	}
	headRevisionID, err := idempotencyrecord.DecodeTaskReference(state.Values[2].Value)
	if err != nil {
		return err
	}
	selected, found, err := currentEnvironmentProjectionAtRevision(ctx, store, environmentID, revision)
	if err != nil || !found {
		return errs.New(errs.KindStateConflict, "Route removal completed replay projection is unavailable")
	}
	projection := selected.Record
	encoded, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		return err
	}
	defer clear(encoded)
	projectionDigest := sha256.Sum256(encoded)
	dependencyDigest, err := blueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		return err
	}
	if descriptor.State != blueprints.EnvironmentBlueprintStagePublished ||
		descriptor.Claim.EnvironmentID != environmentID ||
		descriptor.Claim.RevisionID != candidateRevisionID ||
		descriptor.Claim.TaskID != candidateRevisionID ||
		descriptor.Claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
		seal != blueprints.EnvironmentBlueprintSealFromDescriptor(descriptor) || headRevisionID != candidateRevisionID ||
		selected.Revision != state.Values[2].ModRevision || projection.EnvironmentID != environmentID || projection.RevisionID != candidateRevisionID ||
		uint64(len(encoded)) != descriptor.ProjectionBytes || projectionDigest != descriptor.ProjectionSHA256 ||
		dependencyDigest != descriptor.DependencyDigest {
		return errs.New(errs.KindStateConflict, "Route removal completed replay candidate changed")
	}
	return nil
}

func clearRouteHeadPublication(publication routeHeadPublication) {
	for _, value := range publication.values {
		clear(value)
	}
}

func classifyRouteHeadConflict(conditions []etcdstore.Condition) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "Route desired head compare evidence is incomplete")
		}
		for index, condition := range conditions {
			if !conditionMatchesRead(condition, values[index]) {
				return errs.New(errs.KindStateConflict, "Route desired head changed")
			}
		}
		return errs.New(errs.KindStateConflict, "Route desired head changed")
	}
}
