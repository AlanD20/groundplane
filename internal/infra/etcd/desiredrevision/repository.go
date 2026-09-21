package desiredrevision

import (
	"context"
	"crypto/sha256"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	MeasureTransaction(
		context.Context,
		[]etcdstore.Condition,
		[]etcdstore.Mutation,
	) (etcdstore.TransactionBudget, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

type Repository struct{ store store }

func NewRepository(backend etcdstore.Store) (*Repository, error) { return newRepository(backend) }
func newRepository(backend store) (*Repository, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "desired revision store is required")
	}
	return &Repository{store: backend}, nil
}

// ClaimEnvironmentBlueprintStage atomically reserves a descriptor and its
// scoped idempotency locator. If a locator already exists, the returned claim
// contains the winner's protected evidence and candidate IDs.
func (repository *Repository) ClaimEnvironmentBlueprintStage(
	ctx context.Context,
	request blueprints.EnvironmentBlueprintStageClaimRequest,
) (blueprints.EnvironmentBlueprintStageClaim, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, err
	}
	claim := blueprints.EnvironmentBlueprintStageClaim{
		DescriptorID: ids.NewULID(), EnvironmentID: request.EnvironmentID,
		RevisionID: request.CandidateRevisionID, TaskID: request.CandidateTaskID,
		Locator: request.Locator, Intent: request.Intent, BaselineHeadRevision: request.BaselineHeadRevision,
		SourceKind: request.SourceKind, RenderGeneration: request.RenderGeneration,
		ProjectionSchema: request.ProjectionSchema, CreatedAt: request.CreatedAt,
	}
	if err := blueprints.ValidateEnvironmentBlueprintStageClaim(claim); err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, err
	}
	descriptor := blueprints.EnvironmentBlueprintStageDescriptor{
		Claim: blueprints.CloneEnvironmentBlueprintStageClaim(claim), State: blueprints.EnvironmentBlueprintStageOpen,
		UpdatedAt: claim.CreatedAt,
	}
	descriptorValue, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, err
	}
	defer clear(descriptorValue)
	protectedDigest, err := blueprints.ProtectedBlueprintIntentDigest(claim.Intent)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, err
	}
	locatorValue, err := blueprints.EncodeEnvironmentBlueprintStageLocator(claim.DescriptorID, protectedDigest)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, err
	}
	defer clear(locatorValue)
	descriptorKey := blueprints.EnvironmentBlueprintDescriptorKeyByID(claim.DescriptorID)
	locatorKey, _, err := blueprints.EnvironmentBlueprintLocatorKey(claim.Locator)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, err
	}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(claim.Locator)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, err
	}
	conditions := []etcdstore.Condition{{Key: descriptorKey}, {Key: locatorKey}, {Key: markerKey}}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: descriptorKey, Value: descriptorValue},
		{Type: etcdstore.MutationPut, Key: locatorKey, Value: locatorValue},
	}
	if err := etcd.ValidateBlueprintTransaction(repository.store, conditions, mutations, 5, blueprints.EnvironmentBlueprintStageTransactionBytes); err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, err
	}
	defer clearKeyValues(result.FailureReads)
	if result.Succeeded {
		return blueprints.CloneEnvironmentBlueprintStageClaim(claim), nil
	}
	if len(result.FailureReads) != len(conditions) {
		return blueprints.EnvironmentBlueprintStageClaim{}, blueprints.CorruptEnvironmentBlueprintStage()
	}
	if result.FailureReads[2] != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindStateConflict,
			"public idempotency authority already owns the Blueprint staging locator",
		)
	}
	if result.FailureReads[1] == nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindInternal,
			"Blueprint descriptor identity collided without a locator owner",
		)
	}
	existingID, existingDigest, err := blueprints.DecodeEnvironmentBlueprintStageLocator(result.FailureReads[1].Value)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, err
	}
	existingKey := blueprints.EnvironmentBlueprintDescriptorPrefix + recordcodec.EncodeKeySegment(existingID)
	existing, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{existingKey}, Revision: result.Revision},
	)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, err
	}
	if existing == nil || len(existing.Values) != 1 || existing.Values[0] == nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, blueprints.CorruptEnvironmentBlueprintStage()
	}
	defer clear(existing.Values[0].Value)
	stored, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(existing.Values[0].Value)
	if err != nil || stored.Claim.DescriptorID != existingID ||
		stored.State == blueprints.EnvironmentBlueprintStagePublished || stored.State == blueprints.EnvironmentBlueprintStageAbandoned {
		return blueprints.EnvironmentBlueprintStageClaim{}, blueprints.CorruptEnvironmentBlueprintStage()
	}
	storedDigest, err := blueprints.ProtectedBlueprintIntentDigest(stored.Claim.Intent)
	if err != nil || storedDigest != existingDigest || stored.Claim.Locator != claim.Locator {
		return blueprints.EnvironmentBlueprintStageClaim{}, blueprints.CorruptEnvironmentBlueprintStage()
	}
	winner := blueprints.CloneEnvironmentBlueprintStageClaim(stored.Claim)
	winner.Existing = true
	return winner, nil
}

// StageEnvironmentBlueprintRevision binds validated stream metadata, writes
// immutable chunks in bounded batches, and seals the root. The normalized
// 2 MiB limit is checked before the first durable stream write.
func (repository *Repository) StageEnvironmentBlueprintRevision(
	ctx context.Context,
	request blueprints.EnvironmentBlueprintStageRequest,
) (blueprints.EnvironmentBlueprintSeal, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return blueprints.EnvironmentBlueprintSeal{}, err
	}
	streams, err := blueprints.BuildEnvironmentBlueprintStreams(request)
	if err != nil {
		return blueprints.EnvironmentBlueprintSeal{}, err
	}
	defer clear(streams.Audit)
	defer clear(streams.Projection)
	descriptor, revision, err := repository.bindEnvironmentBlueprintStage(ctx, streams.Descriptor)
	if err != nil {
		return blueprints.EnvironmentBlueprintSeal{}, err
	}
	for descriptor.State == blueprints.EnvironmentBlueprintStageOpen && descriptor.NextAuditChunk < descriptor.AuditChunks {
		descriptor, revision, err = repository.writeEnvironmentBlueprintStageBatch(
			ctx, descriptor, revision, blueprints.EnvironmentBlueprintChunkAudit, streams.Audit,
		)
		if err != nil {
			return blueprints.EnvironmentBlueprintSeal{}, err
		}
	}
	for descriptor.State == blueprints.EnvironmentBlueprintStageOpen && descriptor.NextProjectionChunk < descriptor.ProjectionChunks {
		descriptor, revision, err = repository.writeEnvironmentBlueprintStageBatch(
			ctx, descriptor, revision, blueprints.EnvironmentBlueprintChunkProjection, streams.Projection,
		)
		if err != nil {
			return blueprints.EnvironmentBlueprintSeal{}, err
		}
	}
	return repository.sealEnvironmentBlueprintStage(ctx, descriptor, revision)
}

func (repository *Repository) bindEnvironmentBlueprintStage(
	ctx context.Context,
	want blueprints.EnvironmentBlueprintStageDescriptor,
) (blueprints.EnvironmentBlueprintStageDescriptor, int64, error) {
	key := blueprints.EnvironmentBlueprintDescriptorKeyByID(want.Claim.DescriptorID)
	result, err := repository.store.Get(ctx, key)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	if result == nil || result.Entry == nil {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(
			errs.KindStateConflict,
			"Blueprint staging claim is unavailable",
		)
	}
	defer clear(result.Entry.Value)
	stored, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(result.Entry.Value)
	if err != nil || !blueprints.SameEnvironmentBlueprintStageClaim(stored.Claim, want.Claim) {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, blueprints.CorruptEnvironmentBlueprintStage()
	}
	if stored.State == blueprints.EnvironmentBlueprintStageAbandoned {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(
			errs.KindStateConflict,
			"Blueprint staging claim was abandoned",
		)
	}
	if stored.Bound {
		if !blueprints.SameEnvironmentBlueprintStageStreams(stored, want) {
			return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(
				errs.KindInternal,
				"Blueprint staging stream input changed",
			)
		}
		return stored, result.Entry.ModRevision, nil
	}
	next := want
	next.Claim = blueprints.CloneEnvironmentBlueprintStageClaim(stored.Claim)
	next.UpdatedAt = blueprints.NextBlueprintProgressTime(stored.UpdatedAt)
	value, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(next)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{{Key: key, ModRevision: result.Entry.ModRevision}}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}}
	if err := etcd.ValidateBlueprintTransaction(repository.store, conditions, mutations, 2, blueprints.EnvironmentBlueprintStageTransactionBytes); err != nil {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	if transaction.Succeeded {
		return next, transaction.Revision, nil
	}
	return repository.reloadEnvironmentBlueprintStage(ctx, want, 0, 0, nil, 0)
}

func (repository *Repository) writeEnvironmentBlueprintStageBatch(
	ctx context.Context,
	descriptor blueprints.EnvironmentBlueprintStageDescriptor,
	descriptorRevision int64,
	family uint8,
	stream []byte,
) (blueprints.EnvironmentBlueprintStageDescriptor, int64, error) {
	start, total := descriptor.NextAuditChunk, descriptor.AuditChunks
	if family == blueprints.EnvironmentBlueprintChunkProjection {
		start, total = descriptor.NextProjectionChunk, descriptor.ProjectionChunks
	} else if family != blueprints.EnvironmentBlueprintChunkAudit {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(errs.KindInternal, "Blueprint chunk family is invalid")
	}
	end := start + blueprints.EnvironmentBlueprintStageBatchChunks
	if end > total {
		end = total
	}
	if start >= end {
		return descriptor, descriptorRevision, nil
	}
	next := descriptor
	if family == blueprints.EnvironmentBlueprintChunkAudit {
		next.NextAuditChunk = end
	} else {
		next.NextProjectionChunk = end
	}
	next.UpdatedAt = blueprints.NextBlueprintProgressTime(descriptor.UpdatedAt)
	nextValue, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(next)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	defer clear(nextValue)
	conditions := make([]etcdstore.Condition, 0, int(end-start)+1)
	mutations := make([]etcdstore.Mutation, 0, int(end-start)+1)
	for index := start; index < end; index++ {
		key := blueprints.EnvironmentBlueprintChunkKeyFor(
			descriptor.Claim.EnvironmentID,
			descriptor.Claim.RevisionID,
			family,
			index,
		)
		from := int(index) * blueprints.EnvironmentBlueprintChunkBytes
		to := from + blueprints.EnvironmentBlueprintChunkBytes
		if to > len(stream) {
			to = len(stream)
		}
		data := stream[from:to]
		chunkValue, encodeErr := blueprints.EncodeEnvironmentBlueprintChunk(blueprints.EnvironmentBlueprintChunk{
			Family: family, Sequence: index, LogicalOffset: uint64(from),
			LogicalLength: uint32(len(data)), Digest: sha256.Sum256(data), Data: data,
		})
		if encodeErr != nil {
			clearMutationValues(mutations)
			return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, encodeErr
		}
		conditions = append(conditions, etcdstore.Condition{Key: key})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: chunkValue})
	}
	descriptorKey := blueprints.EnvironmentBlueprintDescriptorKeyByID(descriptor.Claim.DescriptorID)
	conditions = append(conditions, etcdstore.Condition{Key: descriptorKey, ModRevision: descriptorRevision})
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: descriptorKey, Value: nextValue})
	defer clearMutationValues(mutations)
	if err := etcd.ValidateBlueprintTransaction(
		repository.store,
		conditions,
		mutations,
		2*blueprints.EnvironmentBlueprintStageBatchChunks+2,
		blueprints.EnvironmentBlueprintStageTransactionBytes,
	); err != nil {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	if transaction.Succeeded {
		return next, transaction.Revision, nil
	}
	return repository.reloadEnvironmentBlueprintStage(ctx, descriptor, family, start, stream, end)
}

func (repository *Repository) reloadEnvironmentBlueprintStage(
	ctx context.Context,
	want blueprints.EnvironmentBlueprintStageDescriptor,
	family uint8,
	start uint32,
	stream []byte,
	minimum uint32,
) (blueprints.EnvironmentBlueprintStageDescriptor, int64, error) {
	key := blueprints.EnvironmentBlueprintDescriptorKeyByID(want.Claim.DescriptorID)
	result, err := repository.store.Get(ctx, key)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	if result == nil || result.Entry == nil {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(
			errs.KindStateConflict,
			"Blueprint staging state changed",
		)
	}
	defer clear(result.Entry.Value)
	stored, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(result.Entry.Value)
	if err != nil || !blueprints.SameEnvironmentBlueprintStageClaim(stored.Claim, want.Claim) ||
		(stored.Bound && !blueprints.SameEnvironmentBlueprintStageStreams(stored, want)) ||
		stored.NextAuditChunk < want.NextAuditChunk || stored.NextProjectionChunk < want.NextProjectionChunk {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, blueprints.CorruptEnvironmentBlueprintStage()
	}
	if !stored.Bound || stored.State == blueprints.EnvironmentBlueprintStageAbandoned {
		return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(
			errs.KindStateConflict,
			"Blueprint staging state changed",
		)
	}
	if minimum != 0 {
		progress := stored.NextAuditChunk
		if family == blueprints.EnvironmentBlueprintChunkProjection {
			progress = stored.NextProjectionChunk
		}
		if progress < minimum {
			return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(
				errs.KindStateConflict,
				"Blueprint staging batch kept changing",
			)
		}
		if err := repository.verifyEnvironmentBlueprintChunkRange(ctx, stored, family, start, progress, stream, result.ReadRevision); err != nil {
			return blueprints.EnvironmentBlueprintStageDescriptor{}, 0, err
		}
	}
	return stored, result.Entry.ModRevision, nil
}

func (repository *Repository) verifyEnvironmentBlueprintChunkRange(
	ctx context.Context,
	descriptor blueprints.EnvironmentBlueprintStageDescriptor,
	family uint8,
	start, end uint32,
	stream []byte,
	revision int64,
) error {
	keys := make([]string, 0, int(end-start))
	for index := start; index < end; index++ {
		keys = append(keys, blueprints.EnvironmentBlueprintChunkKeyFor(
			descriptor.Claim.EnvironmentID, descriptor.Claim.RevisionID, family, index,
		))
	}
	values, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if values == nil || len(values.Values) != len(keys) {
		return blueprints.CorruptEnvironmentBlueprintStage()
	}
	defer clearKeyValues(values.Values)
	for offset, entry := range values.Values {
		if entry == nil || entry.Key != keys[offset] {
			return blueprints.CorruptEnvironmentBlueprintStage()
		}
		index := start + uint32(offset)
		from := int(index) * blueprints.EnvironmentBlueprintChunkBytes
		to := from + blueprints.EnvironmentBlueprintChunkBytes
		if to > len(stream) {
			to = len(stream)
		}
		chunk, err := blueprints.DecodeEnvironmentBlueprintChunk(entry.Value)
		if err != nil || !blueprints.MatchingEnvironmentBlueprintChunk(chunk, family, index, stream[from:to]) {
			clear(chunk.Data)
			return blueprints.CorruptEnvironmentBlueprintStage()
		}
		clear(chunk.Data)
	}
	return nil
}

func (repository *Repository) sealEnvironmentBlueprintStage(
	ctx context.Context,
	descriptor blueprints.EnvironmentBlueprintStageDescriptor,
	descriptorRevision int64,
) (blueprints.EnvironmentBlueprintSeal, error) {
	seal := blueprints.EnvironmentBlueprintSealFromDescriptor(descriptor)
	if descriptor.State == blueprints.EnvironmentBlueprintStageSealed ||
		descriptor.State == blueprints.EnvironmentBlueprintStagePublished {
		return repository.requireEnvironmentBlueprintSeal(ctx, seal)
	}
	if descriptor.State != blueprints.EnvironmentBlueprintStageOpen ||
		descriptor.NextAuditChunk != descriptor.AuditChunks ||
		descriptor.NextProjectionChunk != descriptor.ProjectionChunks {
		return blueprints.EnvironmentBlueprintSeal{}, errs.New(
			errs.KindStateConflict,
			"Blueprint staging descriptor is not sealable",
		)
	}
	keys := blueprints.EnvironmentBlueprintChunkKeys(descriptor)
	chunks, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return blueprints.EnvironmentBlueprintSeal{}, err
	}
	if chunks == nil || len(chunks.Values) != len(keys) {
		return blueprints.EnvironmentBlueprintSeal{}, blueprints.CorruptEnvironmentBlueprintStage()
	}
	defer clearKeyValues(chunks.Values)
	conditions := make([]etcdstore.Condition, 0, len(keys)+2)
	for index, chunk := range chunks.Values {
		if chunk == nil || chunk.Key != keys[index] || chunk.ModRevision <= 0 {
			return blueprints.EnvironmentBlueprintSeal{}, blueprints.CorruptEnvironmentBlueprintStage()
		}
		conditions = append(conditions, etcdstore.Condition{Key: chunk.Key, ModRevision: chunk.ModRevision})
	}
	if err := blueprints.VerifyEnvironmentBlueprintChunks(descriptor, chunks.Values); err != nil {
		return blueprints.EnvironmentBlueprintSeal{}, err
	}
	descriptorKey := blueprints.EnvironmentBlueprintDescriptorKeyByID(descriptor.Claim.DescriptorID)
	rootKey := blueprints.EnvironmentBlueprintRootKey(descriptor.Claim.EnvironmentID, descriptor.Claim.RevisionID)
	conditions = append(conditions,
		etcdstore.Condition{Key: descriptorKey, ModRevision: descriptorRevision},
		etcdstore.Condition{Key: rootKey},
	)
	rootValue, err := blueprints.EncodeEnvironmentBlueprintSeal(seal)
	if err != nil {
		return blueprints.EnvironmentBlueprintSeal{}, err
	}
	defer clear(rootValue)
	sealed := descriptor
	sealed.State = blueprints.EnvironmentBlueprintStageSealed
	sealed.UpdatedAt = blueprints.NextBlueprintProgressTime(descriptor.UpdatedAt)
	descriptorValue, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(sealed)
	if err != nil {
		return blueprints.EnvironmentBlueprintSeal{}, err
	}
	defer clear(descriptorValue)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: rootKey, Value: rootValue},
		{Type: etcdstore.MutationPut, Key: descriptorKey, Value: descriptorValue},
	}
	if err := etcd.ValidateBlueprintTransaction(repository.store, conditions, mutations, 57, blueprints.EnvironmentBlueprintSealTransactionBytes); err != nil {
		return blueprints.EnvironmentBlueprintSeal{}, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return blueprints.EnvironmentBlueprintSeal{}, err
	}
	if !transaction.Succeeded {
		return repository.requireEnvironmentBlueprintSeal(ctx, seal)
	}
	return seal, nil
}

func (repository *Repository) requireEnvironmentBlueprintSeal(
	ctx context.Context,
	want blueprints.EnvironmentBlueprintSeal,
) (blueprints.EnvironmentBlueprintSeal, error) {
	result, err := repository.store.Get(
		ctx,
		blueprints.EnvironmentBlueprintRootKey(want.EnvironmentID, want.RevisionID),
	)
	if err != nil {
		return blueprints.EnvironmentBlueprintSeal{}, err
	}
	if result == nil || result.Entry == nil {
		return blueprints.EnvironmentBlueprintSeal{}, errs.New(
			errs.KindStateConflict,
			"Blueprint sealed root is unavailable",
		)
	}
	defer clear(result.Entry.Value)
	stored, err := blueprints.DecodeEnvironmentBlueprintSeal(result.Entry.Value)
	if err != nil || stored != want {
		return blueprints.EnvironmentBlueprintSeal{}, blueprints.CorruptEnvironmentBlueprintStage()
	}
	return stored, nil
}
