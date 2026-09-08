package desiredrevision

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	Get(context.Context, string) (*etcd.GetResult, error)
	GetMany(context.Context, etcd.GetManyRequest) (*etcd.GetManyResult, error)
	Range(context.Context, etcd.RangeRequest) (*etcd.RangeResult, error)
	Transact(context.Context, []etcd.Condition, []etcd.Mutation) (etcd.TransactionResult, error)
}

type Repository struct{ store store }

func NewRepository(backend etcd.Store) (*Repository, error) { return newRepository(backend) }
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
	request etcd.EnvironmentBlueprintStageClaimRequest,
) (etcd.EnvironmentBlueprintStageClaim, error) {
	if err := etcd.ValidateCapabilityContext(ctx); err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	claim := etcd.EnvironmentBlueprintStageClaim{
		DescriptorID: ids.NewULID(), EnvironmentID: request.EnvironmentID,
		RevisionID: request.CandidateRevisionID, TaskID: request.CandidateTaskID,
		Locator: request.Locator, Intent: request.Intent, BaselineHeadRevision: request.BaselineHeadRevision,
		SourceKind: request.SourceKind, RenderGeneration: request.RenderGeneration,
		ProjectionSchema: request.ProjectionSchema, CreatedAt: request.CreatedAt,
	}
	if err := etcd.ValidateDesiredRevisionClaim(claim); err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	descriptor := etcd.EnvironmentBlueprintStageDescriptor{
		Claim: etcd.CloneDesiredRevisionClaim(claim), State: etcd.EnvironmentBlueprintStageOpen,
		UpdatedAt: claim.CreatedAt,
	}
	descriptorValue, err := etcd.EncodeDesiredRevisionDescriptor(descriptor)
	if err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	defer clear(descriptorValue)
	protectedDigest, err := etcd.ProtectedDesiredRevisionIntentDigest(claim.Intent)
	if err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	locatorValue, err := etcd.EncodeDesiredRevisionLocator(claim.DescriptorID, protectedDigest)
	if err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	defer clear(locatorValue)
	descriptorKey := etcd.DesiredRevisionDescriptorKey(claim.DescriptorID)
	locatorKey, _, err := etcd.DesiredRevisionLocatorKey(claim.Locator)
	if err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	markerKey, err := etcd.CapabilityIdempotencyMarkerKey(claim.Locator)
	if err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	conditions := []etcd.Condition{{Key: descriptorKey}, {Key: locatorKey}, {Key: markerKey}}
	mutations := []etcd.Mutation{
		{Type: etcd.MutationPut, Key: descriptorKey, Value: descriptorValue},
		{Type: etcd.MutationPut, Key: locatorKey, Value: locatorValue},
	}
	if err := etcd.ValidateDesiredRevisionTransaction(repository.store, conditions, mutations, 5, etcd.EnvironmentBlueprintStageTransactionBytes); err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	defer clearKeyValues(result.FailureReads)
	if result.Succeeded {
		return etcd.CloneDesiredRevisionClaim(claim), nil
	}
	if len(result.FailureReads) != len(conditions) {
		return etcd.EnvironmentBlueprintStageClaim{}, etcd.CorruptDesiredRevisionStage()
	}
	if result.FailureReads[2] != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindStateConflict,
			"public idempotency authority already owns the Blueprint staging locator",
		)
	}
	if result.FailureReads[1] == nil {
		return etcd.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindInternal,
			"Blueprint descriptor identity collided without a locator owner",
		)
	}
	existingID, existingDigest, err := etcd.DecodeDesiredRevisionLocator(result.FailureReads[1].Value)
	if err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	existingKey := etcd.EnvironmentBlueprintDescriptorPrefix + etcd.EncodeCapabilityKeySegment(existingID)
	existing, err := repository.store.GetMany(
		ctx,
		etcd.GetManyRequest{Keys: []string{existingKey}, Revision: result.Revision},
	)
	if err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	if existing == nil || len(existing.Values) != 1 || existing.Values[0] == nil {
		return etcd.EnvironmentBlueprintStageClaim{}, etcd.CorruptDesiredRevisionStage()
	}
	defer clear(existing.Values[0].Value)
	stored, err := etcd.DecodeDesiredRevisionDescriptor(existing.Values[0].Value)
	if err != nil || stored.Claim.DescriptorID != existingID ||
		stored.State == etcd.EnvironmentBlueprintStagePublished || stored.State == etcd.EnvironmentBlueprintStageAbandoned {
		return etcd.EnvironmentBlueprintStageClaim{}, etcd.CorruptDesiredRevisionStage()
	}
	storedDigest, err := etcd.ProtectedDesiredRevisionIntentDigest(stored.Claim.Intent)
	if err != nil || storedDigest != existingDigest || stored.Claim.Locator != claim.Locator {
		return etcd.EnvironmentBlueprintStageClaim{}, etcd.CorruptDesiredRevisionStage()
	}
	winner := etcd.CloneDesiredRevisionClaim(stored.Claim)
	winner.Existing = true
	return winner, nil
}

// StageEnvironmentBlueprintRevision binds validated stream metadata, writes
// immutable chunks in bounded batches, and seals the root. The normalized
// 2 MiB limit is checked before the first durable stream write.
func (repository *Repository) StageEnvironmentBlueprintRevision(
	ctx context.Context,
	request etcd.EnvironmentBlueprintStageRequest,
) (etcd.EnvironmentBlueprintSeal, error) {
	if err := etcd.ValidateCapabilityContext(ctx); err != nil {
		return etcd.EnvironmentBlueprintSeal{}, err
	}
	streams, err := etcd.BuildDesiredRevisionStreams(request)
	if err != nil {
		return etcd.EnvironmentBlueprintSeal{}, err
	}
	defer clear(streams.Audit)
	defer clear(streams.Projection)
	descriptor, revision, err := repository.bindEnvironmentBlueprintStage(ctx, streams.Descriptor)
	if err != nil {
		return etcd.EnvironmentBlueprintSeal{}, err
	}
	for descriptor.State == etcd.EnvironmentBlueprintStageOpen && descriptor.NextAuditChunk < descriptor.AuditChunks {
		descriptor, revision, err = repository.writeEnvironmentBlueprintStageBatch(
			ctx, descriptor, revision, etcd.EnvironmentBlueprintChunkAudit, streams.Audit,
		)
		if err != nil {
			return etcd.EnvironmentBlueprintSeal{}, err
		}
	}
	for descriptor.State == etcd.EnvironmentBlueprintStageOpen && descriptor.NextProjectionChunk < descriptor.ProjectionChunks {
		descriptor, revision, err = repository.writeEnvironmentBlueprintStageBatch(
			ctx, descriptor, revision, etcd.EnvironmentBlueprintChunkProjection, streams.Projection,
		)
		if err != nil {
			return etcd.EnvironmentBlueprintSeal{}, err
		}
	}
	return repository.sealEnvironmentBlueprintStage(ctx, descriptor, revision)
}

func (repository *Repository) bindEnvironmentBlueprintStage(
	ctx context.Context,
	want etcd.EnvironmentBlueprintStageDescriptor,
) (etcd.EnvironmentBlueprintStageDescriptor, int64, error) {
	key := etcd.DesiredRevisionDescriptorKey(want.Claim.DescriptorID)
	result, err := repository.store.Get(ctx, key)
	if err != nil {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	if result == nil || result.Entry == nil {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(
			errs.KindStateConflict,
			"Blueprint staging claim is unavailable",
		)
	}
	defer clear(result.Entry.Value)
	stored, err := etcd.DecodeDesiredRevisionDescriptor(result.Entry.Value)
	if err != nil || !etcd.SameDesiredRevisionClaim(stored.Claim, want.Claim) {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, etcd.CorruptDesiredRevisionStage()
	}
	if stored.State == etcd.EnvironmentBlueprintStageAbandoned {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(
			errs.KindStateConflict,
			"Blueprint staging claim was abandoned",
		)
	}
	if stored.Bound {
		if !etcd.SameDesiredRevisionStreams(stored, want) {
			return etcd.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(
				errs.KindInternal,
				"Blueprint staging stream input changed",
			)
		}
		return stored, result.Entry.ModRevision, nil
	}
	next := want
	next.Claim = etcd.CloneDesiredRevisionClaim(stored.Claim)
	next.UpdatedAt = etcd.NextDesiredRevisionProgressTime(stored.UpdatedAt)
	value, err := etcd.EncodeDesiredRevisionDescriptor(next)
	if err != nil {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	defer clear(value)
	conditions := []etcd.Condition{{Key: key, ModRevision: result.Entry.ModRevision}}
	mutations := []etcd.Mutation{{Type: etcd.MutationPut, Key: key, Value: value}}
	if err := etcd.ValidateDesiredRevisionTransaction(repository.store, conditions, mutations, 2, etcd.EnvironmentBlueprintStageTransactionBytes); err != nil {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	if transaction.Succeeded {
		return next, transaction.Revision, nil
	}
	return repository.reloadEnvironmentBlueprintStage(ctx, want, 0, 0, nil, 0)
}

func (repository *Repository) writeEnvironmentBlueprintStageBatch(
	ctx context.Context,
	descriptor etcd.EnvironmentBlueprintStageDescriptor,
	descriptorRevision int64,
	family uint8,
	stream []byte,
) (etcd.EnvironmentBlueprintStageDescriptor, int64, error) {
	start, total := descriptor.NextAuditChunk, descriptor.AuditChunks
	if family == etcd.EnvironmentBlueprintChunkProjection {
		start, total = descriptor.NextProjectionChunk, descriptor.ProjectionChunks
	} else if family != etcd.EnvironmentBlueprintChunkAudit {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(errs.KindInternal, "Blueprint chunk family is invalid")
	}
	end := start + etcd.EnvironmentBlueprintStageBatchChunks
	if end > total {
		end = total
	}
	if start >= end {
		return descriptor, descriptorRevision, nil
	}
	next := descriptor
	if family == etcd.EnvironmentBlueprintChunkAudit {
		next.NextAuditChunk = end
	} else {
		next.NextProjectionChunk = end
	}
	next.UpdatedAt = etcd.NextDesiredRevisionProgressTime(descriptor.UpdatedAt)
	nextValue, err := etcd.EncodeDesiredRevisionDescriptor(next)
	if err != nil {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	defer clear(nextValue)
	conditions := make([]etcd.Condition, 0, int(end-start)+1)
	mutations := make([]etcd.Mutation, 0, int(end-start)+1)
	for index := start; index < end; index++ {
		key := etcd.DesiredRevisionChunkKey(
			descriptor.Claim.EnvironmentID,
			descriptor.Claim.RevisionID,
			family,
			index,
		)
		from := int(index) * etcd.EnvironmentBlueprintChunkBytes
		to := from + etcd.EnvironmentBlueprintChunkBytes
		if to > len(stream) {
			to = len(stream)
		}
		data := stream[from:to]
		chunkValue, encodeErr := etcd.EncodeDesiredRevisionChunk(etcd.EnvironmentBlueprintChunk{
			Family: family, Sequence: index, LogicalOffset: uint64(from),
			LogicalLength: uint32(len(data)), Digest: sha256.Sum256(data), Data: data,
		})
		if encodeErr != nil {
			clearMutationValues(mutations)
			return etcd.EnvironmentBlueprintStageDescriptor{}, 0, encodeErr
		}
		conditions = append(conditions, etcd.Condition{Key: key})
		mutations = append(mutations, etcd.Mutation{Type: etcd.MutationPut, Key: key, Value: chunkValue})
	}
	descriptorKey := etcd.DesiredRevisionDescriptorKey(descriptor.Claim.DescriptorID)
	conditions = append(conditions, etcd.Condition{Key: descriptorKey, ModRevision: descriptorRevision})
	mutations = append(mutations, etcd.Mutation{Type: etcd.MutationPut, Key: descriptorKey, Value: nextValue})
	defer clearMutationValues(mutations)
	if err := etcd.ValidateDesiredRevisionTransaction(
		repository.store,
		conditions,
		mutations,
		2*etcd.EnvironmentBlueprintStageBatchChunks+2,
		etcd.EnvironmentBlueprintStageTransactionBytes,
	); err != nil {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	if transaction.Succeeded {
		return next, transaction.Revision, nil
	}
	return repository.reloadEnvironmentBlueprintStage(ctx, descriptor, family, start, stream, end)
}

func (repository *Repository) reloadEnvironmentBlueprintStage(
	ctx context.Context,
	want etcd.EnvironmentBlueprintStageDescriptor,
	family uint8,
	start uint32,
	stream []byte,
	minimum uint32,
) (etcd.EnvironmentBlueprintStageDescriptor, int64, error) {
	key := etcd.DesiredRevisionDescriptorKey(want.Claim.DescriptorID)
	result, err := repository.store.Get(ctx, key)
	if err != nil {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, err
	}
	if result == nil || result.Entry == nil {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(
			errs.KindStateConflict,
			"Blueprint staging state changed",
		)
	}
	defer clear(result.Entry.Value)
	stored, err := etcd.DecodeDesiredRevisionDescriptor(result.Entry.Value)
	if err != nil || !etcd.SameDesiredRevisionClaim(stored.Claim, want.Claim) ||
		(stored.Bound && !etcd.SameDesiredRevisionStreams(stored, want)) ||
		stored.NextAuditChunk < want.NextAuditChunk || stored.NextProjectionChunk < want.NextProjectionChunk {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, etcd.CorruptDesiredRevisionStage()
	}
	if !stored.Bound || stored.State == etcd.EnvironmentBlueprintStageAbandoned {
		return etcd.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(
			errs.KindStateConflict,
			"Blueprint staging state changed",
		)
	}
	if minimum != 0 {
		progress := stored.NextAuditChunk
		if family == etcd.EnvironmentBlueprintChunkProjection {
			progress = stored.NextProjectionChunk
		}
		if progress < minimum {
			return etcd.EnvironmentBlueprintStageDescriptor{}, 0, errs.New(
				errs.KindStateConflict,
				"Blueprint staging batch kept changing",
			)
		}
		if err := repository.verifyEnvironmentBlueprintChunkRange(ctx, stored, family, start, progress, stream, result.ReadRevision); err != nil {
			return etcd.EnvironmentBlueprintStageDescriptor{}, 0, err
		}
	}
	return stored, result.Entry.ModRevision, nil
}

func (repository *Repository) verifyEnvironmentBlueprintChunkRange(
	ctx context.Context,
	descriptor etcd.EnvironmentBlueprintStageDescriptor,
	family uint8,
	start, end uint32,
	stream []byte,
	revision int64,
) error {
	keys := make([]string, 0, int(end-start))
	for index := start; index < end; index++ {
		keys = append(keys, etcd.DesiredRevisionChunkKey(
			descriptor.Claim.EnvironmentID, descriptor.Claim.RevisionID, family, index,
		))
	}
	values, err := repository.store.GetMany(ctx, etcd.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if values == nil || len(values.Values) != len(keys) {
		return etcd.CorruptDesiredRevisionStage()
	}
	defer clearKeyValues(values.Values)
	for offset, entry := range values.Values {
		if entry == nil || entry.Key != keys[offset] {
			return etcd.CorruptDesiredRevisionStage()
		}
		index := start + uint32(offset)
		from := int(index) * etcd.EnvironmentBlueprintChunkBytes
		to := from + etcd.EnvironmentBlueprintChunkBytes
		if to > len(stream) {
			to = len(stream)
		}
		chunk, err := etcd.DecodeDesiredRevisionChunk(entry.Value)
		if err != nil || !etcd.MatchingDesiredRevisionChunk(chunk, family, index, stream[from:to]) {
			clear(chunk.Data)
			return etcd.CorruptDesiredRevisionStage()
		}
		clear(chunk.Data)
	}
	return nil
}

func (repository *Repository) sealEnvironmentBlueprintStage(
	ctx context.Context,
	descriptor etcd.EnvironmentBlueprintStageDescriptor,
	descriptorRevision int64,
) (etcd.EnvironmentBlueprintSeal, error) {
	seal := etcd.DesiredRevisionSealFromDescriptor(descriptor)
	if descriptor.State == etcd.EnvironmentBlueprintStageSealed ||
		descriptor.State == etcd.EnvironmentBlueprintStagePublished {
		return repository.requireEnvironmentBlueprintSeal(ctx, seal)
	}
	if descriptor.State != etcd.EnvironmentBlueprintStageOpen || descriptor.NextAuditChunk != descriptor.AuditChunks ||
		descriptor.NextProjectionChunk != descriptor.ProjectionChunks {
		return etcd.EnvironmentBlueprintSeal{}, errs.New(
			errs.KindStateConflict,
			"Blueprint staging descriptor is not sealable",
		)
	}
	keys := etcd.DesiredRevisionChunkKeys(descriptor)
	chunks, err := repository.store.GetMany(ctx, etcd.GetManyRequest{Keys: keys})
	if err != nil {
		return etcd.EnvironmentBlueprintSeal{}, err
	}
	if chunks == nil || len(chunks.Values) != len(keys) {
		return etcd.EnvironmentBlueprintSeal{}, etcd.CorruptDesiredRevisionStage()
	}
	defer clearKeyValues(chunks.Values)
	conditions := make([]etcd.Condition, 0, len(keys)+2)
	for index, chunk := range chunks.Values {
		if chunk == nil || chunk.Key != keys[index] || chunk.ModRevision <= 0 {
			return etcd.EnvironmentBlueprintSeal{}, etcd.CorruptDesiredRevisionStage()
		}
		conditions = append(conditions, etcd.Condition{Key: chunk.Key, ModRevision: chunk.ModRevision})
	}
	if err := etcd.VerifyDesiredRevisionChunks(descriptor, chunks.Values); err != nil {
		return etcd.EnvironmentBlueprintSeal{}, err
	}
	descriptorKey := etcd.DesiredRevisionDescriptorKey(descriptor.Claim.DescriptorID)
	rootKey := etcd.DesiredRevisionRootKey(descriptor.Claim.EnvironmentID, descriptor.Claim.RevisionID)
	conditions = append(conditions,
		etcd.Condition{Key: descriptorKey, ModRevision: descriptorRevision},
		etcd.Condition{Key: rootKey},
	)
	rootValue, err := etcd.EncodeDesiredRevisionSeal(seal)
	if err != nil {
		return etcd.EnvironmentBlueprintSeal{}, err
	}
	defer clear(rootValue)
	sealed := descriptor
	sealed.State = etcd.EnvironmentBlueprintStageSealed
	sealed.UpdatedAt = etcd.NextDesiredRevisionProgressTime(descriptor.UpdatedAt)
	descriptorValue, err := etcd.EncodeDesiredRevisionDescriptor(sealed)
	if err != nil {
		return etcd.EnvironmentBlueprintSeal{}, err
	}
	defer clear(descriptorValue)
	mutations := []etcd.Mutation{
		{Type: etcd.MutationPut, Key: rootKey, Value: rootValue},
		{Type: etcd.MutationPut, Key: descriptorKey, Value: descriptorValue},
	}
	if err := etcd.ValidateDesiredRevisionTransaction(repository.store, conditions, mutations, 57, etcd.EnvironmentBlueprintSealTransactionBytes); err != nil {
		return etcd.EnvironmentBlueprintSeal{}, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcd.EnvironmentBlueprintSeal{}, err
	}
	if !transaction.Succeeded {
		return repository.requireEnvironmentBlueprintSeal(ctx, seal)
	}
	return seal, nil
}

func (repository *Repository) requireEnvironmentBlueprintSeal(
	ctx context.Context,
	want etcd.EnvironmentBlueprintSeal,
) (etcd.EnvironmentBlueprintSeal, error) {
	result, err := repository.store.Get(ctx, etcd.DesiredRevisionRootKey(want.EnvironmentID, want.RevisionID))
	if err != nil {
		return etcd.EnvironmentBlueprintSeal{}, err
	}
	if result == nil || result.Entry == nil {
		return etcd.EnvironmentBlueprintSeal{}, errs.New(errs.KindStateConflict, "Blueprint sealed root is unavailable")
	}
	defer clear(result.Entry.Value)
	stored, err := etcd.DecodeDesiredRevisionSeal(result.Entry.Value)
	if err != nil || stored != want {
		return etcd.EnvironmentBlueprintSeal{}, etcd.CorruptDesiredRevisionStage()
	}
	return stored, nil
}
