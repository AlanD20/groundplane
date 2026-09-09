package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintTransactionSizer interface {
	transactionSize([]Condition, []Mutation) (int, error)
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintPublication(
	ctx context.Context,
	claim EnvironmentBlueprintStageClaim,
	revision EnvironmentDesiredRevisionIdentity,
	projection EnvironmentComposeProjection,
	task TaskRecord,
	marker IdempotencyMarker,
	expectedHeadRevision int64,
) (environmentBlueprintPublicationEvidence, error) {
	digest, err := EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	if err := validateEnvironmentBlueprintStageClaim(claim); err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	if err := validateEnvironmentDesiredRevisionIdentity(revision); err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	descriptorKey := environmentBlueprintDescriptorKeyByID(claim.DescriptorID)
	descriptorRead, err := repository.store.Get(ctx, descriptorKey)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	if descriptorRead == nil || descriptorRead.Entry == nil {
		return environmentBlueprintPublicationEvidence{}, errs.New(
			errs.KindStateConflict,
			"Blueprint sealed staging evidence is unavailable",
		)
	}
	defer clear(descriptorRead.Entry.Value)
	descriptor, err := decodeEnvironmentBlueprintStageDescriptor(descriptorRead.Entry.Value)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	locatorKey, _, err := environmentBlueprintLocatorKey(descriptor.Claim.Locator)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	rootKey := environmentBlueprintRootKey(revision.EnvironmentID, revision.RevisionID)
	keys := []string{rootKey, descriptorKey, locatorKey}
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: descriptorRead.ReadRevision})
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	if result == nil || len(result.Values) != len(keys) || result.Values[0] == nil ||
		result.Values[1] == nil || result.Values[2] == nil {
		return environmentBlueprintPublicationEvidence{}, errs.New(
			errs.KindStateConflict,
			"Blueprint sealed staging evidence is unavailable",
		)
	}
	defer clearKeyValues(result.Values)
	seal, err := decodeEnvironmentBlueprintSeal(result.Values[0].Value)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	descriptor, err = decodeEnvironmentBlueprintStageDescriptor(result.Values[1].Value)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	descriptorID, locatorDigest, err := decodeEnvironmentBlueprintStageLocator(result.Values[2].Value)
	protectedDigest, digestErr := protectedBlueprintIntentDigest(descriptor.Claim.Intent)
	taskEnvironmentID, materializes, taskEnvironmentErr := desiredRevisionTaskEnvironment(task)
	if err != nil || digestErr != nil || !sameEnvironmentBlueprintStageClaim(descriptor.Claim, claim) ||
		descriptorID != descriptor.Claim.DescriptorID || locatorDigest != protectedDigest ||
		descriptor.State != EnvironmentBlueprintStageSealed || seal != environmentBlueprintSealFromDescriptor(descriptor) ||
		seal.EnvironmentID != revision.EnvironmentID || seal.RevisionID != revision.RevisionID ||
		seal.BaselineHeadRevision != expectedHeadRevision || seal.DependencyDigest != digest ||
		descriptor.Claim.TaskID != task.ID || taskEnvironmentErr != nil || !materializes ||
		taskEnvironmentID != revision.EnvironmentID ||
		task.Params[EnvironmentDesiredRevisionParam] != revision.RevisionID ||
		uint64(task.RenderGeneration) != descriptor.Claim.RenderGeneration || marker.TaskID != task.ID ||
		!sameBlueprintProtectedIntent(marker.Intent, descriptor.Claim.Intent) {
		return environmentBlueprintPublicationEvidence{}, errs.New(
			errs.KindStateConflict,
			"Blueprint sealed staging evidence changed",
		)
	}
	published := descriptor
	published.State = EnvironmentBlueprintStagePublished
	published.UpdatedAt = nextBlueprintProgressTime(descriptor.UpdatedAt)
	publishedValue, err := encodeEnvironmentBlueprintStageDescriptor(published)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	return environmentBlueprintPublicationEvidence{
		seal: seal, rootRevision: result.Values[0].ModRevision,
		descriptorRevision: result.Values[1].ModRevision, descriptorKey: descriptorKey,
		locatorRevision: result.Values[2].ModRevision, locatorKey: locatorKey,
		publishedDescriptor: publishedValue,
	}, nil
}

func sameBlueprintProtectedIntent(left, right ProtectedIntentRecord) bool {
	return left.EnvelopeVersion == right.EnvelopeVersion && left.Cipher == right.Cipher &&
		left.DigestAlgorithm == right.DigestAlgorithm && left.CiphertextDigest == right.CiphertextDigest &&
		bytes.Equal(left.Ciphertext, right.Ciphertext)
}

func environmentBlueprintChunkKeys(descriptor EnvironmentBlueprintStageDescriptor) []string {
	keys := make([]string, 0, int(descriptor.AuditChunks+descriptor.ProjectionChunks))
	for index := uint32(0); index < descriptor.AuditChunks; index++ {
		keys = append(keys, environmentBlueprintChunkKeyFor(
			descriptor.Claim.EnvironmentID, descriptor.Claim.RevisionID, EnvironmentBlueprintChunkAudit, index,
		))
	}
	for index := uint32(0); index < descriptor.ProjectionChunks; index++ {
		keys = append(keys, environmentBlueprintChunkKeyFor(
			descriptor.Claim.EnvironmentID, descriptor.Claim.RevisionID, EnvironmentBlueprintChunkProjection, index,
		))
	}
	return keys
}

func verifyEnvironmentBlueprintChunks(descriptor EnvironmentBlueprintStageDescriptor, values []*KeyValue) error {
	if len(values) != int(descriptor.AuditChunks+descriptor.ProjectionChunks) {
		return corruptEnvironmentBlueprintStage()
	}
	auditHasher := sha256.New()
	projectionHasher := sha256.New()
	var auditBytes, projectionBytes uint64
	for index, entry := range values {
		chunk, err := decodeEnvironmentBlueprintChunk(entry.Value)
		if err != nil {
			return err
		}
		if index < int(descriptor.AuditChunks) {
			if chunk.Family != EnvironmentBlueprintChunkAudit || chunk.Sequence != uint32(index) {
				clear(chunk.Data)
				return corruptEnvironmentBlueprintStage()
			}
			_, _ = auditHasher.Write(chunk.Data)
			auditBytes += uint64(len(chunk.Data))
		} else {
			sequence := uint32(index) - descriptor.AuditChunks
			if chunk.Family != EnvironmentBlueprintChunkProjection || chunk.Sequence != sequence {
				clear(chunk.Data)
				return corruptEnvironmentBlueprintStage()
			}
			_, _ = projectionHasher.Write(chunk.Data)
			projectionBytes += uint64(len(chunk.Data))
		}
		clear(chunk.Data)
	}
	var auditDigest, projectionDigest [sha256.Size]byte
	copy(auditDigest[:], auditHasher.Sum(nil))
	copy(projectionDigest[:], projectionHasher.Sum(nil))
	if auditBytes != descriptor.AuditBytes || projectionBytes != descriptor.ProjectionBytes ||
		auditDigest != descriptor.AuditSHA256 || projectionDigest != descriptor.ProjectionSHA256 {
		return corruptEnvironmentBlueprintStage()
	}
	return nil
}

func environmentBlueprintSealFromDescriptor(descriptor EnvironmentBlueprintStageDescriptor) EnvironmentBlueprintSeal {
	return EnvironmentBlueprintSeal{
		EnvironmentID: descriptor.Claim.EnvironmentID, RevisionID: descriptor.Claim.RevisionID,
		SourceKind: descriptor.Claim.SourceKind, RenderGeneration: descriptor.Claim.RenderGeneration,
		ProjectionSchema: descriptor.Claim.ProjectionSchema,
		AuditChunks:      descriptor.AuditChunks, AuditBytes: descriptor.AuditBytes, AuditSHA256: descriptor.AuditSHA256,
		ProjectionChunks: descriptor.ProjectionChunks, ProjectionBytes: descriptor.ProjectionBytes,
		ProjectionSHA256: descriptor.ProjectionSHA256, ProjectionResources: descriptor.ProjectionResources,
		BaselineHeadRevision: descriptor.Claim.BaselineHeadRevision, DependencyDigest: descriptor.DependencyDigest,
	}
}

func (repository *HierarchyRepository) readEnvironmentBlueprintStream(
	ctx context.Context,
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
		keys[index] = environmentBlueprintChunkKeyFor(seal.EnvironmentID, seal.RevisionID, familyID, uint32(index))
	}
	return repository.readEnvironmentBlueprintStreamAtRevision(ctx, seal, family, keys, 0)
}

func (repository *HierarchyRepository) readEnvironmentBlueprintStreamAtRevision(
	ctx context.Context,
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
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, 0, err
	}
	if result == nil || len(result.Values) != len(keys) {
		return nil, 0, corruptEnvironmentBlueprintStage()
	}
	defer clearKeyValues(result.Values)
	stream := make([]byte, 0, int(length))
	for index, entry := range result.Values {
		if entry == nil || entry.Key != keys[index] {
			clear(stream)
			return nil, 0, corruptEnvironmentBlueprintStage()
		}
		chunk, err := decodeEnvironmentBlueprintChunk(entry.Value)
		if err != nil || chunk.Family != familyID || chunk.Sequence != uint32(index) {
			clear(chunk.Data)
			clear(stream)
			return nil, 0, corruptEnvironmentBlueprintStage()
		}
		stream = append(stream, chunk.Data...)
		clear(chunk.Data)
	}
	computed := sha256.Sum256(stream)
	if uint64(len(stream)) != length || computed != digest {
		clear(stream)
		return nil, 0, corruptEnvironmentBlueprintStage()
	}
	return stream, result.ReadRevision, nil
}

func (repository *HierarchyRepository) getEnvironmentComposeProjectionAtRevision(
	ctx context.Context,
	environmentID string,
	revision int64,
) (Versioned[EnvironmentComposeProjection], bool, error) {
	headResult, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{environmentBlueprintHeadKey(environmentID)}, Revision: revision,
	})
	if err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if headResult == nil || len(headResult.Values) != 1 {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	if headResult.Values[0] == nil {
		return Versioned[EnvironmentComposeProjection]{ReadRevision: headResult.ReadRevision}, false, nil
	}
	revisionID, err := decodeTaskReference(headResult.Values[0].Value)
	if err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	rootResult, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{environmentBlueprintRootKey(environmentID, revisionID)}, Revision: headResult.ReadRevision,
	})
	if err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if rootResult == nil || len(rootResult.Values) != 1 || rootResult.Values[0] == nil {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	seal, err := decodeEnvironmentBlueprintSeal(rootResult.Values[0].Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	keys := make([]string, int(seal.ProjectionChunks))
	for index := range keys {
		keys[index] = environmentBlueprintChunkKeyFor(
			environmentID, revisionID, EnvironmentBlueprintChunkProjection, uint32(index),
		)
	}
	stream, readRevision, err := repository.readEnvironmentBlueprintStreamAtRevision(
		ctx, seal, "projection", keys, headResult.ReadRevision,
	)
	if err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	defer clear(stream)
	projection, err := decodeEnvironmentComposeProjection(stream)
	if err != nil || projection.EnvironmentID != environmentID || projection.RevisionID != revisionID {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	return Versioned[EnvironmentComposeProjection]{
		Record: projection, Revision: headResult.Values[0].ModRevision, ReadRevision: readRevision,
	}, true, nil
}

func validateBlueprintTransaction(
	store hierarchyStore,
	conditions []Condition,
	mutations []Mutation,
	maximumOperations int,
	maximumBytes int,
) error {
	operations := len(conditions) + len(mutations)
	if operations == 0 || operations > maximumOperations {
		return errs.New(errs.KindInternal, "Blueprint transaction operation budget exceeded")
	}
	valueBytes := 0
	for _, condition := range conditions {
		if len(condition.Key) == 0 || len(condition.Key) > EnvironmentBlueprintKeyMaxBytes {
			return errs.New(errs.KindInternal, "Blueprint transaction key exceeds 2 KiB")
		}
	}
	for _, mutation := range mutations {
		if len(mutation.Key) == 0 || len(mutation.Key) > EnvironmentBlueprintKeyMaxBytes {
			return errs.New(errs.KindInternal, "Blueprint transaction key exceeds 2 KiB")
		}
		if mutation.Type == MutationPut {
			valueBytes += len(mutation.Value)
		}
	}
	conservative := valueBytes + operations*EnvironmentBlueprintKeyMaxBytes + operations*64 + 128
	if conservative > maximumBytes {
		return errs.New(errs.KindInternal, "Blueprint transaction conservative byte budget exceeded")
	}
	if sizer, ok := store.(blueprintTransactionSizer); ok {
		actual, err := sizer.transactionSize(conditions, mutations)
		if err != nil {
			return err
		}
		if actual > maximumBytes {
			return errs.New(errs.KindInternal, "Blueprint transaction encoded byte budget exceeded")
		}
	}
	return nil
}

func protectedBlueprintIntentDigest(intent ProtectedIntentRecord) ([sha256.Size]byte, error) {
	if err := validateProtectedIntent(intent); err != nil {
		return [sha256.Size]byte{}, err
	}
	decoded, err := hex.DecodeString(intent.CiphertextDigest)
	if err != nil || len(decoded) != sha256.Size {
		return [sha256.Size]byte{}, corruptEnvironmentBlueprintStage()
	}
	var result [sha256.Size]byte
	copy(result[:], decoded)
	return result, nil
}

func matchingEnvironmentBlueprintChunk(
	chunk EnvironmentBlueprintChunk,
	family uint8,
	sequence uint32,
	data []byte,
) bool {
	return chunk.Family == family && chunk.Sequence == sequence &&
		chunk.LogicalOffset == uint64(sequence)*EnvironmentBlueprintChunkBytes &&
		chunk.LogicalLength == uint32(len(data)) && chunk.Digest == sha256.Sum256(data) &&
		string(chunk.Data) == string(data)
}

func sameEnvironmentBlueprintStageStreams(left, right EnvironmentBlueprintStageDescriptor) bool {
	return left.Bound == right.Bound && left.AuditChunks == right.AuditChunks && left.AuditBytes == right.AuditBytes &&
		left.AuditSHA256 == right.AuditSHA256 && left.ProjectionChunks == right.ProjectionChunks &&
		left.ProjectionBytes == right.ProjectionBytes && left.ProjectionSHA256 == right.ProjectionSHA256 &&
		left.ProjectionResources == right.ProjectionResources && left.DependencyDigest == right.DependencyDigest
}

func nextBlueprintProgressTime(previous time.Time) time.Time {
	now := time.Now().UTC()
	if !now.After(previous) {
		return previous.Add(time.Nanosecond)
	}
	return now
}
