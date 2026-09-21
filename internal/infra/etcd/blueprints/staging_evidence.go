package blueprints

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"
)

func SameBlueprintProtectedIntent(left, right idempotencyrecord.ProtectedIntentRecord) bool {
	return left.EnvelopeVersion == right.EnvelopeVersion && left.Cipher == right.Cipher &&
		left.DigestAlgorithm == right.DigestAlgorithm && left.CiphertextDigest == right.CiphertextDigest &&
		bytes.Equal(left.Ciphertext, right.Ciphertext)
}

func EnvironmentBlueprintChunkKeys(descriptor EnvironmentBlueprintStageDescriptor) []string {
	keys := make([]string, 0, int(descriptor.AuditChunks+descriptor.ProjectionChunks))
	for index := uint32(0); index < descriptor.AuditChunks; index++ {
		keys = append(keys, EnvironmentBlueprintChunkKeyFor(
			descriptor.Claim.EnvironmentID, descriptor.Claim.RevisionID, EnvironmentBlueprintChunkAudit, index,
		))
	}
	for index := uint32(0); index < descriptor.ProjectionChunks; index++ {
		keys = append(keys, EnvironmentBlueprintChunkKeyFor(
			descriptor.Claim.EnvironmentID, descriptor.Claim.RevisionID, EnvironmentBlueprintChunkProjection, index,
		))
	}
	return keys
}

func VerifyEnvironmentBlueprintChunks(descriptor EnvironmentBlueprintStageDescriptor, values []*etcdstore.KeyValue) error {
	if len(values) != int(descriptor.AuditChunks+descriptor.ProjectionChunks) {
		return CorruptEnvironmentBlueprintStage()
	}
	auditHasher := sha256.New()
	projectionHasher := sha256.New()
	var auditBytes, projectionBytes uint64
	for index, entry := range values {
		chunk, err := DecodeEnvironmentBlueprintChunk(entry.Value)
		if err != nil {
			return err
		}
		if index < int(descriptor.AuditChunks) {
			if chunk.Family != EnvironmentBlueprintChunkAudit || chunk.Sequence != uint32(index) {
				clear(chunk.Data)
				return CorruptEnvironmentBlueprintStage()
			}
			_, _ = auditHasher.Write(chunk.Data)
			auditBytes += uint64(len(chunk.Data))
		} else {
			sequence := uint32(index) - descriptor.AuditChunks
			if chunk.Family != EnvironmentBlueprintChunkProjection || chunk.Sequence != sequence {
				clear(chunk.Data)
				return CorruptEnvironmentBlueprintStage()
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
		return CorruptEnvironmentBlueprintStage()
	}
	return nil
}

func EnvironmentBlueprintSealFromDescriptor(descriptor EnvironmentBlueprintStageDescriptor) EnvironmentBlueprintSeal {
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

func ProtectedBlueprintIntentDigest(intent idempotencyrecord.ProtectedIntentRecord) ([sha256.Size]byte, error) {
	if err := idempotencyrecord.ValidateProtectedIntent(intent); err != nil {
		return [sha256.Size]byte{}, err
	}
	decoded, err := hex.DecodeString(intent.CiphertextDigest)
	if err != nil || len(decoded) != sha256.Size {
		return [sha256.Size]byte{}, CorruptEnvironmentBlueprintStage()
	}
	var result [sha256.Size]byte
	copy(result[:], decoded)
	return result, nil
}

func MatchingEnvironmentBlueprintChunk(
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

func SameEnvironmentBlueprintStageStreams(left, right EnvironmentBlueprintStageDescriptor) bool {
	return left.Bound == right.Bound && left.AuditChunks == right.AuditChunks && left.AuditBytes == right.AuditBytes &&
		left.AuditSHA256 == right.AuditSHA256 && left.ProjectionChunks == right.ProjectionChunks &&
		left.ProjectionBytes == right.ProjectionBytes && left.ProjectionSHA256 == right.ProjectionSHA256 &&
		left.ProjectionResources == right.ProjectionResources && left.DependencyDigest == right.DependencyDigest
}

func NextBlueprintProgressTime(previous time.Time) time.Time {
	now := time.Now().UTC()
	if !now.After(previous) {
		return previous.Add(time.Nanosecond)
	}
	return now
}
