package etcd

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	environmentBlueprintDescriptorRecord recordcodec.Kind = iota + 1
	environmentBlueprintLocatorRecord
	environmentBlueprintChunkRecord
	environmentBlueprintRootRecord
)

const environmentBlueprintRecordSchema = uint16(1)

type blueprintRecordWriter struct {
	value []byte
	err   error
}

func (writer *blueprintRecordWriter) uint8(value uint8) {
	writer.value = append(writer.value, value)
}

func (writer *blueprintRecordWriter) uint16(value uint16) {
	encoded := make([]byte, 2)
	binary.BigEndian.PutUint16(encoded, value)
	writer.value = append(writer.value, encoded...)
}

func (writer *blueprintRecordWriter) uint32(value uint32) {
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, value)
	writer.value = append(writer.value, encoded...)
}

func (writer *blueprintRecordWriter) uint64(value uint64) {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	writer.value = append(writer.value, encoded...)
}

func (writer *blueprintRecordWriter) digest(value [sha256.Size]byte) {
	writer.value = append(writer.value, value[:]...)
}

func (writer *blueprintRecordWriter) bytes(value []byte) {
	if writer.err != nil {
		return
	}
	if uint64(len(value)) > math.MaxUint32 {
		writer.err = errs.New(errs.KindValidationFailed, "Blueprint durable byte field is too large")
		return
	}
	writer.uint32(uint32(len(value)))
	writer.value = append(writer.value, value...)
}

func (writer *blueprintRecordWriter) string(value string) {
	if !utf8.ValidString(value) {
		writer.err = errs.New(errs.KindValidationFailed, "Blueprint durable string is not UTF-8")
		return
	}
	writer.bytes([]byte(value))
}

func (writer *blueprintRecordWriter) timestamp(value time.Time) {
	if !validBlueprintRecordTime(value) {
		writer.err = errs.New(errs.KindValidationFailed, "Blueprint durable timestamp is invalid")
		return
	}
	writer.uint64(uint64(value.UnixNano()))
}

type blueprintRecordReader struct {
	value  []byte
	offset int
	err    error
}

func (reader *blueprintRecordReader) take(length int) []byte {
	if reader.err != nil || length < 0 || reader.offset > len(reader.value)-length {
		reader.err = corruptEnvironmentBlueprintStage()
		return nil
	}
	result := reader.value[reader.offset : reader.offset+length]
	reader.offset += length
	return result
}

func (reader *blueprintRecordReader) uint8() uint8 {
	value := reader.take(1)
	if len(value) != 1 {
		return 0
	}
	return value[0]
}

func (reader *blueprintRecordReader) uint16() uint16 {
	value := reader.take(2)
	if len(value) != 2 {
		return 0
	}
	return binary.BigEndian.Uint16(value)
}

func (reader *blueprintRecordReader) uint32() uint32 {
	value := reader.take(4)
	if len(value) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(value)
}

func (reader *blueprintRecordReader) uint64() uint64 {
	value := reader.take(8)
	if len(value) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(value)
}

func (reader *blueprintRecordReader) digest() [sha256.Size]byte {
	var result [sha256.Size]byte
	copy(result[:], reader.take(sha256.Size))
	return result
}

func (reader *blueprintRecordReader) bytes(maximum int) []byte {
	length := reader.uint32()
	if reader.err != nil || uint64(length) > uint64(maximum) {
		reader.err = corruptEnvironmentBlueprintStage()
		return nil
	}
	return append([]byte(nil), reader.take(int(length))...)
}

func (reader *blueprintRecordReader) string(maximum int) string {
	value := reader.bytes(maximum)
	if reader.err != nil || !utf8.Valid(value) {
		reader.err = corruptEnvironmentBlueprintStage()
		return ""
	}
	return string(value)
}

func (reader *blueprintRecordReader) timestamp() time.Time {
	nanoseconds := reader.uint64()
	if nanoseconds > math.MaxInt64 {
		reader.err = corruptEnvironmentBlueprintStage()
		return time.Time{}
	}
	return time.Unix(0, int64(nanoseconds)).UTC()
}

func (reader *blueprintRecordReader) done() error {
	if reader.err != nil || reader.offset != len(reader.value) {
		return corruptEnvironmentBlueprintStage()
	}
	return nil
}

func encodeEnvironmentBlueprintStageDescriptor(value EnvironmentBlueprintStageDescriptor) ([]byte, error) {
	if err := validateEnvironmentBlueprintStageDescriptor(value); err != nil {
		return nil, err
	}
	writer := blueprintRecordWriter{}
	writer.uint16(environmentBlueprintRecordSchema)
	encodeEnvironmentBlueprintClaim(&writer, value.Claim)
	writer.uint8(uint8(value.State))
	if value.Bound {
		writer.uint8(1)
	} else {
		writer.uint8(0)
	}
	writer.uint32(value.AuditChunks)
	writer.uint64(value.AuditBytes)
	writer.digest(value.AuditSHA256)
	writer.uint32(value.ProjectionChunks)
	writer.uint64(value.ProjectionBytes)
	writer.digest(value.ProjectionSHA256)
	writer.uint32(value.ProjectionResources)
	writer.digest(value.DependencyDigest)
	writer.uint32(value.NextAuditChunk)
	writer.uint32(value.NextProjectionChunk)
	writer.timestamp(value.UpdatedAt)
	if writer.err != nil {
		clear(writer.value)
		return nil, writer.err
	}
	encoded, err := recordcodec.Encode(environmentBlueprintDescriptorRecord, writer.value)
	clear(writer.value)
	if err != nil {
		return nil, err
	}
	if len(encoded) > environmentBlueprintDescriptorMaxBytes {
		clear(encoded)
		return nil, errs.New(errs.KindValidationFailed, "Blueprint staging descriptor exceeds 4 KiB")
	}
	return encoded, nil
}

func decodeEnvironmentBlueprintStageDescriptor(value []byte) (EnvironmentBlueprintStageDescriptor, error) {
	payload, err := recordcodec.Decode(
		value,
		environmentBlueprintDescriptorRecord,
		environmentBlueprintDescriptorMaxBytes-recordcodec.HeaderBytes,
	)
	if err != nil {
		return EnvironmentBlueprintStageDescriptor{}, corruptEnvironmentBlueprintStage()
	}
	defer clear(payload)
	reader := blueprintRecordReader{value: payload}
	if reader.uint16() != environmentBlueprintRecordSchema {
		return EnvironmentBlueprintStageDescriptor{}, corruptEnvironmentBlueprintStage()
	}
	descriptor := EnvironmentBlueprintStageDescriptor{Claim: decodeEnvironmentBlueprintClaim(&reader)}
	descriptor.State = EnvironmentBlueprintStageState(reader.uint8())
	bound := reader.uint8()
	if bound > 1 {
		return EnvironmentBlueprintStageDescriptor{}, corruptEnvironmentBlueprintStage()
	}
	descriptor.Bound = bound == 1
	descriptor.AuditChunks = reader.uint32()
	descriptor.AuditBytes = reader.uint64()
	descriptor.AuditSHA256 = reader.digest()
	descriptor.ProjectionChunks = reader.uint32()
	descriptor.ProjectionBytes = reader.uint64()
	descriptor.ProjectionSHA256 = reader.digest()
	descriptor.ProjectionResources = reader.uint32()
	descriptor.DependencyDigest = reader.digest()
	descriptor.NextAuditChunk = reader.uint32()
	descriptor.NextProjectionChunk = reader.uint32()
	descriptor.UpdatedAt = reader.timestamp()
	if reader.done() != nil || validateEnvironmentBlueprintStageDescriptor(descriptor) != nil {
		clear(descriptor.Claim.Intent.Ciphertext)
		return EnvironmentBlueprintStageDescriptor{}, corruptEnvironmentBlueprintStage()
	}
	return descriptor, nil
}

func encodeEnvironmentBlueprintClaim(writer *blueprintRecordWriter, claim EnvironmentBlueprintStageClaim) {
	writer.string(claim.DescriptorID)
	writer.string(claim.EnvironmentID)
	writer.string(claim.RevisionID)
	writer.string(claim.TaskID)
	writer.string(string(claim.Locator.ScopeKind))
	writer.string(claim.Locator.ScopeID)
	writer.string(claim.Locator.Method)
	writer.string(claim.Locator.Route)
	writer.string(claim.Locator.Key)
	writer.uint8(claim.Intent.EnvelopeVersion)
	writer.string(claim.Intent.Cipher)
	writer.string(claim.Intent.DigestAlgorithm)
	digest, err := hex.DecodeString(claim.Intent.CiphertextDigest)
	if err != nil || len(digest) != sha256.Size {
		writer.err = errs.New(errs.KindValidationFailed, "protected Blueprint intent digest is invalid")
	} else {
		var raw [sha256.Size]byte
		copy(raw[:], digest)
		writer.digest(raw)
	}
	writer.bytes(claim.Intent.Ciphertext)
	writer.uint64(uint64(claim.BaselineHeadRevision))
	writer.uint8(uint8(claim.SourceKind))
	writer.uint64(claim.RenderGeneration)
	writer.uint16(claim.ProjectionSchema)
	writer.timestamp(claim.CreatedAt)
}

func decodeEnvironmentBlueprintClaim(reader *blueprintRecordReader) EnvironmentBlueprintStageClaim {
	claim := EnvironmentBlueprintStageClaim{
		DescriptorID:  reader.string(128),
		EnvironmentID: reader.string(128),
		RevisionID:    reader.string(128),
		TaskID:        reader.string(128),
	}
	claim.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeKind(reader.string(32)), ScopeID: reader.string(128),
		Method: reader.string(16), Route: reader.string(1024), Key: reader.string(128),
	}
	claim.Intent.EnvelopeVersion = reader.uint8()
	claim.Intent.Cipher = reader.string(64)
	claim.Intent.DigestAlgorithm = reader.string(32)
	digest := reader.digest()
	claim.Intent.CiphertextDigest = hex.EncodeToString(digest[:])
	claim.Intent.Ciphertext = reader.bytes(maximumIntentCiphertext)
	baseline := reader.uint64()
	if baseline > math.MaxInt64 {
		reader.err = corruptEnvironmentBlueprintStage()
	} else {
		claim.BaselineHeadRevision = int64(baseline)
	}
	claim.SourceKind = EnvironmentBlueprintSourceKind(reader.uint8())
	claim.RenderGeneration = reader.uint64()
	claim.ProjectionSchema = reader.uint16()
	claim.CreatedAt = reader.timestamp()
	return claim
}

func encodeEnvironmentBlueprintStageLocator(descriptorID string, digest [sha256.Size]byte) ([]byte, error) {
	if !validBlueprintString(descriptorID, 128) || zeroDigest(digest) {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint staging locator is invalid")
	}
	writer := blueprintRecordWriter{}
	writer.string(descriptorID)
	writer.digest(digest)
	encoded, err := recordcodec.Encode(environmentBlueprintLocatorRecord, writer.value)
	clear(writer.value)
	return encoded, err
}

func decodeEnvironmentBlueprintStageLocator(value []byte) (string, [sha256.Size]byte, error) {
	payload, err := recordcodec.Decode(value, environmentBlueprintLocatorRecord, 256)
	if err != nil {
		return "", [sha256.Size]byte{}, corruptEnvironmentBlueprintStage()
	}
	defer clear(payload)
	reader := blueprintRecordReader{value: payload}
	descriptorID := reader.string(128)
	digest := reader.digest()
	if reader.done() != nil || !validBlueprintString(descriptorID, 128) || zeroDigest(digest) {
		return "", [sha256.Size]byte{}, corruptEnvironmentBlueprintStage()
	}
	return descriptorID, digest, nil
}

func encodeEnvironmentBlueprintChunk(value EnvironmentBlueprintChunk) ([]byte, error) {
	if err := validateEnvironmentBlueprintChunk(value); err != nil {
		return nil, err
	}
	writer := blueprintRecordWriter{}
	writer.uint8(value.Family)
	writer.uint32(value.Sequence)
	writer.uint64(value.LogicalOffset)
	writer.uint32(value.LogicalLength)
	writer.digest(value.Digest)
	writer.bytes(value.Data)
	if len(writer.value) != 53+len(value.Data) {
		clear(writer.value)
		return nil, errs.New(errs.KindInternal, "Blueprint chunk payload framing changed")
	}
	encoded, err := recordcodec.Encode(environmentBlueprintChunkRecord, writer.value)
	clear(writer.value)
	if err != nil {
		return nil, err
	}
	if len(encoded) > 97+EnvironmentBlueprintChunkBytes {
		clear(encoded)
		return nil, errs.New(errs.KindInternal, "Blueprint chunk record exceeds 61,537 bytes")
	}
	return encoded, nil
}

func decodeEnvironmentBlueprintChunk(value []byte) (EnvironmentBlueprintChunk, error) {
	payload, err := recordcodec.Decode(value, environmentBlueprintChunkRecord, 53+EnvironmentBlueprintChunkBytes)
	if err != nil {
		return EnvironmentBlueprintChunk{}, corruptEnvironmentBlueprintStage()
	}
	defer clear(payload)
	reader := blueprintRecordReader{value: payload}
	chunk := EnvironmentBlueprintChunk{
		Family: reader.uint8(), Sequence: reader.uint32(), LogicalOffset: reader.uint64(),
		LogicalLength: reader.uint32(), Digest: reader.digest(),
	}
	chunk.Data = reader.bytes(EnvironmentBlueprintChunkBytes)
	if reader.done() != nil || validateEnvironmentBlueprintChunk(chunk) != nil {
		clear(chunk.Data)
		return EnvironmentBlueprintChunk{}, corruptEnvironmentBlueprintStage()
	}
	return chunk, nil
}

func validateEnvironmentBlueprintChunk(value EnvironmentBlueprintChunk) error {
	if value.Family != EnvironmentBlueprintChunkAudit && value.Family != EnvironmentBlueprintChunkProjection {
		return errs.New(errs.KindValidationFailed, "Blueprint chunk family is invalid")
	}
	if len(value.Data) == 0 || len(value.Data) > EnvironmentBlueprintChunkBytes ||
		value.LogicalLength != uint32(len(value.Data)) ||
		value.LogicalOffset != uint64(value.Sequence)*EnvironmentBlueprintChunkBytes ||
		sha256.Sum256(value.Data) != value.Digest {
		return errs.New(errs.KindValidationFailed, "Blueprint chunk authority is invalid")
	}
	return nil
}

func encodeEnvironmentBlueprintSeal(value EnvironmentBlueprintSeal) ([]byte, error) {
	if err := validateEnvironmentBlueprintSeal(value); err != nil {
		return nil, err
	}
	writer := blueprintRecordWriter{}
	writer.uint16(environmentBlueprintRecordSchema)
	writer.string(value.EnvironmentID)
	writer.string(value.RevisionID)
	writer.uint8(uint8(value.SourceKind))
	writer.uint64(value.RenderGeneration)
	writer.uint16(value.ProjectionSchema)
	writer.uint32(value.AuditChunks)
	writer.uint64(value.AuditBytes)
	writer.digest(value.AuditSHA256)
	writer.uint32(value.ProjectionChunks)
	writer.uint64(value.ProjectionBytes)
	writer.digest(value.ProjectionSHA256)
	writer.uint32(value.ProjectionResources)
	writer.uint64(uint64(value.BaselineHeadRevision))
	writer.digest(value.DependencyDigest)
	encoded, err := recordcodec.Encode(environmentBlueprintRootRecord, writer.value)
	clear(writer.value)
	if err != nil {
		return nil, err
	}
	if len(encoded) > environmentBlueprintRootMaxBytes {
		clear(encoded)
		return nil, errs.New(errs.KindValidationFailed, "Blueprint sealed root exceeds 8 KiB")
	}
	return encoded, nil
}

func decodeEnvironmentBlueprintSeal(value []byte) (EnvironmentBlueprintSeal, error) {
	payload, err := recordcodec.Decode(
		value,
		environmentBlueprintRootRecord,
		environmentBlueprintRootMaxBytes-recordcodec.HeaderBytes,
	)
	if err != nil {
		return EnvironmentBlueprintSeal{}, corruptEnvironmentBlueprintStage()
	}
	defer clear(payload)
	reader := blueprintRecordReader{value: payload}
	if reader.uint16() != environmentBlueprintRecordSchema {
		return EnvironmentBlueprintSeal{}, corruptEnvironmentBlueprintStage()
	}
	seal := EnvironmentBlueprintSeal{
		EnvironmentID: reader.string(128), RevisionID: reader.string(128),
		SourceKind: EnvironmentBlueprintSourceKind(reader.uint8()), RenderGeneration: reader.uint64(),
		ProjectionSchema: reader.uint16(), AuditChunks: reader.uint32(), AuditBytes: reader.uint64(),
		AuditSHA256: reader.digest(), ProjectionChunks: reader.uint32(), ProjectionBytes: reader.uint64(),
		ProjectionSHA256: reader.digest(), ProjectionResources: reader.uint32(),
	}
	baseline := reader.uint64()
	if baseline > math.MaxInt64 {
		reader.err = corruptEnvironmentBlueprintStage()
	} else {
		seal.BaselineHeadRevision = int64(baseline)
	}
	seal.DependencyDigest = reader.digest()
	if reader.done() != nil || validateEnvironmentBlueprintSeal(seal) != nil {
		return EnvironmentBlueprintSeal{}, corruptEnvironmentBlueprintStage()
	}
	return seal, nil
}

func validateEnvironmentBlueprintSeal(value EnvironmentBlueprintSeal) error {
	if value.BaselineHeadRevision < 0 || value.RenderGeneration == 0 || value.ProjectionSchema == 0 ||
		value.AuditBytes == 0 || value.AuditBytes > environmentBlueprintMaximumAuditBytes ||
		value.ProjectionBytes == 0 || value.ProjectionBytes > EnvironmentBlueprintProjectionMaxBytes ||
		value.AuditChunks != chunkCount32(int(value.AuditBytes)) ||
		value.ProjectionChunks != chunkCount32(int(value.ProjectionBytes)) ||
		value.AuditChunks > environmentBlueprintMaximumAuditChunks ||
		value.ProjectionChunks > environmentBlueprintMaximumProjectionChunks ||
		value.AuditChunks+value.ProjectionChunks > environmentBlueprintMaximumChunks ||
		value.ProjectionResources > 512 || zeroDigest(value.AuditSHA256) ||
		zeroDigest(value.ProjectionSHA256) || zeroDigest(value.DependencyDigest) {
		return errs.New(errs.KindValidationFailed, "Blueprint sealed root is invalid")
	}
	if value.SourceKind != EnvironmentBlueprintSourceApply && value.SourceKind != EnvironmentBlueprintSourceMutation {
		return errs.New(errs.KindValidationFailed, "Blueprint sealed root source is invalid")
	}
	if ids.Validate(ids.KindEnvironment, value.EnvironmentID) != nil || ids.Validate(ids.KindTask, value.RevisionID) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint sealed root identity is invalid")
	}
	return nil
}
