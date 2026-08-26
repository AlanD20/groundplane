package etcd

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	EnvironmentBlueprintChunkBytes              = 60 * 1024
	EnvironmentBlueprintStageBatchChunks        = 12
	environmentBlueprintMaximumAuditBytes       = 1_049_100
	EnvironmentBlueprintProjectionMaxBytes      = 2 * 1024 * 1024
	environmentBlueprintMaximumAuditChunks      = 18
	environmentBlueprintMaximumProjectionChunks = 35
	environmentBlueprintMaximumChunks           = 53
	EnvironmentBlueprintStageTransactionBytes   = 797_580
	EnvironmentBlueprintSealTransactionBytes    = 132_800
	EnvironmentBlueprintGCTransactionBytes      = 143_616
	EnvironmentBlueprintStageExpiry             = 5 * time.Minute
	environmentBlueprintDescriptorMaxBytes      = 4 * 1024
	environmentBlueprintRootMaxBytes            = 8 * 1024
	EnvironmentBlueprintKeyMaxBytes             = 2 * 1024
)

const (
	environmentBlueprintPrivatePrefix    = "/v1/private/blueprint-staging/"
	EnvironmentBlueprintDescriptorPrefix = environmentBlueprintPrivatePrefix + "descriptors/"
	environmentBlueprintLocatorPrefix    = environmentBlueprintPrivatePrefix + "locators/"
	environmentBlueprintRevisionRoot     = "/v1/records/environment-blueprint-revisions/"
)

type EnvironmentBlueprintSourceKind uint8

const (
	EnvironmentBlueprintSourceApply EnvironmentBlueprintSourceKind = iota + 1
	EnvironmentBlueprintSourceMutation
	EnvironmentDesiredProjectionSchema uint16 = 1
)

type EnvironmentBlueprintStageState uint8

const (
	EnvironmentBlueprintStageOpen EnvironmentBlueprintStageState = iota + 1
	EnvironmentBlueprintStageSealed
	EnvironmentBlueprintStagePublished
	EnvironmentBlueprintStageAbandoned
)

// EnvironmentBlueprintSeal is the immutable integrity root selected by an
// Environment desired head. Digests remain raw bytes in memory and on disk.
type EnvironmentBlueprintSeal struct {
	EnvironmentID        string
	RevisionID           string
	SourceKind           EnvironmentBlueprintSourceKind
	RenderGeneration     uint64
	ProjectionSchema     uint16
	AuditChunks          uint32
	AuditBytes           uint64
	AuditSHA256          [sha256.Size]byte
	ProjectionChunks     uint32
	ProjectionBytes      uint64
	ProjectionSHA256     [sha256.Size]byte
	ProjectionResources  uint32
	BaselineHeadRevision int64
	DependencyDigest     [sha256.Size]byte
}

// EnvironmentBlueprintStageClaimRequest contains the stable candidate and
// protected replay evidence reserved before any chunk write.
type EnvironmentBlueprintStageClaimRequest struct {
	EnvironmentID        string
	CandidateRevisionID  string
	CandidateTaskID      string
	Locator              IdempotencyLocator
	Intent               ProtectedIntentRecord
	BaselineHeadRevision int64
	SourceKind           EnvironmentBlueprintSourceKind
	RenderGeneration     uint64
	ProjectionSchema     uint16
	CreatedAt            time.Time
}

// EnvironmentBlueprintStageClaim is returned by the locator claim. Existing
// is true when another request already owns the locator. The Controller must
// compare the returned protected intent before it resumes that claim.
type EnvironmentBlueprintStageClaim struct {
	DescriptorID         string
	EnvironmentID        string
	RevisionID           string
	TaskID               string
	Locator              IdempotencyLocator
	Intent               ProtectedIntentRecord
	BaselineHeadRevision int64
	SourceKind           EnvironmentBlueprintSourceKind
	RenderGeneration     uint64
	ProjectionSchema     uint16
	CreatedAt            time.Time
	Existing             bool
}

type EnvironmentBlueprintStageRequest struct {
	Claim            EnvironmentBlueprintStageClaim
	Blueprint        *EnvironmentBlueprintRevision
	Mutation         *EnvironmentDesiredMutationAudit
	Projection       EnvironmentComposeProjection
	DependencyDigest [sha256.Size]byte
}

type EnvironmentDesiredRevisionIdentity struct {
	EnvironmentID string
	RevisionID    string
}

func validateEnvironmentDesiredRevisionIdentity(value EnvironmentDesiredRevisionIdentity) error {
	if ids.Validate(ids.KindEnvironment, value.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, value.RevisionID) != nil {
		return errs.New(errs.KindValidationFailed, "Environment desired revision identity is invalid")
	}
	return nil
}

type EnvironmentDesiredMutationAudit struct {
	Volume *EnvironmentVolumeMutationAudit
}

type EnvironmentVolumeMutationAction uint8

const (
	EnvironmentVolumeMutationAdd EnvironmentVolumeMutationAction = iota + 1
	EnvironmentVolumeMutationEdit
	EnvironmentVolumeMutationRemove
)

type EnvironmentVolumeMutationAudit struct {
	Action             EnvironmentVolumeMutationAction
	VolumeID           string
	Slug               string
	Key                string
	KeySupplied        bool
	PreconditionDigest [sha256.Size]byte
}

type EnvironmentBlueprintStageDescriptor struct {
	Claim               EnvironmentBlueprintStageClaim
	State               EnvironmentBlueprintStageState
	Bound               bool
	AuditChunks         uint32
	AuditBytes          uint64
	AuditSHA256         [sha256.Size]byte
	ProjectionChunks    uint32
	ProjectionBytes     uint64
	ProjectionSHA256    [sha256.Size]byte
	ProjectionResources uint32
	DependencyDigest    [sha256.Size]byte
	NextAuditChunk      uint32
	NextProjectionChunk uint32
	UpdatedAt           time.Time
}

type EnvironmentBlueprintStreams struct {
	Audit      []byte
	Projection []byte
	Descriptor EnvironmentBlueprintStageDescriptor
}

type environmentBlueprintPublicationEvidence struct {
	seal                EnvironmentBlueprintSeal
	rootRevision        int64
	descriptorRevision  int64
	descriptorKey       string
	locatorRevision     int64
	locatorKey          string
	publishedDescriptor []byte
}

type EnvironmentBlueprintChunk struct {
	Family        uint8
	Sequence      uint32
	LogicalOffset uint64
	LogicalLength uint32
	Digest        [sha256.Size]byte
	Data          []byte
}

const (
	EnvironmentBlueprintChunkAudit uint8 = iota + 1
	EnvironmentBlueprintChunkProjection
)

func environmentBlueprintDescriptorKeyByID(descriptorID string) string {
	return EnvironmentBlueprintDescriptorPrefix + encodeDynamicSegment(descriptorID)
}

func environmentBlueprintRootKey(environmentID string, revisionID string) string {
	return environmentBlueprintRevisionPrefixFinal(environmentID, revisionID) + "root"
}

func environmentBlueprintChunkKeyFor(environmentID, revisionID string, family uint8, index uint32) string {
	familyName := "audit"
	if family == EnvironmentBlueprintChunkProjection {
		familyName = "projection"
	}
	sequence := make([]byte, 4)
	binary.BigEndian.PutUint32(sequence, index)
	return environmentBlueprintRevisionPrefixFinal(environmentID, revisionID) +
		"chunks/" + familyName + "/" + encodeBlueprintDynamicBytes(sequence)
}

func environmentBlueprintChunkKey(environmentID, revisionID, family string, index int) string {
	familyID := EnvironmentBlueprintChunkAudit
	if family == "projection" {
		familyID = EnvironmentBlueprintChunkProjection
	}
	return environmentBlueprintChunkKeyFor(environmentID, revisionID, familyID, uint32(index))
}

func environmentBlueprintRevisionPrefixFinal(environmentID, revisionID string) string {
	return environmentBlueprintRevisionRoot + encodeDynamicSegment(environmentID) + "/" +
		encodeDynamicSegment(revisionID) + "/"
}

func environmentBlueprintLocatorKey(locator IdempotencyLocator) (string, [sha256.Size]byte, error) {
	if _, err := idempotencyMarkerKey(locator); err != nil {
		return "", [sha256.Size]byte{}, err
	}
	scope := canonicalBlueprintLocatorScope(locator)
	keyDigest := sha256.Sum256([]byte(locator.Key))
	key := environmentBlueprintLocatorPrefix + encodeBlueprintDynamicBytes(scope) + "/" +
		encodeBlueprintDynamicBytes(keyDigest[:])
	if len(key) > EnvironmentBlueprintKeyMaxBytes {
		return "", [sha256.Size]byte{}, errs.New(errs.KindValidationFailed, "Blueprint staging locator key exceeds 2 KiB")
	}
	return key, keyDigest, nil
}

func canonicalBlueprintLocatorScope(locator IdempotencyLocator) []byte {
	fields := []string{string(locator.ScopeKind), locator.ScopeID, locator.Method, locator.Route}
	length := 0
	for _, field := range fields {
		length += 4 + len(field)
	}
	result := make([]byte, 0, length)
	for _, field := range fields {
		encodedLength := make([]byte, 4)
		binary.BigEndian.PutUint32(encodedLength, uint32(len(field)))
		result = append(result, encodedLength...)
		result = append(result, field...)
	}
	return result
}

func encodeBlueprintDynamicBytes(value []byte) string {
	return "~" + base64.RawURLEncoding.EncodeToString(value)
}

func decodeBlueprintDynamicBytes(segment string) ([]byte, error) {
	if !strings.HasPrefix(segment, "~") || len(segment) == 1 {
		return nil, corruptEnvironmentBlueprintStage()
	}
	decoded, err := base64.RawURLEncoding.DecodeString(segment[1:])
	if err != nil || encodeBlueprintDynamicBytes(decoded) != segment {
		return nil, corruptEnvironmentBlueprintStage()
	}
	return decoded, nil
}

func buildEnvironmentBlueprintStreams(request EnvironmentBlueprintStageRequest) (EnvironmentBlueprintStreams, error) {
	claim := request.Claim
	if err := validateEnvironmentBlueprintStageClaim(claim); err != nil {
		return EnvironmentBlueprintStreams{}, err
	}
	if request.Projection.EnvironmentID != claim.EnvironmentID ||
		request.Projection.RevisionID != claim.RevisionID || zeroDigest(request.DependencyDigest) {
		return EnvironmentBlueprintStreams{}, errs.New(errs.KindValidationFailed, "Blueprint staging input does not match its claim")
	}
	var audit []byte
	if claim.SourceKind == EnvironmentBlueprintSourceApply {
		if request.Blueprint == nil || request.Mutation != nil ||
			request.Blueprint.EnvironmentID != claim.EnvironmentID ||
			request.Blueprint.RevisionID != claim.RevisionID ||
			!request.Blueprint.CreatedAt.Equal(claim.CreatedAt) {
			return EnvironmentBlueprintStreams{}, errs.New(errs.KindValidationFailed, "Blueprint apply audit does not match its claim")
		}
		var err error
		audit, err = encodeEnvironmentBlueprintAuditStream(*request.Blueprint)
		if err != nil {
			return EnvironmentBlueprintStreams{}, err
		}
	} else {
		if request.Blueprint != nil || request.Mutation == nil {
			return EnvironmentBlueprintStreams{}, errs.New(errs.KindValidationFailed, "desired mutation audit does not match its claim")
		}
		var err error
		audit, err = encodeEnvironmentDesiredMutationAudit(*request.Mutation)
		if err != nil {
			return EnvironmentBlueprintStreams{}, err
		}
	}
	projection, err := encodeEnvironmentComposeProjection(request.Projection)
	if err != nil {
		clear(audit)
		return EnvironmentBlueprintStreams{}, err
	}
	if len(projection) > EnvironmentBlueprintProjectionMaxBytes {
		clear(audit)
		clear(projection)
		return EnvironmentBlueprintStreams{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint normalized projection exceeds the 2 MiB ceiling",
		)
	}
	auditDigest := sha256.Sum256(audit)
	projectionDigest := sha256.Sum256(projection)
	descriptor := EnvironmentBlueprintStageDescriptor{
		Claim: claim, State: EnvironmentBlueprintStageOpen, Bound: true,
		AuditChunks: chunkCount32(len(audit)), AuditBytes: uint64(len(audit)), AuditSHA256: auditDigest,
		ProjectionChunks: chunkCount32(len(projection)), ProjectionBytes: uint64(len(projection)),
		ProjectionSHA256:    projectionDigest,
		ProjectionResources: uint32(environmentComposeProjectionResourceCount(request.Projection)),
		DependencyDigest:    request.DependencyDigest, UpdatedAt: claim.CreatedAt,
	}
	if err := validateEnvironmentBlueprintStageDescriptor(descriptor); err != nil {
		clear(audit)
		clear(projection)
		return EnvironmentBlueprintStreams{}, err
	}
	return EnvironmentBlueprintStreams{Audit: audit, Projection: projection, Descriptor: descriptor}, nil
}

func encodeEnvironmentDesiredMutationAudit(value EnvironmentDesiredMutationAudit) ([]byte, error) {
	if err := validateEnvironmentDesiredMutationAudit(value); err != nil {
		return nil, err
	}
	volume := value.Volume
	body := blueprintRecordWriter{}
	body.uint16(environmentBlueprintRecordSchema)
	body.string(volume.VolumeID)
	body.string(volume.Slug)
	body.string(volume.Key)
	if volume.KeySupplied {
		body.uint8(1)
	} else {
		body.uint8(0)
	}
	body.digest(volume.PreconditionDigest)
	if body.err != nil {
		clear(body.value)
		return nil, body.err
	}
	stream := make([]byte, 12, 12+len(body.value))
	copy(stream[:4], []byte("GPMU"))
	binary.BigEndian.PutUint16(stream[4:6], environmentBlueprintRecordSchema)
	stream[6] = 1
	stream[7] = byte(volume.Action)
	binary.BigEndian.PutUint32(stream[8:12], uint32(len(body.value)))
	stream = append(stream, body.value...)
	clear(body.value)
	return stream, nil
}

func decodeEnvironmentDesiredMutationAudit(value []byte) (EnvironmentDesiredMutationAudit, error) {
	if len(value) < 12 || string(value[:4]) != "GPMU" ||
		binary.BigEndian.Uint16(value[4:6]) != environmentBlueprintRecordSchema || value[6] != 1 ||
		int(binary.BigEndian.Uint32(value[8:12])) != len(value)-12 {
		return EnvironmentDesiredMutationAudit{}, corruptEnvironmentBlueprintStage()
	}
	reader := blueprintRecordReader{value: value[12:]}
	if reader.uint16() != environmentBlueprintRecordSchema {
		return EnvironmentDesiredMutationAudit{}, corruptEnvironmentBlueprintStage()
	}
	volume := &EnvironmentVolumeMutationAudit{
		Action: EnvironmentVolumeMutationAction(value[7]), VolumeID: reader.string(128),
		Slug: reader.string(63), Key: reader.string(255),
	}
	keySupplied := reader.uint8()
	if keySupplied > 1 {
		return EnvironmentDesiredMutationAudit{}, corruptEnvironmentBlueprintStage()
	}
	volume.KeySupplied = keySupplied == 1
	volume.PreconditionDigest = reader.digest()
	result := EnvironmentDesiredMutationAudit{Volume: volume}
	if reader.done() != nil || validateEnvironmentDesiredMutationAudit(result) != nil {
		return EnvironmentDesiredMutationAudit{}, corruptEnvironmentBlueprintStage()
	}
	return result, nil
}

func validateEnvironmentDesiredMutationAudit(value EnvironmentDesiredMutationAudit) error {
	if value.Volume == nil || ids.Validate(ids.KindVolume, value.Volume.VolumeID) != nil ||
		!validEnvironmentVolumeSlug(value.Volume.Slug) || !validEnvironmentVolumeKey(value.Volume.Key) {
		return errs.New(errs.KindValidationFailed, "Volume desired mutation audit is invalid")
	}
	switch value.Volume.Action {
	case EnvironmentVolumeMutationAdd:
		if !zeroDigest(value.Volume.PreconditionDigest) {
			return errs.New(errs.KindValidationFailed, "Volume add audit has a remove precondition")
		}
	case EnvironmentVolumeMutationEdit:
		if value.Volume.KeySupplied || !zeroDigest(value.Volume.PreconditionDigest) {
			return errs.New(errs.KindValidationFailed, "Volume edit audit changes immutable input")
		}
	case EnvironmentVolumeMutationRemove:
		if !value.Volume.KeySupplied || zeroDigest(value.Volume.PreconditionDigest) {
			return errs.New(errs.KindValidationFailed, "Volume remove audit lacks its accepted precondition")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Volume desired mutation action is invalid")
	}
	return nil
}

func validEnvironmentVolumeSlug(value string) bool {
	if len(value) < 1 || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, character := range []byte(value) {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
			return false
		}
	}
	return true
}

func validEnvironmentVolumeKey(value string) bool {
	return volumeidentity.ValidateKey(value) == nil
}

func chunkCount32(length int) uint32 {
	if length == 0 {
		return 0
	}
	return uint32((length + EnvironmentBlueprintChunkBytes - 1) / EnvironmentBlueprintChunkBytes)
}

func environmentComposeProjectionResourceCount(projection EnvironmentComposeProjection) int {
	return len(projection.Services) + len(projection.Networks) + len(projection.Volumes) +
		len(projection.Routes) + len(projection.SuppressedRoutes) + len(projection.Components) + len(projection.Entries)
}

func EnvironmentBlueprintDependencyDigest(projection EnvironmentComposeProjection) ([sha256.Size]byte, error) {
	digest, _, err := EnvironmentBlueprintProjectionEvidence(projection)
	return digest, err
}

func EnvironmentBlueprintProjectionEvidence(
	projection EnvironmentComposeProjection,
) ([sha256.Size]byte, uint64, error) {
	value, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		return [sha256.Size]byte{}, 0, err
	}
	defer clear(value)
	return sha256.Sum256(value), uint64(len(value)), nil
}

func validateEnvironmentBlueprintStageClaim(claim EnvironmentBlueprintStageClaim) error {
	if ids.Validate(ids.KindTask, "task_"+claim.DescriptorID) != nil ||
		ids.Validate(ids.KindEnvironment, claim.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, claim.RevisionID) != nil || ids.Validate(ids.KindTask, claim.TaskID) != nil ||
		claim.BaselineHeadRevision < 0 || claim.RenderGeneration == 0 || claim.ProjectionSchema == 0 ||
		!validBlueprintRecordTime(claim.CreatedAt) || validateProtectedIntent(claim.Intent) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint staging claim is invalid")
	}
	if claim.SourceKind != EnvironmentBlueprintSourceApply && claim.SourceKind != EnvironmentBlueprintSourceMutation {
		return errs.New(errs.KindValidationFailed, "Blueprint staging source kind is invalid")
	}
	if claim.Locator.ScopeKind != IdempotencyScopeEnvironment || claim.Locator.ScopeID != claim.EnvironmentID {
		return errs.New(errs.KindValidationFailed, "Blueprint staging locator must belong to its Environment")
	}
	if _, _, err := environmentBlueprintLocatorKey(claim.Locator); err != nil {
		return err
	}
	return nil
}

func validateEnvironmentBlueprintStageDescriptor(value EnvironmentBlueprintStageDescriptor) error {
	if err := validateEnvironmentBlueprintStageClaim(value.Claim); err != nil || !validBlueprintRecordTime(value.UpdatedAt) ||
		value.UpdatedAt.Before(value.Claim.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Blueprint staging descriptor is invalid")
	}
	if !value.Bound {
		if (value.State != EnvironmentBlueprintStageOpen && value.State != EnvironmentBlueprintStageAbandoned) ||
			value.AuditChunks != 0 || value.AuditBytes != 0 ||
			value.ProjectionChunks != 0 || value.ProjectionBytes != 0 || value.ProjectionResources != 0 ||
			value.NextAuditChunk != 0 || value.NextProjectionChunk != 0 || !zeroDigest(value.AuditSHA256) ||
			!zeroDigest(value.ProjectionSHA256) || !zeroDigest(value.DependencyDigest) {
			return errs.New(errs.KindValidationFailed, "unbound Blueprint staging descriptor has stream authority")
		}
		return nil
	}
	if value.AuditBytes == 0 || value.AuditBytes > environmentBlueprintMaximumAuditBytes ||
		value.ProjectionBytes == 0 || value.ProjectionBytes > EnvironmentBlueprintProjectionMaxBytes ||
		value.AuditChunks != chunkCount32(int(value.AuditBytes)) ||
		value.ProjectionChunks != chunkCount32(int(value.ProjectionBytes)) ||
		value.AuditChunks > environmentBlueprintMaximumAuditChunks ||
		value.ProjectionChunks > environmentBlueprintMaximumProjectionChunks ||
		value.AuditChunks+value.ProjectionChunks > environmentBlueprintMaximumChunks ||
		value.ProjectionResources > 512 || value.NextAuditChunk > value.AuditChunks ||
		value.NextProjectionChunk > value.ProjectionChunks || zeroDigest(value.AuditSHA256) ||
		zeroDigest(value.ProjectionSHA256) || zeroDigest(value.DependencyDigest) {
		return errs.New(errs.KindValidationFailed, "Blueprint staging stream authority is invalid")
	}
	switch value.State {
	case EnvironmentBlueprintStageOpen:
	case EnvironmentBlueprintStageSealed, EnvironmentBlueprintStagePublished:
		if value.NextAuditChunk != value.AuditChunks || value.NextProjectionChunk != value.ProjectionChunks {
			return errs.New(errs.KindValidationFailed, "sealed Blueprint staging descriptor is incomplete")
		}
	case EnvironmentBlueprintStageAbandoned:
	default:
		return errs.New(errs.KindValidationFailed, "Blueprint staging descriptor state is invalid")
	}
	return nil
}

func validBlueprintRecordTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.UnixNano() >= 0 &&
		value.Equal(time.Unix(0, value.UnixNano()).UTC())
}

func zeroDigest(value [sha256.Size]byte) bool { return value == [sha256.Size]byte{} }

func sameEnvironmentBlueprintStageClaim(left, right EnvironmentBlueprintStageClaim) bool {
	return left.DescriptorID == right.DescriptorID && left.EnvironmentID == right.EnvironmentID &&
		left.RevisionID == right.RevisionID && left.TaskID == right.TaskID && left.Locator == right.Locator &&
		left.Intent.EnvelopeVersion == right.Intent.EnvelopeVersion && left.Intent.Cipher == right.Intent.Cipher &&
		left.Intent.DigestAlgorithm == right.Intent.DigestAlgorithm &&
		left.Intent.CiphertextDigest == right.Intent.CiphertextDigest &&
		bytes.Equal(left.Intent.Ciphertext, right.Intent.Ciphertext) &&
		left.BaselineHeadRevision == right.BaselineHeadRevision && left.SourceKind == right.SourceKind &&
		left.RenderGeneration == right.RenderGeneration && left.ProjectionSchema == right.ProjectionSchema &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func cloneEnvironmentBlueprintStageClaim(value EnvironmentBlueprintStageClaim) EnvironmentBlueprintStageClaim {
	value.Intent.Ciphertext = append([]byte(nil), value.Intent.Ciphertext...)
	return value
}

func validBlueprintString(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

func corruptEnvironmentBlueprintStage() error {
	return errs.New(errs.KindInternal, "Blueprint staging authority is corrupt")
}

func blueprintChunkLabel(family uint8) string {
	if family == EnvironmentBlueprintChunkAudit {
		return "audit"
	}
	if family == EnvironmentBlueprintChunkProjection {
		return "projection"
	}
	return fmt.Sprintf("invalid-%d", family)
}
