package etcd

import (
	"crypto/sha256"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"time"
)

const (
	EnvironmentBlueprintChunkBytes        = 60 * 1024
	EnvironmentBlueprintStageBatchChunks  = 12
	environmentBlueprintMaximumAuditBytes = 1_049_100

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
	Locator              idempotencyrecord.IdempotencyLocator
	Intent               idempotencyrecord.ProtectedIntentRecord
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
	Locator              idempotencyrecord.IdempotencyLocator
	Intent               idempotencyrecord.ProtectedIntentRecord
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
	Projection       projectionrecord.EnvironmentComposeProjection
	DependencyDigest [sha256.Size]byte
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
