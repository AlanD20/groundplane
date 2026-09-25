package blueprints

import (
	"crypto/sha256"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func BuildEnvironmentBlueprintStreams(request EnvironmentBlueprintStageRequest) (EnvironmentBlueprintStreams, error) {
	claim := request.Claim
	if err := ValidateEnvironmentBlueprintStageClaim(claim); err != nil {
		return EnvironmentBlueprintStreams{}, err
	}
	if request.DesiredInput.EnvironmentID != claim.EnvironmentID ||
		request.DesiredInput.RevisionID != claim.RevisionID ||
		request.DesiredInput.RenderGeneration != claim.RenderGeneration || zeroDigest(request.DependencyDigest) {
		return EnvironmentBlueprintStreams{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint staging input does not match its claim",
		)
	}
	var audit []byte
	if claim.SourceKind == EnvironmentBlueprintSourceApply {
		if request.Blueprint == nil || request.Mutation != nil ||
			request.Blueprint.EnvironmentID != claim.EnvironmentID ||
			request.Blueprint.RevisionID != claim.RevisionID ||
			!request.Blueprint.CreatedAt.Equal(claim.CreatedAt) {
			return EnvironmentBlueprintStreams{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint apply audit does not match its claim",
			)
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
	projection, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(request.DesiredInput)
	if err != nil {
		clear(audit)
		return EnvironmentBlueprintStreams{}, err
	}
	if len(projection) > projectionrecord.EnvironmentBlueprintProjectionMaxBytes {
		clear(audit)
		clear(projection)
		return EnvironmentBlueprintStreams{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint normalized desired input exceeds the 2 MiB ceiling",
		)
	}
	auditDigest := sha256.Sum256(audit)
	projectionDigest := sha256.Sum256(projection)
	input := request.DesiredInput.Input
	projectionResources := len(input.RuntimeFiles) + len(input.ServiceExtensions) + len(input.Requires) +
		len(input.Attachments) + len(input.Entries) + len(input.Routes) + len(input.Scripts) +
		len(input.Components) + len(input.ReleaseGroups)
	if input.Backup != nil {
		projectionResources++
	}
	descriptor := EnvironmentBlueprintStageDescriptor{
		Claim: claim, State: EnvironmentBlueprintStageOpen, Bound: true,
		AuditChunks: ChunkCount32(len(audit)), AuditBytes: uint64(len(audit)), AuditSHA256: auditDigest,
		ProjectionChunks: ChunkCount32(len(projection)), ProjectionBytes: uint64(len(projection)),
		ProjectionSHA256: projectionDigest, ProjectionResources: uint32(projectionResources),
		DependencyDigest: request.DependencyDigest, UpdatedAt: claim.CreatedAt,
	}
	if err := validateEnvironmentBlueprintStageDescriptor(descriptor); err != nil {
		clear(audit)
		clear(projection)
		return EnvironmentBlueprintStreams{}, err
	}
	return EnvironmentBlueprintStreams{Audit: audit, Projection: projection, Descriptor: descriptor}, nil
}
