package etcd

import (
	"crypto/sha256"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func buildEnvironmentBlueprintStreams(request EnvironmentBlueprintStageRequest) (EnvironmentBlueprintStreams, error) {
	claim := request.Claim
	if err := validateEnvironmentBlueprintStageClaim(claim); err != nil {
		return EnvironmentBlueprintStreams{}, err
	}
	if request.Projection.EnvironmentID != claim.EnvironmentID ||
		request.Projection.RevisionID != claim.RevisionID || zeroDigest(request.DependencyDigest) {
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
	projectionResources := len(request.Projection.DesiredZones) + len(request.Projection.DesiredServices) +
		len(request.Projection.DesiredRoutes) + len(request.Projection.Volumes) +
		len(request.Projection.VolumeMounts) + len(request.Projection.Components) +
		len(request.Projection.Entries)
	descriptor := EnvironmentBlueprintStageDescriptor{
		Claim: claim, State: EnvironmentBlueprintStageOpen, Bound: true,
		AuditChunks: chunkCount32(len(audit)), AuditBytes: uint64(len(audit)), AuditSHA256: auditDigest,
		ProjectionChunks: chunkCount32(len(projection)), ProjectionBytes: uint64(len(projection)),
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
