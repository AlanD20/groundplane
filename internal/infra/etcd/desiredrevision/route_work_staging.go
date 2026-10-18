package desiredrevision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RouteWorkStage struct {
	Claim        blueprints.EnvironmentBlueprintStageClaim
	Current      keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]
	Candidate    projectionrecord.EnvironmentComposeProjection
	Audit        blueprints.EnvironmentRouteMutationAudit
	ReadRevision int64
}

// StageRouteWorkRevision seals authored Route intent independently of execution.
// Unchanged authoring comes from the exact prior desired input, not a runtime
// projection reconstructed after hooks or configuration rendering.
func (repository *Repository) StageRouteWorkRevision(ctx context.Context, request RouteWorkStage) (int64, error) {
	claim, current, candidate := request.Claim, request.Current, request.Candidate
	if ctx == nil || repository == nil || blueprints.ValidateEnvironmentBlueprintStageClaim(claim) != nil ||
		claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
		claim.EnvironmentID != current.Record.EnvironmentID || claim.EnvironmentID != candidate.EnvironmentID ||
		claim.BaselineHeadRevision != current.Revision || current.Revision <= 0 ||
		request.ReadRevision < current.Revision || claim.RevisionID != candidate.RevisionID ||
		claim.TaskID != claim.RevisionID || claim.RenderGeneration != candidate.RenderGeneration ||
		candidate.RenderGeneration != current.Record.RenderGeneration+1 ||
		request.Audit.BaseRevisionID != current.Record.RevisionID {
		return 0, errs.New(errs.KindValidationFailed, "Route desired staging identity is inconsistent")
	}
	audit := blueprints.EnvironmentDesiredMutationAudit{Route: &request.Audit}
	if err := blueprints.ValidateEnvironmentDesiredMutationAudit(audit); err != nil {
		return 0, err
	}
	if err := projectionrecord.ValidateEnvironmentComposeProjectionAdvance(current.Record, true, candidate); err != nil {
		return 0, err
	}
	digest, err := blueprints.EnvironmentBlueprintDependencyDigest(candidate)
	if err != nil {
		return 0, err
	}
	desired, err := desiredauthoring.RouteInput(ctx, repository.store, &current.Record, candidate, request.ReadRevision)
	if err != nil {
		return 0, err
	}
	key := hierarchy.EnvironmentMutationEpochKey(claim.EnvironmentID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: request.ReadRevision})
	if err != nil {
		return 0, err
	}
	if read == nil {
		return 0, errs.New(errs.KindStateConflict, "Route Environment snapshot is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != request.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > request.ReadRevision {
		return 0, errs.New(errs.KindStateConflict, "Route Environment epoch is unavailable")
	}
	if _, err := repository.StageEnvironmentBlueprintRevision(ctx, blueprints.EnvironmentBlueprintStageRequest{
		Claim: claim, Mutation: &audit, DesiredInput: desired, DependencyDigest: digest,
	}); err != nil {
		return 0, err
	}
	return read.Values[0].ModRevision, nil
}
