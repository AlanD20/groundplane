package network

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (service *zoneDeletionService) compileAcceptedZoneRemoval(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, payload acceptedZoneRemoval,
) (result taskdependencies.Prepared, err error) {
	zoneID, impactToken := payload.ZoneID, payload.ImpactToken
	zone, err := service.repository.GetZone(ctx, zoneID)
	if err != nil {
		return result, err
	}
	backing := zone.Record.Desired.OwnerKind == core.ZoneOwnerBackingProject
	if backing {
		if service.impacts == nil {
			return result, errs.New(
				errs.KindInternal,
				"Zone removal impact service is not configured",
			)
		}
		impact, impactErr := service.impacts.GetZoneRemovalImpact(ctx, zoneID)
		if impactErr != nil {
			return result, impactErr
		}
		if impactToken == "" || impact.ImpactToken != impactToken {
			return result, errs.New(
				errs.KindStateConflict,
				"backing Zone removal impact changed",
			)
		}
	}
	environment, err := service.repository.GetEnvironment(ctx, zone.Record.EnvironmentID)
	if err != nil {
		return result, err
	}
	if environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return result, errs.New(
			errs.KindResourceInUse,
			"Zone Environment is not ready",
		)
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return result, err
	}
	if accepted.Task.TargetID != zoneID ||
		accepted.Task.Scope != (workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID}) {
		return result, errs.New(errs.KindStateConflict, "Zone ownership changed after removal was accepted")
	}
	ordinaryOwnership := project.Record.Kind == hierarchyrecord.ProjectKindTenant &&
		zone.Record.Desired.OwnerKind == core.ZoneOwnerEnvironment && zone.Record.Desired.OwnerID == environment.Record.ID
	backingOwnership := project.Record.Kind == hierarchyrecord.ProjectKindBacking &&
		zone.Record.Desired.OwnerKind == core.ZoneOwnerBackingProject && zone.Record.Desired.OwnerID == project.Record.ID
	if (!backing && !ordinaryOwnership) || (backing && !backingOwnership) {
		return result, errs.New(
			errs.KindStateConflict,
			"Zone ownership is inconsistent",
		)
	}
	authorities, found, err := service.repository.GetEnvironmentZoneRemovalAuthorities(ctx, environment.Record.ID)
	if err != nil {
		return result, err
	}
	if !found {
		return result, errs.New(
			errs.KindStateConflict,
			"Zone desired revision is missing",
		)
	}
	projection := authorities.Desired
	now := accepted.Task.CreatedAt
	candidateRevisionID := accepted.Task.ID
	candidate, affected, err := buildZoneRemovalProjection(
		projection.Record, zone.Record.Desired.ID, zone.Record.Desired.Name,
		candidateRevisionID, projection.Record.RenderGeneration+1,
	)
	if err != nil {
		return result, err
	}
	projectionEvidence, err := controllerrevision.PreflightProjection(candidate)
	if err != nil {
		return result, err
	}
	claim, err := service.repository.ClaimEnvironmentBlueprintStage(
		ctx,
		blueprints.EnvironmentBlueprintStageClaimRequest{
			EnvironmentID: environment.Record.ID, CandidateRevisionID: candidateRevisionID,
			CandidateTaskID: candidateRevisionID, Locator: payload.Locator, Intent: payload.Intent, AcceptedTaskRevision: accepted.Revision, TaskLocator: &payload.Locator,
			BaselineHeadRevision: projection.Revision, SourceKind: blueprints.EnvironmentBlueprintSourceMutation,
			RenderGeneration: candidate.RenderGeneration, ProjectionSchema: blueprints.EnvironmentDesiredInputSchema,
			CreatedAt: now,
		},
	)
	if err != nil {
		return result, err
	}
	if claim.Existing {
		if !blueprints.SameBlueprintProtectedIntent(payload.Intent, claim.Intent) {
			return result, errs.New(
				errs.KindIdempotencyMismatch,
				"idempotency key was used for another Zone removal",
			)
		}
		candidate, affected, err = buildZoneRemovalProjection(
			projection.Record, zone.Record.Desired.ID, zone.Record.Desired.Name,
			claim.RevisionID, claim.RenderGeneration,
		)
		if err != nil {
			return result, err
		}
		projectionEvidence, err = controllerrevision.PreflightProjection(candidate)
		if err != nil {
			return result, err
		}
	}
	if claim.EnvironmentID != environment.Record.ID || claim.RevisionID != claim.TaskID ||
		claim.Locator != payload.Locator || claim.BaselineHeadRevision != projection.Revision ||
		claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
		claim.RenderGeneration != candidate.RenderGeneration ||
		claim.ProjectionSchema != blueprints.EnvironmentDesiredInputSchema {
		return result, errs.New(errs.KindStateConflict, "Zone staged baseline changed")
	}
	var detachIDs []string
	result.Finalize = func(cleanup context.Context, applied bool) error {
		if applied {
			_, err := service.repository.FinishNativePreparationDraft(cleanup, accepted.Task.ID, true)
			return err
		}
		abandoned, err := service.repository.AbandonNetworkWorkInput(cleanup, accepted,
			ids.DeriveAt(ids.KindWorkItem, claim.CreatedAt, claim.TaskID, "network"), detachIDs)
		if err != nil || !abandoned {
			return err
		}
		err = service.repository.AbandonEnvironmentBlueprintStage(cleanup, claim)
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindStateConflict {
			err = nil
		}
		if err != nil {
			return err
		}
		_, err = service.repository.FinishNativePreparationDraft(cleanup, accepted.Task.ID, false)
		return err
	}
	prepared, err := service.prepareZoneRemovalWork(ctx, etcd.NetworkWorkPublication{
		Project: project, Environment: environment, Zone: zone, Authorities: authorities,
		Claim: claim, Projection: candidate}, projectionEvidence, affected, accepted, at, payload.ImpactToken, &detachIDs)
	if err != nil {
		return result, err
	}
	prepared.Finalize = result.Finalize
	return prepared, nil

}
