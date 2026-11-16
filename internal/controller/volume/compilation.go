package volume

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
	"time"
)

func (service *MutationService) compileAcceptedMutation(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, payload acceptedVolumeMutation,
) (result taskdependencies.Prepared, err error) {
	request := volumeMutationRequest{
		action:        payload.Action,
		environmentID: payload.EnvironmentID,
		volumeID:      payload.VolumeID,
		slug:          payload.Slug,
		key:           payload.Key,
		keySupplied:   payload.KeySupplied,
		impactToken:   payload.ImpactToken,
		confirmKey:    payload.ConfirmKey,
	}
	placement, err := service.repository.ResolveResourceHost(ctx)
	if err != nil {
		return result, err
	}
	environment, err := service.repository.GetEnvironment(ctx, request.environmentID)
	if err != nil {
		return result, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return result, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return result, err
	}
	if accepted.Task.Scope.EnvironmentID != environment.Record.ID ||
		accepted.Task.Scope.ProjectID != project.Record.ID ||
		accepted.Task.Scope.TenantID != tenant.Record.ID ||
		payload.VolumeID != accepted.Task.TargetID {
		return result, errs.New(errs.KindStateConflict, "Volume ownership differs from the accepted action")
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return result, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Volume mutation",
		)
	}
	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, request.environmentID)
	if err != nil {
		return result, err
	}
	current, hasCurrent, err := service.repository.GetEnvironmentComposeProjection(ctx, request.environmentID)
	if err != nil {
		return result, err
	}
	expectedHeadRevision, generation, err := desiredState(
		request.environmentID, head, hasHead, current, hasCurrent,
	)
	if err != nil {
		return result, err
	}
	if generation > math.MaxInt32 {
		return result, errs.New(
			errs.KindResourceInUse,
			"Environment render generation is exhausted",
		)
	}
	if err := validateVolumeMutationAgainstProjection(request, current.Record, hasCurrent); err != nil {
		return result, err
	}
	if request.action == volumeMutationActionRemove {
		identity := projectionrecord.EnvironmentVolumeIdentity{}
		for _, candidate := range current.Record.Volumes {
			if candidate.ID == request.volumeID {
				identity = candidate
				break
			}
		}
		backupImpact, impactErr := service.repository.ResolveVolumeRemovalImpactAtRevision(
			ctx, request.environmentID, request.volumeID, current.ReadRevision, service.now().UTC(),
		)
		if impactErr != nil {
			return result, impactErr
		}
		items := volumeRemovalImpactItems(current.Record, request.volumeID, backupImpact)
		expectedImpact, impactErr := volumeImpactDigest(current.Record, identity, backupImpact, items)
		if impactErr != nil {
			return result, impactErr
		}
		if request.impactToken != expectedImpact || request.confirmKey != identity.Key {
			return result, errs.New(
				errs.KindStateConflict, "Volume removal impact changed before publication",
			)
		}
	}

	candidateTaskID := accepted.Task.ID
	candidateRequest, candidateProjection, _, _, err := buildVolumeMutationCandidate(
		tenant.Record.ID, project.Record.ID, environment.Record,
		current.Record, hasCurrent, request, candidateTaskID, generation,
	)
	if err != nil {
		return result, err
	}
	createdAt := accepted.Task.CreatedAt
	var removalPolicy etcd.VolumeRemovalBackupPolicyPreparation
	if request.action == volumeMutationActionRemove {
		removalPolicy, err = service.policies.PrepareVolumeRemovalBackupPolicy(
			ctx,
			request.environmentID,
			request.volumeID,
			current.ReadRevision,
		)
		if err != nil {
			return result, err
		}
		candidateProjection.Backup = removalPolicy.Projection()
	}
	claim, _, err := desiredrevision.PreflightAndClaim(
		ctx, service.repository, candidateProjection, desiredrevision.ClaimInput{
			EnvironmentID: request.environmentID, CandidateTaskID: candidateTaskID, AcceptedTaskRevision: accepted.Revision, TaskLocator: &payload.Locator,
			Locator: payload.Locator, Intent: payload.Intent, BaselineHeadRevision: expectedHeadRevision,
			SourceKind: blueprints.EnvironmentBlueprintSourceMutation,
			MatchExistingIntent: func(ctx context.Context, existing idempotencyrecord.ProtectedIntentRecord) (bool, error) {
				return blueprints.SameBlueprintProtectedIntent(payload.Intent, existing), nil
			},
			RenderGeneration: generation, CreatedAt: createdAt,
		},
	)
	if err != nil {
		return result, err
	}
	result.Finalize = func(cleanup context.Context, applied bool) error {
		if applied {
			_, err := service.repository.FinishNativePreparationDraft(cleanup, accepted.Task.ID, true)
			return err
		}
		abandoned, err := service.repository.AbandonVolumeWorkInput(cleanup, accepted,
			ids.DeriveAt(ids.KindWorkItem, claim.CreatedAt, claim.TaskID, "volume"))
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

	request, candidate, oldArtifact, newArtifact, err := buildVolumeMutationCandidate(
		tenant.Record.ID, project.Record.ID, environment.Record,
		current.Record, hasCurrent, candidateRequest, claim.RevisionID, generation,
	)
	if err != nil {
		return result, err
	}
	intentDigest, err := hex.DecodeString(claim.Intent.CiphertextDigest)
	if err != nil || len(intentDigest) != sha256.Size {
		return result, errs.New(
			errs.KindInternal,
			"Volume protected intent digest is invalid",
		)
	}
	if request.action == volumeMutationActionRemove {
		candidate.Backup = removalPolicy.Projection()
	}
	projectionEvidence, err := desiredrevision.PreflightProjection(candidate)
	if err != nil {
		return result, err
	}
	previousRevisionID := ""
	if hasHead {
		previousRevisionID = head.Record.RevisionID
	}
	desiredInput, err := desiredrevision.DeriveCurrentMutationDesiredInput(
		ctx, service.repository, request.environmentID, previousRevisionID, candidate,
		environment.Record.NetworkPool, func(input *core.BlueprintDesiredInput) error {
			return updateAuthoredBackupVolumeReferences(input, current.Record, request)
		},
	)
	if err != nil {
		return result, err
	}
	dependencyDigest := projectionEvidence.DependencyDigest
	var precondition [sha256.Size]byte
	if request.action == volumeMutationActionRemove && hasCurrent {
		precondition, err = blueprints.EnvironmentBlueprintDependencyDigest(current.Record)
		if err != nil {
			return result, err
		}
	}
	action := blueprints.EnvironmentVolumeMutationAdd
	if request.action == volumeMutationActionEdit {
		action = blueprints.EnvironmentVolumeMutationEdit
	} else if request.action == volumeMutationActionRemove {
		action = blueprints.EnvironmentVolumeMutationRemove
	}
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, blueprints.EnvironmentBlueprintStageRequest{
		Claim: claim,
		Mutation: &blueprints.EnvironmentDesiredMutationAudit{Volume: &blueprints.EnvironmentVolumeMutationAudit{
			Action: action, VolumeID: request.volumeID, Slug: request.slug, Key: request.key,
			KeySupplied: request.keySupplied, PreconditionDigest: precondition,
		}},
		DesiredInput: desiredInput, DependencyDigest: dependencyDigest,
	}); err != nil {
		return result, err
	}
	task, item, captured, err := buildVolumeWork(workexecution.Scope{
		TenantID: tenant.Record.ID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID,
	}, placement, claim, request, oldArtifact, newArtifact, intentDigest,
		volumeMutationConsumerIDs(current.Record, request.volumeID))
	if err != nil {
		return result, err
	}
	task.AcceptedIntent = accepted.Task.AcceptedIntent
	task.After = append([]string(nil), accepted.Task.After...)
	task.CreatedAt, task.UpdatedAt, task.Deadline = accepted.Task.CreatedAt, at, accepted.Task.Deadline
	timeout, err := volumeexecution.ExecutionBudget(captured.Action)
	if err != nil {
		return result, err
	}
	deadline := acceptedaction.ExecutionDeadline(task, at, timeout)
	item.CreatedAt, item.UpdatedAt, item.Deadline = at, at, deadline
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return result, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: payload.Locator, Intent: payload.Intent,
		Response: idempotencyrecord.IdempotencyResponse{
			Status:      202,
			ContentKind: "application/json",
			Body:        responseBody,
		},
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt}
	if request.action == volumeMutationActionRemove {
		marker.ReplayTarget = &idempotencyrecord.IdempotencyReplayTarget{
			Kind: idempotencyrecord.IdempotencyReplayTargetVolume,
			ID:   task.TargetID,
		}
	}
	authority, err := service.repository.PrepareVolumeWork(ctx, etcd.VolumeWorkPublication{
		Project: project, Environment: environment, ExpectedHeadRevision: expectedHeadRevision,
		Claim: claim, Projection: candidate, Policy: removalPolicy, Task: task, Item: item, Input: captured, Marker: marker})
	if err != nil {
		return result, err
	}
	result.Task, result.Graph, result.Authority = task, []workexecution.Item{item}, authority
	return result, nil
}
