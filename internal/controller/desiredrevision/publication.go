package desiredrevision

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Repository interface {
	ClaimEnvironmentBlueprintStage(context.Context, etcd.EnvironmentBlueprintStageClaimRequest) (etcd.EnvironmentBlueprintStageClaim, error)
	StageEnvironmentBlueprintRevision(context.Context, etcd.EnvironmentBlueprintStageRequest) (etcd.EnvironmentBlueprintSeal, error)
	PublishEnvironmentDesiredRevisionWithTask(
		context.Context,
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.EnvironmentRecord],
		int64,
		etcd.EnvironmentBlueprintStageClaim,
		etcd.EnvironmentDesiredRevisionIdentity,
		etcd.EnvironmentComposeProjection,
		[]etcd.EnvironmentBlueprintZoneChange,
		[]etcd.EnvironmentBlueprintServiceChange,
		[]etcd.EnvironmentBlueprintRouteChange,
		etcd.ReleaseGroupBlueprintPreparedMutation,
		etcd.ComponentTaskPreparation,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type ClaimInput struct {
	EnvironmentID        string
	CandidateTaskID      string
	Locator              etcd.IdempotencyLocator
	Intent               etcd.ProtectedIntentRecord
	MatchExistingIntent  func(context.Context, etcd.ProtectedIntentRecord) (bool, error)
	BaselineHeadRevision int64
	SourceKind           etcd.EnvironmentBlueprintSourceKind
	RenderGeneration     uint64
	CreatedAt            time.Time
}

func Claim(
	ctx context.Context,
	repository Repository,
	input ClaimInput,
) (etcd.EnvironmentBlueprintStageClaim, error) {
	if input.SourceKind != etcd.EnvironmentBlueprintSourceApply &&
		input.SourceKind != etcd.EnvironmentBlueprintSourceMutation {
		return etcd.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindInternal,
			"Environment desired revision source kind is invalid",
		)
	}
	claim, err := repository.ClaimEnvironmentBlueprintStage(ctx, etcd.EnvironmentBlueprintStageClaimRequest{
		EnvironmentID:        input.EnvironmentID,
		CandidateRevisionID:  input.CandidateTaskID,
		CandidateTaskID:      input.CandidateTaskID,
		Locator:              input.Locator,
		Intent:               input.Intent,
		BaselineHeadRevision: input.BaselineHeadRevision,
		SourceKind:           input.SourceKind,
		RenderGeneration:     input.RenderGeneration,
		ProjectionSchema:     etcd.EnvironmentDesiredProjectionSchema,
		CreatedAt:            input.CreatedAt,
	})
	if err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, err
	}
	if claim.RevisionID != claim.TaskID {
		return etcd.EnvironmentBlueprintStageClaim{}, errs.New(errs.KindInternal, "Blueprint staged revision and Task identity diverged")
	}
	if claim.Existing {
		if input.MatchExistingIntent == nil {
			return etcd.EnvironmentBlueprintStageClaim{}, errs.New(
				errs.KindInternal,
				"Environment desired revision replay matcher is not configured",
			)
		}
		matched, matchErr := input.MatchExistingIntent(ctx, claim.Intent)
		if matchErr != nil {
			return etcd.EnvironmentBlueprintStageClaim{}, matchErr
		}
		if !matched {
			return etcd.EnvironmentBlueprintStageClaim{}, errs.New(
				errs.KindIdempotencyMismatch,
				"idempotency key was used for a different desired-state request",
			)
		}
	}
	if claim.EnvironmentID != input.EnvironmentID || claim.Locator != input.Locator ||
		claim.BaselineHeadRevision != input.BaselineHeadRevision ||
		claim.RenderGeneration != input.RenderGeneration ||
		claim.SourceKind != input.SourceKind ||
		claim.ProjectionSchema != etcd.EnvironmentDesiredProjectionSchema {
		return etcd.EnvironmentBlueprintStageClaim{}, errs.New(errs.KindStateConflict, "Blueprint staged baseline changed")
	}
	return claim, nil
}

type PublishInput struct {
	Project                 etcd.Versioned[etcd.ProjectRecord]
	Environment             etcd.Versioned[etcd.EnvironmentRecord]
	ExpectedHeadRevision    int64
	Claim                   etcd.EnvironmentBlueprintStageClaim
	Evidence                Evidence
	Locator                 etcd.IdempotencyLocator
	Blueprint               etcd.EnvironmentBlueprintRevision
	Projection              etcd.EnvironmentComposeProjection
	DependencyDigest        [sha256.Size]byte
	ZoneChanges             []etcd.EnvironmentBlueprintZoneChange
	ServiceChanges          []etcd.EnvironmentBlueprintServiceChange
	RouteChanges            []etcd.EnvironmentBlueprintRouteChange
	ReleaseGroupPreparation etcd.ReleaseGroupBlueprintPreparedMutation
	ComponentPreparation    etcd.ComponentTaskPreparation
	Task                    etcd.TaskRecord
}

type ProjectionEvidence struct {
	DependencyDigest [sha256.Size]byte
	NormalizedBytes  uint64
}

func PreflightProjection(projection etcd.EnvironmentComposeProjection) (ProjectionEvidence, error) {
	digest, normalizedBytes, err := etcd.EnvironmentBlueprintProjectionEvidence(projection)
	if err != nil {
		return ProjectionEvidence{}, err
	}
	return ProjectionEvidence{DependencyDigest: digest, NormalizedBytes: normalizedBytes}, nil
}

func PreflightAndClaim(
	ctx context.Context,
	repository Repository,
	projection etcd.EnvironmentComposeProjection,
	input ClaimInput,
) (etcd.EnvironmentBlueprintStageClaim, ProjectionEvidence, error) {
	evidence, err := PreflightProjection(projection)
	if err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, ProjectionEvidence{}, err
	}
	claim, err := Claim(ctx, repository, input)
	if err != nil {
		return etcd.EnvironmentBlueprintStageClaim{}, ProjectionEvidence{}, err
	}
	return claim, evidence, nil
}

func Publish(
	ctx context.Context,
	repository Repository,
	idempotency *Idempotency,
	input PublishInput,
) (etcd.IdempotencyResponse, error) {
	if _, err := repository.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim: input.Claim, Blueprint: &input.Blueprint,
		Projection: input.Projection, DependencyDigest: input.DependencyDigest,
	}); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: input.Task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: input.Locator, Intent: input.Claim.Intent, Response: response,
		TaskID: input.Task.ID, CreatedAt: input.Claim.CreatedAt, UpdatedAt: input.Claim.CreatedAt,
	}
	result, publicationErr := repository.PublishEnvironmentDesiredRevisionWithTask(
		ctx, input.Project, input.Environment, input.ExpectedHeadRevision,
		input.Claim,
		etcd.EnvironmentDesiredRevisionIdentity{EnvironmentID: input.Environment.Record.ID, RevisionID: input.Task.ID},
		input.Projection, input.ZoneChanges, input.ServiceChanges, input.RouteChanges,
		input.ReleaseGroupPreparation, input.ComponentPreparation, input.Task, marker,
	)
	var resolution idempotentintent.Resolution
	if publicationErr != nil {
		if !unknownOutcome(publicationErr) {
			return etcd.IdempotencyResponse{}, publicationErr
		}
		resolution, err = idempotency.ResolveUnknown(ctx, input.Locator, input.Evidence, publicationErr)
	} else {
		resolution, err = idempotency.ResolveKnown(ctx, input.Evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case idempotentintent.ResolutionApplied:
		return cloneResponse(response), nil
	case idempotentintent.ResolutionReplay:
		return cloneResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment Blueprint resolution is invalid")
	}
}

func unknownOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

func cloneResponse(response etcd.IdempotencyResponse) etcd.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}
