package desiredrevision

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"time"

	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ClaimRepository interface {
	ClaimEnvironmentBlueprintStage(
		context.Context,
		blueprints.EnvironmentBlueprintStageClaimRequest,
	) (blueprints.EnvironmentBlueprintStageClaim, error)
}

type AbandonmentRepository interface {
	AbandonEnvironmentBlueprintStage(context.Context, blueprints.EnvironmentBlueprintStageClaim) error
}

type PublicationRepository interface {
	AbandonmentRepository
	StageEnvironmentBlueprintRevision(
		context.Context,
		blueprints.EnvironmentBlueprintStageRequest,
	) (blueprints.EnvironmentBlueprintSeal, error)
	PublishEnvironmentBlueprintAuthoredRevision(
		context.Context,
		etcd.BlueprintAuthoredPublication,
	) (etcdstore.TransactionResult, error)
}

// Repository stages captured desired input for resource-owned publication.
type Repository interface {
	ClaimRepository
	StageEnvironmentBlueprintRevision(
		context.Context,
		blueprints.EnvironmentBlueprintStageRequest,
	) (blueprints.EnvironmentBlueprintSeal, error)
}

type ClaimInput struct {
	EnvironmentID        string
	CandidateTaskID      string
	Locator              idempotencyrecord.IdempotencyLocator
	Intent               idempotencyrecord.ProtectedIntentRecord
	MatchExistingIntent  func(context.Context, idempotencyrecord.ProtectedIntentRecord) (bool, error)
	BaselineHeadRevision int64
	SourceKind           blueprints.EnvironmentBlueprintSourceKind
	RenderGeneration     uint64
	CreatedAt            time.Time
}

func Claim(
	ctx context.Context,
	repository ClaimRepository,
	input ClaimInput,
) (blueprints.EnvironmentBlueprintStageClaim, error) {
	if input.SourceKind != blueprints.EnvironmentBlueprintSourceApply &&
		input.SourceKind != blueprints.EnvironmentBlueprintSourceMutation {
		return blueprints.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindInternal,
			"Environment desired revision source kind is invalid",
		)
	}
	claim, err := repository.ClaimEnvironmentBlueprintStage(ctx, blueprints.EnvironmentBlueprintStageClaimRequest{
		EnvironmentID:        input.EnvironmentID,
		CandidateRevisionID:  input.CandidateTaskID,
		CandidateTaskID:      input.CandidateTaskID,
		Locator:              input.Locator,
		Intent:               input.Intent,
		BaselineHeadRevision: input.BaselineHeadRevision,
		SourceKind:           input.SourceKind,
		RenderGeneration:     input.RenderGeneration,
		ProjectionSchema:     blueprints.EnvironmentDesiredInputSchema,
		CreatedAt:            input.CreatedAt,
	})
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, err
	}
	if claim.RevisionID != claim.TaskID {
		return blueprints.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindInternal,
			"Blueprint staged revision and Task identity diverged",
		)
	}
	if claim.Existing {
		if input.MatchExistingIntent == nil {
			return blueprints.EnvironmentBlueprintStageClaim{}, errs.New(
				errs.KindInternal,
				"Environment desired revision replay matcher is not configured",
			)
		}
		matched, matchErr := input.MatchExistingIntent(ctx, claim.Intent)
		if matchErr != nil {
			return blueprints.EnvironmentBlueprintStageClaim{}, matchErr
		}
		if !matched {
			return blueprints.EnvironmentBlueprintStageClaim{}, errs.New(
				errs.KindIdempotencyMismatch,
				"idempotency key was used for a different desired-state request",
			)
		}
	}
	if claim.EnvironmentID != input.EnvironmentID || claim.Locator != input.Locator ||
		claim.BaselineHeadRevision != input.BaselineHeadRevision ||
		claim.RenderGeneration != input.RenderGeneration ||
		claim.SourceKind != input.SourceKind ||
		claim.ProjectionSchema != blueprints.EnvironmentDesiredInputSchema {
		return blueprints.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindStateConflict,
			"Blueprint staged baseline changed",
		)
	}
	return claim, nil
}

type ProjectionEvidence struct {
	DependencyDigest [sha256.Size]byte
	NormalizedBytes  uint64
}

const stagedPublicationAbandonTimeout = 30 * time.Second

// StagedPublication is the opaque bridge between private desired-revision
// staging and final publication. Private fields prevent callers from
// substituting a projection or seal after staging.
type StagedPublication struct {
	state *stagedPublicationState
}

type stagedPublicationState struct {
	mu           sync.Mutex
	consumed     bool
	locator      idempotencyrecord.IdempotencyLocator
	claim        blueprints.EnvironmentBlueprintStageClaim
	seal         blueprints.EnvironmentBlueprintSeal
	projection   projectionrecord.EnvironmentComposeProjection
	desiredInput projectionrecord.EnvironmentDesiredInput
}

type StageInput struct {
	Claim        blueprints.EnvironmentBlueprintStageClaim
	Blueprint    blueprints.EnvironmentBlueprintRevision
	DesiredInput projectionrecord.EnvironmentDesiredInput
	Projection   projectionrecord.EnvironmentComposeProjection
}

// TaskID returns the sole Task identity bound into this publication.
func (publication StagedPublication) TaskID() string {
	if publication.state == nil {
		return ""
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	return publication.state.claim.TaskID
}

// Stage seals the exact normalized candidate before downstream planning.
func Stage(
	ctx context.Context,
	repository PublicationRepository,
	input StageInput,
) (StagedPublication, error) {
	if ctx == nil || repository == nil {
		return StagedPublication{}, errs.New(
			errs.KindInternal,
			"Environment desired revision staging is not configured",
		)
	}
	projectionBytes, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(input.DesiredInput)
	if err != nil {
		return StagedPublication{}, err
	}
	defer clear(projectionBytes)
	projectionDigest := sha256.Sum256(projectionBytes)
	dependencyDigest := projectionDigest
	if input.Projection.EnvironmentID != "" {
		evidence, err := PreflightProjection(input.Projection)
		if err != nil {
			return StagedPublication{}, err
		}
		dependencyDigest = evidence.DependencyDigest
	}
	detachedInput, err := projectionrecord.DecodeEnvironmentDesiredInputStorage(projectionBytes)
	if err != nil {
		return StagedPublication{}, err
	}
	seal, err := repository.StageEnvironmentBlueprintRevision(ctx, blueprints.EnvironmentBlueprintStageRequest{
		Claim: input.Claim, Blueprint: &input.Blueprint,
		DesiredInput: detachedInput, DependencyDigest: dependencyDigest,
	})
	if err != nil {
		return StagedPublication{}, err
	}
	if seal.EnvironmentID != input.Claim.EnvironmentID || seal.RevisionID != input.Claim.RevisionID ||
		seal.SourceKind != input.Claim.SourceKind || seal.RenderGeneration != input.Claim.RenderGeneration ||
		seal.ProjectionSchema != input.Claim.ProjectionSchema ||
		seal.BaselineHeadRevision != input.Claim.BaselineHeadRevision ||
		seal.DependencyDigest != dependencyDigest || seal.ProjectionBytes != uint64(len(projectionBytes)) ||
		seal.ProjectionSHA256 != projectionDigest {
		return StagedPublication{}, errs.New(
			errs.KindInternal,
			"Environment desired revision seal does not match its candidate",
		)
	}
	return StagedPublication{state: &stagedPublicationState{
		locator: input.Claim.Locator, claim: input.Claim, seal: seal,
		projection:   projectionrecord.CloneEnvironmentComposeProjection(input.Projection),
		desiredInput: detachedInput,
	}}, nil
}

func (publication StagedPublication) reserveAbandonment() (blueprints.EnvironmentBlueprintStageClaim, error) {
	if publication.state == nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindValidationFailed,
			"Environment desired staged publication is invalid",
		)
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	if publication.state.consumed {
		return blueprints.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindStateConflict,
			"Environment desired staged publication was already consumed",
		)
	}
	publication.state.consumed = true
	return publication.state.claim, nil
}

func abandonKnownFailure(
	ctx context.Context,
	repository AbandonmentRepository,
	claim blueprints.EnvironmentBlueprintStageClaim,
	cause error,
) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stagedPublicationAbandonTimeout)
	defer cancel()
	if err := repository.AbandonEnvironmentBlueprintStage(cleanupCtx, claim); err != nil {
		return err
	}
	return cause
}

// AbandonClaim releases a private desired-revision claim after a known
// pre-publication failure. It must not be used after the final publication
// transaction has been attempted, because that outcome may be unknown.
func AbandonClaim(
	ctx context.Context,
	repository AbandonmentRepository,
	claim blueprints.EnvironmentBlueprintStageClaim,
	cause error,
) error {
	if ctx == nil || repository == nil || cause == nil {
		return errs.New(errs.KindInternal, "Environment desired revision claim abandonment is not configured")
	}
	return abandonKnownFailure(ctx, repository, claim, cause)
}

// Abandon releases a sealed candidate after a known pre-publication failure.
func Abandon(
	ctx context.Context,
	repository AbandonmentRepository,
	publication StagedPublication,
	cause error,
) error {
	if ctx == nil || repository == nil || cause == nil {
		return errs.New(errs.KindInternal, "Environment desired revision abandonment is not configured")
	}
	claim, err := publication.reserveAbandonment()
	if err != nil {
		return err
	}
	return abandonKnownFailure(ctx, repository, claim, cause)
}

func PreflightProjection(projection projectionrecord.EnvironmentComposeProjection) (ProjectionEvidence, error) {
	digest, normalizedBytes, err := blueprints.EnvironmentBlueprintProjectionEvidence(projection)
	if err != nil {
		return ProjectionEvidence{}, err
	}
	return ProjectionEvidence{DependencyDigest: digest, NormalizedBytes: normalizedBytes}, nil
}

func PreflightAndClaim(
	ctx context.Context,
	repository ClaimRepository,
	projection projectionrecord.EnvironmentComposeProjection,
	input ClaimInput,
) (blueprints.EnvironmentBlueprintStageClaim, ProjectionEvidence, error) {
	evidence, err := PreflightProjection(projection)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, ProjectionEvidence{}, err
	}
	claim, err := Claim(ctx, repository, input)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, ProjectionEvidence{}, err
	}
	return claim, evidence, nil
}

func unknownOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

func cloneResponse(response idempotencyrecord.IdempotencyResponse) idempotencyrecord.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}
