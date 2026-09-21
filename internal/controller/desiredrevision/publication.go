package desiredrevision

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"net/http"
	"net/netip"
	"sync"
	"time"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ClaimRepository interface {
	ClaimEnvironmentBlueprintStage(
		context.Context,
		etcd.EnvironmentBlueprintStageClaimRequest,
	) (etcd.EnvironmentBlueprintStageClaim, error)
}

type PublicationRepository interface {
	StageEnvironmentBlueprintRevision(
		context.Context,
		etcd.EnvironmentBlueprintStageRequest,
	) (etcd.EnvironmentBlueprintSeal, error)
	AbandonEnvironmentBlueprintStage(context.Context, etcd.EnvironmentBlueprintStageClaim) error
	PublishEnvironmentBlueprintDesiredRevision(
		context.Context,
		netip.Prefix,
		string,
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		int64,
		etcd.EnvironmentBlueprintStageClaim,
		etcd.EnvironmentDesiredRevisionIdentity,
		projectionrecord.EnvironmentComposeProjection,
		[]blueprints.EnvironmentBlueprintZoneChange,
		[]blueprints.EnvironmentBlueprintServiceChange,
		[]blueprints.EnvironmentBlueprintRouteChange,
		etcd.ReleaseGroupBlueprintPreparedMutation,
		etcd.ComponentTaskPreparation,
		etcd.BlueprintAttachTaskPreparation,
		etcd.BlueprintBackupPolicyPreparation,
		etcd.BlueprintScriptPublication,
		etcd.BlueprintReleasePublication,
		etcd.BlueprintRequirementGate,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type PublicationIdempotency interface {
	ResolveKnown(context.Context, Evidence, etcd.IdempotencyTransactionResult) (requestidempotency.Resolution, error)
	ResolveUnknown(context.Context, idempotencyrecord.IdempotencyLocator, Evidence, error) (requestidempotency.Resolution, error)
}

// Repository is the aggregate used by mutation services that both stage and
// publish direct desired revisions. Claim-only and Blueprint publication
// boundaries use the narrower interfaces above.
type Repository interface {
	ClaimRepository
	StageEnvironmentBlueprintRevision(
		context.Context,
		etcd.EnvironmentBlueprintStageRequest,
	) (etcd.EnvironmentBlueprintSeal, error)
	PublishEnvironmentDesiredRevisionWithTask(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		int64,
		etcd.EnvironmentBlueprintStageClaim,
		etcd.EnvironmentDesiredRevisionIdentity,
		projectionrecord.EnvironmentComposeProjection,
		[]blueprints.EnvironmentBlueprintZoneChange,
		[]blueprints.EnvironmentBlueprintServiceChange,
		[]blueprints.EnvironmentBlueprintRouteChange,
		etcd.ReleaseGroupBlueprintPreparedMutation,
		etcd.ComponentTaskPreparation,
		etcd.BlueprintAttachTaskPreparation,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type ClaimInput struct {
	EnvironmentID        string
	CandidateTaskID      string
	Locator              idempotencyrecord.IdempotencyLocator
	Intent               idempotencyrecord.ProtectedIntentRecord
	MatchExistingIntent  func(context.Context, idempotencyrecord.ProtectedIntentRecord) (bool, error)
	BaselineHeadRevision int64
	SourceKind           etcd.EnvironmentBlueprintSourceKind
	RenderGeneration     uint64
	CreatedAt            time.Time
}

func Claim(
	ctx context.Context,
	repository ClaimRepository,
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
		return etcd.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindInternal,
			"Blueprint staged revision and Task identity diverged",
		)
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
		return etcd.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindStateConflict,
			"Blueprint staged baseline changed",
		)
	}
	return claim, nil
}

type PublishInput struct {
	Project                 etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	Environment             etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	EnvironmentPool         netip.Prefix
	NetworkPool             string
	ExpectedHeadRevision    int64
	Staged                  StagedPublication
	Evidence                Evidence
	Locator                 idempotencyrecord.IdempotencyLocator
	ZoneChanges             []blueprints.EnvironmentBlueprintZoneChange
	ServiceChanges          []blueprints.EnvironmentBlueprintServiceChange
	RouteChanges            []blueprints.EnvironmentBlueprintRouteChange
	ReleaseGroupPreparation etcd.ReleaseGroupBlueprintPreparedMutation
	ComponentPreparation    etcd.ComponentTaskPreparation
	AttachPreparation       etcd.BlueprintAttachTaskPreparation
	BackupPreparation       etcd.BlueprintBackupPolicyPreparation
	ScriptPublication       etcd.BlueprintScriptPublication
	ReleasePublication      etcd.BlueprintReleasePublication
	RequirementGate         etcd.BlueprintRequirementGate
	Task                    etcd.TaskRecord
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
	mu         sync.Mutex
	consumed   bool
	locator    idempotencyrecord.IdempotencyLocator
	claim      etcd.EnvironmentBlueprintStageClaim
	seal       etcd.EnvironmentBlueprintSeal
	projection projectionrecord.EnvironmentComposeProjection
}

type StageInput struct {
	Claim      etcd.EnvironmentBlueprintStageClaim
	Blueprint  blueprints.EnvironmentBlueprintRevision
	Projection projectionrecord.EnvironmentComposeProjection
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
	projectionBytes, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(input.Projection)
	if err != nil {
		return StagedPublication{}, err
	}
	defer clear(projectionBytes)
	projectionDigest := sha256.Sum256(projectionBytes)
	evidence, err := PreflightProjection(input.Projection)
	if err != nil {
		return StagedPublication{}, err
	}
	seal, err := repository.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim: input.Claim, Blueprint: &input.Blueprint,
		Projection: input.Projection, DependencyDigest: evidence.DependencyDigest,
	})
	if err != nil {
		return StagedPublication{}, err
	}
	if seal.EnvironmentID != input.Claim.EnvironmentID || seal.RevisionID != input.Claim.RevisionID ||
		seal.SourceKind != input.Claim.SourceKind || seal.RenderGeneration != input.Claim.RenderGeneration ||
		seal.ProjectionSchema != input.Claim.ProjectionSchema ||
		seal.BaselineHeadRevision != input.Claim.BaselineHeadRevision ||
		seal.DependencyDigest != evidence.DependencyDigest || seal.ProjectionBytes != uint64(len(projectionBytes)) ||
		seal.ProjectionSHA256 != projectionDigest {
		return StagedPublication{}, errs.New(
			errs.KindInternal,
			"Environment desired revision seal does not match its candidate",
		)
	}
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(projectionBytes)
	if err != nil {
		return StagedPublication{}, err
	}
	return StagedPublication{state: &stagedPublicationState{
		locator: input.Claim.Locator, claim: input.Claim, seal: seal, projection: projection,
	}}, nil
}

func (publication StagedPublication) consume(environmentID, taskID string, locator idempotencyrecord.IdempotencyLocator) (
	etcd.EnvironmentBlueprintStageClaim,
	projectionrecord.EnvironmentComposeProjection,
	error,
) {
	if publication.state == nil {
		return etcd.EnvironmentBlueprintStageClaim{}, projectionrecord.EnvironmentComposeProjection{}, errs.New(
			errs.KindValidationFailed,
			"Environment desired staged publication is invalid",
		)
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	if publication.state.consumed {
		return etcd.EnvironmentBlueprintStageClaim{}, projectionrecord.EnvironmentComposeProjection{}, errs.New(
			errs.KindStateConflict,
			"Environment desired staged publication was already consumed",
		)
	}
	publication.state.consumed = true
	claim := publication.state.claim
	if locator != publication.state.locator || claim.Locator != publication.state.locator ||
		claim.EnvironmentID != environmentID || claim.TaskID != taskID ||
		claim.RevisionID != publication.state.seal.RevisionID ||
		claim.EnvironmentID != publication.state.seal.EnvironmentID ||
		publication.state.projection.EnvironmentID != publication.state.seal.EnvironmentID ||
		publication.state.projection.RevisionID != publication.state.seal.RevisionID {
		return claim, projectionrecord.EnvironmentComposeProjection{}, errs.New(
			errs.KindValidationFailed,
			"Environment desired staged publication identity is invalid",
		)
	}
	encoded, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(publication.state.projection)
	if err != nil {
		return claim, projectionrecord.EnvironmentComposeProjection{}, err
	}
	defer clear(encoded)
	if uint64(len(encoded)) != publication.state.seal.ProjectionBytes ||
		sha256.Sum256(encoded) != publication.state.seal.ProjectionSHA256 {
		return claim, projectionrecord.EnvironmentComposeProjection{}, errs.New(
			errs.KindInternal,
			"Environment desired staged projection changed after sealing",
		)
	}
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(encoded)
	if err != nil {
		return claim, projectionrecord.EnvironmentComposeProjection{}, err
	}
	return claim, projection, nil
}

func (publication StagedPublication) reserveAbandonment() (etcd.EnvironmentBlueprintStageClaim, error) {
	if publication.state == nil {
		return etcd.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindValidationFailed,
			"Environment desired staged publication is invalid",
		)
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	if publication.state.consumed {
		return etcd.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindStateConflict,
			"Environment desired staged publication was already consumed",
		)
	}
	publication.state.consumed = true
	return publication.state.claim, nil
}

func abandonKnownFailure(
	ctx context.Context,
	repository PublicationRepository,
	claim etcd.EnvironmentBlueprintStageClaim,
	cause error,
) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stagedPublicationAbandonTimeout)
	defer cancel()
	if err := repository.AbandonEnvironmentBlueprintStage(cleanupCtx, claim); err != nil {
		return err
	}
	return cause
}

// Abandon releases a sealed candidate after a known pre-publication failure.
func Abandon(
	ctx context.Context,
	repository PublicationRepository,
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
	digest, normalizedBytes, err := etcd.EnvironmentBlueprintProjectionEvidence(projection)
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
	repository PublicationRepository,
	idempotency PublicationIdempotency,
	input PublishInput,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil || repository == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Environment desired revision publication is not configured",
		)
	}
	claim, projection, err := input.Staged.consume(input.Environment.Record.ID, input.Task.ID, input.Locator)
	if err != nil {
		if claim.DescriptorID != "" {
			return idempotencyrecord.IdempotencyResponse{}, abandonKnownFailure(ctx, repository, claim, err)
		}
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if idempotency == nil {
		return idempotencyrecord.IdempotencyResponse{}, abandonKnownFailure(ctx, repository, claim, errs.New(
			errs.KindInternal, "Environment Blueprint idempotency is not configured",
		))
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: input.Task.ID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, abandonKnownFailure(
			ctx, repository, claim, errs.Wrap(errs.KindInternal, err),
		)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: input.Locator, Intent: claim.Intent, Response: response,
		TaskID: input.Task.ID, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	identity := etcd.EnvironmentDesiredRevisionIdentity{
		EnvironmentID: input.Environment.Record.ID,
		RevisionID:    input.Task.ID,
	}
	result, publicationErr := repository.PublishEnvironmentBlueprintDesiredRevision(
		ctx, input.EnvironmentPool, input.NetworkPool, input.Project, input.Environment, input.ExpectedHeadRevision,
		claim, identity, projection, input.ZoneChanges, input.ServiceChanges, input.RouteChanges,
		input.ReleaseGroupPreparation, input.ComponentPreparation, input.AttachPreparation,
		input.BackupPreparation,
		input.ScriptPublication, input.ReleasePublication, input.RequirementGate, input.Task, marker,
	)
	var resolution requestidempotency.Resolution
	if publicationErr != nil {
		if !unknownOutcome(publicationErr) {
			return idempotencyrecord.IdempotencyResponse{}, abandonBlueprintKnownFailure(
				ctx, repository, claim, input.ReleasePublication, publicationErr,
			)
		}
		resolution, err = idempotency.ResolveUnknown(ctx, input.Locator, input.Evidence, publicationErr)
	} else {
		resolution, err = idempotency.ResolveKnown(ctx, input.Evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return cloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return cloneResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment Blueprint resolution is invalid")
	}
}

func abandonBlueprintKnownFailure(
	ctx context.Context,
	repository PublicationRepository,
	claim etcd.EnvironmentBlueprintStageClaim,
	release etcd.BlueprintReleasePublication,
	cause error,
) error {
	releaseErr := release.Abandon(ctx)
	desiredErr := abandonKnownFailure(ctx, repository, claim, cause)
	if releaseErr != nil {
		return errors.Join(desiredErr, releaseErr)
	}
	return desiredErr
}

// AbandonBlueprint abandons both exact prepublication authorities after a
// known failure. It is never used for an unknown final transaction outcome.
func AbandonBlueprint(
	ctx context.Context,
	repository PublicationRepository,
	staged StagedPublication,
	release etcd.BlueprintReleasePublication,
	cause error,
) error {
	if err := release.Abandon(ctx); err != nil {
		cause = errors.Join(cause, err)
	}
	return Abandon(ctx, repository, staged, cause)
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
