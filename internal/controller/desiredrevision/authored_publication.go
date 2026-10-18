package desiredrevision

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// consumeAuthored transfers the exact normalized input sealed for a visible
// parent. The absence of a runtime projection is intentional: a Custom Attach
// may still need to produce facts before its dependent files can be rendered.
func (publication StagedPublication) consumeAuthored(
	environmentID, taskID string, locator idempotency.IdempotencyLocator,
) (blueprints.EnvironmentBlueprintStageClaim, projectionrecord.EnvironmentDesiredInput, error) {
	if publication.state == nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, projectionrecord.EnvironmentDesiredInput{}, errs.New(
			errs.KindValidationFailed, "Blueprint authored publication is invalid",
		)
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	if publication.state.consumed {
		return blueprints.EnvironmentBlueprintStageClaim{}, projectionrecord.EnvironmentDesiredInput{}, errs.New(
			errs.KindStateConflict, "Blueprint authored publication was already consumed",
		)
	}
	publication.state.consumed = true
	claim := publication.state.claim
	desired := publication.state.desiredInput
	if claim.EnvironmentID != environmentID || claim.TaskID != taskID ||
		claim.RevisionID != desired.RevisionID || desired.EnvironmentID != environmentID ||
		claim.Locator != locator || publication.state.locator != locator ||
		publication.state.projection.EnvironmentID != "" {
		return claim, projectionrecord.EnvironmentDesiredInput{}, errs.New(
			errs.KindValidationFailed, "Blueprint authored publication identity changed",
		)
	}
	encoded, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(desired)
	if err != nil {
		return claim, projectionrecord.EnvironmentDesiredInput{}, err
	}
	defer clear(encoded)
	if uint64(len(encoded)) != publication.state.seal.ProjectionBytes ||
		sha256.Sum256(encoded) != publication.state.seal.ProjectionSHA256 ||
		sha256.Sum256(encoded) != publication.state.seal.DependencyDigest {
		return claim, projectionrecord.EnvironmentDesiredInput{}, errs.New(
			errs.KindInternal, "Blueprint authored input changed after sealing",
		)
	}
	return claim, projectionrecord.CloneEnvironmentDesiredInput(desired), nil
}

type PublishAuthoredInput struct {
	Project                  keyvalue.Versioned[hierarchy.ProjectRecord]
	Environment              keyvalue.Versioned[hierarchy.EnvironmentRecord]
	ExpectedHeadRevision     int64
	Staged                   StagedPublication
	OwnedIdentities          projectionrecord.EnvironmentOwnedIdentities
	ZoneReservations         []zones.Record
	PrerequisiteAuthority    etcd.BlueprintPrerequisiteAuthority
	AttachInputGenerations   []etcd.BlueprintAttachInputGenerationPublication
	RetainedAttachConditions []keyvalue.Condition
	Evidence                 Evidence
	Locator                  idempotency.IdempotencyLocator
	Parent                   workexecution.Task
	Graph                    blueprintunits.CapturedGraph
	Baseline                 blueprintunits.Snapshot
	ScriptPublication        etcd.BlueprintScriptPublication
}

type AuthoredPublicationIdempotency interface {
	ResolveExisting(context.Context, idempotency.IdempotencyLocator, Evidence) (requestidempotency.Resolution, bool, error)
	ResolveUnknown(context.Context, idempotency.IdempotencyLocator, Evidence, error) (requestidempotency.Resolution, error)
}

func PublishAuthored(
	ctx context.Context,
	repository PublicationRepository,
	idempotencyCoordinator AuthoredPublicationIdempotency,
	input PublishAuthoredInput,
) (idempotency.IdempotencyResponse, error) {
	if ctx == nil || repository == nil || idempotencyCoordinator == nil {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindInternal, "Blueprint authored publication is not configured",
		)
	}
	claim, desired, err := input.Staged.consumeAuthored(
		input.Environment.Record.ID, input.Parent.ID, input.Locator,
	)
	if err != nil {
		if claim.DescriptorID != "" {
			return idempotency.IdempotencyResponse{}, abandonKnownFailure(ctx, repository, claim, err)
		}
		return idempotency.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: input.Parent.ID})
	if err != nil {
		return idempotency.IdempotencyResponse{}, abandonKnownFailure(
			ctx, repository, claim, errs.Wrap(errs.KindInternal, err),
		)
	}
	defer clear(responseBody)
	response := idempotency.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker := idempotency.IdempotencyMarker{
		Kind:    idempotency.IdempotencyMarkerTask,
		State:   idempotency.IdempotencyMarkerPending,
		Locator: input.Locator, Intent: claim.Intent, Response: response,
		TaskID: input.Parent.ID, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	result, publicationErr := repository.PublishEnvironmentBlueprintAuthoredRevision(
		ctx, etcd.BlueprintAuthoredPublication{
			Project: input.Project, Environment: input.Environment,
			ExpectedHeadRevision: input.ExpectedHeadRevision,
			Claim:                claim, DesiredInput: desired, OwnedIdentities: input.OwnedIdentities,
			ZoneReservations:         input.ZoneReservations,
			PrerequisiteAuthority:    input.PrerequisiteAuthority,
			AttachInputGenerations:   input.AttachInputGenerations,
			RetainedAttachConditions: input.RetainedAttachConditions,
			Parent:                   input.Parent, Graph: input.Graph, Baseline: input.Baseline, Marker: marker,
			ScriptPublication: input.ScriptPublication,
		},
	)
	defer keyvalue.ClearValues(result.FailureReads)
	var resolution requestidempotency.Resolution
	if publicationErr != nil {
		if !unknownOutcome(publicationErr) {
			return idempotency.IdempotencyResponse{}, abandonKnownFailure(ctx, repository, claim, publicationErr)
		}
		resolution, err = idempotencyCoordinator.ResolveUnknown(ctx, input.Locator, input.Evidence, publicationErr)
	} else if result.Succeeded {
		return cloneResponse(response), nil
	} else {
		// A failed comparison is known not to have committed. Read the request
		// marker to distinguish a concurrent replay from changed resource state;
		// do not misclassify it as an unknown transport outcome.
		var found bool
		resolution, found, err = idempotencyCoordinator.ResolveExisting(ctx, input.Locator, input.Evidence)
		if err == nil && !found {
			return idempotency.IdempotencyResponse{}, abandonKnownFailure(ctx, repository, claim,
				errs.New(errs.KindStateConflict, "Blueprint sources changed before Apply was accepted; submit the updated Blueprint"))
		}
	}
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return cloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return cloneResponse(resolution.Response), nil
	default:
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindInternal, "Blueprint authored publication resolution is invalid",
		)
	}
}
