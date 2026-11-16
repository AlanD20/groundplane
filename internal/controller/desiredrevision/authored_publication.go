package desiredrevision

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
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
	DetachInputs             []attachremoval.BlueprintInput
	RetainedAttachConditions []keyvalue.Condition
	Locator                  idempotency.IdempotencyLocator
	Parent                   workexecution.Task
	Graph                    blueprintunits.CapturedGraph
	Baseline                 blueprintunits.Snapshot
	ScriptPublication        etcd.BlueprintScriptPublication
}

// PrepareAuthored compiles the desired-head transaction without publishing a
// second Task or replay marker. Whole-Task preparation promotes the original
// accepted identity together with this exact authority.
func PrepareAuthored(
	ctx context.Context,
	repository PublicationRepository,
	input PublishAuthoredInput,
) (taskdependencies.Prepared, error) {
	if ctx == nil || repository == nil {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindInternal,
			"Blueprint authored preparation is not configured",
		)
	}
	claim, desired, err := input.Staged.consumeAuthored(input.Environment.Record.ID, input.Parent.ID, input.Locator)
	if err != nil {
		if claim.DescriptorID != "" {
			err = abandonKnownFailure(ctx, repository, claim, err)
		}
		return taskdependencies.Prepared{}, err
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: input.Parent.ID})
	if err != nil {
		return taskdependencies.Prepared{}, abandonKnownFailure(
			ctx,
			repository,
			claim,
			errs.Wrap(errs.KindInternal, err),
		)
	}
	defer clear(responseBody)
	marker := idempotency.IdempotencyMarker{
		Kind:    idempotency.IdempotencyMarkerTask,
		State:   idempotency.IdempotencyMarkerPending,
		Locator: input.Locator,
		Intent:  claim.Intent,
		Response: idempotency.IdempotencyResponse{
			Status:      http.StatusAccepted,
			ContentKind: "application/json",
			Body:        responseBody,
		},
		TaskID:    input.Parent.ID,
		CreatedAt: claim.CreatedAt,
		UpdatedAt: claim.CreatedAt,
	}
	capture, err := repository.PrepareEnvironmentBlueprintAuthoredRevision(ctx, etcd.BlueprintAuthoredPublication{
		Project: input.Project, Environment: input.Environment, ExpectedHeadRevision: input.ExpectedHeadRevision,
		Claim: claim, DesiredInput: desired, OwnedIdentities: input.OwnedIdentities, ZoneReservations: input.ZoneReservations,
		PrerequisiteAuthority: input.PrerequisiteAuthority, AttachInputGenerations: input.AttachInputGenerations,
		DetachInputs: input.DetachInputs, RetainedAttachConditions: input.RetainedAttachConditions, Parent: input.Parent,
		Graph: input.Graph, Baseline: input.Baseline, Marker: marker, ScriptPublication: input.ScriptPublication})
	if err != nil {
		return taskdependencies.Prepared{}, abandonKnownFailure(ctx, repository, claim, err)
	}
	return taskdependencies.Prepared{
		Task:      input.Parent,
		Graph:     input.Graph.Items,
		Staged:    true,
		Authority: capture.Authority,
		Finalize: func(finalCtx context.Context, committed bool) error {
			var cleanupErr error
			if capture.Finalize != nil {
				cleanupErr = capture.Finalize(finalCtx, committed)
			}
			if !committed {
				err := repository.AbandonEnvironmentBlueprintStage(finalCtx, claim)
				if kind, _ := errs.KindOf(err); err != nil && kind != errs.KindStateConflict {
					cleanupErr = errs.WrapJoined(errs.KindInternal, cleanupErr, err)
				}
			}
			return cleanupErr
		},
	}, nil
}
