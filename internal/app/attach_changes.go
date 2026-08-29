package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *attachMutationService) ListAttaches(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.AttachRecord], error) {
	if ctx == nil {
		return etcd.Page[etcd.AttachRecord]{}, errs.New(errs.KindInternal, "Attach list context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.Page[etcd.AttachRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Attach list requires a stable Environment id",
		)
	}
	if request.Limit < 0 {
		return etcd.Page[etcd.AttachRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Attach list limit must be a positive integer",
		)
	}
	if _, err := service.repository.GetEnvironment(ctx, environmentID); err != nil {
		return etcd.Page[etcd.AttachRecord]{}, err
	}
	return service.repository.ListAttaches(ctx, environmentID, request)
}

func (service *attachMutationService) RenameAttach(
	ctx context.Context,
	attachID string,
	request apiTypes.AttachRenameRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach rename context is required")
	}
	if ids.Validate(ids.KindAttach, attachID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Attach id is invalid")
	}
	if err := etcd.ValidateAttachName(request.Name); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumAttachMutationTries; attempt++ {
		response, err := service.renameAttachOnce(ctx, attachID, request, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumAttachMutationTries-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach rename retry bound was not enforced")
}

func (service *attachMutationService) renameAttachOnce(
	ctx context.Context,
	attachID string,
	request apiTypes.AttachRenameRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetAttach, ID: attachID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodPost, attachRenameRoute, idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayAttachRename(ctx, locator, attachID, request)
	}
	current, err := service.repository.GetAttach(ctx, attachID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	locator = etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: current.Record.EnvironmentID,
		Method: http.MethodPost, Route: attachRenameRoute, Key: idempotencyKey,
	}
	evidence, err := service.idempotency.PrepareRename(ctx, current.Record.EnvironmentID, attachID, request)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach rename replay resolution is invalid")
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	replacement := current.Record
	replacement.Name = request.Name
	responseBody, err := json.Marshal(attachAPI(replacement))
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, service.now().UTC())
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	marker.ReplayTarget = &target
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.RenameAttachIdempotent(
		ctx, environment, project, current, request.Name, marker,
	)
	if mutationErr != nil {
		if !isUnknownAttachChangeOutcome(mutationErr) {
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case idempotentintent.ResolutionApplied:
		return cloneIdempotencyResponse(response), nil
	case idempotentintent.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach rename resolution is invalid")
	}
}

func (service *attachMutationService) replayAttachRename(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	attachID string,
	request apiTypes.AttachRenameRequest,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.PrepareRename(ctx, locator.ScopeID, attachID, request)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != idempotentintent.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach rename replay index is inconsistent")
	}
	return cloneIdempotencyResponse(resolution.Response), nil
}

func attachAPI(record etcd.AttachRecord) apiTypes.Attach {
	return apiTypes.Attach{
		ID: record.ID, Name: record.Name,
		ServiceID: record.ServiceID, Credential: attachAPICredential(record),
		BackingServiceID:     record.BackingServiceID,
		BackingProjectID:     record.BackingProjectID,
		BackingEnvironmentID: record.BackingEnvironmentID, BackingNetworkID: record.BackingNetworkID,
		GrantAttachIDs: append([]string(nil), record.GrantAttachIDs...),
		FactSets:       attachAPIFactSets(record.FactSets), Status: string(record.Status),
	}
}

func attachAPICredential(record etcd.AttachRecord) apiTypes.AttachCredential {
	if record.OwnsCredential() {
		return apiTypes.AttachCredential{Mode: apiTypes.AttachCredentialNew}
	}
	return apiTypes.AttachCredential{
		Mode: apiTypes.AttachCredentialExisting, AttachID: record.CredentialAttachID,
	}
}

func attachAPIFactSets(factSets []etcd.AttachFactSetMetadata) []apiTypes.AttachFactSet {
	response := make([]apiTypes.AttachFactSet, len(factSets))
	for setIndex, set := range factSets {
		facts := make([]apiTypes.AttachFact, len(set.Facts))
		for factIndex, fact := range set.Facts {
			facts[factIndex] = apiTypes.AttachFact{Key: fact.Key, Secret: fact.Secret}
		}
		response[setIndex] = apiTypes.AttachFactSet{GrantAttachID: set.GrantAttachID, Facts: facts}
	}
	return response
}

func isUnknownAttachChangeOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
