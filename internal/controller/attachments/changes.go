package attachments

import (
	"context"
	"encoding/json"
	"errors"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *MutationService) ListAttaches(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[attachrecord.Record], error) {
	if ctx == nil {
		return etcdstore.Page[attachrecord.Record]{}, errs.New(errs.KindInternal, "Attach list context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcdstore.Page[attachrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Attach list requires a stable Environment id",
		)
	}
	if request.Limit < 0 {
		return etcdstore.Page[attachrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Attach list limit must be a positive integer",
		)
	}
	if _, err := service.repository.GetEnvironment(ctx, environmentID); err != nil {
		return etcdstore.Page[attachrecord.Record]{}, err
	}
	return service.repository.ListAttaches(ctx, environmentID, request)
}

func (service *MutationService) RenameAttach(
	ctx context.Context,
	attachID string,
	request apiTypes.AttachRenameRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach rename context is required")
	}
	if ids.Validate(ids.KindAttach, attachID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Attach id is invalid")
	}
	if err := attachrecord.ValidateAttachName(request.Name); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumAttachMutationTries; attempt++ {
		response, err := service.renameAttachOnce(ctx, attachID, request, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumAttachMutationTries-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Attach rename retry bound was not enforced",
	)
}

func (service *MutationService) renameAttachOnce(
	ctx context.Context,
	attachID string,
	request apiTypes.AttachRenameRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetAttach,
		ID:   attachID,
	}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodPost, attachRenameRoute, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayAttachRename(ctx, locator, attachID, request)
	}
	current, err := service.repository.GetAttach(ctx, attachID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	locator = idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: current.Record.EnvironmentID,
		Method: http.MethodPost, Route: attachRenameRoute, Key: idempotencyKey,
	}
	evidence, err := service.idempotency.PrepareRename(ctx, current.Record.EnvironmentID, attachID, request)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Attach rename replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	replacement := current.Record
	replacement.Name = request.Name
	responseBody, err := json.Marshal(attachAPI(replacement))
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	marker, err := idempotencyrecord.NewCompletedDirectIdempotencyMarker(
		locator,
		evidence.durable,
		response,
		service.now().UTC(),
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	marker.ReplayTarget = &target
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.RenameAttachIdempotent(
		ctx, environment, project, current, request.Name, marker,
	)
	if mutationErr != nil {
		if !isUnknownAttachChangeOutcome(mutationErr) {
			return idempotencyrecord.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Attach rename resolution is invalid",
		)
	}
}

func (service *MutationService) replayAttachRename(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	attachID string,
	request apiTypes.AttachRenameRequest,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.idempotency.PrepareRename(ctx, locator.ScopeID, attachID, request)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Attach rename replay index is inconsistent",
		)
	}
	return requestidempotency.CloneResponse(resolution.Response), nil
}

func attachAPI(record attachrecord.Record) apiTypes.Attach {
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

func attachAPICredential(record attachrecord.Record) apiTypes.AttachCredential {
	if record.OwnsCredential() {
		return apiTypes.AttachCredential{Mode: apiTypes.AttachCredentialNew}
	}
	return apiTypes.AttachCredential{
		Mode: apiTypes.AttachCredentialExisting, AttachID: record.CredentialAttachID,
	}
}

func attachAPIFactSets(factSets []attachrecord.FactSetMetadata) []apiTypes.AttachFactSet {
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
