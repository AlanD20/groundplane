package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func (service *entryDesiredMutationService) CreateEntry(
	ctx context.Context,
	input apiTypes.EntryCreateRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Entry creation context is required",
		)
	}
	desired, err := prepareEntryCreation(input)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumEntryCreationAttempts; attempt++ {
		evidence, err := service.creation.Prepare(ctx, input.EnvironmentID, desired)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		request := entryDesiredMutationRequest{
			action: entryDesiredMutationCreate, environmentID: input.EnvironmentID,
			desired: desired, idempotencyKey: idempotencyKey, status: http.StatusCreated,
			locator: idempotencyrecord.IdempotencyLocator{
				ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: input.EnvironmentID,
				Method: http.MethodPost, Route: entryCreationRoute, Key: idempotencyKey,
			},
			evidence: entryDesiredEvidence{
				durable: evidence.durable,
				resolveExisting: func(ctx context.Context, locator idempotencyrecord.IdempotencyLocator) (requestidempotency.Resolution, bool, error) {
					return service.creation.ResolveExisting(ctx, locator, evidence)
				},
				matchesStaged: func(ctx context.Context, existing idempotencyrecord.ProtectedIntentRecord) (bool, error) {
					return service.creation.MatchesStaged(ctx, evidence, existing)
				},
				resolveKnown: func(ctx context.Context, result etcd.IdempotencyTransactionResult) (requestidempotency.Resolution, error) {
					return service.creation.ResolveKnown(ctx, evidence, result)
				},
				resolveUnknown: func(ctx context.Context, locator idempotencyrecord.IdempotencyLocator, original error) (requestidempotency.Resolution, error) {
					return service.creation.ResolveUnknown(ctx, locator, evidence, original)
				},
			},
		}
		response, err := service.mutateEntryOnce(ctx, request)
		clear(evidence.durable.Ciphertext)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryCreationAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Entry creation retry bound was not enforced",
	)
}

func (service *entryDesiredMutationService) EditEntry(
	ctx context.Context,
	entryID string,
	input apiTypes.EntryEditRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindEnvEntry, entryID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Entry edit requires a stable Entry id",
		)
	}
	prepared, err := prepareEntryEditInput(input)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetEntry,
		ID:   entryID,
	}
	locator, indexed, err := service.edit.ResolveReplayLocator(
		ctx,
		target,
		http.MethodPatch,
		entryEditRoute,
		idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayEntryDesiredEdit(ctx, locator, entryID, prepared)
	}
	environmentID, current, err := service.resolveCurrentEntry(ctx, entryID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	desired := current.Entry
	desired.Source = prepared.Source
	desired.Exposure = append([]string(nil), prepared.Exposure...)
	validationEntry := desired
	if validationEntry.Secret && validationEntry.Source.Kind == core.SourceLiteral {
		validationEntry.Source.Literal = ""
	}
	if err := validationEntry.Validate(); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	for attempt := 0; attempt < maximumEntryEditAttempts; attempt++ {
		evidence, err := service.edit.Prepare(ctx, environmentID, entryID, prepared)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		request := entryDesiredMutationRequest{
			action: entryDesiredMutationEdit, environmentID: environmentID, entryID: entryID,
			desired: desired, idempotencyKey: idempotencyKey, status: http.StatusOK,
			locator: idempotencyrecord.IdempotencyLocator{
				ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: environmentID,
				Method: http.MethodPatch, Route: entryEditRoute, Key: idempotencyKey,
			},
			evidence: entryDesiredEvidence{
				durable: evidence.durable,
				resolveExisting: func(ctx context.Context, locator idempotencyrecord.IdempotencyLocator) (requestidempotency.Resolution, bool, error) {
					return service.edit.ResolveExisting(ctx, locator, evidence)
				},
				matchesStaged: func(ctx context.Context, existing idempotencyrecord.ProtectedIntentRecord) (bool, error) {
					return service.edit.MatchesStaged(ctx, evidence, existing)
				},
				resolveKnown: func(ctx context.Context, result etcd.IdempotencyTransactionResult) (requestidempotency.Resolution, error) {
					return service.edit.ResolveKnown(ctx, evidence, result)
				},
				resolveUnknown: func(ctx context.Context, locator idempotencyrecord.IdempotencyLocator, original error) (requestidempotency.Resolution, error) {
					return service.edit.ResolveUnknown(ctx, locator, evidence, original)
				},
			},
		}
		response, err := service.mutateEntryOnce(ctx, request)
		clear(evidence.durable.Ciphertext)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryEditAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Entry edit retry bound was not enforced",
	)
}

func (service *entryDesiredMutationService) RemoveEntry(
	ctx context.Context,
	request entrycontroller.RemoveRequest,
) (entrycontroller.RemovalOutcome, error) {
	if ctx == nil || ids.Validate(ids.KindEnvEntry, request.EntryID) != nil {
		return entrycontroller.RemovalOutcome{}, errs.New(
			errs.KindValidationFailed, "Entry removal requires a stable Entry id",
		)
	}
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetEntry,
		ID:   request.EntryID,
	}
	locator, indexed, err := service.removal.ResolveReplayLocator(
		ctx, target, http.MethodDelete, entryEditRoute, request.IdempotencyKey,
	)
	if err != nil {
		return entrycontroller.RemovalOutcome{}, err
	}
	if indexed {
		evidence, err := service.removal.Prepare(ctx, locator.ScopeID, request.EntryID)
		if err != nil {
			return entrycontroller.RemovalOutcome{}, err
		}
		defer clear(evidence.durable.Ciphertext)
		resolution, existing, err := service.removal.ResolveExisting(ctx, locator, evidence)
		if err != nil {
			return entrycontroller.RemovalOutcome{}, err
		}
		if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
			return entrycontroller.RemovalOutcome{}, errs.New(
				errs.KindInternal,
				"Entry removal replay resolution is invalid",
			)
		}
		return entryDesiredRemovalOutcome(resolution.Response)
	}
	environmentID, current, err := service.resolveCurrentEntry(ctx, request.EntryID)
	if err != nil {
		return entrycontroller.RemovalOutcome{}, err
	}
	for attempt := 0; attempt < maximumEntryEditAttempts; attempt++ {
		evidence, err := service.removal.Prepare(ctx, environmentID, request.EntryID)
		if err != nil {
			return entrycontroller.RemovalOutcome{}, err
		}
		mutation := entryDesiredMutationRequest{
			action: entryDesiredMutationRemove, environmentID: environmentID, entryID: request.EntryID,
			desired: current.Entry, idempotencyKey: request.IdempotencyKey, status: http.StatusAccepted,
			locator: idempotencyrecord.IdempotencyLocator{
				ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: environmentID,
				Method: http.MethodDelete, Route: entryEditRoute, Key: request.IdempotencyKey,
			},
			evidence: entryDesiredEvidence{
				durable: evidence.durable,
				resolveExisting: func(ctx context.Context, locator idempotencyrecord.IdempotencyLocator) (requestidempotency.Resolution, bool, error) {
					return service.removal.ResolveExisting(ctx, locator, evidence)
				},
				matchesStaged: func(ctx context.Context, existing idempotencyrecord.ProtectedIntentRecord) (bool, error) {
					return service.removal.MatchesStaged(ctx, evidence, existing)
				},
				resolveKnown: func(ctx context.Context, result etcd.IdempotencyTransactionResult) (requestidempotency.Resolution, error) {
					return service.removal.ResolveKnown(ctx, evidence, result)
				},
				resolveUnknown: func(ctx context.Context, locator idempotencyrecord.IdempotencyLocator, original error) (requestidempotency.Resolution, error) {
					return service.removal.ResolveUnknown(ctx, locator, evidence, original)
				},
			},
		}
		response, err := service.mutateEntryOnce(ctx, mutation)
		clear(evidence.durable.Ciphertext)
		if err == nil {
			return entryDesiredRemovalOutcome(response)
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryEditAttempts-1 {
			return entrycontroller.RemovalOutcome{}, err
		}
	}
	return entrycontroller.RemovalOutcome{}, errs.New(errs.KindInternal, "Entry removal retry bound was not enforced")
}

func (service *entryDesiredMutationService) replayEntryDesiredEdit(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	entryID string,
	input entryEditInput,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.edit.Prepare(ctx, locator.ScopeID, entryID, input)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.edit.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Entry edit replay resolution is invalid",
		)
	}
	return requestidempotency.CloneResponse(resolution.Response), nil
}
