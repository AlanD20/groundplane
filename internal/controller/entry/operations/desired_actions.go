package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

type acceptedEntryMutation struct {
	Bindings      etcd.EntryMutationBindings        `json:"bindings"`
	Action        entryDesiredMutationAction        `json:"action"`
	EnvironmentID string                            `json:"environment_id"`
	EntryID       string                            `json:"entry_id"`
	Create        *apiTypes.EntryCreateRequest      `json:"create,omitempty"`
	Edit          *apiTypes.EntryEditRequest        `json:"edit,omitempty"`
	Locator       idempotency.IdempotencyLocator    `json:"locator"`
	Intent        idempotency.ProtectedIntentRecord `json:"intent"`
}

func (service *entryDesiredMutationService) CreateEntry(ctx context.Context, input apiTypes.EntryCreateRequest,
	key string,
) (idempotency.IdempotencyResponse, error) {
	desired, err := prepareEntryCreation(input)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	evidence, err := service.creation.Prepare(ctx, input.EnvironmentID, desired, input.After)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := entryMutationLocator(input.EnvironmentID, http.MethodPost, entryCreationRoute, key)
	if resolution, exists, err := service.creation.ResolveExisting(ctx, locator, evidence); err != nil || exists {
		return entryAcceptedReplay(resolution, exists, err)
	}
	return service.acceptEntryMutation(ctx, acceptedEntryMutation{Action: entryDesiredMutationCreate,
		EnvironmentID: input.EnvironmentID, Create: &input, Locator: locator, Intent: evidence.durable},
		desired.Key+desired.Path, input.After, evidence.candidate, desired.Source)
}

func (service *entryDesiredMutationService) EditEntry(ctx context.Context, entryID string,
	input apiTypes.EntryEditRequest, key string,
) (idempotency.IdempotencyResponse, error) {
	prepared, err := prepareEntryEditInput(input)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	prepared.After = append([]string(nil), input.After...)
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetEntry, ID: entryID}
	locator, indexed, err := service.edit.ResolveReplayLocator(ctx, target, http.MethodPatch, entryEditRoute, key)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	environmentID := locator.ScopeID
	if !indexed {
		var found bool
		environmentID, found, err = service.repository.ResolveBlueprintEntryEnvironment(ctx, entryID)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		if !found {
			return idempotency.IdempotencyResponse{}, errs.New(errs.KindEntryNotFound, "Entry was not found")
		}
		locator = entryMutationLocator(environmentID, http.MethodPatch, entryEditRoute, key)
	}
	evidence, err := service.edit.Prepare(ctx, environmentID, entryID, prepared)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	if resolution, exists, err := service.edit.ResolveExisting(ctx, locator, evidence); err != nil || exists {
		return entryAcceptedReplay(resolution, exists, err)
	}
	return service.acceptEntryMutation(ctx, acceptedEntryMutation{Action: entryDesiredMutationEdit,
		EnvironmentID: environmentID, EntryID: entryID, Edit: &input, Locator: locator, Intent: evidence.durable},
		entryID, input.After, evidence.candidate, prepared.Source)
}

func (service *entryDesiredMutationService) RemoveEntry(ctx context.Context,
	request entrycontroller.RemoveRequest,
) (entrycontroller.RemovalOutcome, error) {
	if ctx == nil || ids.Validate(ids.KindEnvEntry, request.EntryID) != nil {
		return entrycontroller.RemovalOutcome{}, errs.New(
			errs.KindValidationFailed,
			"Entry removal requires a stable Entry id",
		)
	}
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetEntry, ID: request.EntryID}
	locator, indexed, err := service.removal.ResolveReplayLocator(
		ctx,
		target,
		http.MethodDelete,
		entryEditRoute,
		request.IdempotencyKey,
	)
	if err != nil {
		return entrycontroller.RemovalOutcome{}, err
	}
	environmentID := locator.ScopeID
	if !indexed {
		var found bool
		environmentID, found, err = service.repository.ResolveBlueprintEntryEnvironment(ctx, request.EntryID)
		if err != nil {
			return entrycontroller.RemovalOutcome{}, err
		}
		if !found {
			return entrycontroller.RemovalOutcome{}, errs.New(errs.KindEntryNotFound, "Entry was not found")
		}
		locator = entryMutationLocator(environmentID, http.MethodDelete, entryEditRoute, request.IdempotencyKey)
	}
	evidence, err := service.removal.Prepare(ctx, environmentID, request.EntryID, request.After)
	if err != nil {
		return entrycontroller.RemovalOutcome{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	if resolution, exists, err := service.removal.ResolveExisting(ctx, locator, evidence); err != nil || exists {
		response, err := entryAcceptedReplay(resolution, exists, err)
		if err != nil {
			return entrycontroller.RemovalOutcome{}, err
		}
		return entryDesiredRemovalOutcome(response)
	}
	response, err := service.acceptEntryMutation(ctx, acceptedEntryMutation{Action: entryDesiredMutationRemove,
		EnvironmentID: environmentID, EntryID: request.EntryID, Locator: locator, Intent: evidence.durable},
		request.EntryID, request.After, evidence.candidate, core.EntrySource{})
	if err != nil {
		return entrycontroller.RemovalOutcome{}, err
	}
	return entryDesiredRemovalOutcome(response)
}

func entryMutationLocator(environmentID, method, route, key string) idempotency.IdempotencyLocator {
	return idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: environmentID, Method: method, Route: route, Key: key}
}

func entryAcceptedReplay(
	resolution requestidempotency.Resolution,
	exists bool,
	err error,
) (idempotency.IdempotencyResponse, error) {
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if !exists || resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry acceptance replay is unresolved")
	}
	return requestidempotency.CloneResponse(resolution.Response), nil
}

func (service *entryDesiredMutationService) acceptEntryMutation(ctx context.Context, input acceptedEntryMutation,
	name string, after []string, evidence requestidempotency.ProtectedEvidence, source core.EntrySource,
) (idempotency.IdempotencyResponse, error) {
	environment, err := service.repository.GetEnvironment(ctx, input.EnvironmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if input.Action != entryDesiredMutationRemove {
		var exposure []string
		if input.Create != nil {
			exposure = input.Create.Exposure
		} else if input.Edit != nil {
			exposure = input.Edit.Exposure
		}
		exposure, err = normalizeEntryExposure(exposure)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		bindings, err := service.work.BindEntryMutationReferences(
			ctx,
			input.EnvironmentID,
			project.Record.ID,
			source.SecretRef,
			exposure,
		)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		input.Bindings = bindings
	}
	at := service.now().UTC()
	taskID, action := ids.New(ids.KindTask), workexecution.ActionUpdate
	if input.Action == entryDesiredMutationCreate {
		action = workexecution.ActionCreate
		input.EntryID = entryStableIDFromRevision(ids.KindEnvEntry, taskID)
	}
	if input.Action == entryDesiredMutationRemove {
		action = workexecution.ActionRemove
	}
	task := workexecution.Task{ID: taskID, Actor: workexecution.OperatorActor, Action: action,
		TargetID: input.EntryID, TargetName: name, Scope: workexecution.Scope{TenantID: project.Record.TenantID,
			ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID}, After: append([]string(nil), after...),
		CreatedAt: at, UpdatedAt: at, Deadline: at.Add(acceptedaction.QueueTimeout)}
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetEntry, ID: input.EntryID}
	var secrets []string
	if source.Kind == core.SourceSecretRef {
		secrets = []string{input.Bindings.SecretID}
	}
	return acceptedaction.Submit(ctx, service.accepted, acceptedaction.Submission{Task: task, Kind: "entry-mutation",
		Locator: input.Locator, Intent: input.Intent, Evidence: evidence, ReplayTarget: &target, SecretIDs: secrets}, input)
}
