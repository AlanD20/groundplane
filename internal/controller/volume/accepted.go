package volume

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type acceptedVolumeMutation struct {
	Action        string                            `json:"action"`
	EnvironmentID string                            `json:"environment_id"`
	VolumeID      string                            `json:"volume_id"`
	Slug          string                            `json:"slug"`
	Key           string                            `json:"key"`
	KeySupplied   bool                              `json:"key_supplied"`
	ImpactToken   string                            `json:"impact_token,omitempty"`
	ConfirmKey    string                            `json:"confirm_key,omitempty"`
	Locator       idempotency.IdempotencyLocator    `json:"locator"`
	Intent        idempotency.ProtectedIntentRecord `json:"intent"`
}

func (service *MutationService) AcceptedPreparations() []taskdependencies.Registration {
	registrations := []taskdependencies.Registration{
		acceptedaction.Registration(
			service.accepted,
			"volume-create",
			workexecution.ActionCreate,
			ids.KindVolume,
			service.compileAcceptedMutation,
		),
		acceptedaction.Registration(
			service.accepted,
			"volume-edit",
			workexecution.ActionUpdate,
			ids.KindVolume,
			service.compileAcceptedMutation,
		),
		acceptedaction.Registration(
			service.accepted,
			"volume-remove",
			workexecution.ActionRemove,
			ids.KindVolume,
			service.compileAcceptedMutation,
		),
	}
	for index := range registrations {
		registrations[index].Cleanup = service.CleanupAcceptedPreparation
	}
	return registrations
}

func (service *MutationService) acceptMutation(
	ctx context.Context,
	request volumeMutationRequest,
) (idempotency.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, request.intent)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := mutationLocator(request.intent, request.idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if existing {
		return replayResponse(resolution)
	}
	environment, err := service.repository.GetEnvironment(ctx, request.environmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	at := service.now().UTC()
	taskID := ids.New(ids.KindTask)
	action, kind := workexecution.ActionCreate, "volume-create"
	switch request.action {
	case volumeMutationActionAdd:
		request.volumeID = stableIDFromTask(ids.KindVolume, taskID)
	case volumeMutationActionEdit:
		action, kind = workexecution.ActionUpdate, "volume-edit"
	case volumeMutationActionRemove:
		action, kind = workexecution.ActionRemove, "volume-remove"
	default:
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Volume action is invalid")
	}
	task := workexecution.Task{ID: taskID, Actor: workexecution.OperatorActor, Action: action,
		TargetID: request.volumeID, TargetName: request.slug, After: append([]string(nil), request.after...),
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environment.Record.ID,
		},
		CreatedAt: at, UpdatedAt: at, Deadline: at.Add(acceptedaction.QueueTimeout)}
	payload := acceptedVolumeMutation{
		Action:        request.action,
		EnvironmentID: request.environmentID,
		VolumeID:      request.volumeID,
		Slug:          request.slug,
		Key:           request.key,
		KeySupplied:   request.keySupplied,
		ImpactToken:   request.impactToken,
		ConfirmKey:    request.confirmKey,
		Locator:       locator,
		Intent:        evidence.durable,
	}
	var target *idempotency.IdempotencyReplayTarget
	if action == workexecution.ActionRemove {
		target = &idempotency.IdempotencyReplayTarget{
			Kind: idempotency.IdempotencyReplayTargetVolume,
			ID:   request.volumeID,
		}
	}
	return acceptedaction.Submit(
		ctx,
		service.accepted,
		acceptedaction.Submission{Task: task, Kind: kind, Locator: locator,
			Intent: evidence.durable, Evidence: evidence.candidate, ReplayTarget: target},
		payload,
	)
}
