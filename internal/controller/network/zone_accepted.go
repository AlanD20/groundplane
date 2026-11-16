package network

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type acceptedZoneRemoval struct {
	ZoneID      string                            `json:"zone_id"`
	ImpactToken string                            `json:"impact_token,omitempty"`
	Locator     idempotency.IdempotencyLocator    `json:"locator"`
	Intent      idempotency.ProtectedIntentRecord `json:"intent"`
}

func (service *zoneDeletionService) AcceptedPreparations() []taskdependencies.Registration {
	registration := acceptedaction.Registration(
		service.accepted,
		"zone-remove",
		workexecution.ActionRemove,
		ids.KindNetwork,
		service.compileAcceptedZoneRemoval,
	)
	registration.Cleanup = service.CleanupAcceptedPreparation
	return []taskdependencies.Registration{registration}
}

func (service *zoneCreationService) RemoveZone(
	ctx context.Context,
	id, key string,
	after []string,
) (idempotency.IdempotencyResponse, error) {
	return service.deletions.RemoveZoneWithImpact(ctx, id, key, "", after)
}

func (service *zoneCreationService) RemoveZoneWithImpact(
	ctx context.Context,
	id, key, impact string,
	after []string,
) (idempotency.IdempotencyResponse, error) {
	return service.deletions.RemoveZoneWithImpact(ctx, id, key, impact, after)
}

func (service *zoneDeletionService) RemoveZone(
	ctx context.Context,
	id, key string,
	after []string,
) (idempotency.IdempotencyResponse, error) {
	return service.RemoveZoneWithImpact(ctx, id, key, "", after)
}

func (service *zoneDeletionService) RemoveZoneWithImpact(
	ctx context.Context,
	zoneID, key, impact string,
	after []string,
) (idempotency.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindNetwork, zoneID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Zone removal requires a valid Zone",
		)
	}
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetZone, ID: zoneID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx,
		target,
		http.MethodDelete,
		zoneDeletionRoute,
		key,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayZoneDeletion(ctx, locator, target, impact, after)
	}
	zone, err := service.repository.GetZone(ctx, zoneID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, zone.Record.EnvironmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	locator = idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID:   environment.Record.ID,
		Method:    http.MethodDelete,
		Route:     zoneDeletionRoute,
		Key:       key,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, zoneID, impact, after)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone removal replay has no response")
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	at := service.now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionRemove,
		TargetID:   zoneID,
		TargetName: zone.Record.Desired.Name,
		After:      append([]string(nil), after...),
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environment.Record.ID,
		},
		CreatedAt: at,
		UpdatedAt: at,
		Deadline:  at.Add(acceptedaction.QueueTimeout),
	}
	return acceptedaction.Submit(
		ctx,
		service.accepted,
		acceptedaction.Submission{Task: task, Kind: "zone-remove", Locator: locator,
			Intent: evidence.durable, Evidence: evidence.candidate, ReplayTarget: &target},
		acceptedZoneRemoval{ZoneID: zoneID, ImpactToken: impact, Locator: locator, Intent: evidence.durable},
	)
}
