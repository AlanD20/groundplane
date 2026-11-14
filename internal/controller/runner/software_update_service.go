package runner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnersoftware"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerupdates"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const runnerUpdateRoute = "/runners/{id}/updates"

type ReleaseCatalog interface {
	List(context.Context) ([]runnersoftware.Release, error)
	Resolve(context.Context, string) (runnersoftware.Release, error)
}

type SoftwareService struct {
	catalog ReleaseCatalog

	runners     *etcd.RunnerRepository
	records     *runnerupdates.Repository
	idempotency *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
}

func NewSoftwareService(
	runnerRecords *etcd.RunnerRepository,
	records *runnerupdates.Repository,
	evidence *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
	catalog ReleaseCatalog,
) (*SoftwareService, error) {
	if runnerRecords == nil || records == nil || evidence == nil || coordinator == nil || catalog == nil {
		return nil, errs.New(errs.KindInternal, "Runner software update admission dependencies are required")
	}
	return &SoftwareService{catalog: catalog,
		runners:     runnerRecords,
		records:     records,
		idempotency: evidence,
		coordinator: coordinator,
	}, nil
}

func (service *SoftwareService) UpdateRunner(
	ctx context.Context,
	runnerID string,
	input apiTypes.RunnerUpdateRequest,
	key string,
) (idempotency.IdempotencyResponse, error) {
	return service.update(ctx, runnerID, input, key, false, nil)
}

func (service *SoftwareService) update(
	ctx context.Context,
	runnerID string,
	input apiTypes.RunnerUpdateRequest,
	key string,
	automatic bool,
	selected *runnersoftware.Release,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil || input.Revision <= 0 ||
		!runnersoftware.ValidVersion(input.Version) {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Runner update requires its current id and revision",
		)
	}
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetRunner, ID: runnerID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx,
		target,
		http.MethodPost,
		runnerUpdateRoute,
		key,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if !indexed {
		current, err := service.runners.GetRunner(ctx, runnerID)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		locator = runnerProvisioningLocator(current.Record.Desired, http.MethodPost, runnerUpdateRoute, key)
	}
	if err := idempotency.ValidateIdempotencyLocator(locator); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: runnerUpdateRoute, Scope: runnerIntentScope(locator),
		Path: []requestidempotency.PathBinding{{Name: "id", Value: runnerID}}, Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(
			requestidempotency.Object(
				requestidempotency.Field{Name: "revision", Value: requestidempotency.Integer(input.Revision)},
				requestidempotency.Field{Name: "version", Value: requestidempotency.String(input.Version)},
				requestidempotency.Field{Name: "automatic", Value: requestidempotency.Bool(automatic)},
			),
		),
	})
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	existing, found, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, evidence)
	if err != nil || found {
		return existing.Response, err
	}
	if indexed {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Runner update replay marker is unavailable",
		)
	}
	source, err := service.runners.LoadRunnerRuntimeSource(ctx, runnerID, input.Revision)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	runtime := source.Runner()
	if runtime.Desired.AutoUpdate != automatic {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Disable automatic updates before selecting a manual version",
		)
	}
	var release runnersoftware.Release
	if selected != nil {
		release = *selected
	} else {
		release, err = service.catalog.Resolve(ctx, input.Version)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
	}
	plan, err := service.runners.LoadAppliedRunnerPlan(ctx, runnerID, runtime.RuntimeEpoch, runtime.ContainerID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if err := corerunner.ValidatePlan(plan); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	now := time.Now().UTC()
	owner := runnerupdates.Owner{
		TaskID:      ids.New(ids.KindTask),
		WorkItemID:  ids.New(ids.KindWorkItem),
		PlanID:      ids.New(ids.KindPlan),
		OperationID: ids.New(ids.KindOperation),
	}
	request := runnersoftware.Request{Release: release,
		RequestID:          ids.NewULID(),
		RunnerID:           runnerID,
		RuntimeEpoch:       runtime.RuntimeEpoch,
		PlanSHA256:         plan.Digest(),
		ControllerEndpoint: plan.Egress.ControllerEndpoint.String(),
		RunnerHome:         plan.Paths.RunnerHome,
	}
	record, err := runnerupdates.NewRecord(owner, source.Configuration(), plan, request, now)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	hash, err := runnerupdates.PlanHash(record)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	actor := workexecution.OperatorActor
	if automatic {
		actor = workexecution.SystemActor
	}
	task := workexecution.Task{ID: owner.TaskID, Actor: actor,
		Scope: workexecution.Scope{TenantID: runtime.Desired.TenantID,
			ProjectID: runtime.Desired.ProjectID(), EnvironmentID: runtime.Desired.EnvironmentID()},
		Action: workexecution.ActionUpdate, TargetID: runnerID, TargetName: runtime.Desired.Slug,
		Intent: workexecution.Input{ID: owner.PlanID, SHA256: hash}, ExpectedItems: 1,
		CreatedAt: now, UpdatedAt: now, Deadline: now.Add(30 * time.Minute)}
	return service.publish(ctx, source, record, task, locator, evidence)
}

func (service *SoftwareService) publish(
	ctx context.Context,
	source etcd.RunnerRuntimeSource,
	record runnerupdates.Record,
	task workexecution.Task,
	locator idempotency.IdempotencyLocator,
	evidence requestidempotency.ProtectedEvidence,
) (idempotency.IdempotencyResponse, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return idempotency.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := idempotency.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        body,
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	marker := idempotency.IdempotencyMarker{
		Kind:      idempotency.IdempotencyMarkerTask,
		State:     idempotency.IdempotencyMarkerPending,
		Locator:   locator,
		Intent:    durable,
		Response:  response,
		TaskID:    task.ID,
		CreatedAt: task.CreatedAt,
		UpdatedAt: task.CreatedAt,
	}
	marker.ReplayTarget = &idempotency.IdempotencyReplayTarget{
		Kind: idempotency.IdempotencyReplayTargetRunner,
		ID:   task.TargetID,
	}
	result, err := service.runners.CreateRunnerSoftwareUpdate(ctx, source, record, task, marker)
	if err != nil {
		kind, _ := errs.KindOf(err)
		if !errors.Is(err, context.DeadlineExceeded) && kind != errs.KindStorageUnavailable {
			return idempotency.IdempotencyResponse{}, err
		}
		resolution, resolveErr := service.coordinator.ResolveUnknown(ctx, service.idempotency, locator, evidence, err)
		return resolution.Response, resolveErr
	}
	resolution, err := service.coordinator.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionApplied {
		return requestidempotency.CloneResponse(response), nil
	}
	return resolution.Response, nil
}
