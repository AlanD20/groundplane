package runner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerconnectioncheck"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/runnerjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const runnerCheckRoute = "/runners/{id}/checks"

type ConnectionCheckService struct {
	runners     *etcd.RunnerRepository
	plans       *runnerjournal.Journal
	records     *runnerchecks.Repository
	idempotency *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
}

func NewConnectionCheckService(runnerRecords *etcd.RunnerRepository, plans *runnerjournal.Journal,
	records *runnerchecks.Repository, evidence *etcd.IdempotencyRepository, coordinator *requestidempotency.Coordinator,
) (*ConnectionCheckService, error) {
	if runnerRecords == nil || plans == nil || records == nil || evidence == nil || coordinator == nil {
		return nil, errs.New(errs.KindInternal, "Runner diagnostic admission dependencies are required")
	}
	return &ConnectionCheckService{
		runners:     runnerRecords,
		plans:       plans,
		records:     records,
		idempotency: evidence,
		coordinator: coordinator,
	}, nil
}

func (service *ConnectionCheckService) CheckConnections(ctx context.Context, runnerID string,
	input apiTypes.RunnerCheckRequest, key string,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil || input.Revision <= 0 {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Runner check requires its current id and revision",
		)
	}
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetRunner, ID: runnerID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx,
		target,
		http.MethodPost,
		runnerCheckRoute,
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
		locator = runnerProvisioningLocator(current.Record.Desired, http.MethodPost, runnerCheckRoute, key)
	}
	if err := idempotency.ValidateIdempotencyLocator(locator); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: runnerCheckRoute, Scope: runnerIntentScope(locator),
		Path: []requestidempotency.PathBinding{{Name: "id", Value: runnerID}}, Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(
			requestidempotency.Object(
				requestidempotency.Field{Name: "revision", Value: requestidempotency.Integer(input.Revision)},
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
			"Runner check replay marker is unavailable",
		)
	}
	source, err := service.runners.LoadRunnerCheckSource(ctx, runnerID, input.Revision)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	runtime := source.Runner()
	plan, err := service.plans.ReadyPlan(ctx, runnerID, runtime.RuntimeEpoch, runtime.CreateTaskID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if err := corerunner.ValidatePlan(plan); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	now := time.Now().UTC()
	owner := runnerchecks.Owner{
		TaskID:      ids.New(ids.KindTask),
		PlanID:      ids.New(ids.KindPlan),
		OperationID: ids.New(ids.KindOperation),
	}
	request := runnerconnectioncheck.Request{
		RequestID:          ids.NewULID(),
		RunnerID:           runnerID,
		RuntimeEpoch:       runtime.RuntimeEpoch,
		PlanSHA256:         plan.Digest(),
		ControllerEndpoint: plan.Egress.ControllerEndpoint.String(),
		RunnerHome:         plan.Paths.RunnerHome,
	}
	record, err := runnerchecks.NewRecord(owner, source.Configuration(), plan, request, now)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	hash, err := runnerchecks.PlanHash(record)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	taskOwner, err := runners.TaskOwner(runtime.Desired)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{ID: owner.TaskID, OperationID: owner.OperationID, PlanID: owner.PlanID, PlanHash: hash,
		Owner: taskOwner, Actor: taskjournal.TaskActorOperator, Executor: taskjournal.TaskExecutorController,
		RenderGeneration: 1, Type: taskjournal.TaskCheck, Target: runnerID, TargetName: runtime.Desired.Slug, IdempotencyKey: key,
		Params: map[string]string{taskjournal.TaskResourceKindParam: runners.TaskResourceRunner,
			taskjournal.TaskRunnerCheckInputParam: record.InputSHA256}, TimeoutSeconds: 60,
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now}
	return service.publish(ctx, source, record, task, locator, evidence)
}

func (service *ConnectionCheckService) publish(
	ctx context.Context,
	source etcd.RunnerCheckSource,
	record runnerchecks.Record,
	task etcd.TaskRecord,
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
		ID:   task.Target,
	}
	result, err := service.runners.CreateRunnerCheck(ctx, source, record, task, marker)
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
