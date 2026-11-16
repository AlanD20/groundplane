package runner

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const runnerCheckRoute = "/runners/{id}/checks"

type ConnectionCheckService struct {
	submitter   *acceptedaction.Submitter
	runners     *etcd.RunnerRepository
	records     *runnerchecks.Repository
	idempotency *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
}

func NewConnectionCheckService(runnerRecords *etcd.RunnerRepository,
	records *runnerchecks.Repository, evidence *etcd.IdempotencyRepository, coordinator *requestidempotency.Coordinator,
	submitter *acceptedaction.Submitter,
) (*ConnectionCheckService, error) {
	if runnerRecords == nil || records == nil || evidence == nil || coordinator == nil || submitter == nil {
		return nil, errs.New(errs.KindInternal, "Runner diagnostic admission dependencies are required")
	}
	return &ConnectionCheckService{
		runners:     runnerRecords,
		submitter:   submitter,
		records:     records,
		idempotency: evidence,
		coordinator: coordinator,
	}, nil
}

func (service *ConnectionCheckService) CheckConnections(ctx context.Context, runnerID string,
	input apiTypes.RunnerCheckRequest, key string,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil || input.Revision < 0 {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Runner check requires a valid id and an optional positive revision",
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
				requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)},
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
	current, err := service.runners.GetRunner(ctx, runnerID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	task := newRunnerAcceptedTask(
		current.Record.Desired,
		workexecution.OperatorActor,
		workexecution.ActionCheck,
		input.After,
		time.Now().UTC(),
	)
	return submitRunnerAction(ctx, service.submitter, task, "runner-check", locator, evidence, input)
}
