package runner

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnersoftware"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
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
	submitter *acceptedaction.Submitter
	catalog   ReleaseCatalog

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
	submitter *acceptedaction.Submitter,
) (*SoftwareService, error) {
	if runnerRecords == nil || records == nil || evidence == nil || coordinator == nil || catalog == nil ||
		submitter == nil {
		return nil, errs.New(errs.KindInternal, "Runner software update admission dependencies are required")
	}
	return &SoftwareService{catalog: catalog, submitter: submitter,
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
	return service.update(ctx, runnerID, input, key, false)
}

func (service *SoftwareService) update(
	ctx context.Context,
	runnerID string,
	input apiTypes.RunnerUpdateRequest,
	key string,
	automatic bool,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil || input.Revision < 0 ||
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
			"Runner update replay marker is unavailable",
		)
	}
	current, err := service.runners.GetRunner(ctx, runnerID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	actor := workexecution.OperatorActor
	if automatic {
		actor = workexecution.SystemActor
	}
	task := newRunnerAcceptedTask(
		current.Record.Desired,
		actor,
		workexecution.ActionUpdate,
		input.After,
		time.Now().UTC(),
	)
	return submitRunnerAction(
		ctx,
		service.submitter,
		task,
		"runner-software-update",
		locator,
		evidence,
		acceptedRunnerUpdate{Request: input, Automatic: automatic},
	)
}
