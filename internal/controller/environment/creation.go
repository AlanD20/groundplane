package environment

import (
	"context"
	"encoding/json"
	"errors"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"net/http"
	"net/netip"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprovision"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	environmentCreationRoute           = "/environments"
	maximumEnvironmentCreationAttempts = 3
)

type environmentCreationRepository interface {
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironmentPoolRegistry(
		context.Context,
	) (etcdstore.Versioned[networkreservations.EnvironmentPoolRegistry], error)
	ResolveResourceHost(context.Context) (workexecution.Placement, error)
	CreateEnvironmentWithWork(
		context.Context,
		string,
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[networkreservations.EnvironmentPoolRegistry],
		hierarchyrecord.EnvironmentRecord,
		[]componentrecord.Record,
		environmentprovision.Input,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type environmentCreationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type environmentCreationIdempotency interface {
	Prepare(context.Context, CreateEnvironmentInput) (environmentCreationEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		environmentCreationEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		environmentCreationEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		environmentCreationEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableEnvironmentCreationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewDurableCreationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEnvironmentCreationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Environment creation idempotency is not configured")
	}
	return &durableEnvironmentCreationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEnvironmentCreationIdempotency) Prepare(
	ctx context.Context,
	input CreateEnvironmentInput,
) (environmentCreationEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: environmentCreationRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeProject, ID: input.ProjectID},
		Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "name", Value: requestidempotency.String(input.Name)},
			requestidempotency.Field{Name: "network_pool", Value: requestidempotency.String(input.NetworkPool)},
			requestidempotency.Field{Name: "project_id", Value: requestidempotency.String(input.ProjectID)},
		)),
	})
	if err != nil {
		return environmentCreationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return environmentCreationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return environmentCreationEvidence{}, err
	}
	return environmentCreationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEnvironmentCreationIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence environmentCreationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEnvironmentCreationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence environmentCreationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEnvironmentCreationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence environmentCreationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type environmentCreationService struct {
	volumeRoot      string
	environmentPool netip.Prefix
	repository      environmentCreationRepository
	idempotency     environmentCreationIdempotency
	now             func() time.Time
}

func NewCreationService(
	volumeRoot string,
	environmentPool string,
	repository environmentCreationRepository,
	idempotency environmentCreationIdempotency,
) (*environmentCreationService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Environment creation service is not configured")
	}
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return nil, err
	}
	root, err := ipam.ParseIPv4Prefix(environmentPool)
	if err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Controller environment pool is invalid")
	}
	return &environmentCreationService{
		volumeRoot: volumeRoot, environmentPool: root,
		repository: repository, idempotency: idempotency, now: time.Now,
	}, nil
}

func (service *environmentCreationService) CreateEnvironment(
	ctx context.Context,
	input CreateEnvironmentInput,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Environment creation context is required",
		)
	}
	for attempt := 0; attempt < maximumEnvironmentCreationAttempts; attempt++ {
		response, err := service.createEnvironmentOnce(ctx, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEnvironmentCreationAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Environment creation retry bound was not enforced",
	)
}

func (service *environmentCreationService) createEnvironmentOnce(
	ctx context.Context,
	input CreateEnvironmentInput,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if err := ValidateEnvironmentCreateInput(input); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	evidence, err := service.idempotency.Prepare(ctx, input)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeProject, ScopeID: input.ProjectID,
		Method: http.MethodPost, Route: environmentCreationRoute, Key: idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Environment creation replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	project, err := service.repository.GetProject(ctx, input.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if project.Record.ID != input.ProjectID || project.Record.Kind != hierarchyrecord.ProjectKindTenant {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	poolRegistry, err := service.repository.GetEnvironmentPoolRegistry(ctx)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	taskID := ids.New(ids.KindTask)
	environmentID := ids.New(ids.KindEnvironment)
	nextPoolRegistry, networkPool, err := poolRegistry.Record.Reserve(
		service.environmentPool,
		environmentID,
		input.NetworkPool,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	poolRegistry.Record = nextPoolRegistry
	environment, err := hierarchyrecord.NewProvisioningEnvironment(
		service.volumeRoot,
		project.Record,
		environmentID,
		input.Name,
		networkPool,
		taskID,
		now,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	components, err := newInitialEnvironmentComponents(environment.ID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	placement, err := service.repository.ResolveResourceHost(ctx)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	capture := environmentprovision.Input{
		TaskID: taskID, WorkItemID: ids.New(ids.KindWorkItem), PlanID: ids.New(ids.KindPlan),
		Scope:  workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID, EnvironmentID: environment.ID},
		HostID: placement.HostID, VolumeRoot: service.volumeRoot, Environment: environment, CreatedAt: now,
	}
	if err := capture.Validate(); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: capture.TaskID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, Intent: evidence.durable, Response: response,
		TaskID: capture.TaskID, CreatedAt: now, UpdatedAt: now,
	}
	result, createErr := service.repository.CreateEnvironmentWithWork(
		ctx,
		service.volumeRoot,
		project,
		poolRegistry,
		environment,
		components,
		capture,
		marker,
	)
	if createErr != nil {
		if !isUnknownEnvironmentCreationOutcome(createErr) {
			return idempotencyrecord.IdempotencyResponse{}, createErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, createErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	if resolution.Kind != requestidempotency.ResolutionApplied {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Environment creation resolution is invalid",
		)
	}
	return cloneIdempotencyResponse(response), nil
}

func newInitialEnvironmentComponents(environmentID string) ([]componentrecord.Record, error) {
	components := make([]componentrecord.Record, 0, 2)
	for _, kind := range []core.ComponentKind{
		core.ComponentKindIngressCaddy,
		core.ComponentKindEdgeCloudflare,
	} {
		record, err := componentrecord.NewRecord(core.Component{
			ID:      ids.New(ids.KindComponent),
			Owner:   core.ComponentOwnerEnvironment,
			OwnerID: environmentID,
			Kind:    kind,
			Enabled: false,
		})
		if err != nil {
			return nil, err
		}
		components = append(components, record)
	}
	return components, nil
}

func isUnknownEnvironmentCreationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
