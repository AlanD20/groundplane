package environment

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	environmentEditRoute               = "/environments/{id}"
	environmentRenameRoute             = "/environments/{id}/rename"
	maximumEnvironmentMutationAttempts = 3
)

type environmentChangeRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	MutateEnvironmentIdempotent(context.Context, etcd.Versioned[etcd.EnvironmentRecord],
		etcd.EnvironmentRecord, etcd.IdempotencyMarker) (etcd.IdempotencyTransactionResult, error)
	ReplaceEnvironmentPoolIdempotent(context.Context, netip.Prefix, etcd.Versioned[etcd.EnvironmentRecord],
		etcd.EnvironmentRecord, etcd.IdempotencyMarker) (etcd.IdempotencyTransactionResult, error)
}

// environmentCapacityRepository supplies the fixed revision used to seal a
// mutation response into protected idempotency evidence.
type environmentCapacityRepository interface {
	ListZoneSubnetReservationsAtRevision(context.Context, string, int64) ([]string, error)
}

type environmentChangeEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type environmentChangeIdempotency interface {
	PrepareEdit(context.Context, string, EditEnvironmentInput) (environmentChangeEvidence, error)
	PrepareRename(context.Context, string, RenameEnvironmentInput) (environmentChangeEvidence, error)
	ResolveExisting(context.Context, etcd.IdempotencyLocator,
		environmentChangeEvidence) (idempotentintent.Resolution, bool, error)
	ResolveKnown(context.Context, environmentChangeEvidence,
		etcd.IdempotencyTransactionResult) (idempotentintent.Resolution, error)
	ResolveUnknown(context.Context, etcd.IdempotencyLocator,
		environmentChangeEvidence, error) (idempotentintent.Resolution, error)
}

type durableEnvironmentChangeIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewDurableChangeIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEnvironmentChangeIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Environment change idempotency is not configured")
	}
	return &durableEnvironmentChangeIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEnvironmentChangeIdempotency) PrepareEdit(
	ctx context.Context,
	id string,
	input EditEnvironmentInput,
) (environmentChangeEvidence, error) {
	return service.prepare(ctx, http.MethodPatch, environmentEditRoute, id, idempotentintent.Object(
		idempotentintent.Field{Name: "network_pool", Value: idempotentintent.String(input.NetworkPool)},
	))
}

func (service *durableEnvironmentChangeIdempotency) PrepareRename(
	ctx context.Context,
	id string,
	input RenameEnvironmentInput,
) (environmentChangeEvidence, error) {
	return service.prepare(ctx, http.MethodPost, environmentRenameRoute, id, idempotentintent.Object(
		idempotentintent.Field{Name: "name", Value: idempotentintent.String(input.Name)},
	))
}

func (service *durableEnvironmentChangeIdempotency) prepare(
	ctx context.Context,
	method string,
	route string,
	id string,
	body idempotentintent.Value,
) (environmentChangeEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: method, Route: route,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: id},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: id}},
		Query: idempotentintent.Object(), Body: idempotentintent.JSONBody(body),
	})
	if err != nil {
		return environmentChangeEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return environmentChangeEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return environmentChangeEvidence{}, err
	}
	return environmentChangeEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEnvironmentChangeIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence environmentChangeEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEnvironmentChangeIdempotency) ResolveKnown(
	ctx context.Context,
	evidence environmentChangeEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEnvironmentChangeIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence environmentChangeEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type environmentChangeService struct {
	repository      environmentChangeRepository
	capacity        environmentCapacityRepository
	idempotency     environmentChangeIdempotency
	environmentPool netip.Prefix
	now             func() time.Time
}

func NewChangeService(
	environmentPool string,
	repository environmentChangeRepository,
	capacity environmentCapacityRepository,
	idempotency environmentChangeIdempotency,
) (*environmentChangeService, error) {
	if repository == nil || capacity == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Environment change service is not configured")
	}
	root, err := ipam.ParseIPv4Prefix(environmentPool)
	if err != nil || root.String() != environmentPool {
		return nil, errs.New(errs.KindInternal, "Environment change pool root is invalid")
	}
	return &environmentChangeService{
		repository: repository, capacity: capacity, idempotency: idempotency, environmentPool: root, now: time.Now,
	}, nil
}

func (service *environmentChangeService) EditEnvironment(
	ctx context.Context,
	id string,
	input EditEnvironmentInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if err := ValidateEnvironmentEditInput(input); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.changeEnvironment(
		ctx, id, http.MethodPatch, environmentEditRoute, idempotencyKey,
		func(ctx context.Context) (environmentChangeEvidence, error) {
			return service.idempotency.PrepareEdit(ctx, id, input)
		},
		func(current etcd.EnvironmentRecord) (etcd.EnvironmentRecord, error) {
			networkPool, err := PrepareEnvironmentEdit(current.NetworkPool, input)
			if err != nil {
				return etcd.EnvironmentRecord{}, err
			}
			replacement := current
			replacement.NetworkPool = networkPool
			return replacement, nil
		},
		func(
			ctx context.Context,
			current etcd.Versioned[etcd.EnvironmentRecord],
			replacement etcd.EnvironmentRecord,
			marker etcd.IdempotencyMarker,
		) (etcd.IdempotencyTransactionResult, error) {
			return service.repository.ReplaceEnvironmentPoolIdempotent(
				ctx, service.environmentPool, current, replacement, marker,
			)
		},
	)
}

func (service *environmentChangeService) RenameEnvironment(
	ctx context.Context,
	id string,
	input RenameEnvironmentInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if err := ValidateEnvironmentRenameInput(input); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.changeEnvironment(
		ctx, id, http.MethodPost, environmentRenameRoute, idempotencyKey,
		func(ctx context.Context) (environmentChangeEvidence, error) {
			return service.idempotency.PrepareRename(ctx, id, input)
		},
		func(current etcd.EnvironmentRecord) (etcd.EnvironmentRecord, error) {
			name, err := PrepareEnvironmentRename(current.Name, input)
			if err != nil {
				return etcd.EnvironmentRecord{}, err
			}
			replacement := current
			replacement.Name = name
			return replacement, nil
		},
		service.repository.MutateEnvironmentIdempotent,
	)
}

type environmentChangeMutation func(
	context.Context,
	etcd.Versioned[etcd.EnvironmentRecord],
	etcd.EnvironmentRecord,
	etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error)

func (service *environmentChangeService) changeEnvironment(
	ctx context.Context,
	id string,
	method string,
	route string,
	idempotencyKey string,
	prepare func(context.Context) (environmentChangeEvidence, error),
	change func(etcd.EnvironmentRecord) (etcd.EnvironmentRecord, error),
	mutate environmentChangeMutation,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment change context is required")
	}
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: id,
		Method: method, Route: route, Key: idempotencyKey,
	}
	for attempt := 0; attempt < maximumEnvironmentMutationAttempts; attempt++ {
		response, err := service.changeEnvironmentOnce(ctx, id, locator, prepare, change, mutate)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEnvironmentMutationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Environment change retry bound was not enforced",
	)
}

func (service *environmentChangeService) changeEnvironmentOnce(
	ctx context.Context,
	id string,
	locator etcd.IdempotencyLocator,
	prepare func(context.Context) (environmentChangeEvidence, error),
	change func(etcd.EnvironmentRecord) (etcd.EnvironmentRecord, error),
	mutate environmentChangeMutation,
) (etcd.IdempotencyResponse, error) {
	evidence, err := prepare(ctx)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Environment change replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	current, err := service.repository.GetEnvironment(ctx, id)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	replacement, err := change(current.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	subnets, err := service.capacity.ListZoneSubnetReservationsAtRevision(ctx, id, current.ReadRevision)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	capacity, err := ProjectNetworkCapacity(replacement.NetworkPool, subnets)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(environmentAPI(replacement, capacity))
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(
		locator, evidence.durable, response, service.now().UTC(),
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := mutate(ctx, current, replacement, marker)
	if mutationErr != nil {
		if !isUnknownEnvironmentChangeOutcome(mutationErr) {
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case idempotentintent.ResolutionApplied:
		return cloneIdempotencyResponse(response), nil
	case idempotentintent.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Environment change resolution is invalid",
		)
	}
}

func environmentAPI(
	record etcd.EnvironmentRecord,
	capacity NetworkCapacity,
) apiTypes.Environment {
	var createTaskID *string
	if record.ProvisioningState != etcd.EnvironmentProvisioningReady && record.CreateTaskID != "" {
		value := record.CreateTaskID
		createTaskID = &value
	}
	return apiTypes.Environment{
		ID: record.ID, ProjectID: record.ProjectID, Name: record.Name,
		NetworkPool: record.NetworkPool, VolumeDir: record.VolumeDir,
		ProvisioningState: apiTypes.EnvironmentProvisioningState(record.ProvisioningState),
		CreateTaskID:      createTaskID,
		DeletionTaskID:    nil,
		NetworkCapacity: apiTypes.EnvironmentNetworkCapacity{
			TotalAddresses:     capacity.TotalAddresses,
			AllocatedAddresses: capacity.AllocatedAddresses,
			AvailableAddresses: capacity.AvailableAddresses, ZoneCount: capacity.ZoneCount,
		},
	}
}

func isUnknownEnvironmentChangeOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
