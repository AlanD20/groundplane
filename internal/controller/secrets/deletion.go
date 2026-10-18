package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secretremoval"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	secretDeletionRoute           = "/secrets/{id}"
	secretDeletionTimeoutSeconds  = int64(30)
	maximumSecretDeletionAttempts = 3
)

type secretDeletionRepository interface {
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetSecret(context.Context, string) (etcdstore.Versioned[secretrecord.Record], error)
	CaptureSecretRemoval(
		context.Context,
		secretrecord.Owner,
		etcdstore.Versioned[secretrecord.Record],
		string,
		string,
		string,
		time.Time,
	) (secretremoval.Input, error)
	PublishSecretRemoval(
		context.Context,
		secretrecord.Owner,
		etcdstore.Versioned[secretrecord.Record],
		secretremoval.Input,
		workexecution.Task,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type secretDeletionEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type secretDeletionIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		idempotencyrecord.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (idempotencyrecord.IdempotencyLocator, bool, error)
	Prepare(context.Context, idempotencyrecord.IdempotencyLocator, string) (secretDeletionEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		secretDeletionEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		secretDeletionEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		secretDeletionEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableSecretDeletionIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewDeletionIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableSecretDeletionIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Secret deletion idempotency is not configured")
	}
	return &durableSecretDeletionIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableSecretDeletionIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target idempotencyrecord.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (idempotencyrecord.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableSecretDeletionIdempotency) Prepare(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	secretID string,
) (secretDeletionEvidence, error) {
	scope := requestidempotency.Scope{Kind: requestidempotency.ScopePlatform}
	if locator.ScopeKind == idempotencyrecord.IdempotencyScopeProject {
		scope = requestidempotency.Scope{Kind: requestidempotency.ScopeProject, ID: locator.ScopeID}
	} else if locator.ScopeKind != idempotencyrecord.IdempotencyScopePlatform || locator.ScopeID != "-" {
		return secretDeletionEvidence{}, errs.New(errs.KindInternal, "Secret deletion replay scope is invalid")
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete, Route: secretDeletionRoute, Scope: scope,
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: secretID}},
		Query: requestidempotency.Object(), Body: requestidempotency.NoBody(),
	})
	if err != nil {
		return secretDeletionEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return secretDeletionEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return secretDeletionEvidence{}, err
	}
	return secretDeletionEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableSecretDeletionIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence secretDeletionEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableSecretDeletionIdempotency) ResolveKnown(
	ctx context.Context,
	evidence secretDeletionEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableSecretDeletionIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence secretDeletionEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type secretDeletionService struct {
	repository  secretDeletionRepository
	idempotency secretDeletionIdempotency
	now         func() time.Time
}

func NewDeletionService(
	repository secretDeletionRepository,
	idempotency secretDeletionIdempotency,
) (*secretDeletionService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Secret deletion service is not configured")
	}
	return &secretDeletionService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *secretDeletionService) DeleteSecret(
	ctx context.Context,
	secretID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Secret deletion context is required",
		)
	}
	if ids.Validate(ids.KindSecret, secretID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Secret id is invalid")
	}
	for attempt := 0; attempt < maximumSecretDeletionAttempts; attempt++ {
		response, err := service.deleteSecretOnce(ctx, secretID, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumSecretDeletionAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Secret deletion retry bound was not enforced",
	)
}

func (service *secretDeletionService) deleteSecretOnce(
	ctx context.Context,
	secretID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetSecret,
		ID:   secretID,
	}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, secretDeletionRoute, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		evidence, err := service.idempotency.Prepare(ctx, locator, secretID)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		defer clear(evidence.durable.Ciphertext)
		resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Secret deletion replay target is inconsistent",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}

	current, err := service.repository.GetSecret(ctx, secretID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	owner := secretrecord.PlatformOwner()
	locator = idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodDelete, Route: secretDeletionRoute, Key: idempotencyKey,
	}
	if current.Record.Secret.Scope == core.SecretScopeProject {
		project, err := service.repository.GetProject(ctx, current.Record.Secret.ProjectID)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		owner = secretrecord.ProjectOwner(project)
		locator.ScopeKind = idempotencyrecord.IdempotencyScopeProject
		locator.ScopeID = project.Record.ID
	}
	evidence, err := service.idempotency.Prepare(ctx, locator, secretID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Secret deletion replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}

	now := service.now().UTC()
	input, err := service.repository.CaptureSecretRemoval(ctx, owner, current,
		ids.New(ids.KindTask), ids.New(ids.KindWorkItem), ids.New(ids.KindPlan), now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	ref, err := input.Reference()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task := workexecution.Task{ID: input.TaskID, Action: workexecution.ActionRemove, Actor: workexecution.OperatorActor,
		Scope:    workexecution.Scope{TenantID: input.TenantID, ProjectID: input.ProjectID},
		TargetID: secretID, TargetName: current.Record.Secret.Key, Intent: ref, ExpectedItems: 1,
		CreatedAt: now, UpdatedAt: now, Deadline: now.Add(time.Duration(secretDeletionTimeoutSeconds) * time.Second)}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
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
		Locator: locator, ReplayTarget: &target, Intent: evidence.durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	result, deleteErr := service.repository.PublishSecretRemoval(
		ctx, owner, current, input, task, marker,
	)
	if deleteErr != nil {
		if !isUnknownSecretDeletionOutcome(deleteErr) {
			return idempotencyrecord.IdempotencyResponse{}, deleteErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, deleteErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Secret deletion resolution is invalid",
		)
	}
}

func isUnknownSecretDeletionOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
