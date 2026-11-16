package secrets

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secretremoval"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"net/http"
	"slices"
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
	PrepareSecretRemovalAuthority(context.Context, secretrecord.Owner,
		etcdstore.Versioned[secretrecord.Record], secretremoval.Input, workexecution.Task,
	) (workqueue.Change, error)
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
	Prepare(context.Context, idempotencyrecord.IdempotencyLocator, string, []string) (secretDeletionEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		secretDeletionEvidence,
	) (requestidempotency.Resolution, bool, error)
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
	after []string,
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
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(requestidempotency.Field{Name: "after", Value: secretDeletionAfter(after)})),
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

type secretDeletionRequest struct {
	SecretID string   `json:"secret_id"`
	After    []string `json:"after,omitempty"`
}

type secretDeletionAcceptance interface {
	SubmitSecretDeletion(
		context.Context,
		acceptedaction.Submission,
		secretDeletionRequest,
	) (idempotencyrecord.IdempotencyResponse, error)
}
type sharedSecretDeletionAcceptance struct{ submitter *acceptedaction.Submitter }

func (acceptance sharedSecretDeletionAcceptance) SubmitSecretDeletion(
	ctx context.Context,
	submission acceptedaction.Submission,
	request secretDeletionRequest,
) (idempotencyrecord.IdempotencyResponse, error) {
	return acceptedaction.Submit(ctx, acceptance.submitter, submission, request)
}

type secretDeletionService struct {
	repository      secretDeletionRepository
	idempotency     secretDeletionIdempotency
	acceptedActions *acceptedaction.Submitter
	acceptance      secretDeletionAcceptance
	now             func() time.Time
}

func NewDeletionService(
	repository secretDeletionRepository,
	idempotency secretDeletionIdempotency,
	accepted *acceptedaction.Submitter,
) (*secretDeletionService, error) {
	if repository == nil || idempotency == nil || accepted == nil {
		return nil, errs.New(errs.KindInternal, "Secret deletion service is not configured")
	}
	return &secretDeletionService{
		repository:      repository,
		idempotency:     idempotency,
		acceptedActions: accepted,
		acceptance:      sharedSecretDeletionAcceptance{accepted},
		now:             time.Now,
	}, nil
}

func (service *secretDeletionService) DeleteSecret(
	ctx context.Context,
	secretID string,
	request apiTypes.TaskRunAfterRequest,
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
		response, err := service.deleteSecretOnce(ctx, secretID, request, idempotencyKey)
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
	request apiTypes.TaskRunAfterRequest,
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
		evidence, err := service.idempotency.Prepare(ctx, locator, secretID, request.After)
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
	evidence, err := service.idempotency.Prepare(ctx, locator, secretID, request.After)
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

	at := service.now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Action:     workexecution.ActionRemove,
		Actor:      workexecution.OperatorActor,
		Scope:      workexecution.Scope{},
		TargetID:   secretID,
		TargetName: current.Record.Secret.Key,
		After:      append([]string(nil), request.After...),
		CreatedAt:  at,
		UpdatedAt:  at,
		Deadline:   at.Add(acceptedaction.QueueTimeout),
	}
	if owner.Project != nil {
		task.Scope = workexecution.Scope{TenantID: owner.Project.Record.TenantID, ProjectID: owner.Project.Record.ID}
	}
	return service.acceptance.SubmitSecretDeletion(
		ctx,
		acceptedaction.Submission{Task: task, Kind: "secret-delete", Locator: locator,
			Intent: evidence.durable, Evidence: evidence.candidate, ReplayTarget: &target},
		secretDeletionRequest{SecretID: secretID, After: append([]string(nil), request.After...)},
	)
}

func secretDeletionAfter(after []string) requestidempotency.Value {
	values := make([]requestidempotency.Value, len(after))
	for index, id := range after {
		values[index] = requestidempotency.String(id)
	}
	return requestidempotency.List(values...)
}

func (service *secretDeletionService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.acceptedActions,
			"secret-delete",
			workexecution.ActionRemove,
			ids.KindSecret,
			service.prepareAcceptedDeletion,
		),
	}
}

func (service *secretDeletionService) prepareAcceptedDeletion(
	ctx context.Context,
	snapshot workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	request secretDeletionRequest,
) (taskdependencies.Prepared, error) {
	task := snapshot.Task
	if request.SecretID != task.TargetID || !slices.Equal(request.After, task.After) {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindInternal,
			"Secret deletion accepted identity is inconsistent",
		)
	}
	current, err := service.repository.GetSecret(ctx, request.SecretID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	owner := secretrecord.PlatformOwner()
	scope := workexecution.Scope{}
	if current.Record.Secret.Scope == core.SecretScopeProject {
		project, err := service.repository.GetProject(ctx, current.Record.Secret.ProjectID)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		owner = secretrecord.ProjectOwner(project)
		scope = workexecution.Scope{TenantID: project.Record.TenantID, ProjectID: project.Record.ID}
	}
	if scope != task.Scope || current.Record.Secret.Key != task.TargetName {
		return taskdependencies.Prepared{}, errs.New(errs.KindStateConflict, "Secret deletion ownership changed")
	}
	input, err := service.repository.CaptureSecretRemoval(
		ctx,
		owner,
		current,
		task.ID,
		ids.New(ids.KindWorkItem),
		ids.New(ids.KindPlan),
		at,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	reference, err := input.Reference()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent = reference
	task.ExpectedItems = 1
	task.UpdatedAt = at
	authority, err := service.repository.PrepareSecretRemovalAuthority(ctx, owner, current, input, task)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	item := workexecution.Item{
		ID:        input.WorkItemID,
		TaskID:    task.ID,
		Operation: workexecution.SecretRemoval,
		TargetID:  request.SecretID,
		Placement: workexecution.Placement{
			Executor: workexecution.Controller,
		},
		Claims:    []workexecution.Claim{{ScopeID: request.SecretID}},
		Input:     &reference,
		Phase:     workexecution.Ready,
		Cleanup:   workexecution.CleanupNone,
		CreatedAt: at,
		UpdatedAt: at,
		Deadline:  acceptedaction.ExecutionDeadline(task, at, time.Duration(secretDeletionTimeoutSeconds)*time.Second),
	}
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, nil
}
