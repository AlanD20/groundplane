package backingchecks

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/controller/serviceobservation"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const checkRoute = "/api/v1/attaches/checks"

type Service struct {
	attaches     *etcd.AttachRepository
	facts        *attachments.FactService
	checks       *checkrecord.Repository
	idempotency  *etcd.IdempotencyRepository
	coordinator  *requestidempotency.Coordinator
	protector    *secretvalue.Protector
	images       workloadseal.Resolver
	observations serviceobservation.Channel
}

func NewService(attaches *etcd.AttachRepository, facts *attachments.FactService, checks *checkrecord.Repository,
	idempotencyRepository *etcd.IdempotencyRepository, coordinator *requestidempotency.Coordinator,
	protector *secretvalue.Protector, images workloadseal.Resolver, observations serviceobservation.Channel,
) (*Service, error) {
	if attaches == nil || facts == nil || checks == nil || idempotencyRepository == nil || coordinator == nil ||
		protector == nil || images == nil || observations == nil {
		return nil, errs.New(errs.KindInternal, "Backing check admission dependencies are required")
	}
	return &Service{attaches: attaches, facts: facts, checks: checks, idempotency: idempotencyRepository,
		coordinator: coordinator, protector: protector, images: images, observations: observations}, nil
}

func (service *Service) CheckAccess(ctx context.Context, environmentID string, input apiTypes.BackingCheckRequest,
	key string,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindAttach, input.AttachID) != nil ||
		input.Revision <= 0 || input.GrantID != "" && ids.Validate(ids.KindAttach, input.GrantID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"access check requires an Environment, Attach and current revision",
		)
	}
	intent := requestidempotency.CanonicalIntentV1{Method: http.MethodPost, Route: checkRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Query: requestidempotency.Object(
			requestidempotency.Field{Name: "environment", Value: requestidempotency.String(environmentID)},
		),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "attach_id", Value: requestidempotency.String(input.AttachID)},
			requestidempotency.Field{Name: "revision", Value: requestidempotency.Integer(input.Revision)},
			requestidempotency.Field{Name: "grant_id", Value: requestidempotency.String(input.GrantID)}))}
	version, digest, err := requestidempotency.Canonicalize(ctx, intent)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: environmentID, Method: http.MethodPost, Route: checkRoute, Key: key}
	if err := idempotency.ValidateIdempotencyLocator(locator); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	resolution, existing, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, evidence)
	if err != nil || existing {
		return resolution.Response, err
	}
	return service.admit(ctx, environmentID, input, locator, evidence)
}

func (service *Service) admit(ctx context.Context, environmentID string, input apiTypes.BackingCheckRequest,
	locator idempotency.IdempotencyLocator, evidence requestidempotency.ProtectedEvidence,
) (idempotency.IdempotencyResponse, error) {
	ctx = imagefence.WithScope(ctx)
	source, err := service.attaches.LoadBackingCheckSource(
		ctx,
		environmentID,
		input.AttachID,
		input.Revision,
		input.GrantID,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer source.Clear()
	adapter, registered := adapters.Get(source.AdapterKey())
	if !registered || adapter.Custom() {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"access checks support built-in PostgreSQL, MySQL and Valkey Attaches",
		)
	}
	images, err := service.images.ResolveWorkloadImages(
		ctx,
		source.AgentID(),
		[]*agentpb.WorkloadImageSelector{source.ImageSelector()},
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	observed, err := service.observations.ObserveServices(
		ctx,
		source.AgentID(),
		[]*agentpb.ServiceObservationTarget{source.ObservationTarget()},
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if err := source.BindHostSelection(images, observed); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	environment, project := source.Scope()
	owner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	checkOwner := checkrecord.Owner{
		TaskID:        ids.New(ids.KindTask),
		WorkItemID:    ids.New(ids.KindWorkItem),
		PlanID:        ids.New(ids.KindPlan),
		OperationID:   ids.New(ids.KindOperation),
		EnvironmentID: environmentID,
	}
	now := time.Now().UTC()
	facts := source.FactSource()
	defer facts.Clear()
	var check checkrecord.Record
	err = service.facts.ResolveStoredAccessCredentials(
		ctx,
		facts.Consumer,
		facts.Owner,
		facts.Grant,
		facts.Facts.Record,
		adapter,
		func(private backingaccess.Request) error {
			configuration, err := source.Configuration(private)
			if err != nil {
				return err
			}
			check, err = SealInput(ctx, service.protector, checkOwner, configuration, private, now)
			return err
		},
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer check.Clear()
	hash, err := checkrecord.PlanHash(check)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	task := workexecution.Task{ID: checkOwner.TaskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionCheck,
		Scope:    workexecution.Scope{TenantID: owner.TenantID, ProjectID: owner.ProjectID, EnvironmentID: owner.EnvironmentID},
		TargetID: input.AttachID, TargetName: facts.Consumer.Record.Name,
		Intent: workexecution.Input{ID: checkOwner.PlanID, SHA256: hash}, ExpectedItems: 1,
		CreatedAt: now, UpdatedAt: now, Deadline: now.Add(60 * time.Second)}
	return service.publish(ctx, source, check, task, locator, evidence)
}
