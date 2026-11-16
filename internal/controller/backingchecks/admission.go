package backingchecks

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/controller/serviceobservation"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
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
	submitter    *acceptedaction.Submitter
}

func NewService(attaches *etcd.AttachRepository, facts *attachments.FactService, checks *checkrecord.Repository,
	idempotencyRepository *etcd.IdempotencyRepository, coordinator *requestidempotency.Coordinator,
	protector *secretvalue.Protector, images workloadseal.Resolver, observations serviceobservation.Channel,
	submitter *acceptedaction.Submitter,
) (*Service, error) {
	if attaches == nil || facts == nil || checks == nil || idempotencyRepository == nil || coordinator == nil ||
		protector == nil || images == nil || observations == nil || submitter == nil {
		return nil, errs.New(errs.KindInternal, "Backing check admission dependencies are required")
	}
	return &Service{attaches: attaches, facts: facts, checks: checks, idempotency: idempotencyRepository,
		coordinator: coordinator, protector: protector, images: images, observations: observations, submitter: submitter}, nil
}

func (service *Service) CheckAccess(ctx context.Context, environmentID string, input apiTypes.BackingCheckRequest,
	key string,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindAttach, input.AttachID) != nil ||
		input.Revision < 0 || input.GrantID != "" && ids.Validate(ids.KindAttach, input.GrantID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Access check requires an Environment and Attach; an exact revision is optional",
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
			requestidempotency.Field{Name: "grant_id", Value: requestidempotency.String(input.GrantID)},
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)}))}
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
	scope, err := service.attaches.ConnectionCheckScope(ctx, environmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	now := time.Now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionCheck,
		Scope:      scope,
		TargetID:   input.AttachID,
		TargetName: "Backing connection",
		After:      append([]string(nil), input.After...),
		CreatedAt:  now,
		UpdatedAt:  now,
		Deadline:   now.Add(acceptedaction.QueueTimeout),
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	return acceptedaction.Submit(
		ctx,
		service.submitter,
		acceptedaction.Submission{
			Task:     task,
			Kind:     "backing-check",
			Locator:  locator,
			Intent:   durable,
			Evidence: evidence,
		},
		input,
	)
}
