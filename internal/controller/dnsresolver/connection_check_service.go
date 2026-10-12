package dnsresolver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const dnsCheckRoute = "/components/{id}/dns-checks"

type ConnectionCheckService struct {
	planner     *ConnectionCheckPlanner
	tasks       *etcd.TaskRepository
	records     *dnschecks.Repository
	idempotency *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
}

func NewConnectionCheckService(planner *ConnectionCheckPlanner, tasks *etcd.TaskRepository,
	records *dnschecks.Repository, evidence *etcd.IdempotencyRepository, coordinator *requestidempotency.Coordinator,
) (*ConnectionCheckService, error) {
	if planner == nil || tasks == nil || records == nil || evidence == nil || coordinator == nil {
		return nil, errs.New(errs.KindInternal, "DNS check admission dependencies are required")
	}
	return &ConnectionCheckService{
		planner:     planner,
		tasks:       tasks,
		records:     records,
		idempotency: evidence,
		coordinator: coordinator,
	}, nil
}

func (service *ConnectionCheckService) CheckConnections(ctx context.Context, componentID string,
	input apiTypes.DNSCheckRequest, key string,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindComponent, componentID) != nil || input.Revision <= 0 || len(input.Queries) > 10 {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"DNS check requires CoreDNS's current revision and at most ten query names",
		)
	}
	queries, canonicalQueries, err := dnsCheckQueries(input.Queries)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: dnsCheckRoute, Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Path: []requestidempotency.PathBinding{{Name: "id", Value: componentID}}, Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "revision", Value: requestidempotency.Integer(input.Revision)},
			requestidempotency.Field{Name: "queries", Value: requestidempotency.List(canonicalQueries...)}))})
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: dnsCheckRoute, Key: key}
	if err := idempotency.ValidateIdempotencyLocator(locator); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	existing, found, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, evidence)
	if err != nil || found {
		return existing.Response, err
	}
	source, request, err := service.planner.Capture(ctx, componentID, input.Revision, queries)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	now := time.Now().UTC()
	owner := dnschecks.Owner{
		TaskID:      ids.New(ids.KindTask),
		PlanID:      ids.New(ids.KindPlan),
		OperationID: ids.New(ids.KindOperation),
	}
	record, err := dnschecks.NewRecord(owner, source.Configuration(), request, now)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	hash, err := dnschecks.PlanHash(record)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	task := etcd.TaskRecord{ID: owner.TaskID, OperationID: owner.OperationID, PlanID: owner.PlanID, PlanHash: hash,
		Owner: taskjournal.PlatformTaskOwner(), Actor: taskjournal.TaskActorOperator, Executor: taskjournal.TaskExecutorController,
		RenderGeneration: 1, Type: taskjournal.TaskCheck, Target: componentID, TargetName: "CoreDNS", IdempotencyKey: key,
		Params: map[string]string{taskjournal.TaskResourceKindParam: taskjournal.TaskResourceComponent,
			taskjournal.TaskDNSCheckInputParam: record.ConfigurationSHA256}, TimeoutSeconds: 90,
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now}
	return service.publish(ctx, source, record, task, locator, evidence)
}

func dnsCheckQueries(
	input []apiTypes.DNSCheckQuery,
) ([]*agentpb.DNSConnectionQuery, []requestidempotency.Value, error) {
	queries := make([]*agentpb.DNSConnectionQuery, 0, len(input))
	canonical := make([]requestidempotency.Value, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for _, supplied := range input {
		domain, name := dnsCheckName(supplied.Domain), dnsCheckName(supplied.Name)
		if !dnsconnectioncheck.ValidName(domain) || !dnsconnectioncheck.ValidName(name) ||
			supplied.Type != "A" && supplied.Type != "NS" {
			return nil, nil, errs.New(
				errs.KindValidationFailed,
				"DNS query requires valid domain/name and type A or NS",
			)
		}
		if _, duplicate := seen[domain]; duplicate {
			return nil, nil, errs.New(errs.KindValidationFailed, "DNS check repeats a forwarding domain")
		}
		seen[domain] = struct{}{}
		kind := agentpb.DNSQueryType_DNS_QUERY_TYPE_A
		if supplied.Type == "NS" {
			kind = agentpb.DNSQueryType_DNS_QUERY_TYPE_NS
		}
		queries = append(queries, &agentpb.DNSConnectionQuery{Domain: domain, Name: name, Type: kind})
		canonical = append(canonical, requestidempotency.Object(
			requestidempotency.Field{Name: "domain", Value: requestidempotency.String(domain)},
			requestidempotency.Field{Name: "name", Value: requestidempotency.String(name)},
			requestidempotency.Field{Name: "type", Value: requestidempotency.String(supplied.Type)}))
	}
	return queries, canonical, nil
}

func dnsCheckName(value string) string {
	value = strings.ToLower(value)
	if value != "" && !strings.HasSuffix(value, ".") {
		value += "."
	}
	return value
}

func (service *ConnectionCheckService) publish(ctx context.Context, source etcd.DNSCheckSource, record dnschecks.Record,
	task etcd.TaskRecord, locator idempotency.IdempotencyLocator, evidence requestidempotency.ProtectedEvidence,
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
	result, err := service.tasks.CreateDNSCheck(ctx, source, record, task, marker)
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
