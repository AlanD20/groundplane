package dnsresolver

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
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
	submitter   *acceptedaction.Submitter
}

func NewConnectionCheckService(planner *ConnectionCheckPlanner, tasks *etcd.TaskRepository,
	records *dnschecks.Repository, evidence *etcd.IdempotencyRepository, coordinator *requestidempotency.Coordinator,
	submitter *acceptedaction.Submitter,
) (*ConnectionCheckService, error) {
	if planner == nil || tasks == nil || records == nil || evidence == nil || coordinator == nil || submitter == nil {
		return nil, errs.New(errs.KindInternal, "DNS check admission dependencies are required")
	}
	return &ConnectionCheckService{
		submitter:   submitter,
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
	if ids.Validate(ids.KindComponent, componentID) != nil || input.Revision < 0 || len(input.Queries) > 10 {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"DNS check requires CoreDNS and at most ten query names; an exact revision is optional",
		)
	}
	_, canonicalQueries, err := dnsCheckQueries(input.Queries)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: dnsCheckRoute, Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Path: []requestidempotency.PathBinding{{Name: "id", Value: componentID}}, Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "revision", Value: requestidempotency.Integer(input.Revision)},
			requestidempotency.Field{Name: "queries", Value: requestidempotency.List(canonicalQueries...)},
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)}))})
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
	now := time.Now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionCheck,
		TargetID:   componentID,
		TargetName: "CoreDNS",
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
		acceptedaction.Submission{Task: task, Kind: "dns-check", Locator: locator, Intent: durable, Evidence: evidence},
		input,
	)
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
