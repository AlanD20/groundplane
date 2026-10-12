// Package environmentdns owns operator DNS edits and their activation view.
package environmentdns

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	model "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Service struct {
	repository  *etcd.EnvironmentBlueprintRepository
	replay      *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
}

func NewService(repository *etcd.EnvironmentBlueprintRepository, replay *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
) (*Service, error) {
	if repository == nil || replay == nil || coordinator == nil {
		return nil, errs.New(errs.KindInternal, "Environment DNS dependencies are required")
	}
	return &Service{repository: repository, replay: replay, coordinator: coordinator}, nil
}

func (service *Service) Get(ctx context.Context, environmentID string) (apiTypes.EnvironmentDNS, error) {
	view, err := service.repository.GetEnvironmentDNS(ctx, environmentID)
	if err != nil {
		return apiTypes.EnvironmentDNS{}, err
	}
	result := apiTypes.EnvironmentDNS{
		EnvironmentID: environmentID, Revision: view.RevisionID, ResolverEnabled: view.ResolverEnabled,
		State: view.State, TaskID: view.TaskID,
		Records:     make([]apiTypes.EnvironmentDNSRecord, 0, len(view.Records)),
		Withdrawals: make([]apiTypes.EnvironmentDNSWithdrawal, 0, len(view.Withdrawals)),
	}
	if result.Revision == "" {
		result.Revision = apiTypes.EnvironmentBlueprintInitialRevision
	}
	for _, record := range view.Records {
		result.Records = append(result.Records, apiTypes.EnvironmentDNSRecord{
			ID: record.Binding.Record.ID, Label: record.Binding.Record.Label,
			Hostname: record.Binding.Record.Hostname(), Scope: string(record.Spec.Namespace.Scope),
			Address: record.Spec.Address, Service: record.Spec.Service, Zone: record.Spec.Zone,
			BoundServiceID: record.Binding.ServiceID, ResolvedAddress: record.Binding.Address,
			State: record.State, TaskID: record.TaskID,
		})
	}
	for _, withdrawal := range view.Withdrawals {
		result.Withdrawals = append(result.Withdrawals, apiTypes.EnvironmentDNSWithdrawal{
			RecordID: withdrawal.RecordID, Hostname: withdrawal.Hostname,
		})
	}
	return result, nil
}

func (service *Service) Create(ctx context.Context, environmentID, revision string,
	input apiTypes.EnvironmentDNSWrite, key string,
) (idempotency.IdempotencyResponse, error) {
	return service.mutate(ctx, environmentID, "", revision, input, key, desiredauthoring.DNSCreate)
}

func (service *Service) Edit(ctx context.Context, environmentID, recordID, revision string,
	input apiTypes.EnvironmentDNSWrite, key string,
) (idempotency.IdempotencyResponse, error) {
	return service.mutate(ctx, environmentID, recordID, revision, input, key, desiredauthoring.DNSEdit)
}

func (service *Service) Remove(
	ctx context.Context,
	environmentID, recordID, revision, key string,
) (idempotency.IdempotencyResponse, error) {
	return service.mutate(
		ctx,
		environmentID,
		recordID,
		revision,
		apiTypes.EnvironmentDNSWrite{},
		key,
		desiredauthoring.DNSRemove,
	)
}

func (service *Service) mutate(ctx context.Context, environmentID, recordID, revision string,
	input apiTypes.EnvironmentDNSWrite, key string, action desiredauthoring.DNSAction,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		(action != desiredauthoring.DNSCreate && ids.Validate(ids.KindDNSRecord, recordID) != nil) ||
		(revision != apiTypes.EnvironmentBlueprintInitialRevision && ids.Validate(ids.KindTask, revision) != nil) {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"DNS edit requires its Environment, record and current Blueprint revision",
		)
	}
	intent := mutationIntent(environmentID, recordID, revision, input, action)
	locator := idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID:   environmentID,
		Method:    intent.Method,
		Route:     intent.Route,
		Key:       key,
	}
	if err := idempotency.ValidateIdempotencyLocator(locator); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, intent)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	evidence, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	if replay, found, err := service.coordinator.ResolveExisting(ctx, service.replay, locator, evidence); err != nil ||
		found {
		return replay.Response, err
	}
	current, err := service.repository.GetEnvironmentDNS(ctx, environmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	currentID := current.RevisionID
	if currentID == "" {
		currentID = apiTypes.EnvironmentBlueprintInitialRevision
	}
	if currentID != revision {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Environment Blueprint changed; refresh DNS records before saving",
		)
	}
	status := http.StatusOK
	if action == desiredauthoring.DNSCreate {
		recordID = ids.New(ids.KindDNSRecord)
		status = http.StatusCreated
	}
	request := desiredauthoring.DNSMutation{Action: action, RecordID: recordID,
		ExpectedHeadRevision: current.HeadRevision, Label: input.Label, Address: input.Address, Service: input.Service, Zone: input.Zone}
	if input.Scope != nil {
		scope := model.NamespaceScope(*input.Scope)
		request.Scope = &scope
	}
	response := idempotency.IdempotencyResponse{Status: http.StatusNoContent, ContentKind: "none"}
	if action != desiredauthoring.DNSRemove {
		body, err := json.Marshal(apiTypes.EnvironmentDNSSaved{EnvironmentID: environmentID, RecordID: recordID})
		if err != nil {
			return idempotency.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
		}
		defer clear(body)
		response = idempotency.IdempotencyResponse{Status: status, ContentKind: "application/json", Body: body}
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	marker, err := idempotency.NewCompletedDirectIdempotencyMarker(locator, durable, response, time.Now().UTC())
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, err := service.repository.MutateEnvironmentDNS(ctx, environmentID, request, marker)
	if err != nil {
		kind, known := errs.KindOf(err)
		if errors.Is(err, context.DeadlineExceeded) || known && kind == errs.KindStorageUnavailable {
			replay, resolveErr := service.coordinator.ResolveUnknown(ctx, service.replay, locator, evidence, err)
			return replay.Response, resolveErr
		}
		return idempotency.IdempotencyResponse{}, err
	}
	resolved, err := service.coordinator.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if resolved.Kind == requestidempotency.ResolutionApplied {
		return requestidempotency.CloneResponse(response), nil
	}
	return resolved.Response, nil
}

func mutationIntent(environmentID, recordID, revision string, input apiTypes.EnvironmentDNSWrite,
	action desiredauthoring.DNSAction,
) requestidempotency.CanonicalIntentV1 {
	method, route := http.MethodPost, "/api/v1/environments/{id}/dns-records"
	path := []requestidempotency.PathBinding{{Name: "id", Value: environmentID}}
	scope := requestidempotency.Null()
	if input.Scope != nil {
		scope = requestidempotency.String(*input.Scope)
	}
	body := requestidempotency.JSONBody(requestidempotency.Object(
		requestidempotency.Field{Name: "label", Value: requestidempotency.String(input.Label)},
		requestidempotency.Field{Name: "scope", Value: scope},
		requestidempotency.Field{Name: "address", Value: requestidempotency.String(input.Address)},
		requestidempotency.Field{Name: "service", Value: requestidempotency.String(input.Service)},
		requestidempotency.Field{Name: "zone", Value: requestidempotency.String(input.Zone)},
	))
	if action != desiredauthoring.DNSCreate {
		method, route = http.MethodPut, route+"/{record}"
		path = append(path, requestidempotency.PathBinding{Name: "record", Value: recordID})
	}
	if action == desiredauthoring.DNSRemove {
		method, body = http.MethodDelete, requestidempotency.NoBody()
	}
	return requestidempotency.CanonicalIntentV1{Method: method, Route: route, Path: path,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Query: requestidempotency.Object(
			requestidempotency.Field{Name: "if_match", Value: requestidempotency.String(revision)},
		),
		Body: body}
}
