package taskalerts

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	markerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	records "github.com/AlanD20/groundplane/internal/infra/etcd/taskalerts"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
	"time"
)

func (service *Service) Save(
	ctx context.Context,
	scope, key string,
	input apiTypes.TaskAlertSettingsWrite,
) (markerrecord.IdempotencyResponse, error) {
	webhook := requestidempotency.Null()
	if input.WebhookURL != nil {
		webhook = requestidempotency.String(*input.WebhookURL)
	}
	body := requestidempotency.JSONBody(requestidempotency.Object(
		requestidempotency.Field{Name: "expected_revision", Value: requestidempotency.String(input.ExpectedRevision)},
		requestidempotency.Field{Name: "enabled", Value: requestidempotency.Bool(input.Enabled)},
		requestidempotency.Field{Name: "webhook_url", Value: webhook},
		requestidempotency.Field{Name: "console_url", Value: requestidempotency.String(input.ConsoleURL)}))
	return service.mutate(
		ctx,
		scope,
		key,
		"/task-alerts",
		http.MethodPut,
		body,
		func() (records.Change, []byte, error) {
			change, result, err := service.prepareConfig(ctx, scope, input)
			if err != nil {
				return change, nil, err
			}
			raw, err := json.Marshal(result)
			return change, raw, err
		},
	)
}

func (service *Service) Test(
	ctx context.Context,
	scope, key string,
	input apiTypes.TaskAlertTestRequest,
) (markerrecord.IdempotencyResponse, error) {
	body := requestidempotency.JSONBody(
		requestidempotency.Object(
			requestidempotency.Field{
				Name:  "expected_revision",
				Value: requestidempotency.String(input.ExpectedRevision),
			},
		),
	)
	return service.mutate(
		ctx,
		scope,
		key,
		"/task-alerts/test",
		http.MethodPost,
		body,
		func() (records.Change, []byte, error) {
			current, err := service.repository.Read(ctx, scope)
			if err != nil {
				return records.Change{}, nil, err
			}
			if current.Config != nil {
				defer clear(current.Config.Webhook)
			}
			if current.Config == nil || !current.Config.Enabled ||
				document(current).Revision != input.ExpectedRevision {
				return records.Change{}, nil, errs.New(
					errs.KindStateConflict,
					"Enable and save the current Discord alert settings before sending a test",
				)
			}
			event := records.Event{
				ID:         ids.New(ids.KindOperation),
				Scope:      scope,
				Status:     "test",
				Action:     "check",
				At:         time.Now().UTC(),
				Generation: current.Config.Generation,
			}
			change, err := service.repository.PrepareTest(ctx, scope, current, event)
			if err != nil {
				return change, nil, err
			}
			raw, err := json.Marshal(apiTypes.TaskAlertTestAccepted{Queued: true, NotificationID: event.ID})
			return change, raw, err
		},
	)
}
func (service *Service) mutate(ctx context.Context, scope, key, route, method string, body requestidempotency.Body,
	prepare func() (records.Change, []byte, error)) (markerrecord.IdempotencyResponse, error) {
	if !records.ValidScope(scope) {
		return markerrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Select exactly one Platform or Environment scope",
		)
	}
	owner := requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: scope}
	locator := markerrecord.IdempotencyLocator{
		ScopeKind: markerrecord.IdempotencyScopeEnvironment,
		ScopeID:   scope,
		Method:    method,
		Route:     route,
		Key:       key,
	}
	if scope == "platform" {
		owner = requestidempotency.Scope{Kind: requestidempotency.ScopePlatform}
		locator.ScopeKind, locator.ScopeID = markerrecord.IdempotencyScopePlatform, "-"
	}
	intent := requestidempotency.CanonicalIntentV1{
		Method: method,
		Route:  route,
		Scope:  owner,
		Query:  requestidempotency.Object(),
		Body:   body,
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, intent)
	if err != nil {
		return markerrecord.IdempotencyResponse{}, err
	}
	evidence, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return markerrecord.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	if replay, found, err := service.coordinator.ResolveExisting(ctx, service.replay, locator, evidence); err != nil ||
		found {
		return replay.Response, err
	}
	change, raw, err := prepare()
	if err != nil {
		return markerrecord.IdempotencyResponse{}, err
	}
	defer clear(raw)
	defer keyvalue.ClearMutationValues(change.Mutations)
	response := markerrecord.IdempotencyResponse{Status: http.StatusOK, ContentKind: "application/json", Body: raw}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return markerrecord.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	marker, err := markerrecord.NewCompletedDirectIdempotencyMarker(locator, durable, response, time.Now().UTC())
	if err != nil {
		return markerrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	plan, err := etcd.NewIdempotencyMutationPlan(
		change.Conditions,
		change.Mutations,
		func(int64, []*keyvalue.KeyValue) error {
			return errs.New(errs.KindStateConflict, "Alert settings or their scope changed; refresh and retry")
		},
	)
	if err != nil {
		return markerrecord.IdempotencyResponse{}, err
	}
	result, err := service.replay.Apply(ctx, marker, plan)
	if err != nil {
		resolved, resolveErr := service.coordinator.ResolveUnknown(ctx, service.replay, locator, evidence, err)
		return resolved.Response, resolveErr
	}
	resolved, err := service.coordinator.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return markerrecord.IdempotencyResponse{}, err
	}
	if resolved.Kind == requestidempotency.ResolutionApplied {
		return requestidempotency.CloneResponse(response), nil
	}
	return resolved.Response, nil
}
