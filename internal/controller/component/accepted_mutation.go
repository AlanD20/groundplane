package component

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

type acceptedComponentMutation struct {
	ComponentID string
	Action      string
	Config      *api.ComponentConfig
	Locator     idempotency.IdempotencyLocator
	Intent      idempotency.ProtectedIntentRecord
}

func (service *MutationService) submitComponent(ctx context.Context, record components.Record, action string,
	config *api.ComponentConfig, after []string, key string,
) (idempotency.IdempotencyResponse, error) {
	if record.Desired.Owner != core.ComponentOwnerEnvironment ||
		!record.Desired.Kind.IsEnvironmentProxy() && record.Desired.Kind != core.ComponentKindEdgeCloudflare {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Component action is unavailable for this owner or implementation",
		)
	}
	scope, name, executionTimeout, err := service.applier.TaskAcceptanceIdentity(ctx, record.Desired.OwnerID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	value, err := componentMutationConfigValue(config)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	route := "/components/{id}/" + action
	method := http.MethodPost
	if action == "configure" {
		route = "/components/{id}/config"
		method = http.MethodPut
	}
	locator := idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: scope.EnvironmentID, Method: method, Route: route, Key: key}
	intent, err := service.acceptedActions.Begin(ctx, requestidempotency.CanonicalIntentV1{
		Method: method, Route: route, Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: scope.EnvironmentID},
		Path: []requestidempotency.PathBinding{
			{Name: "id", Value: record.Desired.ID},
		}, Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{
				Name:  "config",
				Value: value,
			},
			requestidempotency.Field{Name: "after", Value: componentStringList(after)},
		)),
	}, locator)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer intent.Destroy()
	if replay, found := intent.Replay(); found {
		return replay, nil
	}
	at := time.Now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionApply,
		Scope:      scope,
		TargetID:   scope.EnvironmentID,
		TargetName: name,
		After:      append([]string(nil), after...),
		CreatedAt:  at,
		UpdatedAt:  at,
		Deadline:   at.Add(executionTimeout),
	}
	input := acceptedComponentMutation{
		ComponentID: record.Desired.ID,
		Action:      action,
		Config:      config,
		Locator:     locator,
		Intent:      intent.Record(),
	}
	defer clear(input.Intent.Ciphertext)
	submission := intent.Submission(task, "component-mutation")
	if config != nil && config.CloudflareTunnel != nil {
		submission.SecretIDs = []string{config.CloudflareTunnel.SecretID}
	}
	return acceptedaction.Submit(ctx, service.acceptedActions, submission, input)
}

func (service *MutationService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{acceptedaction.Registration(service.acceptedActions, "component-mutation",
		workexecution.ActionApply, ids.KindEnvironment, service.prepareAcceptedComponent)}
}

func (service *MutationService) prepareAcceptedComponent(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	_ int64,
	_ time.Time,
	input acceptedComponentMutation,
) (taskdependencies.Prepared, error) {
	defer clear(input.Intent.Ciphertext)
	current, err := service.components.GetComponent(ctx, input.ComponentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if current.Record.Desired.Owner != core.ComponentOwnerEnvironment ||
		current.Record.Desired.OwnerID != task.Task.Scope.EnvironmentID {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Component no longer belongs to the queued Environment",
		)
	}
	document, err := service.applier.GetBlueprint(ctx, task.Task.Scope.EnvironmentID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if document.EnvironmentID != task.Task.Scope.EnvironmentID || document.Revision == "" {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Component Blueprint authoring identity is unavailable",
		)
	}
	bundle := core.BlueprintBundle{RootPath: "groundplane.yaml", ComposeSources: []string{"groundplane.yaml"},
		Files: []core.BlueprintFile{{Path: "groundplane.yaml", Content: []byte(document.Document)}}}
	err = mutateComponentBlueprint(&bundle, current.Record.Desired.Kind, func(spec *yaml.Node) error {
		if input.Config != nil {
			if err := applyEnvironmentComponentConfig(spec, *input.Config); err != nil {
				return err
			}
		}
		switch input.Action {
		case "enable", "disable":
			return replaceYAMLMappingValue(
				spec,
				"enabled",
				scalarNode("!!bool", strconv.FormatBool(input.Action == "enable")),
			)
		case "update", "configure":
			return nil
		default:
			return errs.New(errs.KindValidationFailed, "Component mutation action is invalid")
		}
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return service.applier.PrepareAcceptedComponentBlueprint(
		ctx,
		task,
		bundle,
		document.Revision,
		input.Locator,
		input.Intent,
	)
}

func componentStringList(values []string) requestidempotency.Value {
	items := make([]requestidempotency.Value, len(values))
	for index, value := range values {
		items[index] = requestidempotency.String(value)
	}
	return requestidempotency.List(items...)
}

func componentMutationConfigValue(config *api.ComponentConfig) (requestidempotency.Value, error) {
	if config == nil {
		return requestidempotency.Null(), nil
	}
	if err := config.Validate(); err != nil {
		return requestidempotency.Value{}, errs.New(errs.KindValidationFailed, err.Error())
	}
	field := func(name string, value requestidempotency.Value) requestidempotency.Field {
		return requestidempotency.Field{Name: name, Value: value}
	}
	switch {
	case config.Caddy != nil:
		return requestidempotency.Object(
			field("implementation", requestidempotency.String("caddy")),
			field(
				"zone_ids",
				componentStringList(config.Caddy.ZoneIDs),
			),
			field("alias", requestidempotency.String(config.Caddy.Alias)),
			field("template", requestidempotency.String(config.Caddy.CaddyfileTemplate)),
		), nil
	case config.Envoy != nil:
		return requestidempotency.Object(
			field("implementation", requestidempotency.String("envoy")),
			field(
				"zone_ids",
				componentStringList(config.Envoy.ZoneIDs),
			),
			field("alias", requestidempotency.String(config.Envoy.Alias)),
		), nil
	case config.CloudflareTunnel != nil:
		return requestidempotency.Object(
			field("implementation", requestidempotency.String("cloudflare-tunnel")),
			field(
				"zone_ids",
				componentStringList(config.CloudflareTunnel.ZoneIDs),
			),
			field("secret_id", requestidempotency.String(config.CloudflareTunnel.SecretID)),
		), nil
	default:
		return requestidempotency.Value{}, errs.New(
			errs.KindValidationFailed,
			"Environment Component configuration is unsupported",
		)
	}
}
