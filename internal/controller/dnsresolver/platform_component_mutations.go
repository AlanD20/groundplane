package dnsresolver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"sort"
	"time"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	platformComponentConfigRoute        = "/components/{id}/config"
	platformComponentEnableRoute        = "/components/{id}/enable"
	platformComponentDisableRoute       = "/components/{id}/disable"
	platformComponentUpdateRoute        = "/components/{id}/update"
	platformComponentTaskTimeoutSeconds = int64(300)
)

type PlatformMutationService struct {
	components  *etcd.ComponentRepository
	tasks       *etcd.TaskRepository
	idempotency *etcd.IdempotencyRepository
	coordinator *idempotentintent.Coordinator
	renderer    componentdns.Renderer
	planner     *PlatformRenderPlanner
	now         func() time.Time
}

func (service *PlatformMutationService) EnablePlatformComponent(
	ctx context.Context,
	componentID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.mutatePlatformComponentLifecycle(
		ctx,
		componentID,
		idempotencyKey,
		"enable",
		platformComponentEnableRoute,
	)
}

func (service *PlatformMutationService) DisablePlatformComponent(
	ctx context.Context,
	componentID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.mutatePlatformComponentLifecycle(
		ctx,
		componentID,
		idempotencyKey,
		"disable",
		platformComponentDisableRoute,
	)
}

func (service *PlatformMutationService) UpdatePlatformComponent(
	ctx context.Context,
	componentID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.mutatePlatformComponentLifecycle(
		ctx,
		componentID,
		idempotencyKey,
		"update",
		platformComponentUpdateRoute,
	)
}

func (service *PlatformMutationService) mutatePlatformComponentLifecycle(
	ctx context.Context,
	componentID string,
	idempotencyKey string,
	action string,
	route string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "CoreDNS lifecycle context is required")
	}
	intent, err := platformComponentLifecycleIntent(ctx, service.coordinator, componentID, action, route)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(intent.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: route, Key: idempotencyKey,
	}
	existing, found, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, intent.protected)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if found {
		return cloneIdempotencyResponse(existing.Response), nil
	}
	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if current.Record.Desired.Owner != core.ComponentOwnerPlatform ||
		current.Record.Desired.Kind != core.ComponentKindCoreDNS {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Platform lifecycle is supported only for CoreDNS",
		)
	}
	desired, err := etcd.ProjectComponentRecord(current.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	ensureService := false
	disableService := false
	switch action {
	case "enable":
		if desired.Config.CoreDNS == nil {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindStateConflict,
				"CoreDNS must be configured before enable",
			)
		}
		desired.Enabled = true
		ensureService = true
	case "disable":
		if !desired.Enabled {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "CoreDNS is already disabled")
		}
		desired.Enabled = false
		disableService = true
	case "update":
		if !desired.Enabled || desired.Config.CoreDNS == nil {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindStateConflict,
				"CoreDNS must be enabled and configured before update",
			)
		}
		ensureService = true
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "CoreDNS lifecycle action is invalid")
	}
	desired.Healthy = false
	if err := ValidateComponent(service.renderer, desired); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	task := newPlatformComponentLifecycleTask(componentID, idempotencyKey, now, ensureService, disableService)
	var renderInput etcd.PlatformComponentTaskRenderInput
	if disableService {
		renderInput, err = service.planner.PrepareDisableTask(ctx, current, desired, task)
	} else {
		renderInput, err = service.planner.PrepareConfigTask(ctx, current, desired, task)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	result, publishErr := service.tasks.ReplacePlatformComponentDesiredWithTask(
		ctx, current, desired, task, renderInput,
		etcd.IdempotencyMarker{
			Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
			Locator: locator, Intent: intent.durable, Response: response,
			TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
		},
	)
	var resolution idempotentintent.Resolution
	if publishErr != nil {
		if !unknownPlatformComponentMutationOutcome(publishErr) {
			return etcd.IdempotencyResponse{}, publishErr
		}
		resolution, err = service.coordinator.ResolveUnknown(
			ctx, service.idempotency, locator, intent.protected, publishErr,
		)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, intent.protected, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if resolution.Kind == idempotentintent.ResolutionReplay {
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	if resolution.Kind != idempotentintent.ResolutionApplied {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Platform Component lifecycle resolution is invalid",
		)
	}
	return cloneIdempotencyResponse(response), nil
}

func platformComponentLifecycleIntent(
	ctx context.Context,
	coordinator *idempotentintent.Coordinator,
	componentID string,
	action string,
	route string,
) (platformComponentProtectedIntent, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost, Route: route,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopePlatform},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: componentID}},
		Query: idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "action", Value: idempotentintent.String(action)},
		)),
	})
	if err != nil {
		return platformComponentProtectedIntent{}, err
	}
	defer digest.Destroy()
	protected, err := coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return platformComponentProtectedIntent{}, err
	}
	durable, err := protected.DurableRecord()
	if err != nil {
		return platformComponentProtectedIntent{}, err
	}
	return platformComponentProtectedIntent{protected: protected, durable: durable}, nil
}

func NewPlatformMutationService(
	components *etcd.ComponentRepository,
	tasks *etcd.TaskRepository,
	idempotency *etcd.IdempotencyRepository,
	coordinator *idempotentintent.Coordinator,
	renderer componentdns.Renderer,
	planner *PlatformRenderPlanner,
) (*PlatformMutationService, error) {
	if components == nil || tasks == nil || idempotency == nil || coordinator == nil || renderer == nil ||
		planner == nil {
		return nil, errs.New(errs.KindInternal, "Platform Component mutation dependencies are not configured")
	}
	return &PlatformMutationService{
		components: components, tasks: tasks, idempotency: idempotency,
		coordinator: coordinator, renderer: renderer, planner: planner, now: time.Now,
	}, nil
}

func (service *PlatformMutationService) ReplacePlatformComponentConfig(
	ctx context.Context,
	componentID string,
	request apiTypes.ComponentConfigMutationRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "CoreDNS config mutation context is required")
	}
	config, err := coreDNSConfigMutation(request.Config)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	intent, err := platformComponentConfigIntent(ctx, service.coordinator, componentID, config)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(intent.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPut, Route: platformComponentConfigRoute, Key: idempotencyKey,
	}
	existing, found, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, intent.protected)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if found {
		if existing.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Platform Component config replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(existing.Response), nil
	}

	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if current.Record.Desired.Owner != core.ComponentOwnerPlatform ||
		current.Record.Desired.Kind != core.ComponentKindCoreDNS {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Platform Component config is supported only for CoreDNS",
		)
	}
	desired, err := etcd.ProjectComponentRecord(current.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	desired.Config = core.ComponentConfig{CoreDNS: &config}
	desired.Healthy = false
	if err := ValidateComponent(service.renderer, desired); err != nil {
		return etcd.IdempotencyResponse{}, err
	}

	now := service.now().UTC()
	ensureService := len(current.Record.Runtime.GeneratedServices) == 0 || !current.Record.Runtime.Healthy
	task := newPlatformComponentConfigTask(componentID, idempotencyKey, now, ensureService)
	renderInput, err := service.planner.PrepareConfigTask(ctx, current, desired, task)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	publicConfig := publicCoreDNSConfig(config)
	taskID := task.ID
	responseBody, err := json.Marshal(apiTypes.ComponentConfigMutationResult{
		Resource: publicConfig, ReconcileTaskID: &taskID,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	result, publishErr := service.tasks.ReplacePlatformComponentDesiredWithTask(
		ctx,
		current,
		desired,
		task,
		renderInput,
		etcd.IdempotencyMarker{
			Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
			Locator: locator, Intent: intent.durable, Response: response,
			TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
		},
	)
	var resolution idempotentintent.Resolution
	if publishErr != nil {
		if !unknownPlatformComponentMutationOutcome(publishErr) {
			return etcd.IdempotencyResponse{}, publishErr
		}
		resolution, err = service.coordinator.ResolveUnknown(
			ctx, service.idempotency, locator, intent.protected, publishErr,
		)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, intent.protected, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if resolution.Kind == idempotentintent.ResolutionReplay {
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	if resolution.Kind != idempotentintent.ResolutionApplied {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Platform Component config resolution is invalid",
		)
	}
	return cloneIdempotencyResponse(response), nil
}

type platformComponentProtectedIntent struct {
	protected idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

func platformComponentConfigIntent(
	ctx context.Context,
	coordinator *idempotentintent.Coordinator,
	componentID string,
	config core.CoreDNSComponentConfig,
) (platformComponentProtectedIntent, error) {
	if coordinator == nil {
		return platformComponentProtectedIntent{}, errs.New(
			errs.KindInternal,
			"Platform Component intent coordinator is not configured",
		)
	}
	forwarders := make([]idempotentintent.Value, len(config.Forwarders))
	for index, forwarder := range config.Forwarders {
		forwarders[index] = idempotentintent.Object(
			idempotentintent.Field{Name: "domain", Value: idempotentintent.String(forwarder.Domain)},
			idempotentintent.Field{Name: "resolvers", Value: coreDNSResolverIntent(forwarder.Resolvers)},
		)
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPut, Route: platformComponentConfigRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopePlatform},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: componentID}},
		Query: idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "config", Value: idempotentintent.Object(
				idempotentintent.Field{Name: "forwarders", Value: idempotentintent.List(forwarders...)},
				idempotentintent.Field{
					Name:  "tailnet_delegation",
					Value: idempotentintent.Bool(config.TailnetDelegation),
				},
				idempotentintent.Field{Name: "upstream_auto", Value: idempotentintent.Bool(config.UpstreamAuto)},
				idempotentintent.Field{
					Name:  "upstream_resolvers",
					Value: coreDNSResolverIntent(config.UpstreamResolvers),
				},
			)},
		)),
	})
	if err != nil {
		return platformComponentProtectedIntent{}, err
	}
	defer digest.Destroy()
	protected, err := coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return platformComponentProtectedIntent{}, err
	}
	durable, err := protected.DurableRecord()
	if err != nil {
		return platformComponentProtectedIntent{}, err
	}
	return platformComponentProtectedIntent{protected: protected, durable: durable}, nil
}

func coreDNSConfigMutation(input apiTypes.ComponentConfigMutationInput) (core.CoreDNSComponentConfig, error) {
	if input.CoreDNS == nil || input.CoreDNS.UpstreamAuto == nil || input.CoreDNS.UpstreamResolvers == nil ||
		input.CoreDNS.Forwarders == nil || input.CoreDNS.TailnetDelegation == nil {
		return core.CoreDNSComponentConfig{}, errs.New(
			errs.KindValidationFailed,
			"CoreDNS config requires every CoreDNS field and accepts no Environment Component fields",
		)
	}
	resolvers, err := parseCoreDNSResolvers(*input.CoreDNS.UpstreamResolvers)
	if err != nil {
		return core.CoreDNSComponentConfig{}, err
	}
	forwarders := make([]core.DNSForwarder, len(*input.CoreDNS.Forwarders))
	for index, forwarder := range *input.CoreDNS.Forwarders {
		parsed, parseErr := parseCoreDNSResolvers(forwarder.Resolvers)
		if parseErr != nil {
			return core.CoreDNSComponentConfig{}, parseErr
		}
		forwarders[index] = core.DNSForwarder{Domain: forwarder.Domain, Resolvers: parsed}
	}
	sort.Slice(forwarders, func(left int, right int) bool { return forwarders[left].Domain < forwarders[right].Domain })
	return core.CoreDNSComponentConfig{
		UpstreamAuto: *input.CoreDNS.UpstreamAuto, UpstreamResolvers: resolvers,
		Forwarders: forwarders, TailnetDelegation: *input.CoreDNS.TailnetDelegation,
	}, nil
}

func parseCoreDNSResolvers(values []string) ([]core.DNSResolverEndpoint, error) {
	result := make([]core.DNSResolverEndpoint, len(values))
	for index, value := range values {
		address, addressErr := netip.ParseAddr(value)
		if addressErr == nil && address.String() == value {
			result[index] = core.DNSResolverEndpoint{Address: value}
			continue
		}
		endpoint, endpointErr := netip.ParseAddrPort(value)
		if endpointErr != nil || endpoint.String() != value {
			return nil, errs.New(errs.KindValidationFailed, "CoreDNS resolver endpoint is not canonical")
		}
		result[index] = core.DNSResolverEndpoint{Address: endpoint.Addr().String(), Port: endpoint.Port()}
	}
	sort.Slice(result, func(left int, right int) bool {
		if result[left].Address != result[right].Address {
			return result[left].Address < result[right].Address
		}
		return result[left].Port < result[right].Port
	})
	return result, nil
}

func coreDNSResolverIntent(resolvers []core.DNSResolverEndpoint) idempotentintent.Value {
	values := make([]idempotentintent.Value, len(resolvers))
	for index, resolver := range resolvers {
		values[index] = idempotentintent.String(formatCoreDNSResolver(resolver))
	}
	return idempotentintent.List(values...)
}

func formatCoreDNSResolver(resolver core.DNSResolverEndpoint) string {
	if resolver.Port == 0 {
		return resolver.Address
	}
	return netip.AddrPortFrom(netip.MustParseAddr(resolver.Address), resolver.Port).String()
}

func publicCoreDNSConfig(config core.CoreDNSComponentConfig) apiTypes.ComponentConfig {
	upstreamAuto := config.UpstreamAuto
	tailnetDelegation := config.TailnetDelegation
	forwarders := make([]apiTypes.ComponentDNSForwarder, len(config.Forwarders))
	for index, forwarder := range config.Forwarders {
		resolvers := make([]string, len(forwarder.Resolvers))
		for resolverIndex, resolver := range forwarder.Resolvers {
			resolvers[resolverIndex] = formatCoreDNSResolver(resolver)
		}
		forwarders[index] = apiTypes.ComponentDNSForwarder{Domain: forwarder.Domain, Resolvers: resolvers}
	}
	resolvers := make([]string, len(config.UpstreamResolvers))
	for index, resolver := range config.UpstreamResolvers {
		resolvers[index] = formatCoreDNSResolver(resolver)
	}
	return apiTypes.ComponentConfig{
		CoreDNS: &apiTypes.CoreDNSComponentConfig{
			UpstreamAuto: upstreamAuto, UpstreamResolvers: resolvers,
			Forwarders: forwarders, TailnetDelegation: tailnetDelegation,
		},
	}
}

func newPlatformComponentConfigTask(
	componentID string,
	idempotencyKey string,
	createdAt time.Time,
	ensureService bool,
) etcd.TaskRecord {
	steps := []etcd.TaskStepRecord{{ID: ids.New(ids.KindStep)}}
	if ensureService {
		steps = append(steps,
			etcd.TaskStepRecord{ID: ids.New(ids.KindStep)},
			etcd.TaskStepRecord{ID: ids.New(ids.KindStep)},
			etcd.TaskStepRecord{ID: ids.New(ids.KindStep)},
		)
	} else {
		steps = append(steps, etcd.TaskStepRecord{ID: ids.New(ids.KindStep)})
	}
	return etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: etcd.PlatformTaskOwner(), Actor: etcd.TaskActorOperator,
		IdempotencyKey: idempotencyKey, Executor: etcd.TaskExecutorAgent,
		PlanID: ids.New(ids.KindPlan), RenderGeneration: 1,
		Type: etcd.TaskUpdate, Target: componentID,
		Params:         map[string]string{etcd.TaskResourceKindParam: etcd.TaskResourceComponent},
		Steps:          steps,
		TimeoutSeconds: platformComponentTaskTimeoutSeconds,
		Status:         etcd.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
}

func newPlatformComponentLifecycleTask(
	componentID string,
	idempotencyKey string,
	createdAt time.Time,
	ensureService bool,
	disableService bool,
) etcd.TaskRecord {
	if !disableService {
		return newPlatformComponentConfigTask(componentID, idempotencyKey, createdAt, ensureService)
	}
	task := newPlatformComponentConfigTask(componentID, idempotencyKey, createdAt, false)
	task.Steps = task.Steps[:1]
	task.Steps = append(task.Steps, etcd.TaskStepRecord{ID: ids.New(ids.KindStep)})
	return task
}

func unknownPlatformComponentMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

func cloneIdempotencyResponse(response etcd.IdempotencyResponse) etcd.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}
