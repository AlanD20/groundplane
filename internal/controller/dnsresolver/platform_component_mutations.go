package dnsresolver

import (
	"context"
	"encoding/json"
	"errors"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"
	"net/netip"
	"sort"
	"time"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
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
	coordinator *requestidempotency.Coordinator
	renderer    componentdns.Renderer
	planner     *PlatformRenderPlanner
	now         func() time.Time
}

func (service *PlatformMutationService) EnablePlatformComponent(
	ctx context.Context,
	componentID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
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
) (idempotencyrecord.IdempotencyResponse, error) {
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
) (idempotencyrecord.IdempotencyResponse, error) {
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
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "CoreDNS lifecycle context is required")
	}
	intent, err := platformComponentLifecycleIntent(ctx, service.coordinator, componentID, action, route)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(intent.durable.Ciphertext)
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: route, Key: idempotencyKey,
	}
	existing, found, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, intent.protected)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if found {
		return cloneIdempotencyResponse(existing.Response), nil
	}
	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if current.Record.Desired.Owner != core.ComponentOwnerPlatform ||
		current.Record.Desired.Kind != core.ComponentKindCoreDNS {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Platform lifecycle is supported only for CoreDNS",
		)
	}
	desired, err := componentrecord.ProjectRecord(current.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	desired, ensureService, disableService, err := platformComponentLifecycleCandidate(desired, action)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if err := ValidateComponent(service.renderer, desired); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
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
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task, err = finalizePlatformComponentTask(task, renderInput)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	result, publishErr := service.tasks.ReplacePlatformComponentDesiredWithTask(
		ctx, current, desired, task, renderInput,
		idempotencyrecord.IdempotencyMarker{
			Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
			Locator: locator, Intent: intent.durable, Response: response,
			TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
		},
	)
	var resolution requestidempotency.Resolution
	if publishErr != nil {
		if !unknownPlatformComponentMutationOutcome(publishErr) {
			return idempotencyrecord.IdempotencyResponse{}, publishErr
		}
		resolution, err = service.coordinator.ResolveUnknown(
			ctx, service.idempotency, locator, intent.protected, publishErr,
		)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, intent.protected, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	if resolution.Kind != requestidempotency.ResolutionApplied {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Platform Component lifecycle resolution is invalid",
		)
	}
	return cloneIdempotencyResponse(response), nil
}

func platformComponentLifecycleIntent(
	ctx context.Context,
	coordinator *requestidempotency.Coordinator,
	componentID string,
	action string,
	route string,
) (platformComponentProtectedIntent, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: route,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: componentID}},
		Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "action", Value: requestidempotency.String(action)},
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
	coordinator *requestidempotency.Coordinator,
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
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "CoreDNS config mutation context is required")
	}
	config, err := coreDNSConfigMutation(request.Config)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent, err := platformComponentConfigIntent(ctx, service.coordinator, componentID, config)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(intent.durable.Ciphertext)
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPut, Route: platformComponentConfigRoute, Key: idempotencyKey,
	}
	existing, found, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, intent.protected)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if found {
		if existing.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Platform Component config replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(existing.Response), nil
	}

	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if current.Record.Desired.Owner != core.ComponentOwnerPlatform ||
		current.Record.Desired.Kind != core.ComponentKindCoreDNS {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Platform Component config is supported only for CoreDNS",
		)
	}
	desired, err := componentrecord.ProjectRecord(current.Record)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	desired = platformComponentConfigCandidate(desired, config)
	if err := ValidateComponent(service.renderer, desired); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}

	now := service.now().UTC()
	ensureService := len(current.Record.Runtime.GeneratedServices) == 0 || !current.Record.Runtime.Healthy
	task := newPlatformComponentConfigTask(componentID, idempotencyKey, now, ensureService)
	renderInput, err := service.planner.PrepareConfigTask(ctx, current, desired, task)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task, err = finalizePlatformComponentTask(task, renderInput)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	publicConfig := publicCoreDNSConfig(config)
	taskID := task.ID
	responseBody, err := json.Marshal(apiTypes.ComponentConfigMutationResult{
		Resource: publicConfig, ReconcileTaskID: &taskID,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	result, publishErr := service.tasks.ReplacePlatformComponentDesiredWithTask(
		ctx,
		current,
		desired,
		task,
		renderInput,
		idempotencyrecord.IdempotencyMarker{
			Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
			Locator: locator, Intent: intent.durable, Response: response,
			TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
		},
	)
	var resolution requestidempotency.Resolution
	if publishErr != nil {
		if !unknownPlatformComponentMutationOutcome(publishErr) {
			return idempotencyrecord.IdempotencyResponse{}, publishErr
		}
		resolution, err = service.coordinator.ResolveUnknown(
			ctx, service.idempotency, locator, intent.protected, publishErr,
		)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, intent.protected, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	if resolution.Kind != requestidempotency.ResolutionApplied {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Platform Component config resolution is invalid",
		)
	}
	return cloneIdempotencyResponse(response), nil
}

type platformComponentProtectedIntent struct {
	protected requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

func platformComponentConfigIntent(
	ctx context.Context,
	coordinator *requestidempotency.Coordinator,
	componentID string,
	config core.CoreDNSComponentConfig,
) (platformComponentProtectedIntent, error) {
	if coordinator == nil {
		return platformComponentProtectedIntent{}, errs.New(
			errs.KindInternal,
			"Platform Component intent coordinator is not configured",
		)
	}
	forwarders := make([]requestidempotency.Value, len(config.Forwarders))
	for index, forwarder := range config.Forwarders {
		forwarders[index] = requestidempotency.Object(
			requestidempotency.Field{Name: "domain", Value: requestidempotency.String(forwarder.Domain)},
			requestidempotency.Field{Name: "resolvers", Value: coreDNSResolverIntent(forwarder.Resolvers)},
		)
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPut, Route: platformComponentConfigRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: componentID}},
		Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "config", Value: requestidempotency.Object(
				requestidempotency.Field{
					Name:  "corefile_template",
					Value: requestidempotency.String(config.CorefileTemplate),
				},
				requestidempotency.Field{Name: "forwarders", Value: requestidempotency.List(forwarders...)},
				requestidempotency.Field{
					Name:  "tailnet_delegation",
					Value: requestidempotency.Bool(config.TailnetDelegation),
				},
				requestidempotency.Field{Name: "upstream_auto", Value: requestidempotency.Bool(config.UpstreamAuto)},
				requestidempotency.Field{
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
	if input.CoreDNS == nil || input.CoreDNS.CorefileTemplate == nil || input.CoreDNS.UpstreamAuto == nil ||
		input.CoreDNS.UpstreamResolvers == nil ||
		input.CoreDNS.Forwarders == nil ||
		input.CoreDNS.TailnetDelegation == nil {
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
		CorefileTemplate: *input.CoreDNS.CorefileTemplate,
		UpstreamAuto:     *input.CoreDNS.UpstreamAuto, UpstreamResolvers: resolvers,
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

func coreDNSResolverIntent(resolvers []core.DNSResolverEndpoint) requestidempotency.Value {
	values := make([]requestidempotency.Value, len(resolvers))
	for index, resolver := range resolvers {
		values[index] = requestidempotency.String(formatCoreDNSResolver(resolver))
	}
	return requestidempotency.List(values...)
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
			CorefileTemplate: config.CorefileTemplate,
			UpstreamAuto:     upstreamAuto, UpstreamResolvers: resolvers,
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
	steps := []taskjournal.TaskStepRecord{{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)}}
	if ensureService {
		steps = append(steps,
			taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)},
			taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)},
			taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)},
		)
	} else {
		steps = append(steps, taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)})
	}
	return etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: taskjournal.PlatformTaskOwner(), Actor: taskjournal.TaskActorOperator,
		IdempotencyKey: idempotencyKey, Executor: taskjournal.TaskExecutorAgent,
		PlanID: ids.New(ids.KindPlan), RenderGeneration: 1,
		Type: taskjournal.TaskUpdate, Target: componentID,
		Params:         map[string]string{taskjournal.TaskResourceKindParam: taskjournal.TaskResourceComponent},
		Steps:          steps,
		TimeoutSeconds: platformComponentTaskTimeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
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
	task.Steps = append(task.Steps, taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)})
	return task
}

func unknownPlatformComponentMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

func cloneIdempotencyResponse(response idempotencyrecord.IdempotencyResponse) idempotencyrecord.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}
