package controller

import (
	"context"
	"log/slog"
	"net/http"
	"reflect"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ServiceReader interface {
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	GetServiceNativeCompose(context.Context, string, string) (string, error)
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
}

type ServiceMutator interface {
	CreateService(context.Context, apiTypes.ServiceCreate, string) (etcd.IdempotencyResponse, error)
	EditService(context.Context, string, apiTypes.ServiceEdit, string) (etcd.IdempotencyResponse, error)
	StartService(context.Context, string, string) (etcd.IdempotencyResponse, error)
	StopService(context.Context, string, string) (etcd.IdempotencyResponse, error)
	DestroyService(context.Context, string, string) (etcd.IdempotencyResponse, error)
}

type serviceListInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit       int    `query:"limit" required:"false"`
	Cursor      string `query:"cursor" required:"false"`
}

type servicePageOutput struct {
	Body apiTypes.Page[apiTypes.Service]
}

type serviceShowInput struct {
	ID string `path:"id" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type serviceCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.ServiceCreate
}

type serviceEditInput struct {
	ID             string `path:"id" pattern:"^svc_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.ServiceEdit
}

type serviceOutput struct{ Body apiTypes.Service }

type serviceDetailOutput struct{ Body apiTypes.ServiceDetail }

type serviceMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerServices() {
	s.registerServiceLifecycleRoutes()
	serviceSchema := s.API.OpenAPI().Components.Schemas.Schema(reflect.TypeFor[apiTypes.Service](), true, "Service")
	huma.Register(s.API, huma.Operation{
		OperationID: "service.list", Method: http.MethodGet, Path: "/services",
		Summary: "List services", Tags: []string{"Service"},
		Middlewares: huma.Middlewares{s.validateServiceListQuery},
	}, s.listServices)
	huma.Register(
		s.API,
		huma.Operation{
			OperationID: "service.show",
			Method:      http.MethodGet,
			Path:        "/services/{id}",
			Summary:     "Show a service",
			Tags:        []string{"Service"},
		},
		s.showService,
	)
	huma.Register(s.API, huma.Operation{
		OperationID:   "service.create",
		Method:        http.MethodPost,
		Path:          "/services",
		Summary:       "Create a service",
		Tags:          []string{"Service"},
		DefaultStatus: http.StatusCreated,
		Responses: map[string]*huma.Response{
			"201": {
				Description: http.StatusText(http.StatusCreated),
				Content:     map[string]*huma.MediaType{"application/json": {Schema: serviceSchema}},
			},
		},
	}, s.createService)
	huma.Register(s.API, huma.Operation{
		OperationID:   "service.edit",
		Method:        http.MethodPatch,
		Path:          "/services/{id}",
		Summary:       "Edit a service's direct desired fields",
		Tags:          []string{"Service"},
		DefaultStatus: http.StatusOK,
		Responses: map[string]*huma.Response{
			"200": {
				Description: http.StatusText(http.StatusOK),
				Content:     map[string]*huma.MediaType{"application/json": {Schema: serviceSchema}},
			},
		},
	}, s.editService)
	s.setRoutePolicy("POST /api/v1/services", routePolicy{body: jsonBody})
	s.setRoutePolicy("PATCH /api/v1/services/{id}", routePolicy{body: jsonBody})
}

func (s *Server) listServices(
	ctx context.Context,
	request *serviceListInput,
) (*servicePageOutput, error) {
	if s.services == nil {
		return nil, errs.New(errs.KindInternal, "Service reader is not configured")
	}
	pageRequest, err := serviceListRequest(request.Environment, request.Limit, request.Cursor)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	page, err := s.services.ListServices(ctx, request.Environment, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.Page[apiTypes.Service]{
		Items: make([]apiTypes.Service, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = serviceResponse(item.Record)
	}
	return &servicePageOutput{Body: response}, nil
}

func (s *Server) showService(ctx context.Context, request *serviceShowInput) (*serviceDetailOutput, error) {
	if s.services == nil {
		return nil, errs.New(errs.KindInternal, "Service reader is not configured")
	}
	record, err := s.services.GetService(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	nativeCompose, err := s.services.GetServiceNativeCompose(
		ctx,
		record.Record.EnvironmentID,
		record.Record.Desired.Name,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &serviceDetailOutput{Body: apiTypes.ServiceDetail{
		Service:       serviceResponse(record.Record),
		NativeCompose: nativeCompose,
	}}, nil
}

func (s *Server) createService(ctx context.Context, request *serviceCreateInput) (*serviceMutationOutput, error) {
	if s.serviceMutations == nil {
		return nil, errs.New(errs.KindInternal, "Service mutator is not configured")
	}
	response, err := s.serviceMutations.CreateService(ctx, request.Body, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.serviceMutationResponse(response), nil
}

func (s *Server) editService(ctx context.Context, request *serviceEditInput) (*serviceMutationOutput, error) {
	if s.serviceMutations == nil {
		return nil, errs.New(errs.KindInternal, "Service mutator is not configured")
	}
	response, err := s.serviceMutations.EditService(ctx, request.ID, request.Body, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.serviceMutationResponse(response), nil
}

func (s *Server) serviceMutationResponse(response etcd.IdempotencyResponse) *serviceMutationOutput {
	return &serviceMutationOutput{
		Status:      response.Status,
		ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error("controller: write Service mutation response", slog.Any("error", err))
			}
		},
	}
}

func serviceListRequest(environmentID string, limit int, cursor string) (etcd.PageRequest, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Service list requires a stable Environment id",
		)
	}
	if limit < 0 {
		return etcd.PageRequest{}, errs.New(errs.KindValidationFailed, "Service list limit must be a positive integer")
	}
	return etcd.PageRequest{Limit: limit, Cursor: cursor}, nil
}

func serviceResponse(record etcd.ServiceRecord) apiTypes.Service {
	return apiTypes.Service{
		ID:            record.Desired.ID,
		EnvironmentID: record.EnvironmentID,
		Name:          record.Desired.Name,
		Image:         record.Desired.Image,
		RuntimeIntent: apiTypes.ServiceRuntimeIntent(record.Runtime.RuntimeIntent),
		Zones:         append([]string(nil), record.Desired.Zones...),
		Strategy:      string(record.Desired.Strategy),
		OnFailure:     apiTypes.OnFailure(record.Desired.OnFailure),
		Replicas:      record.Desired.Replicas,
		Healthcheck:   serviceHealthcheckResponse(record.Desired.Healthcheck),
		Resources: apiTypes.ServiceResources{
			Mem:  record.Desired.Resources.Mem,
			CPUs: record.Desired.Resources.CPUs,
		},
		Command: append(
			[]string(nil),
			record.Desired.Command...),
		Mounts: serviceMountResponses(record.Desired.Mounts),
		Aliases: serviceAliasResponse(
			record.Desired.Aliases,
		),
		DependsOn: serviceDependencyResponses(record.Desired.DependsOn),
		Expose:    append([]string(nil), record.Desired.Expose...),
		Restart:   record.Desired.Restart,
		Logging: apiTypes.ServiceLogging{
			MaxSize: record.Desired.Logging.MaxSize,
			MaxFile: record.Desired.Logging.MaxFile,
		},
		Adapter:          record.Desired.Adapter,
		FactsPrefix:      record.Desired.FactsPrefix,
		Label:            record.Desired.Label,
		BackingNetworkID: record.BackingNetworkID,
	}
}

func serviceHealthcheckResponse(value core.Healthcheck) *apiTypes.ServiceHealthcheck {
	if value == (core.Healthcheck{}) {
		return nil
	}
	return &apiTypes.ServiceHealthcheck{
		HTTP:        value.HTTP,
		TCP:         value.TCP,
		Pgrep:       value.Pgrep,
		Interval:    value.Interval,
		Timeout:     value.Timeout,
		StartPeriod: value.StartPeriod,
		Retries:     value.Retries,
	}
}

func serviceMountResponses(values []core.Mount) []apiTypes.ServiceMount {
	result := make([]apiTypes.ServiceMount, len(values))
	for index, value := range values {
		result[index] = apiTypes.ServiceMount{Volume: value.Volume, File: value.File, Mount: value.Mount, RO: value.RO}
	}
	return result
}

func serviceAliasResponse(values map[string][]string) map[string][]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string][]string, len(values))
	for zone, aliases := range values {
		result[zone] = append([]string(nil), aliases...)
	}
	return result
}

func serviceDependencyResponses(values map[string]core.ServiceDependency) map[string]apiTypes.ServiceDependency {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]apiTypes.ServiceDependency, len(values))
	for name, dependency := range values {
		result[name] = apiTypes.ServiceDependency{
			Condition: dependency.Condition.String(),
			Phases:    dependency.PhaseStrings(),
		}
	}
	return result
}

func (s *Server) validateServiceListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		if key != "environment" && key != "limit" && key != "cursor" {
			s.writeServiceProblem(ctx, "Service list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeServiceProblem(ctx, "Service list query contains duplicate values")
			return
		}
	}
	next(ctx)
}

func (s *Server) writeServiceProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Service request problem", slog.Any("error", err))
	}
}
