package controller

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

const maximumComponentPageSize = 200

type ComponentReader interface {
	ListComponents(context.Context, string, bool, string) ([]apiTypes.Component, error)
	GetComponent(context.Context, string) (apiTypes.Component, error)
	GetComponentConfig(context.Context, string) (apiTypes.ComponentConfigResponse, error)
	GetRouter(context.Context, string) (apiTypes.Router, error)
}

type ComponentMutator interface {
	EnableComponent(context.Context, string, apiTypes.ComponentEnableRequest, string) (etcd.IdempotencyResponse, error)
	DisableComponent(context.Context, string, string) (etcd.IdempotencyResponse, error)
	UpdateComponent(context.Context, string, string) (etcd.IdempotencyResponse, error)
	SetComponentConfig(
		context.Context,
		string,
		apiTypes.ComponentConfigMutationRequest,
		string,
	) (etcd.IdempotencyResponse, error)
}

type componentListInput struct {
	Environment string `query:"environment" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Platform    bool   `query:"platform"`
	Kind        string `query:"kind"`
	Limit       int    `query:"limit" required:"false" minimum:"1" maximum:"200"`
	Cursor      string `query:"cursor" required:"false"`
}

type componentListOutput struct {
	Body apiTypes.Page[apiTypes.Component]
}
type componentOutput struct{ Body apiTypes.Component }
type componentConfigOutput struct {
	Body apiTypes.ComponentConfigResponse
}

type componentIDInput struct {
	ID string `path:"id" pattern:"^cmp_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type componentActionInput struct {
	ID             string `path:"id" pattern:"^cmp_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type componentConfigInput struct {
	ID             string `path:"id" pattern:"^cmp_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.ComponentConfigMutationRequest
}

type componentEnableInput struct {
	ID             string                           `path:"id" pattern:"^cmp_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string                           `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           *apiTypes.ComponentEnableRequest `required:"false"`
}

type componentRouterInput struct {
	ID string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type componentRouterOutput struct{ Body apiTypes.Router }

func (s *Server) registerComponents() {
	componentConfigSchema(s.API.OpenAPI().Components.Schemas)
	componentConfigResponseSchema(s.API.OpenAPI().Components.Schemas)
	taskAcceptedSchema := openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted")
	configResultSchema := openAPISchema[apiTypes.ComponentConfigMutationResult](
		s.API.OpenAPI().Components.Schemas,
		"ComponentConfigMutationResult",
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "component.list", Method: http.MethodGet, Path: "/components",
		Summary: "List Components", Tags: []string{"Component"},
		Middlewares: huma.Middlewares{s.validateComponentListQuery},
	}, s.listComponents)
	huma.Register(s.API, huma.Operation{
		OperationID: "component.show", Method: http.MethodGet, Path: "/components/{id}",
		Summary: "Show a Component", Tags: []string{"Component"},
	}, s.showComponent)
	huma.Register(s.API, huma.Operation{
		OperationID: "component-config.show", Method: http.MethodGet, Path: "/components/{id}/config",
		Summary: "Show Component config", Tags: []string{"Component"},
	}, s.showComponentConfig)
	configSetOperation := huma.Operation{
		OperationID: "component-config.set", Method: http.MethodPut, Path: "/components/{id}/config",
		Summary: "Replace Component config", Tags: []string{"Component"}, DefaultStatus: http.StatusOK,
		Responses: map[string]*huma.Response{strconv.Itoa(http.StatusOK): {
			Description: http.StatusText(http.StatusOK),
			Content:     map[string]*huma.MediaType{"application/json": {Schema: configResultSchema}},
		}},
	}
	configSetOperation.RequestBody = &huma.RequestBody{
		Required: true,
		Content: map[string]*huma.MediaType{
			"application/json": {
				Schema: componentConfigMutationRequestSchema(s.API.OpenAPI().Components.Schemas),
			},
		},
	}
	huma.Register(s.API, configSetOperation, s.setComponentConfig)
	huma.Register(s.API, huma.Operation{
		OperationID: "component.enable", Method: http.MethodPost, Path: "/components/{id}/enable",
		Summary: "Enable a Component with optional configuration", Tags: []string{"Component"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectComponentQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
		RequestBody: &huma.RequestBody{Required: false, Content: map[string]*huma.MediaType{
			"application/json": {Schema: componentEnableRequestSchema(s.API.OpenAPI().Components.Schemas)},
		}},
	}, s.enableComponent)
	for _, action := range []struct {
		id      string
		path    string
		handler func(context.Context, *componentActionInput) (*componentMutationOutput, error)
	}{
		{id: "component.disable", path: "/components/{id}/disable", handler: s.disableComponent},
		{id: "component.update", path: "/components/{id}/update", handler: s.updateComponent},
	} {
		huma.Register(s.API, huma.Operation{
			OperationID: action.id, Method: http.MethodPost, Path: action.path,
			Summary: action.id, Tags: []string{"Component"}, DefaultStatus: http.StatusAccepted,
			Middlewares: huma.Middlewares{s.rejectComponentMutationBody, s.rejectComponentQuery},
			Responses:   attachMutationResponses(taskAcceptedSchema),
		}, action.handler)
	}
	huma.Register(s.API, huma.Operation{
		OperationID: "router.show", Method: http.MethodGet, Path: "/environments/{id}/router",
		Summary: "Show Environment router", Tags: []string{"Component"},
	}, s.showComponentRouter)
	s.setRoutePolicy("PUT /api/v1/components/{id}/config", routePolicy{body: jsonBody})
	s.setRoutePolicy("POST /api/v1/components/{id}/enable", routePolicy{body: jsonBody})
}

func (s *Server) listComponents(ctx context.Context, request *componentListInput) (*componentListOutput, error) {
	if s.components == nil {
		return nil, errs.New(errs.KindInternal, "Component reader is not configured")
	}
	if (request.Environment == "") == !request.Platform {
		return nil, errs.New(errs.KindMalformedRequest, "exactly one Component owner scope is required")
	}
	if request.Cursor != "" {
		return nil, errs.New(errs.KindMalformedRequest, "Component pagination cursor is invalid")
	}
	items, err := s.components.ListComponents(ctx, request.Environment, request.Platform, request.Kind)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	limit := request.Limit
	if limit == 0 {
		limit = maximumComponentPageSize
	}
	if len(items) > limit {
		items = items[:limit]
	}
	return &componentListOutput{Body: apiTypes.Page[apiTypes.Component]{Items: items}}, nil
}

func (s *Server) showComponent(ctx context.Context, request *componentIDInput) (*componentOutput, error) {
	if s.components == nil {
		return nil, errs.New(errs.KindInternal, "Component reader is not configured")
	}
	component, err := s.components.GetComponent(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &componentOutput{Body: component}, nil
}

func (s *Server) showComponentConfig(ctx context.Context, request *componentIDInput) (*componentConfigOutput, error) {
	if s.components == nil {
		return nil, errs.New(errs.KindInternal, "Component reader is not configured")
	}
	response, err := s.components.GetComponentConfig(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &componentConfigOutput{Body: response}, nil
}

func (s *Server) setComponentConfig(
	ctx context.Context,
	request *componentConfigInput,
) (*componentMutationOutput, error) {
	if s.componentMutations == nil {
		return nil, errs.New(errs.KindInternal, "Component mutation service is not configured")
	}
	response, err := s.componentMutations.SetComponentConfig(ctx, request.ID, request.Body, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return componentMutationResponse(response), nil
}

func (s *Server) enableComponent(ctx context.Context, request *componentEnableInput) (*componentMutationOutput, error) {
	if s.componentMutations == nil {
		return nil, errs.New(errs.KindInternal, "Component mutation service is not configured")
	}
	body := apiTypes.ComponentEnableRequest{}
	if request.Body != nil {
		body = *request.Body
	}
	response, err := s.componentMutations.EnableComponent(ctx, request.ID, body, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return componentMutationResponse(response), nil
}

func (s *Server) disableComponent(
	ctx context.Context,
	request *componentActionInput,
) (*componentMutationOutput, error) {
	if s.componentMutations == nil {
		return nil, errs.New(errs.KindInternal, "Component mutation service is not configured")
	}
	response, err := s.componentMutations.DisableComponent(ctx, request.ID, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return componentMutationResponse(response), nil
}

func (s *Server) updateComponent(ctx context.Context, request *componentActionInput) (*componentMutationOutput, error) {
	if s.componentMutations == nil {
		return nil, errs.New(errs.KindInternal, "Component mutation service is not configured")
	}
	response, err := s.componentMutations.UpdateComponent(ctx, request.ID, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return componentMutationResponse(response), nil
}

type componentMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func componentMutationResponse(response etcd.IdempotencyResponse) *componentMutationOutput {
	return &componentMutationOutput{
		Status:      response.Status,
		ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			_, _ = ctx.BodyWriter().Write(response.Body)
		},
	}
}

func (s *Server) showComponentRouter(
	ctx context.Context,
	request *componentRouterInput,
) (*componentRouterOutput, error) {
	if s.components == nil {
		return nil, errs.New(errs.KindInternal, "Component reader is not configured")
	}
	router, err := s.components.GetRouter(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &componentRouterOutput{Body: router}, nil
}

func (s *Server) validateComponentListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		if key != "environment" && key != "platform" && key != "kind" && key != "limit" && key != "cursor" {
			s.writeComponentRequestProblem(ctx, "Component list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeComponentRequestProblem(ctx, "Component list query contains duplicate values")
			return
		}
	}
	next(ctx)
}

func (s *Server) rejectComponentQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeComponentRequestProblem(ctx, "Component request query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) rejectComponentMutationBody(ctx huma.Context, next func(huma.Context)) {
	var probe [1]byte
	count, err := ctx.BodyReader().Read(probe[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		s.writeComponentRequestProblem(ctx, "Component mutation body is not allowed")
		return
	}
	next(ctx)
}

func (s *Server) writeComponentRequestProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Component request problem", "error", err)
	}
}
