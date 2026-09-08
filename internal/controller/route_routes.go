package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"reflect"

	"github.com/AlanD20/groundplane/internal/common/ids"
	corenetwork "github.com/AlanD20/groundplane/internal/core/network"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type RouteReader interface {
	GetRoute(context.Context, string) (corenetwork.Route, error)
	ListRoutes(context.Context, string, corenetwork.PageRequest) (corenetwork.Page[corenetwork.Route], error)
}

type RouteMutator interface {
	CreateRoute(context.Context, corenetwork.CreateRouteRequest, string) (corenetwork.MutationResponse, error)
	EditRoute(context.Context, string, corenetwork.EditRouteRequest, string) (corenetwork.MutationResponse, error)
	RemoveRoute(context.Context, string, string) (corenetwork.MutationResponse, error)
}

type routeListInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit       int    `query:"limit" required:"false"`
	Cursor      string `query:"cursor" required:"false"`
}

type routeShowInput struct {
	ID string `path:"id" pattern:"^rte_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type routeCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.RouteCreate
}

type routeEditInput struct {
	ID             string `path:"id" pattern:"^rte_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.RouteEdit
}

type routeRemoveInput struct {
	ID             string `path:"id" pattern:"^rte_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type routeOutput struct {
	Body apiTypes.Route
}

type routePageOutput struct {
	Body apiTypes.Page[apiTypes.Route]
}

type routeMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerRoutes() {
	huma.Register(s.API, huma.Operation{
		OperationID: "route.list", Method: http.MethodGet, Path: "/routes",
		Summary: "List routes", Tags: []string{"Route"},
	}, s.listRoutes)
	huma.Register(s.API, huma.Operation{
		OperationID: "route.show", Method: http.MethodGet, Path: "/routes/{id}",
		Summary: "Show a route", Tags: []string{"Route"},
	}, s.showRoute)
	huma.Register(s.API, huma.Operation{
		OperationID: "route.create", Method: http.MethodPost, Path: "/routes",
		Summary: "Create a route", Tags: []string{"Route"}, DefaultStatus: http.StatusAccepted,
		Responses: map[string]*huma.Response{
			"202": {
				Description: http.StatusText(http.StatusAccepted),
				Content: map[string]*huma.MediaType{
					"application/json": {Schema: s.API.OpenAPI().Components.Schemas.Schema(
						reflect.TypeFor[apiTypes.RouteTaskAccepted](), true, "RouteTaskAccepted",
					)},
				},
			},
		},
	}, s.createRoute)
	huma.Register(s.API, huma.Operation{
		OperationID: "route.edit", Method: http.MethodPatch, Path: "/routes/{id}",
		Summary: "Edit route exposure", Tags: []string{"Route"}, DefaultStatus: http.StatusAccepted,
		Responses: map[string]*huma.Response{
			"202": {
				Description: http.StatusText(http.StatusAccepted),
				Content: map[string]*huma.MediaType{
					"application/json": {Schema: s.API.OpenAPI().Components.Schemas.Schema(
						reflect.TypeFor[apiTypes.RouteTaskAccepted](), true, "RouteTaskAccepted",
					)},
				},
			},
		},
	}, s.editRoute)
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](), true, "TaskAccepted",
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "route.remove", Method: http.MethodDelete, Path: "/routes/{id}",
		Summary: "Remove a route", Tags: []string{"Route"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectRouteDeleteBody, s.rejectRouteDeleteQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
	}, s.removeRoute)
	s.setRoutePolicy("POST /api/v1/routes", routePolicy{body: jsonBody})
	s.setRoutePolicy("PATCH /api/v1/routes/{id}", routePolicy{body: jsonBody})
}

func (s *Server) listRoutes(
	ctx context.Context,
	request *routeListInput,
) (*routePageOutput, error) {
	if s.routeReads == nil {
		return nil, errs.New(errs.KindInternal, "Route reader is not configured")
	}
	pageRequest, err := routeListRequest(request.Environment, request.Limit, request.Cursor)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	page, err := s.routeReads.ListRoutes(ctx, request.Environment, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.Page[apiTypes.Route]{
		Items: make([]apiTypes.Route, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = routeResponse(item)
	}
	return &routePageOutput{Body: response}, nil
}

func (s *Server) showRoute(
	ctx context.Context,
	request *routeShowInput,
) (*routeOutput, error) {
	if s.routeReads == nil {
		return nil, errs.New(errs.KindInternal, "Route reader is not configured")
	}
	route, err := s.routeReads.GetRoute(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &routeOutput{Body: routeResponse(route)}, nil
}

func (s *Server) createRoute(
	ctx context.Context,
	request *routeCreateInput,
) (*routeMutationOutput, error) {
	if s.routeMutations == nil {
		return nil, errs.New(errs.KindInternal, "Route mutator is not configured")
	}
	response, err := s.routeMutations.CreateRoute(ctx, corenetwork.CreateRouteRequest{
		EnvironmentID:   request.Body.EnvironmentID,
		Host:            request.Body.Host,
		Path:            request.Body.Path,
		Exposure:        corenetwork.RouteExposure(request.Body.Exposure),
		TargetServiceID: request.Body.TargetServiceID,
		TargetPort:      request.Body.TargetPort,
	}, request.IdempotencyKey)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Error(
				"controller: create Route",
				slog.String("environment_id", request.Body.EnvironmentID),
				slog.String("host", request.Body.Host),
				slog.String("path", request.Body.Path),
				slog.Any("error", err),
			)
		}
		return nil, normalizeProjectError(err)
	}
	return s.routeMutationResponse(response), nil
}

func (s *Server) editRoute(
	ctx context.Context,
	request *routeEditInput,
) (*routeMutationOutput, error) {
	if s.routeMutations == nil {
		return nil, errs.New(errs.KindInternal, "Route mutator is not configured")
	}
	response, err := s.routeMutations.EditRoute(
		ctx, request.ID, corenetwork.EditRouteRequest{Exposure: corenetwork.RouteExposure(request.Body.Exposure)},
		request.IdempotencyKey,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.routeMutationResponse(response), nil
}

func (s *Server) removeRoute(
	ctx context.Context,
	request *routeRemoveInput,
) (*routeMutationOutput, error) {
	if s.routeMutations == nil {
		return nil, errs.New(errs.KindInternal, "Route mutator is not configured")
	}
	response, err := s.routeMutations.RemoveRoute(ctx, request.ID, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.routeMutationResponse(response), nil
}

func (s *Server) rejectRouteDeleteBody(ctx huma.Context, next func(huma.Context)) {
	var probe [1]byte
	count, err := ctx.BodyReader().Read(probe[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		s.writeRouteProblem(ctx, "Route deletion body is not allowed")
		return
	}
	next(ctx)
}

func (s *Server) rejectRouteDeleteQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeRouteProblem(ctx, "Route deletion query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) writeRouteProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Route request problem", slog.Any("error", err))
	}
}

func (s *Server) routeMutationResponse(response corenetwork.MutationResponse) *routeMutationOutput {
	return &routeMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error("controller: write Route mutation response", slog.Any("error", err))
			}
		},
	}
}

func routeListRequest(environmentID string, limit int, cursor string) (corenetwork.PageRequest, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return corenetwork.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Route list requires a stable Environment id",
		)
	}
	if limit < 0 {
		return corenetwork.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Route list limit must be a positive integer",
		)
	}
	return corenetwork.PageRequest{Limit: limit, Cursor: cursor}, nil
}

func routeResponse(record corenetwork.Route) apiTypes.Route {
	return apiTypes.Route{
		ID: record.ID, EnvironmentID: record.EnvironmentID, Host: record.Host,
		Path: record.Path, Exposure: string(record.Exposure),
		TargetServiceID: record.TargetServiceID, TargetPort: record.TargetPort,
		Status: string(record.Status),
	}
}
