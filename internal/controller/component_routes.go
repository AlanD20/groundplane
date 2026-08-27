package controller

import (
	"context"
	"net/http"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

const maximumComponentPageSize = 200

type ComponentReader interface {
	ListComponents(context.Context, string, bool, string) ([]apiTypes.Component, error)
	GetComponent(context.Context, string) (apiTypes.Component, error)
	GetComponentConfig(context.Context, string) (apiTypes.ComponentConfig, error)
	GetRouter(context.Context, string) (apiTypes.Router, error)
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
type componentConfigOutput struct{ Body apiTypes.ComponentConfig }

type componentIDInput struct {
	ID string `path:"id" pattern:"^cmp_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type componentRouterInput struct {
	ID string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type componentRouterOutput struct{ Body apiTypes.Router }

func (s *Server) registerComponents() {
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
	huma.Register(s.API, huma.Operation{
		OperationID: "router.show", Method: http.MethodGet, Path: "/environments/{id}/router",
		Summary: "Show Environment router", Tags: []string{"Component"},
	}, s.showComponentRouter)
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
	config, err := s.components.GetComponentConfig(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &componentConfigOutput{Body: config}, nil
}

func (s *Server) showComponentRouter(ctx context.Context, request *componentRouterInput) (*componentRouterOutput, error) {
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

func (s *Server) writeComponentRequestProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Component request problem", "error", err)
	}
}
