package controller

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type HostReader interface {
	Show(ctx context.Context) (api.Host, error)
}

type hostOutput struct {
	Body api.Host
}

func (s *Server) registerHost() {
	huma.Register(s.API, huma.Operation{
		OperationID: "host.show",
		Method:      http.MethodGet,
		Path:        "/host",
		Summary:     "Show host health",
		Tags:        []string{"Host"},
	}, s.showHost)
	controllerUpdateStateSchema(s.API.OpenAPI().Components.Schemas)
}

func controllerUpdateStateSchema(registry huma.Registry) {
	reference := openAPISchema[api.ControllerUpdateState](registry, "ControllerUpdateState")
	schema := registry.SchemaFromRef(reference.Ref)
	candidate := openAPISchema[api.ControllerRelease](registry, "ControllerRelease")
	last := openAPISchema[api.ControllerUpdateSummary](registry, "ControllerUpdateSummary")
	schema.Properties["candidate"] = &huma.Schema{OneOf: []*huma.Schema{candidate, {Type: "null"}}}
	schema.Properties["last_update"] = &huma.Schema{OneOf: []*huma.Schema{last, {Type: "null"}}}
}

func (s *Server) showHost(ctx context.Context, _ *struct{}) (*hostOutput, error) {
	if s.host == nil {
		return nil, errs.New(errs.KindInternal, "host service is not configured")
	}
	host, err := s.host.Show(ctx)
	if err != nil {
		return nil, err
	}
	return &hostOutput{Body: host}, nil
}
