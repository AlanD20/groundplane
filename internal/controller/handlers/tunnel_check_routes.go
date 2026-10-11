package handlers

import (
	"context"
	"net/http"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type tunnelCheckInput struct {
	ID          string `path:"id" pattern:"^cmp_[0-9A-HJKMNP-TV-Z]{26}$"`
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key         string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body        apiTypes.TunnelCheckRequest
}
type tunnelCheckShowInput struct {
	ID          string `path:"id" pattern:"^cmp_[0-9A-HJKMNP-TV-Z]{26}$"`
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Task        string `query:"task" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
}

func (s *Server) registerTunnelChecks() {
	accepted := openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted")
	huma.Register(s.API, huma.Operation{OperationID: "component.tunnel-check", Method: http.MethodPost,
		Path: "/components/{id}/tunnel-checks", Summary: "Check Tunnel readiness, a serving Service origin and public DNS/TLS/HTTP", Tags: []string{"Component"},
		DefaultStatus: http.StatusAccepted, Responses: attachMutationResponses(accepted)}, s.checkTunnel)
	huma.Register(s.API, huma.Operation{OperationID: "component.tunnel-check.show", Method: http.MethodGet,
		Path: "/components/{id}/tunnel-checks", Summary: "Show a standalone Tunnel check Task or the latest retained result", Tags: []string{"Component"}}, s.showTunnelCheck)
	s.setRoutePolicy("POST /api/v1/components/{id}/tunnel-checks", routePolicy{body: jsonBody})
}

func (s *Server) checkTunnel(ctx context.Context, request *tunnelCheckInput) (*componentMutationOutput, error) {
	if s.edgeChecks == nil {
		return nil, errs.New(errs.KindInternal, "Tunnel checks are not configured")
	}
	response, err := s.edgeChecks.CheckTunnelConnections(
		ctx,
		request.Environment,
		request.ID,
		request.Body,
		request.Key,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if s.controllerTaskWake != nil {
		s.controllerTaskWake()
	}
	return componentMutationResponse(response), nil
}

func (s *Server) showTunnelCheck(ctx context.Context, request *tunnelCheckShowInput) (*edgeCheckOutput, error) {
	if s.edgeChecks == nil {
		return nil, errs.New(errs.KindInternal, "Tunnel checks are not configured")
	}
	result, err := s.edgeChecks.GetTunnelConnectionCheck(ctx, request.Environment, request.ID, request.Task)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &edgeCheckOutput{Body: result}, nil
}
