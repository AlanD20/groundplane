package handlers

import (
	"context"
	"net/http"
	"reflect"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type EdgeChecker interface {
	CheckConnections(
		context.Context,
		string,
		apiTypes.EdgeCheckRequest,
		string,
	) (idempotency.IdempotencyResponse, error)
	GetConnectionCheck(context.Context, string, string) (apiTypes.EdgeCheckResult, error)
	GetLatestConnectionCheck(context.Context, string, string) (apiTypes.EdgeCheckResult, error)
	CheckTunnelConnections(
		context.Context,
		string,
		string,
		apiTypes.TunnelCheckRequest,
		string,
	) (idempotency.IdempotencyResponse, error)
	GetTunnelConnectionCheck(context.Context, string, string, string) (apiTypes.EdgeCheckResult, error)
}

type edgeCheckInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key         string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body        apiTypes.EdgeCheckRequest
}

type edgeCheckShowInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Task        string `query:"task" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	Route       string `query:"route" pattern:"^rte_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type edgeCheckOutput struct{ Body apiTypes.EdgeCheckResult }

func (s *Server) registerEdgeChecks() {
	accepted := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](),
		true,
		"TaskAccepted",
	)
	huma.Register(s.API, huma.Operation{OperationID: "route.check", Method: http.MethodPost, Path: "/routes/checks",
		Summary: "Check Tunnel readiness, Route origin and public DNS/TLS/HTTP", Tags: []string{"Route"},
		DefaultStatus: http.StatusAccepted, Responses: attachMutationResponses(accepted)}, s.checkEdgeConnections)
	huma.Register(s.API, huma.Operation{OperationID: "route.check.show", Method: http.MethodGet, Path: "/routes/checks",
		Summary: "Show a check Task or a Route's latest retained check", Tags: []string{"Route"}}, s.showEdgeCheck)
	s.setRoutePolicy("POST /api/v1/routes/checks", routePolicy{body: jsonBody})
	s.registerTunnelChecks()
}

func (s *Server) checkEdgeConnections(ctx context.Context, request *edgeCheckInput) (*attachMutationOutput, error) {
	if s.edgeChecks == nil {
		return nil, errs.New(errs.KindInternal, "Tunnel checks are not configured")
	}
	response, err := s.edgeChecks.CheckConnections(ctx, request.Environment, request.Body, request.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if s.controllerTaskWake != nil {
		s.controllerTaskWake()
	}
	return s.attachMutationResponse(response, "check"), nil
}

func (s *Server) showEdgeCheck(ctx context.Context, request *edgeCheckShowInput) (*edgeCheckOutput, error) {
	if s.edgeChecks == nil {
		return nil, errs.New(errs.KindInternal, "Tunnel checks are not configured")
	}
	if (request.Task == "") == (request.Route == "") {
		return nil, errs.New(errs.KindValidationFailed, "Tunnel check lookup requires exactly one Task or Route")
	}
	var result apiTypes.EdgeCheckResult
	var err error
	if request.Task != "" {
		result, err = s.edgeChecks.GetConnectionCheck(ctx, request.Environment, request.Task)
	} else {
		result, err = s.edgeChecks.GetLatestConnectionCheck(ctx, request.Environment, request.Route)
	}
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &edgeCheckOutput{Body: result}, nil
}
