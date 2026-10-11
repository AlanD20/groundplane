package handlers

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type RunnerChecker interface {
	CheckConnections(
		context.Context,
		string,
		apiTypes.RunnerCheckRequest,
		string,
	) (idempotency.IdempotencyResponse, error)
	GetConnectionCheck(context.Context, string, string) (apiTypes.RunnerCheckResult, error)
}

type runnerCheckInput struct {
	ID   string `path:"id" pattern:"^run_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key  string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body apiTypes.RunnerCheckRequest
}
type runnerCheckShowInput struct {
	ID   string `path:"id" pattern:"^run_[0-9A-HJKMNP-TV-Z]{26}$"`
	Task string `query:"task" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
}
type runnerCheckOutput struct{ Body apiTypes.RunnerCheckResult }

func (s *Server) registerRunnerChecks() {
	accepted := openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted")
	huma.Register(s.API, huma.Operation{OperationID: "runner.check", Method: http.MethodPost,
		Path: "/runners/{id}/checks", Summary: "Check connections from the registered Runner", Tags: []string{"Runner"},
		DefaultStatus: http.StatusAccepted, Responses: attachMutationResponses(accepted)}, s.checkRunnerConnections)
	huma.Register(s.API, huma.Operation{OperationID: "runner.check.show", Method: http.MethodGet,
		Path: "/runners/{id}/checks", Summary: "Show a Runner check Task or latest retained result", Tags: []string{"Runner"}}, s.showRunnerCheck)
	s.setRoutePolicy("POST /api/v1/runners/{id}/checks", routePolicy{body: jsonBody})
}

func (s *Server) checkRunnerConnections(
	ctx context.Context,
	request *runnerCheckInput,
) (*componentMutationOutput, error) {
	if s.runnerChecks == nil {
		return nil, errs.New(errs.KindInternal, "Runner connection checks are not configured")
	}
	response, err := s.runnerChecks.CheckConnections(ctx, request.ID, request.Body, request.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if s.controllerTaskWake != nil {
		s.controllerTaskWake()
	}
	return componentMutationResponse(response), nil
}

func (s *Server) showRunnerCheck(ctx context.Context, request *runnerCheckShowInput) (*runnerCheckOutput, error) {
	if s.runnerChecks == nil {
		return nil, errs.New(errs.KindInternal, "Runner connection checks are not configured")
	}
	result, err := s.runnerChecks.GetConnectionCheck(ctx, request.ID, request.Task)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &runnerCheckOutput{Body: result}, nil
}
