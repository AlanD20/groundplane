package controller

import (
	"context"
	"log/slog"
	"net/http"
	"reflect"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type runnerCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.RunnerCreateRequest
}

type runnerRetryInput struct {
	ID             string `path:"id" required:"true" pattern:"^run_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.RunnerRetryRequest
}

func (s *Server) registerRunnerProvisioning() {
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](), true, "TaskAccepted",
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "runner.create", Method: http.MethodPost, Path: "/runners",
		Summary: "Create a managed Runner", Tags: []string{"Runner"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectRunnerProvisioningQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
	}, s.createRunner)
	huma.Register(s.API, huma.Operation{
		OperationID: "runner.retry", Method: http.MethodPost, Path: "/runners/{id}/retry",
		Summary: "Retry failed Runner creation", Tags: []string{"Runner"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectRunnerProvisioningQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
	}, s.retryRunner)
	s.setRoutePolicy("POST /api/v1/runners", routePolicy{body: jsonBody})
	s.setRoutePolicy("POST /api/v1/runners/{id}/retry", routePolicy{body: jsonBody})
}

func (s *Server) createRunner(
	ctx context.Context,
	request *runnerCreateInput,
) (*runnerMutationOutput, error) {
	if s.runnerProvisioning == nil {
		return nil, errs.New(errs.KindInternal, "Runner provisioner is not configured")
	}
	defer func() { request.Body.RegistrationToken = "" }()
	response, err := s.runnerProvisioning.CreateRunner(ctx, request.Body, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.runnerMutationResponse(response), nil
}

func (s *Server) retryRunner(
	ctx context.Context,
	request *runnerRetryInput,
) (*runnerMutationOutput, error) {
	if s.runnerProvisioning == nil {
		return nil, errs.New(errs.KindInternal, "Runner provisioner is not configured")
	}
	defer func() { request.Body.RegistrationToken = "" }()
	response, err := s.runnerProvisioning.RetryRunner(
		ctx, request.ID, request.Body, request.IdempotencyKey,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.runnerMutationResponse(response), nil
}

func (s *Server) rejectRunnerProvisioningQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, "Runner mutation query is invalid"); err != nil && s.Logger != nil {
			s.Logger.Error("controller: write Runner mutation problem", slog.Any("error", err))
		}
		return
	}
	next(ctx)
}
