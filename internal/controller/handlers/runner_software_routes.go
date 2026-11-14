package handlers

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type RunnerSoftware interface {
	Versions(context.Context, string) (apiTypes.RunnerVersions, error)
	UpdateRunner(context.Context, string, apiTypes.RunnerUpdateRequest, string) (idempotency.IdempotencyResponse, error)
	GetUpdate(context.Context, string, string) (apiTypes.RunnerUpdateResult, error)
}
type runnerVersionOutput struct{ Body apiTypes.RunnerVersions }
type runnerUpdateInput struct {
	ID   string `path:"id" pattern:"^run_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key  string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body apiTypes.RunnerUpdateRequest
}
type runnerUpdateOutput struct{ Body apiTypes.RunnerUpdateResult }

func (s *Server) registerRunnerSoftware() {
	accepted := openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted")
	huma.Register(
		s.API,
		huma.Operation{
			OperationID: "runner.versions",
			Method:      http.MethodGet,
			Path:        "/runners/{id}/versions",
			Summary:     "List verified GitHub Runner versions for this host",
			Tags:        []string{"Runner"},
		},
		s.runnerVersions,
	)
	huma.Register(
		s.API,
		huma.Operation{
			OperationID:   "runner.update",
			Method:        http.MethodPost,
			Path:          "/runners/{id}/updates",
			Summary:       "Update only this Runner upstream software after its job completes",
			Tags:          []string{"Runner"},
			DefaultStatus: http.StatusAccepted,
			Responses:     attachMutationResponses(accepted),
		},
		s.updateRunnerSoftware,
	)
	huma.Register(
		s.API,
		huma.Operation{
			OperationID: "runner.update.show",
			Method:      http.MethodGet,
			Path:        "/runners/{id}/updates",
			Summary:     "Show latest or selected Runner software update",
			Tags:        []string{"Runner"},
		},
		s.showRunnerUpdate,
	)
	s.setRoutePolicy("POST /api/v1/runners/{id}/updates", routePolicy{body: jsonBody})
}
func (s *Server) runnerVersions(ctx context.Context, input *runnerShowInput) (*runnerVersionOutput, error) {
	if s.runnerSoftware == nil {
		return nil, errs.New(errs.KindInternal, "Runner software updates are not configured")
	}
	result, err := s.runnerSoftware.Versions(ctx, input.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &runnerVersionOutput{Body: result}, nil
}
func (s *Server) updateRunnerSoftware(ctx context.Context, input *runnerUpdateInput) (*componentMutationOutput, error) {
	if s.runnerSoftware == nil {
		return nil, errs.New(errs.KindInternal, "Runner software updates are not configured")
	}
	result, err := s.runnerSoftware.UpdateRunner(ctx, input.ID, input.Body, input.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if s.controllerTaskWake != nil {
		s.controllerTaskWake()
	}
	return componentMutationResponse(result), nil
}
func (s *Server) showRunnerUpdate(ctx context.Context, input *runnerCheckShowInput) (*runnerUpdateOutput, error) {
	if s.runnerSoftware == nil {
		return nil, errs.New(errs.KindInternal, "Runner software updates are not configured")
	}
	result, err := s.runnerSoftware.GetUpdate(ctx, input.ID, input.Task)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &runnerUpdateOutput{Body: result}, nil
}
