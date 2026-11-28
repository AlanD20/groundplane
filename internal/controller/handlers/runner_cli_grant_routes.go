package handlers

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/controller/runneridentity"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type RunnerCLIGrantOperator interface {
	ReadCLIGrant(context.Context, string) (api.RunnerCLIGrantStatus, error)
	WriteCLIGrant(context.Context, string, int64, api.RunnerCLIGrantWrite, string) (api.RunnerCLIGrantStatus, error)
	RevokeCLIGrant(context.Context, string, int64, string) error
}

type RunnerCLIGrantMutationInput struct {
	ID      string `path:"id" pattern:"^run_[0-9A-HJKMNP-TV-Z]{26}$"`
	IfMatch string `header:"If-Match" required:"true"`
	Key     string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type runnerCLIGrantWriteInput struct {
	RunnerCLIGrantMutationInput
	Body api.RunnerCLIGrantWrite
}

type runnerCLIGrantOutput struct {
	CacheControl string `header:"Cache-Control"`
	ETag         string `header:"ETag"`
	Body         api.RunnerCLIGrantStatus
}

func (s *Server) registerRunnerCLIGrant(operator RunnerCLIGrantOperator) {
	const route = runneridentity.CLIGrantRoute
	huma.Register(s.API, huma.Operation{
		OperationID: "runner.cli-grant.show", Method: http.MethodGet, Path: route,
		Summary: "Show the CLI grant attached to the Runner's scoped credential", Tags: []string{"Runner"},
	}, func(ctx context.Context, input *runnerShowInput) (*runnerCLIGrantOutput, error) {
		if operator == nil {
			return nil, errs.New(errs.KindStateConflict, "Scoped Runner CLI grants are not configured")
		}
		view, err := operator.ReadCLIGrant(ctx, input.ID)
		return runnerCLIGrantResponse(view, err)
	})
	huma.Register(s.API, huma.Operation{
		OperationID: "runner.cli-grant.set", Method: http.MethodPut, Path: route,
		Summary: "Replace the exact Runner CLI grant for the checked revision", Tags: []string{"Runner"},
	}, func(ctx context.Context, input *runnerCLIGrantWriteInput) (*runnerCLIGrantOutput, error) {
		if operator == nil {
			return nil, errs.New(errs.KindStateConflict, "Scoped Runner CLI grants are not configured")
		}
		revision, err := gitWriteRevision(input.IfMatch, true)
		if err != nil {
			return nil, normalizeProjectError(err)
		}
		view, err := operator.WriteCLIGrant(ctx, input.ID, revision, input.Body, input.Key)
		return runnerCLIGrantResponse(view, err)
	})
	huma.Register(s.API, huma.Operation{
		OperationID: "runner.cli-grant.revoke", Method: http.MethodDelete, Path: route,
		DefaultStatus: http.StatusNoContent,
		Summary:       "Revoke CLI actions while keeping Runner build authentication", Tags: []string{"Runner"},
	}, func(ctx context.Context, input *RunnerCLIGrantMutationInput) (*struct{}, error) {
		if operator == nil {
			return nil, errs.New(errs.KindStateConflict, "Scoped Runner CLI grants are not configured")
		}
		revision, err := gitWriteRevision(input.IfMatch, false)
		if err != nil {
			return nil, normalizeProjectError(err)
		}
		if err := operator.RevokeCLIGrant(ctx, input.ID, revision, input.Key); err != nil {
			return nil, normalizeProjectError(err)
		}
		return &struct{}{}, nil
	})
	s.setRoutePolicy("PUT /api/v1"+route, routePolicy{body: jsonBody})
}

func runnerCLIGrantResponse(view api.RunnerCLIGrantStatus, err error) (*runnerCLIGrantOutput, error) {
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &runnerCLIGrantOutput{ETag: gitETag(view.Revision), CacheControl: "no-store", Body: view}, nil
}
