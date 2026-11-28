package handlers

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type RunnerScopedMigrationOperator interface {
	ReadScopedMigration(context.Context, string) (api.RunnerScopedMigrationStatus, error)
	StartScopedMigration(
		context.Context,
		string,
		int64,
		api.RunnerScopedMigrationRequest,
		string,
	) (idempotency.IdempotencyResponse, error)
}

type runnerScopedMigrationInput struct {
	ID      string `path:"id" pattern:"^run_[0-9A-HJKMNP-TV-Z]{26}$"`
	IfMatch string `header:"If-Match" required:"true"`
	Key     string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body    api.RunnerScopedMigrationRequest
}

type runnerScopedMigrationOutput struct {
	ETag string `header:"ETag"`
	Body api.RunnerScopedMigrationStatus
}

func (s *Server) registerRunnerScopedMigration(operator RunnerScopedMigrationOperator) {
	const path = "/runners/{id}/scoped-migration"
	huma.Register(s.API, huma.Operation{
		OperationID: "runner.scoped-migration.show", Method: http.MethodGet, Path: path,
		Summary: "Show scoped Runner migration status", Tags: []string{"Runner"},
	}, func(ctx context.Context, input *runnerShowInput) (*runnerScopedMigrationOutput, error) {
		if operator == nil {
			return nil, errs.New(errs.KindStateConflict, "Scoped Runner migration is not configured")
		}
		result, err := operator.ReadScopedMigration(ctx, input.ID)
		if err != nil {
			return nil, normalizeProjectError(err)
		}
		return &runnerScopedMigrationOutput{ETag: gitETag(result.Revision), Body: result}, nil
	})
	huma.Register(s.API, huma.Operation{
		OperationID: "runner.scoped-migration.start", Method: http.MethodPost, Path: path,
		Summary: "Move an installed Runner to scoped access after its current job completes", Tags: []string{"Runner"},
		DefaultStatus: http.StatusAccepted,
		Responses: attachMutationResponses(
			openAPISchema[api.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted"),
		),
	}, func(ctx context.Context, input *runnerScopedMigrationInput) (*componentMutationOutput, error) {
		if operator == nil {
			return nil, errs.New(errs.KindStateConflict, "Scoped Runner migration is not configured")
		}
		revision, err := gitWriteRevision(input.IfMatch, false)
		if err != nil {
			return nil, normalizeProjectError(err)
		}
		response, err := operator.StartScopedMigration(ctx, input.ID, revision, input.Body, input.Key)
		if err != nil {
			return nil, normalizeProjectError(err)
		}
		if s.controllerTaskWake != nil {
			s.controllerTaskWake()
		}
		return componentMutationResponse(response), nil
	})
	s.setRoutePolicy("POST /api/v1"+path, routePolicy{body: jsonBody})
}
