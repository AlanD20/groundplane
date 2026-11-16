package handlers

import (
	"context"
	"net/http"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type backupRunInput struct {
	ID             string                        `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string                        `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           *apiTypes.TaskRunAfterRequest `required:"false"`
}

func (s *Server) registerBackupRuns() {
	taskAcceptedSchema := openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted")
	s.setRoutePolicy("POST /api/v1/environments/{id}/backup-run", routePolicy{body: jsonBody})
	huma.Register(s.API, huma.Operation{
		OperationID:   "backup.run",
		Method:        http.MethodPost,
		Path:          "/environments/{id}/backup-run",
		Summary:       "Run all configured backup sources now",
		Tags:          []string{"Backup"},
		DefaultStatus: http.StatusAccepted,
		Middlewares:   huma.Middlewares{s.rejectTaskMutationQuery},
		Responses:     attachMutationResponses(taskAcceptedSchema),
	}, s.runBackup)
}

func (s *Server) runBackup(ctx context.Context, request *backupRunInput) (*taskMutationOutput, error) {
	if s.backupRuns == nil {
		return nil, errs.New(errs.KindInternal, "backup run service is not configured")
	}
	response, err := s.backupRuns.RunBackup(ctx, request.ID, request.IdempotencyKey, taskRunAfter(request.Body))
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	return s.taskMutationResponse(response), nil
}
