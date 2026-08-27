package controller

import (
	"context"
	"net/http"
	"reflect"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type backupRunInput struct {
	ID             string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

func (s *Server) registerBackupRuns() {
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](), true, "TaskAccepted",
	)
	huma.Register(s.API, huma.Operation{
		OperationID:   "backup.run",
		Method:        http.MethodPost,
		Path:          "/environments/{id}/backup-run",
		Summary:       "Run all configured backup sources now",
		Tags:          []string{"Backup"},
		DefaultStatus: http.StatusAccepted,
		Middlewares:   huma.Middlewares{s.rejectTaskMutationBody, s.rejectTaskMutationQuery},
		Responses:     attachMutationResponses(taskAcceptedSchema),
	}, s.runBackup)
}

func (s *Server) runBackup(ctx context.Context, request *backupRunInput) (*taskMutationOutput, error) {
	if s.backupRuns == nil {
		return nil, errs.New(errs.KindInternal, "backup run service is not configured")
	}
	response, err := s.backupRuns.RunBackup(ctx, request.ID, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	return s.taskMutationResponse(response), nil
}
