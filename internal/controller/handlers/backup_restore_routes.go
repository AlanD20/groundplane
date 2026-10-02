package handlers

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type BackupRestorer interface {
	Restore(context.Context, string, string, apiTypes.RestoreRequest) (idempotency.IdempotencyResponse, error)
}

type backupRestoreInput struct {
	ID   string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key  string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body apiTypes.RestoreRequest
}

func (s *Server) registerBackupRestore() {
	accepted := openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted")
	huma.Register(s.API, huma.Operation{
		OperationID: "backup.restore", Method: http.MethodPost, Path: "/environments/{id}/restore",
		Summary: "Restore the selected Recovery Point to its original target", Tags: []string{"Backup"},
		DefaultStatus: http.StatusAccepted, Middlewares: huma.Middlewares{s.rejectTaskMutationQuery},
		Responses: attachMutationResponses(accepted),
	}, s.restoreBackup)
}

func (s *Server) restoreBackup(ctx context.Context, request *backupRestoreInput) (*taskMutationOutput, error) {
	if s.backupRestores == nil {
		return nil, errs.New(errs.KindInternal, "Restore service is not configured")
	}
	response, err := s.backupRestores.Restore(ctx, request.ID, request.Key, request.Body)
	request.Body.AgeIdentity = ""
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	return s.taskMutationResponse(response), nil
}
