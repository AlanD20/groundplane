package handlers

import (
	"context"
	"net/http"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type RecoveryPointRemover interface {
	RemoveRecoveryPoint(context.Context, string, string, string) (idempotencyrecord.IdempotencyResponse, error)
}

type recoveryPointRemoveInput struct {
	ID    string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Point string `path:"point" pattern:"^rp_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key   string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

func (s *Server) registerRecoveryPointRemoval() {
	huma.Register(s.API, huma.Operation{
		OperationID: "backup.points.remove", Method: http.MethodDelete,
		Path: "/environments/{id}/recovery-points/{point}", Summary: "Permanently delete one Recovery Point archive",
		Tags: []string{"Backup"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectTaskMutationBody, s.rejectTaskMutationQuery},
		Responses: attachMutationResponses(
			openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted"),
		),
	}, s.removeRecoveryPoint)
}

func (s *Server) removeRecoveryPoint(
	ctx context.Context,
	input *recoveryPointRemoveInput,
) (*taskMutationOutput, error) {
	if s.recoveryPointRemover == nil {
		return nil, errs.New(errs.KindInternal, "recovery point removal is not configured")
	}
	response, err := s.recoveryPointRemover.RemoveRecoveryPoint(ctx, input.ID, input.Point, input.Key)
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	return s.taskMutationResponse(response), nil
}
