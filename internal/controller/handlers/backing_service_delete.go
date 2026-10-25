package handlers

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/danielgtaylor/huma/v2"
)

func (s *Server) registerBackingServiceDeletion() {
	huma.Register(s.API, huma.Operation{
		OperationID: "backing-service.delete", Method: http.MethodDelete,
		Path:    hierarchydeletion.BackingDeleteRoute,
		Summary: "Permanently delete a Backing Service and its data",
		Tags:    []string{"Backing service"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectTaskMutationBody, s.rejectTaskMutationQuery},
	}, func(ctx context.Context, input *backingServiceActionInput) (*hierarchyDeleteOutput, error) {
		if s.hierarchyDeletions == nil {
			return nil, hierarchyDeletionUnavailable()
		}
		accepted, err := s.hierarchyDeletions.Delete(ctx, hierarchydeletion.DeleteRequest{
			TargetKind: hierarchydeletion.TargetBackingService,
			TargetID:   input.ProjectID, IdempotencyKey: input.IdempotencyKey,
		})
		if err != nil {
			return nil, err
		}
		output := &hierarchyDeleteOutput{Status: http.StatusAccepted}
		output.Body.TaskID = accepted.TaskID
		return output, nil
	})
}
