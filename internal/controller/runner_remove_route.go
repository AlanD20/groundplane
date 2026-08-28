package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"reflect"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type runnerRemoveInput struct {
	ID             string `path:"id" required:"true" pattern:"^run_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

func (s *Server) registerRunnerRemove() {
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](), true, "TaskAccepted",
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "runner.remove", Method: http.MethodDelete, Path: "/runners/{id}",
		Summary: "Remove a managed Runner", Tags: []string{"Runner"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectRunnerRemoveBody, s.rejectRunnerRemoveQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
	}, s.removeRunner)
}

func (s *Server) removeRunner(
	ctx context.Context,
	request *runnerRemoveInput,
) (*runnerMutationOutput, error) {
	if s.runnerRemovals == nil {
		return nil, errs.New(errs.KindInternal, "Runner remover is not configured")
	}
	response, err := s.runnerRemovals.RemoveRunner(ctx, request.ID, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.runnerMutationResponse(response), nil
}

func (s *Server) rejectRunnerRemoveBody(ctx huma.Context, next func(huma.Context)) {
	var probe [1]byte
	count, err := ctx.BodyReader().Read(probe[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		s.writeRunnerRemoveProblem(ctx, "Runner removal body is not allowed")
		return
	}
	next(ctx)
}

func (s *Server) rejectRunnerRemoveQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeRunnerRemoveProblem(ctx, "Runner removal query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) writeRunnerRemoveProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Runner removal problem", slog.Any("error", err))
	}
}
