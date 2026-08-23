package controller

import (
	"context"
	"errors"
	"net/http"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type taskShowInput struct {
	ID string `path:"id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type taskOutput struct {
	Body apiTypes.Task
}

func (s *Server) registerTasks() {
	huma.Register(s.API, huma.Operation{
		OperationID: "task.show", Method: http.MethodGet, Path: "/tasks/{id}",
		Summary: "Show a task", Tags: []string{"Task"},
	}, s.showTask)
	s.registerTaskEventStream()
}

func (s *Server) showTask(ctx context.Context, request *taskShowInput) (*taskOutput, error) {
	if s.tasks == nil {
		return nil, errs.New(errs.KindInternal, "Task repository is not configured")
	}
	task, err := s.tasks.GetTask(ctx, request.ID)
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	events, err := s.tasks.ListTaskEvents(ctx, task.Record.ID, task.ReadRevision)
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	response, err := taskResponse(task.Record, events)
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	return &taskOutput{Body: response}, nil
}

func normalizeTaskRouteError(err error) error {
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		return domainError
	}
	return errs.Wrap(errs.KindInternal, err)
}
