package controller

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type taskListInput struct {
	Limit  int    `query:"limit" required:"false"`
	Cursor string `query:"cursor" required:"false"`
}

type taskShowInput struct {
	ID string `path:"id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type taskPageOutput struct {
	Body apiTypes.Page[apiTypes.Task]
}

type taskOutput struct {
	Body apiTypes.Task
}

func (s *Server) registerTasks() {
	huma.Register(s.API, huma.Operation{
		OperationID: "task.list", Method: http.MethodGet, Path: "/tasks",
		Summary: "List tasks", Tags: []string{"Task"},
		Middlewares: huma.Middlewares{s.validateTaskListQuery},
	}, s.listTasks)
	huma.Register(s.API, huma.Operation{
		OperationID: "activity.list", Method: http.MethodGet, Path: "/activity",
		Summary: "List activity", Tags: []string{"Task"},
		Middlewares: huma.Middlewares{s.validateTaskListQuery},
	}, s.listTasks)
	huma.Register(s.API, huma.Operation{
		OperationID: "task.show", Method: http.MethodGet, Path: "/tasks/{id}",
		Summary: "Show a task", Tags: []string{"Task"},
	}, s.showTask)
	s.registerTaskEventStream()
}

func (s *Server) listTasks(ctx context.Context, request *taskListInput) (*taskPageOutput, error) {
	if s.tasks == nil {
		return nil, errs.New(errs.KindInternal, "Task repository is not configured")
	}
	if request.Limit < 0 {
		return nil, errs.New(errs.KindValidationFailed, "Task list limit must be a positive integer")
	}
	page, err := s.tasks.ListTasks(ctx, etcd.PageRequest{Limit: request.Limit, Cursor: request.Cursor})
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	response := apiTypes.Page[apiTypes.Task]{
		Items: make([]apiTypes.Task, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, versioned := range page.Items {
		response.Items[index], err = taskListResponse(versioned.Record)
		if err != nil {
			return nil, normalizeTaskRouteError(err)
		}
	}
	return &taskPageOutput{Body: response}, nil
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

func taskListResponse(record etcd.TaskRecord) (apiTypes.Task, error) {
	status, err := taskAPIStatus(record.Status)
	if err != nil {
		return apiTypes.Task{}, err
	}
	return apiTypes.Task{
		ID: record.ID, OperationID: record.OperationID, RetryOf: record.RetryOf,
		PlanHash: record.PlanHash, Type: string(record.Type), Target: record.Target, Status: status,
	}, nil
}

func (s *Server) validateTaskListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		switch key {
		case "environment", "workspace":
			s.writeTaskListProblem(ctx, http.StatusNotImplemented, "scoped Task listing is not implemented")
			return
		case "limit", "cursor":
		default:
			s.writeTaskListProblem(ctx, http.StatusBadRequest, "Task list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeTaskListProblem(ctx, http.StatusBadRequest, "Task list query contains duplicate values")
			return
		}
	}
	next(ctx)
}

func (s *Server) writeTaskListProblem(ctx huma.Context, status int, detail string) {
	if err := huma.WriteErr(s.API, ctx, status, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Task list problem", slog.Any("error", err))
	}
}
