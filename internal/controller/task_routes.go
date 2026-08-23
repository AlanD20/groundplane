package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"reflect"

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

type taskMutationInput struct {
	ID             string `path:"id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true"`
}

type taskPageOutput struct {
	Body apiTypes.Page[apiTypes.Task]
}

type taskOutput struct {
	Body apiTypes.Task
}

type taskMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerTasks() {
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](), true, "TaskAccepted",
	)
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
	huma.Register(s.API, huma.Operation{
		OperationID: "task.retry", Method: http.MethodPost, Path: "/tasks/{id}/retry",
		Summary: "Retry a failed task", Tags: []string{"Task"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectTaskMutationBody, s.rejectTaskMutationQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
	}, s.retryTask)
	huma.Register(s.API, huma.Operation{
		OperationID: "task.abort", Method: http.MethodPost, Path: "/tasks/{id}/abort",
		Summary: "Abort an in-flight task", Tags: []string{"Task"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectTaskMutationBody, s.rejectTaskMutationQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
	}, s.abortTask)
	s.registerTaskEventStream()
}

func (s *Server) retryTask(
	ctx context.Context,
	request *taskMutationInput,
) (*taskMutationOutput, error) {
	if s.taskMutations == nil {
		return nil, errs.New(errs.KindInternal, "Task retrier is not configured")
	}
	response, err := s.taskMutations.RetryTask(ctx, request.ID, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	return s.taskMutationResponse(response), nil
}

func (s *Server) abortTask(
	ctx context.Context,
	request *taskMutationInput,
) (*taskMutationOutput, error) {
	if s.taskAborts == nil {
		return nil, errs.New(errs.KindInternal, "Task abort service is not configured")
	}
	response, err := s.taskAborts.AbortTask(ctx, request.ID, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	return s.taskMutationResponse(response), nil
}

func (s *Server) taskMutationResponse(response etcd.IdempotencyResponse) *taskMutationOutput {
	return &taskMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error("controller: write Task mutation response", slog.Any("error", err))
			}
		},
	}
}

func (s *Server) rejectTaskMutationBody(ctx huma.Context, next func(huma.Context)) {
	var probe [1]byte
	count, err := ctx.BodyReader().Read(probe[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		s.writeTaskRequestProblem(ctx, "Task mutation body is not allowed")
		return
	}
	next(ctx)
}

func (s *Server) rejectTaskMutationQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeTaskRequestProblem(ctx, "Task mutation query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) writeTaskRequestProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Task mutation problem", slog.Any("error", err))
	}
}

func (s *Server) writeTaskProblem(w http.ResponseWriter, err error) {
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		s.writeProblem(w, domainError)
		return
	}
	s.writeProblem(w, errs.Wrap(errs.KindInternal, err))
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
