package handlers

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskinspection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"time"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type taskListInput struct {
	ResourceKind string `query:"resource_kind"`
	Scope        string `query:"scope" default:"all"`
	Limit        int64  `query:"limit" default:"25" minimum:"1" maximum:"100"`
	After        string `query:"after"`
	Revision     int64  `query:"revision" minimum:"0"`
	Action       string `query:"action"`
	Status       string `query:"status"`
}
type taskShowInput struct {
	ID       string `path:"id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit    int64  `query:"limit" default:"25" minimum:"1" maximum:"100"`
	After    string `query:"after"`
	Revision int64  `query:"revision" minimum:"0"`
}

type taskMutationInput struct {
	ID             string `path:"id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true"`
}

type taskPageOutput struct {
	Body apiTypes.ExecutionPage[apiTypes.TaskSummary]
}

type taskOutput struct {
	Body apiTypes.TaskDetails
}

type taskMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerTasks() {
	s.registerTaskWork()
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
		Summary: "Retry the selected captured work attempt within the same Task", Tags: []string{"Task"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectTaskMutationQuery},
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
	request *taskRetryInput,
) (*taskMutationOutput, error) {
	if s.taskMutations == nil {
		return nil, errs.New(errs.KindInternal, "Task retrier is not configured")
	}
	response, err := s.taskMutations.RetryTask(ctx, request.ID, request.IdempotencyKey, request.Body)
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

func (s *Server) taskMutationResponse(response idempotencyrecord.IdempotencyResponse) *taskMutationOutput {
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
	if s.taskInspection == nil {
		return nil, errs.New(errs.KindInternal, "task inspection is not configured")
	}
	page, err := s.taskInspection.List(ctx, request.Scope, request.After, request.Limit, request.Revision,
		taskinspection.Filter{Action: workexecution.Action(request.Action), Status: request.Status, ResourceKind: request.ResourceKind})
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	response := apiTypes.ExecutionPage[apiTypes.TaskSummary]{Items: []apiTypes.TaskSummary{}, More: page.More, After: page.After, Revision: page.Revision, ScanLimited: page.ScanLimited}
	for _, task := range page.Tasks {
		response.Items = append(response.Items, executionSummary(task))
	}
	return &taskPageOutput{Body: response}, nil
}
func (s *Server) showTask(ctx context.Context, request *taskShowInput) (*taskOutput, error) {
	if s.taskInspection == nil {
		return nil, errs.New(errs.KindInternal, "task inspection is not configured")
	}
	page, err := s.taskInspection.WorkPage(ctx, request.ID, request.After, request.Limit, request.Revision)
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	work := apiTypes.ExecutionPage[apiTypes.TaskWork]{Items: []apiTypes.TaskWork{}, More: page.More, After: page.After, Revision: page.Revision}
	for _, item := range page.Work {
		work.Items = append(work.Items, executionWork(item))
	}
	detail := apiTypes.TaskDetails{Task: executionSummary(page.Task), Work: work}
	if value := page.Preparation; value != nil {
		detail.Preparation = &apiTypes.TaskPreparation{Reason: value.Reason, Outcome: value.Outcome,
			Summary: value.Summary, NextAction: value.NextAction, FinishedAt: value.FinishedAt}
	}
	return &taskOutput{Body: detail}, nil
}

func normalizeTaskRouteError(err error) error {
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		return domainError
	}
	return errs.Wrap(errs.KindInternal, err)
}

func taskAPITime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

func (s *Server) validateTaskListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	query := requestURL.Query()
	for key, values := range query {
		switch key {
		case "scope", "limit", "after", "revision", "action", "status", "resource_kind":
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
