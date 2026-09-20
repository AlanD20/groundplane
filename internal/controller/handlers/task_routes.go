package handlers

import (
	"context"
	"errors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type taskListInput struct {
	Limit       int    `query:"limit" required:"false" minimum:"1" maximum:"200"`
	Cursor      string `query:"cursor" required:"false"`
	Environment string `query:"environment" required:"false" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Project     string `query:"project" required:"false" pattern:"^prj_[0-9A-HJKMNP-TV-Z]{26}$"`
	Workspace   string `query:"workspace" required:"false" pattern:"^(platform|tnt_[0-9A-HJKMNP-TV-Z]{26})$"`
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
	taskStepOpenAPISchema(s.API.OpenAPI().Components.Schemas)
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
	if s.tasks == nil {
		return nil, errs.New(errs.KindInternal, "Task repository is not configured")
	}
	if request.Limit < 0 {
		return nil, errs.New(errs.KindValidationFailed, "Task list limit must be a positive integer")
	}
	scope, err := taskListScope(request)
	if err != nil {
		return nil, err
	}
	page, err := s.tasks.ListTasksByScope(
		ctx,
		scope,
		etcdstore.PageRequest{Limit: request.Limit, Cursor: request.Cursor},
	)
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
	workspace, err := taskAPIWorkspace(record.Owner.WorkspaceType)
	if err != nil {
		return apiTypes.Task{}, err
	}
	actor, err := taskAPIActor(record.Actor)
	if err != nil {
		return apiTypes.Task{}, err
	}
	taskType, err := taskAPIType(record.Type, record.Actor)
	if err != nil {
		return apiTypes.Task{}, err
	}
	if record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() {
		return apiTypes.Task{}, errs.New(errs.KindInternal, "task has an invalid durable timeline")
	}
	return apiTypes.Task{
		ID: record.ID, OperationID: record.OperationID, RetryOf: record.RetryOf,
		PlanHash: record.PlanHash, Type: taskType, Target: record.Target, Status: status,
		WorkspaceType: workspace, TenantID: record.Owner.TenantID, ProjectID: record.Owner.ProjectID,
		EnvironmentID: record.Owner.EnvironmentID, Actor: actor,
		CreatedAt: record.CreatedAt.UTC(), UpdatedAt: record.UpdatedAt.UTC(),
		StartedAt: taskAPITime(record.StartedAt), FinishedAt: taskAPITime(record.FinishedAt),
	}, nil
}

func taskAPIType(taskType etcd.TaskType, actor etcd.TaskActor) (string, error) {
	switch taskType {
	case etcd.TaskDeploy, etcd.TaskRollback, etcd.TaskBackup, etcd.TaskBackupPrune,
		etcd.TaskRestore, etcd.TaskAttach, etcd.TaskDetach, etcd.TaskRun,
		etcd.TaskScript, etcd.TaskProvision, etcd.TaskCreate, etcd.TaskUpdate,
		etcd.TaskRemove, etcd.TaskStart, etcd.TaskStop, etcd.TaskDestroy,
		etcd.TaskRotate:
	default:
		return "", errs.New(errs.KindInternal, "task has an invalid durable type")
	}
	if taskType == etcd.TaskBackupPrune && actor != etcd.TaskActorSystem {
		return "", errs.New(errs.KindInternal, "backup_prune task has an invalid durable actor")
	}
	return string(taskType), nil
}

func taskListScope(request *taskListInput) (etcd.TaskListScope, error) {
	scopeCount := 0
	for _, value := range []string{request.Environment, request.Project, request.Workspace} {
		if value != "" {
			scopeCount++
		}
	}
	if scopeCount > 1 {
		return etcd.TaskListScope{}, errs.New(errs.KindValidationFailed, "task list scopes are mutually exclusive")
	}
	if request.Environment != "" {
		if ids.Validate(ids.KindEnvironment, request.Environment) != nil {
			return etcd.TaskListScope{}, errs.New(errs.KindValidationFailed, "task list Environment scope is invalid")
		}
		return etcd.TaskListScope{Kind: etcd.TaskListScopeEnvironment, ID: request.Environment}, nil
	}
	if request.Project != "" {
		if ids.Validate(ids.KindProject, request.Project) != nil {
			return etcd.TaskListScope{}, errs.New(errs.KindValidationFailed, "task list Project scope is invalid")
		}
		return etcd.TaskListScope{Kind: etcd.TaskListScopeProject, ID: request.Project}, nil
	}
	if request.Workspace == "platform" {
		return etcd.TaskListScope{Kind: etcd.TaskListScopePlatformWorkspace}, nil
	}
	if request.Workspace != "" {
		if ids.Validate(ids.KindTenant, request.Workspace) != nil {
			return etcd.TaskListScope{}, errs.New(errs.KindValidationFailed, "task list Tenant workspace is invalid")
		}
		return etcd.TaskListScope{Kind: etcd.TaskListScopeTenantWorkspace, ID: request.Workspace}, nil
	}
	return etcd.TaskListScope{Kind: etcd.TaskListScopeGlobal}, nil
}

func taskAPIWorkspace(workspace etcd.TaskWorkspaceType) (apiTypes.TaskWorkspaceType, error) {
	switch workspace {
	case etcd.TaskWorkspacePlatform:
		return apiTypes.TaskWorkspacePlatform, nil
	case etcd.TaskWorkspaceTenant:
		return apiTypes.TaskWorkspaceTenant, nil
	default:
		return "", errs.New(errs.KindInternal, "task has an invalid durable workspace")
	}
}

func taskAPIActor(actor etcd.TaskActor) (apiTypes.TaskActor, error) {
	switch actor {
	case etcd.TaskActorOperator:
		return apiTypes.TaskActorOperator, nil
	case etcd.TaskActorSystem:
		return apiTypes.TaskActorSystem, nil
	default:
		return "", errs.New(errs.KindInternal, "task has an invalid durable actor")
	}
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
		case "environment", "project", "workspace", "limit", "cursor":
		default:
			s.writeTaskListProblem(ctx, http.StatusBadRequest, "Task list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeTaskListProblem(ctx, http.StatusBadRequest, "Task list query contains duplicate values")
			return
		}
	}
	scopeCount := 0
	for _, key := range []string{"environment", "project", "workspace"} {
		if query.Has(key) {
			scopeCount++
		}
	}
	if scopeCount > 1 {
		s.writeTaskListProblem(ctx, http.StatusUnprocessableEntity, "Task list scopes are mutually exclusive")
		return
	}
	next(ctx)
}

func (s *Server) writeTaskListProblem(ctx huma.Context, status int, detail string) {
	if err := huma.WriteErr(s.API, ctx, status, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Task list problem", slog.Any("error", err))
	}
}
