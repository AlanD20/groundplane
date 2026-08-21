// Package controller is the API server: schema validation, task
// sequencing, serialization locks, config rendering, the secret store,
// and the scheduler tick. It is the ONE backend — Console, CLI, and
// scripts all speak this REST/JSON surface. See architecture.md,
// "internal/controller", and api-cli.md's full resource map, which this
// file's route table mirrors 1:1.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
)

// Server holds everything a request handler needs. Construct once in
// cmd/controller and pass down; handlers are methods on *Server so they
// share the store/logger without globals.
type Server struct {
	Store  etcd.Store
	Logger *slog.Logger
	Mux    *http.ServeMux
	API    huma.API

	host          HostReader
	console       fs.FS
	dispatcher    *Dispatcher
	tasks         taskQueries
	routePolicies map[string]routePolicy
}

type Options struct {
	Host    HostReader
	Console fs.FS
	Tasks   *etcd.TaskRepository
}

type taskQueries interface {
	GetTask(context.Context, string) (etcd.Versioned[etcd.TaskRecord], error)
	ListTasks(context.Context, etcd.PageRequest) (etcd.Page[etcd.TaskRecord], error)
	ListTaskEvents(context.Context, string, int64) (etcd.TaskEventSnapshot, error)
}

func New(store etcd.Store, logger *slog.Logger, options Options) *Server {
	configureProblemResponses()
	mux := http.NewServeMux()
	config := huma.DefaultConfig("Groundplane API", version.Value)
	config.OpenAPIPath = ""
	config.DocsPath = ""
	config.SchemasPath = ""
	config.RejectUnknownQueryParameters = true

	s := &Server{
		Store:         store,
		Logger:        logger,
		Mux:           mux,
		API:           humago.NewWithPrefix(mux, "/api/v1", config),
		host:          options.Host,
		console:       options.Console,
		dispatcher:    NewDispatcher(),
		tasks:         options.Tasks,
		routePolicies: make(map[string]routePolicy),
	}
	s.routes()
	s.registerHost()
	return s
}

// routes registers every endpoint from api-cli.md's resource map. Method
// + pattern routing (Go 1.22 net/http). Unimplemented resources remain
// explicit 501 handlers until their vertical is delivered; typed Huma routes,
// such as GET /host, register alongside this table.
func (s *Server) routes() {
	mux := s.Mux

	// OpenAPI — served from the code-first generator once wired (Huma /
	// oapi-codegen). TODO.
	mux.HandleFunc("GET /openapi.json", s.notImplemented)

	// tenant — destructive delete is a task (api-cli.md's resource map)
	mux.HandleFunc("GET /api/v1/tenants", s.notImplemented)
	s.jsonRoute("POST /api/v1/tenants", s.notImplemented)
	mux.HandleFunc("GET /api/v1/tenants/{id}", s.notImplemented)
	s.jsonRoute("PATCH /api/v1/tenants/{id}", s.notImplemented)
	s.jsonRoute("POST /api/v1/tenants/{id}/rename", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/tenants/{id}", s.acceptTask)

	// project (?kind=tenant|backing)
	mux.HandleFunc("GET /api/v1/projects", s.notImplemented)
	s.jsonRoute("POST /api/v1/projects", s.notImplemented)
	mux.HandleFunc("GET /api/v1/projects/{id}", s.notImplemented)
	s.jsonRoute("PATCH /api/v1/projects/{id}", s.notImplemented)
	s.jsonRoute("POST /api/v1/projects/{id}/rename", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/projects/{id}", s.acceptTask)

	// environment (?project=)
	mux.HandleFunc("GET /api/v1/environments", s.notImplemented)
	s.jsonRoute("POST /api/v1/environments", s.notImplemented)
	mux.HandleFunc("GET /api/v1/environments/{id}", s.notImplemented)
	s.jsonRoute("POST /api/v1/environments/{id}/rename", s.notImplemented)
	s.streamRoute("GET /api/v1/environments/{id}/logs", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/environments/{id}", s.acceptTask)

	// environment singleton sub-resources
	mux.HandleFunc("GET /api/v1/environments/{id}/backup-policy", s.notImplemented)
	s.jsonRoute("PUT /api/v1/environments/{id}/backup-policy", s.notImplemented)
	s.blueprintRoute("PUT /api/v1/environments/{id}/blueprint", s.acceptTask)
	mux.HandleFunc("GET /api/v1/environments/{id}/recovery-points", s.notImplemented)
	s.jsonRoute("POST /api/v1/environments/{id}/backup-run", s.acceptTask)
	s.jsonRoute("POST /api/v1/environments/{id}/restore", s.acceptTask)
	s.jsonRoute("POST /api/v1/environments/{id}/rotate-key", s.acceptTask)
	mux.HandleFunc("POST /api/v1/environments/{id}/export-key", s.notImplemented)
	// router: READ-ONLY projection grouping ingress components — GET only,
	// never PUT (api-cli.md, section 4). Managed entirely through /components.
	mux.HandleFunc("GET /api/v1/environments/{id}/router", s.notImplemented)

	// service (?environment=) — deploy/rollback/start/stop/destroy return a task
	mux.HandleFunc("GET /api/v1/services", s.notImplemented)
	s.jsonRoute("POST /api/v1/services", s.notImplemented)
	mux.HandleFunc("GET /api/v1/services/{id}", s.notImplemented) // includes the release ledger
	s.jsonRoute("PATCH /api/v1/services/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/services/{id}", s.acceptTask)
	s.jsonRoute("POST /api/v1/services/{id}/deploy", s.acceptTask) // {tag?, strategy?, on_failure?}
	s.jsonRoute("POST /api/v1/services/{id}/rollback", s.acceptTask)
	s.jsonRoute("POST /api/v1/services/{id}/start", s.acceptTask)
	s.jsonRoute("POST /api/v1/services/{id}/stop", s.acceptTask)
	s.jsonRoute("POST /api/v1/services/{id}/destroy", s.acceptTask)
	s.streamRoute("GET /api/v1/services/{id}/logs", s.notImplemented)

	// release-group (?environment=)
	mux.HandleFunc("GET /api/v1/release-groups", s.notImplemented)
	s.jsonRoute("POST /api/v1/release-groups", s.notImplemented)
	mux.HandleFunc("GET /api/v1/release-groups/{id}", s.notImplemented)
	s.jsonRoute("PATCH /api/v1/release-groups/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/release-groups/{id}", s.acceptTask)
	s.jsonRoute("POST /api/v1/release-groups/{id}/deploy", s.acceptTask)
	s.jsonRoute("POST /api/v1/release-groups/{id}/rollback", s.acceptTask)

	// attach — attach provisions (joins the owned external network,
	// publishes facts), detach deprovisions; both tasks
	mux.HandleFunc("GET /api/v1/attaches", s.notImplemented)
	s.jsonRoute("POST /api/v1/attaches", s.acceptTask) // {service_id, backing_service_id, name?, grants?}
	mux.HandleFunc("DELETE /api/v1/attaches/{id}", s.acceptTask)

	// zone / route / volume / entry / script (?environment=) — destructive delete is a task
	for _, res := range []string{"zones", "routes", "volumes", "entries", "scripts"} {
		mux.HandleFunc("GET /api/v1/"+res, s.notImplemented)
		s.jsonRoute("POST /api/v1/"+res, s.notImplemented)
		mux.HandleFunc("GET /api/v1/"+res+"/{id}", s.notImplemented)
		s.jsonRoute("PATCH /api/v1/"+res+"/{id}", s.notImplemented)
		mux.HandleFunc("DELETE /api/v1/"+res+"/{id}", s.acceptTask)
	}
	mux.HandleFunc("GET /api/v1/entries/{id}/value", s.notImplemented)
	s.jsonRoute("POST /api/v1/scripts/{id}/run", s.acceptTask) // {parameters?}

	// component (?environment= or ?platform=true) — one resource across both owners
	mux.HandleFunc("GET /api/v1/components", s.notImplemented)
	mux.HandleFunc("GET /api/v1/components/{id}", s.notImplemented)
	s.jsonRoute("POST /api/v1/components/{id}/enable", s.acceptTask)
	s.jsonRoute("POST /api/v1/components/{id}/disable", s.acceptTask)
	s.jsonRoute("POST /api/v1/components/{id}/update", s.acceptTask)
	mux.HandleFunc("GET /api/v1/components/{id}/config", s.notImplemented)
	s.jsonRoute("PUT /api/v1/components/{id}/config", s.notImplemented)

	// backing-service
	mux.HandleFunc("GET /api/v1/backing-services", s.notImplemented)
	s.jsonRoute("POST /api/v1/backing-services", s.notImplemented)
	mux.HandleFunc("GET /api/v1/backing-services/{project_id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/backing-services/{project_id}", s.acceptTask)
	s.jsonRoute("POST /api/v1/backing-services/{project_id}/start", s.acceptTask)
	s.jsonRoute("POST /api/v1/backing-services/{project_id}/stop", s.acceptTask)
	s.jsonRoute("POST /api/v1/backing-services/{project_id}/destroy", s.acceptTask)

	// secret (?project=) — project-scoped, locked
	mux.HandleFunc("GET /api/v1/secrets", s.notImplemented)
	s.jsonRoute("POST /api/v1/secrets", s.notImplemented)
	mux.HandleFunc("GET /api/v1/secrets/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/secrets/{id}", s.acceptTask)
	mux.HandleFunc(
		"GET /api/v1/secrets/{id}/value",
		s.notImplemented,
	) // reveal — Console-only preference, not access control

	// connector (?environment= required) — environment-scoped only
	mux.HandleFunc("GET /api/v1/connectors", s.notImplemented)
	s.jsonRoute("POST /api/v1/connectors", s.notImplemented)
	mux.HandleFunc("GET /api/v1/connectors/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/connectors/{id}", s.acceptTask)

	// runner (?tenant= or ?project=) — org-scoped or repo-scoped
	mux.HandleFunc("GET /api/v1/runners", s.notImplemented)
	s.jsonRoute(
		"POST /api/v1/runners",
		s.notImplemented,
	) // {tenant_id|project_id, registration_token} — token discarded after registration
	mux.HandleFunc("GET /api/v1/runners/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/runners/{id}", s.acceptTask)

	// task / activity
	mux.HandleFunc("GET /api/v1/tasks", s.taskList)
	mux.HandleFunc("GET /api/v1/tasks/{id}", s.taskShow)
	s.streamRoute("GET /api/v1/tasks/{id}/events", s.notImplemented)
	s.jsonRoute("POST /api/v1/tasks/{id}/retry", s.retryTask)
	s.jsonRoute("POST /api/v1/tasks/{id}/abort", s.acceptTask)
	mux.HandleFunc("GET /api/v1/activity", s.taskList) // exact JSON alias of Task list

	// host / agents
	s.jsonRoute("POST /api/v1/agents", s.acceptTask)
	mux.HandleFunc("GET /api/v1/agents", s.notImplemented)
	mux.HandleFunc("GET /api/v1/agents/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/agents/{id}", s.acceptTask)
	mux.HandleFunc("GET /api/v1/agents/{id}/config", s.notImplemented)
	s.jsonRoute("PUT /api/v1/agents/{id}/config", s.notImplemented)
	// The signed update transport still needs its own independent limit
	// decision; it must never inherit the ordinary JSON or Blueprint ceiling.
	mux.HandleFunc("POST /api/v1/agents/{id}/update", s.acceptTask)
}

func (s *Server) retryTask(w http.ResponseWriter, r *http.Request) {
	task, err := s.dispatcher.Retry(r.Context(), r.PathValue("id"))
	if err != nil {
		var domainError *errs.Error
		if errors.As(err, &domainError) {
			s.writeProblem(w, domainError)
			return
		}
		s.writeProblem(w, errs.Wrap(errs.KindInternal, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(apiTypes.TaskAccepted{TaskID: task.ID}); err != nil {
		s.Logger.Error("controller: write retry response", slog.Any("error", err))
	}
}

func (s *Server) taskShow(w http.ResponseWriter, r *http.Request) {
	if s.tasks == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Task repository is not configured"))
		return
	}
	task, err := s.tasks.GetTask(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeTaskProblem(w, err)
		return
	}
	events, err := s.tasks.ListTaskEvents(r.Context(), task.Record.ID, task.ReadRevision)
	if err != nil {
		s.writeTaskProblem(w, err)
		return
	}
	response, err := taskResponse(task.Record, events)
	if err != nil {
		s.writeTaskProblem(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.Logger.Error("controller: write Task response", slog.Any("error", err))
	}
}

func (s *Server) taskList(w http.ResponseWriter, r *http.Request) {
	if s.tasks == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Task repository is not configured"))
		return
	}
	if r.URL.Query().Get("environment") != "" || r.URL.Query().Get("workspace") != "" {
		s.writeProblem(w, errs.New(errs.KindNotImplemented, "scoped Task listing is not implemented"))
		return
	}
	request, err := taskPageRequest(r)
	if err != nil {
		s.writeTaskProblem(w, err)
		return
	}
	page, err := s.tasks.ListTasks(r.Context(), request)
	if err != nil {
		s.writeTaskProblem(w, err)
		return
	}
	response := apiTypes.Page[apiTypes.Task]{
		Items: make([]apiTypes.Task, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, versioned := range page.Items {
		record := versioned.Record
		status, err := taskAPIStatus(record.Status)
		if err != nil {
			s.writeTaskProblem(w, err)
			return
		}
		response.Items[index] = apiTypes.Task{
			ID: record.ID, OperationID: record.OperationID, RetryOf: record.RetryOf,
			PlanHash: record.PlanHash, Type: string(record.Type), Target: record.Target, Status: status,
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.Logger.Error("controller: write Task list response", slog.Any("error", err))
	}
}

func taskPageRequest(r *http.Request) (etcd.PageRequest, error) {
	query := r.URL.Query()
	if len(query["limit"]) > 1 || len(query["cursor"]) > 1 {
		return etcd.PageRequest{}, errs.New(errs.KindMalformedRequest, "Task pagination query is duplicated")
	}
	request := etcd.PageRequest{Cursor: query.Get("cursor")}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil {
			return etcd.PageRequest{}, errs.New(errs.KindMalformedRequest, "Task pagination limit is invalid")
		}
		request.Limit = limit
	}
	return request, nil
}

func (s *Server) writeTaskProblem(w http.ResponseWriter, err error) {
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		s.writeProblem(w, domainError)
		return
	}
	s.writeProblem(w, errs.Wrap(errs.KindInternal, err))
}

func taskResponse(record etcd.TaskRecord, snapshot etcd.TaskEventSnapshot) (apiTypes.Task, error) {
	status, err := taskAPIStatus(record.Status)
	if err != nil {
		return apiTypes.Task{}, err
	}
	stepStatus := make(map[string]apiTypes.TaskStatus, len(record.Steps))
	for _, step := range record.Steps {
		stepStatus[step.ID] = apiTypes.TaskPending
	}
	for _, event := range snapshot.Events {
		mapped, err := taskEventAPIStatus(event.State)
		if err != nil {
			return apiTypes.Task{}, err
		}
		if _, exists := stepStatus[event.Identity.StepID]; !exists {
			return apiTypes.Task{}, errs.New(errs.KindInternal, "Task event references an unknown step")
		}
		stepStatus[event.Identity.StepID] = mapped
	}
	response := apiTypes.Task{
		ID: record.ID, OperationID: record.OperationID, RetryOf: record.RetryOf,
		PlanHash: record.PlanHash, Type: string(record.Type), Target: record.Target, Status: status,
		Steps: make([]apiTypes.TaskStep, len(record.Steps)),
	}
	for index, step := range record.Steps {
		response.Steps[index] = apiTypes.TaskStep{Name: step.ID, Status: stepStatus[step.ID]}
	}
	return response, nil
}

func taskAPIStatus(status etcd.TaskStatus) (apiTypes.TaskStatus, error) {
	switch status {
	case etcd.TaskStatusPending:
		return apiTypes.TaskPending, nil
	case etcd.TaskStatusRunning:
		return apiTypes.TaskRunning, nil
	case etcd.TaskStatusCompleted:
		return apiTypes.TaskCompleted, nil
	case etcd.TaskStatusFailed:
		return apiTypes.TaskFailed, nil
	case etcd.TaskStatusAborted:
		return apiTypes.TaskAborted, nil
	case etcd.TaskStatusTimedOut:
		return apiTypes.TaskTimedOut, nil
	default:
		return "", errs.New(errs.KindInternal, "Task has an invalid durable status")
	}
}

func taskEventAPIStatus(status etcd.TaskEventState) (apiTypes.TaskStatus, error) {
	switch status {
	case etcd.TaskEventStatePending:
		return apiTypes.TaskPending, nil
	case etcd.TaskEventStateRunning:
		return apiTypes.TaskRunning, nil
	case etcd.TaskEventStateCompleted:
		return apiTypes.TaskCompleted, nil
	case etcd.TaskEventStateFailed:
		return apiTypes.TaskFailed, nil
	case etcd.TaskEventStateAborted:
		return apiTypes.TaskAborted, nil
	case etcd.TaskEventStateTimedOut:
		return apiTypes.TaskTimedOut, nil
	default:
		return "", errs.New(errs.KindInternal, "Task event has an invalid durable status")
	}
}

func (s *Server) notImplemented(w http.ResponseWriter, r *http.Request) {
	s.writeProblem(w, errs.New(errs.KindNotImplemented, "not implemented"))
}

// acceptTask is the shared shape for every action endpoint: dispatch a
// Task and return 202 {task_id} immediately (mvp.md's task pipeline —
// nothing blocks on a long operation).
func (s *Server) acceptTask(w http.ResponseWriter, r *http.Request) {
	// TODO: decode the typed request body, validate, call into
	// internal/controller's task dispatcher (serialization lock checked
	// here — KindDeployInFlight on a second in-flight deploy for the same
	// service), write the task to etcd, return its id.
	s.writeProblem(w, errs.New(errs.KindNotImplemented, "not implemented"))
}

// writeProblem serializes err as RFC 7807 problem+json at its own
// descriptor-owned HTTPStatus; call sites cannot override the Kind's public
// Code, Class, or status.
func (s *Server) writeProblem(w http.ResponseWriter, err *errs.Error) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(err.HTTPStatus())
	if encodeErr := json.NewEncoder(w).Encode(err.ToProblem()); encodeErr != nil {
		s.Logger.Error("controller: write problem response", slog.Any("error", encodeErr))
	}
}
