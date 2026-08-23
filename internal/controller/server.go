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

	"github.com/AlanD20/groundplane/internal/common/ids"
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

	host                  HostReader
	agents                AgentReader
	agentMutations        AgentMutator
	tenants               TenantReader
	tenantMutations       TenantMutator
	tenantChanges         TenantChanger
	projects              ProjectReader
	projectMutations      ProjectMutator
	projectChanges        ProjectChanger
	backingServices       BackingServiceReader
	environments          EnvironmentReader
	services              ServiceReader
	serviceMutations      ServiceMutator
	zones                 ZoneReader
	zoneMutations         ZoneMutator
	routeReads            RouteReader
	routeMutations        RouteMutator
	scriptReads           ScriptReader
	scriptMutations       ScriptMutator
	entries               EntryReader
	entryMutations        EntryMutator
	secrets               SecretReader
	secretMutations       SecretMutator
	secretDeletions       SecretDeleter
	environmentMutations  EnvironmentMutator
	environmentChanges    EnvironmentChanger
	environmentBlueprints EnvironmentBlueprintMutator
	environmentDeletions  EnvironmentDeleter
	attachMutations       AttachMutator
	attachFacts           AttachFactReader
	taskMutations         TaskRetrier
	console               fs.FS
	tasks                 taskQueries
	taskEventStreams      taskEventStreamOpener
	routePolicies         map[string]routePolicy
}

type Options struct {
	Host                  HostReader
	Agents                AgentReader
	AgentMutations        AgentMutator
	Tenants               TenantReader
	TenantMutations       TenantMutator
	TenantChanges         TenantChanger
	Projects              ProjectReader
	ProjectMutations      ProjectMutator
	ProjectChanges        ProjectChanger
	BackingServices       BackingServiceReader
	Environments          EnvironmentReader
	Services              ServiceReader
	ServiceMutations      ServiceMutator
	Zones                 ZoneReader
	ZoneMutations         ZoneMutator
	Routes                RouteReader
	RouteMutations        RouteMutator
	Scripts               ScriptReader
	ScriptMutations       ScriptMutator
	Entries               EntryReader
	EntryMutations        EntryMutator
	Secrets               SecretReader
	SecretMutations       SecretMutator
	SecretDeletions       SecretDeleter
	EnvironmentMutations  EnvironmentMutator
	EnvironmentChanges    EnvironmentChanger
	EnvironmentBlueprints EnvironmentBlueprintMutator
	EnvironmentDeletions  EnvironmentDeleter
	AttachMutations       AttachMutator
	AttachFacts           AttachFactReader
	TaskMutations         TaskRetrier
	Console               fs.FS
	Tasks                 *etcd.TaskRepository
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
		Store:                 store,
		Logger:                logger,
		Mux:                   mux,
		API:                   humago.NewWithPrefix(mux, "/api/v1", config),
		host:                  options.Host,
		agents:                options.Agents,
		agentMutations:        options.AgentMutations,
		tenants:               options.Tenants,
		tenantMutations:       options.TenantMutations,
		tenantChanges:         options.TenantChanges,
		projects:              options.Projects,
		projectMutations:      options.ProjectMutations,
		projectChanges:        options.ProjectChanges,
		backingServices:       options.BackingServices,
		environments:          options.Environments,
		services:              options.Services,
		serviceMutations:      options.ServiceMutations,
		zones:                 options.Zones,
		zoneMutations:         options.ZoneMutations,
		routeReads:            options.Routes,
		routeMutations:        options.RouteMutations,
		scriptReads:           options.Scripts,
		scriptMutations:       options.ScriptMutations,
		entries:               options.Entries,
		entryMutations:        options.EntryMutations,
		secrets:               options.Secrets,
		secretMutations:       options.SecretMutations,
		secretDeletions:       options.SecretDeletions,
		environmentMutations:  options.EnvironmentMutations,
		environmentChanges:    options.EnvironmentChanges,
		environmentBlueprints: options.EnvironmentBlueprints,
		environmentDeletions:  options.EnvironmentDeletions,
		attachMutations:       options.AttachMutations,
		attachFacts:           options.AttachFacts,
		taskMutations:         options.TaskMutations,
		console:               options.Console,
		tasks:                 options.Tasks,
		routePolicies:         make(map[string]routePolicy),
	}
	if options.Tasks != nil {
		s.taskEventStreams = repositoryTaskEventStreamOpener{repository: options.Tasks}
	}
	s.routes()
	s.registerTenants()
	s.registerProjects()
	s.registerBackingServices()
	s.registerEnvironments()
	s.registerEnvironmentBlueprints()
	s.registerServices()
	s.registerZones()
	s.registerRoutes()
	s.registerScripts()
	s.registerEntries()
	s.registerSecrets()
	s.registerAttaches()
	s.registerTasks()
	s.registerHost()
	return s
}

// HTTPHandler returns the complete Controller HTTP surface without opening a
// listener. Release validation uses it to exercise the production dispatcher
// and embedded Console through the same server assembled by application DI.
func (s *Server) HTTPHandler() http.Handler {
	return s.requestHandler()
}

// routes registers every endpoint from api-cli.md's resource map. Method
// + pattern routing (Go 1.22 net/http). Unimplemented resources remain
// explicit 501 handlers until their vertical is delivered; typed Huma routes,
// such as GET /host, register alongside this table.
func (s *Server) routes() {
	mux := s.Mux

	// OpenAPI is serialized from this Server's registered Huma operations; it
	// is never loaded from a generated file or maintained as a parallel route table.
	mux.HandleFunc("GET /openapi.json", s.openAPI)

	// Tenant reads and synchronous mutations are typed Huma operations.
	// Destructive delete remains a Task route until the accepted parent-cascade
	// contract has a durable executor.
	mux.HandleFunc("DELETE /api/v1/tenants/{id}", s.acceptTask)

	// Project reads and synchronous mutations are typed Huma operations.
	// Destructive delete remains a Task route until the accepted parent-cascade
	// contract has a durable executor.
	mux.HandleFunc("DELETE /api/v1/projects/{id}", s.acceptTask)

	// environment (?project=). Typed list/show/create/rename operations are
	// registered through Huma after the legacy mux surface is assembled.
	s.streamRoute("GET /api/v1/environments/{id}/logs", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/environments/{id}", s.environmentDelete)

	// environment singleton sub-resources
	mux.HandleFunc("GET /api/v1/environments/{id}/backup-policy", s.notImplemented)
	s.jsonRoute("PUT /api/v1/environments/{id}/backup-policy", s.notImplemented)
	mux.HandleFunc("GET /api/v1/environments/{id}/recovery-points", s.notImplemented)
	s.jsonRoute("POST /api/v1/environments/{id}/backup-run", s.acceptTask)
	s.jsonRoute("POST /api/v1/environments/{id}/restore", s.acceptTask)
	s.jsonRoute("POST /api/v1/environments/{id}/rotate-key", s.acceptTask)
	mux.HandleFunc("POST /api/v1/environments/{id}/export-key", s.notImplemented)
	// router: READ-ONLY projection grouping ingress components — GET only,
	// never PUT (api-cli.md, section 4). Managed entirely through /components.
	mux.HandleFunc("GET /api/v1/environments/{id}/router", s.notImplemented)

	// service (?environment=) — deploy/rollback/start/stop/destroy return a task
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

	// Zone reads and synchronous creation are typed Huma operations. Task-backed
	// removal remains explicit; Zone fields are immutable and have no PATCH.

	// Route reads, mutations, and task-backed removal are typed Huma operations.

	// Volume metadata remains explicit. Script reads and protected metadata
	// mutations are typed Huma operations; run and removal remain task placeholders.
	mux.HandleFunc("GET /api/v1/volumes", s.notImplemented)
	s.jsonRoute("POST /api/v1/volumes", s.notImplemented)
	mux.HandleFunc("GET /api/v1/volumes/{id}", s.notImplemented)
	s.jsonRoute("PATCH /api/v1/volumes/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/volumes/{id}", s.acceptTask)
	mux.HandleFunc("DELETE /api/v1/scripts/{id}", s.acceptTask)
	// Entry reads, protected mutations, and explicit reveal are typed Huma operations.
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
	s.jsonRoute("POST /api/v1/backing-services", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/backing-services/{project_id}", s.acceptTask)
	s.jsonRoute("POST /api/v1/backing-services/{project_id}/start", s.acceptTask)
	s.jsonRoute("POST /api/v1/backing-services/{project_id}/stop", s.acceptTask)
	s.jsonRoute("POST /api/v1/backing-services/{project_id}/destroy", s.acceptTask)

	// Secret reads, protected create/delete, and explicit reveal are typed
	// Huma operations.

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
	mux.HandleFunc("POST /api/v1/tasks/{id}/retry", s.retryTask)
	s.jsonRoute("POST /api/v1/tasks/{id}/abort", s.acceptTask)

	// host / agents
	s.registerAgents()
	// The signed update transport still needs its own independent limit
	// decision; it must never inherit the ordinary JSON or Blueprint ceiling.
	mux.HandleFunc("POST /api/v1/agents/{id}/update", s.acceptTask)
}

func (s *Server) retryTask(w http.ResponseWriter, r *http.Request) {
	if s.taskMutations == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Task retrier is not configured"))
		return
	}
	taskID := r.PathValue("id")
	if ids.Validate(ids.KindTask, taskID) != nil {
		s.writeTaskProblem(w, errs.New(errs.KindMalformedRequest, "Task id is invalid"))
		return
	}
	if len(r.URL.Query()) != 0 {
		s.writeTaskProblem(w, errs.New(errs.KindMalformedRequest, "Task retry query is invalid"))
		return
	}
	if err := validateBodylessAgentMutation(r); err != nil {
		s.writeTaskProblem(w, errs.New(errs.KindMalformedRequest, "Task retry body is not allowed"))
		return
	}
	response, err := s.taskMutations.RetryTask(r.Context(), taskID, r.Header.Get(idempotencyKeyHeader))
	if err != nil {
		s.writeTaskProblem(w, err)
		return
	}
	w.Header().Set("Content-Type", response.ContentKind)
	w.WriteHeader(response.Status)
	if _, err := w.Write(response.Body); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write retry response", slog.Any("error", err))
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
