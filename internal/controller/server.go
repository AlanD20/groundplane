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
	"log/slog"
	"net"
	"net/http"
	"sync"

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

	etcdEndpoints []string
	dispatcher    *Dispatcher
}

type Options struct {
	EtcdEndpoints []string
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
		etcdEndpoints: append([]string(nil), options.EtcdEndpoints...),
		dispatcher:    NewDispatcher(),
	}
	s.routes()
	s.registerHost()
	return s
}

// routes registers every endpoint from api-cli.md's resource map. Method
// + pattern routing (Go 1.22 net/http). Each handler is a stub returning
// 501 until backed by real logic — wire internal/controller/handlers as
// each resource is implemented; this file's job is to keep the ROUTE
// TABLE authoritative and complete, matching the CLI 1:1.
func (s *Server) routes() {
	mux := s.Mux

	// OpenAPI — served from the code-first generator once wired (Huma /
	// oapi-codegen). TODO.
	mux.HandleFunc("GET /openapi.json", s.notImplemented)

	// tenant — destructive delete is a task (api-cli.md's resource map)
	mux.HandleFunc("GET /api/v1/tenants", s.notImplemented)
	mux.HandleFunc("POST /api/v1/tenants", s.notImplemented)
	mux.HandleFunc("GET /api/v1/tenants/{id}", s.notImplemented)
	mux.HandleFunc("PUT /api/v1/tenants/{id}", s.notImplemented)
	mux.HandleFunc("PATCH /api/v1/tenants/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/tenants/{id}", s.acceptTask)

	// project (?kind=tenant|backing)
	mux.HandleFunc("GET /api/v1/projects", s.notImplemented)
	mux.HandleFunc("POST /api/v1/projects", s.notImplemented)
	mux.HandleFunc("GET /api/v1/projects/{id}", s.notImplemented)
	mux.HandleFunc("PUT /api/v1/projects/{id}", s.notImplemented)
	mux.HandleFunc("PATCH /api/v1/projects/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/projects/{id}", s.acceptTask)

	// environment (?project=)
	mux.HandleFunc("GET /api/v1/environments", s.notImplemented)
	mux.HandleFunc("POST /api/v1/environments", s.notImplemented)
	mux.HandleFunc("GET /api/v1/environments/{id}", s.notImplemented)
	mux.HandleFunc("PUT /api/v1/environments/{id}", s.notImplemented)
	mux.HandleFunc("POST /api/v1/environments/{id}/rename", s.notImplemented)
	mux.HandleFunc("GET /api/v1/environments/{id}/logs", s.notImplemented) // SSE
	mux.HandleFunc("DELETE /api/v1/environments/{id}", s.acceptTask)

	// environment singleton sub-resources
	mux.HandleFunc("GET /api/v1/environments/{id}/backup-policy", s.notImplemented)
	mux.HandleFunc("PUT /api/v1/environments/{id}/backup-policy", s.notImplemented)
	mux.HandleFunc("GET /api/v1/environments/{id}/recovery-points", s.notImplemented)
	mux.HandleFunc("POST /api/v1/environments/{id}/backup-run", s.acceptTask)
	mux.HandleFunc("POST /api/v1/environments/{id}/restore", s.acceptTask)
	mux.HandleFunc("POST /api/v1/environments/{id}/rotate-key", s.acceptTask)
	mux.HandleFunc("POST /api/v1/environments/{id}/export-key", s.notImplemented)
	// router: READ-ONLY projection grouping ingress components — GET only,
	// never PUT (api-cli.md, section 4). Managed entirely through /components.
	mux.HandleFunc("GET /api/v1/environments/{id}/router", s.notImplemented)

	// service (?environment=) — deploy/rollback/start/stop/destroy return a task
	mux.HandleFunc("GET /api/v1/services", s.notImplemented)
	mux.HandleFunc("POST /api/v1/services", s.notImplemented)
	mux.HandleFunc("GET /api/v1/services/{id}", s.notImplemented) // includes the release ledger
	mux.HandleFunc("PUT /api/v1/services/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/services/{id}", s.acceptTask)
	mux.HandleFunc("POST /api/v1/services/{id}/deploy", s.acceptTask) // {tag?, strategy?, on_failure?}
	mux.HandleFunc("POST /api/v1/services/{id}/rollback", s.acceptTask)
	mux.HandleFunc("POST /api/v1/services/{id}/start", s.acceptTask)
	mux.HandleFunc("POST /api/v1/services/{id}/stop", s.acceptTask)
	mux.HandleFunc("POST /api/v1/services/{id}/destroy", s.acceptTask)
	mux.HandleFunc("GET /api/v1/services/{id}/logs", s.notImplemented) // SSE

	// release-group (?environment=)
	mux.HandleFunc("GET /api/v1/release-groups", s.notImplemented)
	mux.HandleFunc("POST /api/v1/release-groups", s.notImplemented)
	mux.HandleFunc("GET /api/v1/release-groups/{id}", s.notImplemented)
	mux.HandleFunc("PUT /api/v1/release-groups/{id}", s.notImplemented)
	mux.HandleFunc("PATCH /api/v1/release-groups/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/release-groups/{id}", s.acceptTask)
	mux.HandleFunc("POST /api/v1/release-groups/{id}/deploy", s.acceptTask)
	mux.HandleFunc("POST /api/v1/release-groups/{id}/rollback", s.acceptTask)

	// attach — attach provisions (joins the owned external network,
	// publishes facts), detach deprovisions; both tasks
	mux.HandleFunc("GET /api/v1/attaches", s.notImplemented)
	mux.HandleFunc("POST /api/v1/attaches", s.acceptTask) // {service_id, backing_service_id, name?, grants?}
	mux.HandleFunc("DELETE /api/v1/attaches/{id}", s.acceptTask)

	// zone / route / volume / entry / script (?environment=) — destructive delete is a task
	for _, res := range []string{"zones", "routes", "volumes", "entries", "scripts"} {
		mux.HandleFunc("GET /api/v1/"+res, s.notImplemented)
		mux.HandleFunc("POST /api/v1/"+res, s.notImplemented)
		mux.HandleFunc("GET /api/v1/"+res+"/{id}", s.notImplemented)
		mux.HandleFunc("PUT /api/v1/"+res+"/{id}", s.notImplemented)
		mux.HandleFunc("PATCH /api/v1/"+res+"/{id}", s.notImplemented)
		mux.HandleFunc("DELETE /api/v1/"+res+"/{id}", s.acceptTask)
	}
	mux.HandleFunc("POST /api/v1/scripts/{id}/run", s.acceptTask) // {parameters?}

	// component (?environment= or ?platform=true) — one resource across both owners
	mux.HandleFunc("GET /api/v1/components", s.notImplemented)
	mux.HandleFunc("POST /api/v1/components", s.notImplemented)
	mux.HandleFunc("GET /api/v1/components/{id}", s.notImplemented)
	mux.HandleFunc("PUT /api/v1/components/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/components/{id}", s.acceptTask)
	mux.HandleFunc("POST /api/v1/components/{id}/enable", s.acceptTask)
	mux.HandleFunc("POST /api/v1/components/{id}/disable", s.acceptTask)
	mux.HandleFunc("POST /api/v1/components/{id}/update", s.acceptTask)
	mux.HandleFunc("GET /api/v1/components/{id}/config", s.notImplemented)
	mux.HandleFunc("PUT /api/v1/components/{id}/config", s.notImplemented)

	// backing-service
	mux.HandleFunc("GET /api/v1/backing-services", s.notImplemented)
	mux.HandleFunc("POST /api/v1/backing-services", s.notImplemented)
	mux.HandleFunc("GET /api/v1/backing-services/{project_id}", s.notImplemented)
	mux.HandleFunc("PUT /api/v1/backing-services/{project_id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/backing-services/{project_id}", s.acceptTask)
	mux.HandleFunc("POST /api/v1/backing-services/{project_id}/start", s.acceptTask)
	mux.HandleFunc("POST /api/v1/backing-services/{project_id}/stop", s.acceptTask)
	mux.HandleFunc("POST /api/v1/backing-services/{project_id}/destroy", s.acceptTask)

	// secret (?project=) — project-scoped, locked
	mux.HandleFunc("GET /api/v1/secrets", s.notImplemented)
	mux.HandleFunc("POST /api/v1/secrets", s.notImplemented)
	mux.HandleFunc("GET /api/v1/secrets/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/secrets/{id}", s.acceptTask)
	mux.HandleFunc("GET /api/v1/secrets/{id}/value", s.notImplemented) // reveal — Console-only preference, not access control

	// connector (?environment= required) — environment-scoped only
	mux.HandleFunc("GET /api/v1/connectors", s.notImplemented)
	mux.HandleFunc("POST /api/v1/connectors", s.notImplemented)
	mux.HandleFunc("GET /api/v1/connectors/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/connectors/{id}", s.acceptTask)

	// runner (?tenant= or ?project=) — org-scoped or repo-scoped
	mux.HandleFunc("GET /api/v1/runners", s.notImplemented)
	mux.HandleFunc("POST /api/v1/runners", s.notImplemented) // {tenant_id|project_id, registration_token} — token discarded after registration
	mux.HandleFunc("DELETE /api/v1/runners/{id}", s.notImplemented)

	// task / activity
	mux.HandleFunc("GET /api/v1/tasks", s.notImplemented)
	mux.HandleFunc("GET /api/v1/tasks/{id}", s.notImplemented)
	mux.HandleFunc("GET /api/v1/tasks/{id}/events", s.notImplemented) // SSE
	mux.HandleFunc("POST /api/v1/tasks/{id}/retry", s.retryTask)
	mux.HandleFunc("POST /api/v1/tasks/{id}/abort", s.acceptTask)
	mux.HandleFunc("GET /api/v1/activity", s.notImplemented) // documented alias of GET /tasks?workspace=

	// host / agents
	mux.HandleFunc("POST /api/v1/agents", s.acceptTask)
	mux.HandleFunc("GET /api/v1/agents", s.notImplemented)
	mux.HandleFunc("GET /api/v1/agents/{id}", s.notImplemented)
	mux.HandleFunc("DELETE /api/v1/agents/{id}", s.acceptTask)
	mux.HandleFunc("GET /api/v1/agents/{id}/config", s.notImplemented)
	mux.HandleFunc("PUT /api/v1/agents/{id}/config", s.notImplemented)
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
		s.writeProblem(w, errs.Wrap(errs.CodeInternal, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(apiTypes.TaskAccepted{TaskID: task.ID}); err != nil {
		s.Logger.Error("controller: write retry response", slog.Any("error", err))
	}
}

func (s *Server) notImplemented(w http.ResponseWriter, r *http.Request) {
	s.writeProblem(w, errs.New(errs.CodeNotImplemented, "not implemented"))
}

// acceptTask is the shared shape for every action endpoint: dispatch a
// Task and return 202 {task_id} immediately (mvp.md's task pipeline —
// nothing blocks on a long operation).
func (s *Server) acceptTask(w http.ResponseWriter, r *http.Request) {
	// TODO: decode the typed request body, validate, call into
	// internal/controller's task dispatcher (serialization lock checked
	// here — CodeDeployInFlight on a second in-flight deploy for the same
	// service), write the task to etcd, return its id.
	s.writeProblem(w, errs.New(errs.CodeNotImplemented, "not implemented"))
}

// writeProblem serializes err as RFC 7807 problem+json at its own
// HTTPStatus() — derived from Class, so a new Code never needs a new
// status decision at the call site (see pkg/errs, "HTTPStatus").
func (s *Server) writeProblem(w http.ResponseWriter, err *errs.Error) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(err.HTTPStatus())
	if encodeErr := json.NewEncoder(w).Encode(err.ToProblem()); encodeErr != nil {
		s.Logger.Error("controller: write problem response", slog.Any("error", encodeErr))
	}
}

type readyListener struct {
	net.Listener
	ready chan struct{}
	once  sync.Once
}

func (l *readyListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.ready) })
	return l.Listener.Accept()
}

// Serve starts the HTTP server and blocks until ctx is cancelled.
func (s *Server) Serve(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, err)
	}
	srv := &http.Server{
		Addr:    addr,
		Handler: s.Mux,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}
	trackedListener := &readyListener{Listener: listener, ready: make(chan struct{})}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(trackedListener)
	}()
	s.Logger.Info("controller: listening", "addr", addr)

	// net/http tracks the listener before its first Accept call. Wait for
	// that point so cancellation cannot race with Serve registration and
	// leave an untracked listener blocked forever.
	select {
	case <-trackedListener.ready:
	case err := <-serveErr:
		return classifyServeError(err)
	}

	select {
	case err := <-serveErr:
		return classifyServeError(err)
	case <-ctx.Done():
		closeErr := srv.Close()
		err := <-serveErr
		if closeErr != nil {
			return errs.Wrap(errs.CodeInternal, closeErr)
		}
		classifiedErr := classifyServeError(err)
		if classifiedErr != nil && !errors.Is(classifiedErr, http.ErrServerClosed) {
			return classifiedErr
		}
		return nil
	}
}

func classifyServeError(err error) error {
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return errs.Wrap(errs.CodeInternal, err)
}
