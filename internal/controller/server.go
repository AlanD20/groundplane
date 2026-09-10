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
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/version"
	environmentcapability "github.com/AlanD20/groundplane/internal/controller/environment"
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
	Store       etcd.Store
	Logger      *slog.Logger
	Mux         *http.ServeMux
	API         huma.API
	onHTTPReady func()

	host                    HostReader
	controllerConfig        ControllerConfigStore
	controllerUpdates       ControllerUpdater
	agents                  AgentReader
	agentMutations          AgentMutator
	tenants                 TenantReader
	tenantMutations         TenantMutator
	tenantChanges           TenantChanger
	projects                ProjectReader
	projectMutations        ProjectMutator
	projectChanges          ProjectChanger
	backingServices         BackingServiceReader
	backingServiceMutations BackingServiceMutator
	components              ComponentReader
	componentMutations      ComponentMutator
	environments            *environmentcapability.Reader
	services                ServiceReader
	serviceMutations        ServiceMutator
	releaseGroups           ReleaseGroupReader
	releaseGroupMutations   ReleaseGroupMutator
	releases                ReleaseReader
	releaseOperations       ReleaseOperator
	zones                   ZoneReader
	zoneMutations           ZoneMutator
	routeReads              RouteReader
	routeMutations          RouteMutator
	scriptReads             ScriptReader
	scriptMutations         ScriptMutator
	entries                 EntryReader
	entryMutations          EntryMutator
	secrets                 SecretReader
	secretMutations         SecretMutator
	secretDeletions         SecretDeleter
	connectors              ConnectorReader
	connectorMutations      ConnectorMutator
	connectorDeletions      ConnectorDeleter
	runners                 RunnerReader
	runnerProvisioning      RunnerProvisioner
	runnerMutations         RunnerMutator
	runnerRemovals          RunnerRemover
	backupPolicies          BackupPolicyReader
	backupPolicyMutations   BackupPolicyMutator
	recoveryPoints          RecoveryPointReader
	backupRuns              BackupRunMutator
	backupKeyMutations      BackupKeyMutator
	backupKeyExports        BackupKeyExporter
	volumes                 VolumeReader
	volumeMutations         VolumeMutator
	environmentMutations    EnvironmentMutator
	environmentChanges      EnvironmentChanger
	environmentBlueprints   EnvironmentBlueprintService
	hierarchyDeletions      HierarchyDeletionService
	attachMutations         AttachMutator
	attachFacts             AttachFactReader
	taskMutations           TaskRetrier
	taskAborts              TaskAborter
	controllerTaskWake      func()
	agentTaskWake           func()
	console                 fs.FS
	tasks                   taskQueries
	taskEventStreams        taskEventStreamOpener
	logs                    *LogService
	routePolicies           map[string]routePolicy
}

type Options struct {
	OnHTTPReady             func()
	Host                    HostReader
	ControllerConfig        ControllerConfigStore
	ControllerUpdates       ControllerUpdater
	Agents                  AgentReader
	AgentMutations          AgentMutator
	Tenants                 TenantReader
	TenantMutations         TenantMutator
	TenantChanges           TenantChanger
	Projects                ProjectReader
	ProjectMutations        ProjectMutator
	ProjectChanges          ProjectChanger
	BackingServices         BackingServiceReader
	BackingServiceMutations BackingServiceMutator
	Components              ComponentReader
	ComponentMutations      ComponentMutator
	Environments            *environmentcapability.Reader
	Services                ServiceReader
	ServiceMutations        ServiceMutator
	ReleaseGroups           ReleaseGroupReader
	ReleaseGroupMutations   ReleaseGroupMutator
	Releases                ReleaseReader
	ReleaseOperations       ReleaseOperator
	Zones                   ZoneReader
	ZoneMutations           ZoneMutator
	Routes                  RouteReader
	RouteMutations          RouteMutator
	Scripts                 ScriptReader
	ScriptMutations         ScriptMutator
	Entries                 EntryReader
	EntryMutations          EntryMutator
	Secrets                 SecretReader
	SecretMutations         SecretMutator
	SecretDeletions         SecretDeleter
	Connectors              ConnectorReader
	ConnectorMutations      ConnectorMutator
	ConnectorDeletions      ConnectorDeleter
	Runners                 RunnerReader
	RunnerProvisioning      RunnerProvisioner
	RunnerMutations         RunnerMutator
	RunnerRemovals          RunnerRemover
	BackupPolicies          BackupPolicyReader
	BackupPolicyMutations   BackupPolicyMutator
	RecoveryPoints          RecoveryPointReader
	BackupRuns              BackupRunMutator
	BackupKeyMutations      BackupKeyMutator
	BackupKeyExports        BackupKeyExporter
	Volumes                 VolumeReader
	VolumeMutations         VolumeMutator
	EnvironmentMutations    EnvironmentMutator
	EnvironmentChanges      EnvironmentChanger
	EnvironmentBlueprints   EnvironmentBlueprintService
	HierarchyDeletions      HierarchyDeletionService
	AttachMutations         AttachMutator
	AttachFacts             AttachFactReader
	TaskMutations           TaskRetrier
	TaskAborts              TaskAborter
	ControllerTaskWake      func()
	AgentTaskWake           func()
	Console                 fs.FS
	Tasks                   *etcd.TaskRepository
	Logs                    *LogService
}

type taskQueries interface {
	GetTask(context.Context, string) (etcd.Versioned[etcd.TaskRecord], error)
	ListTasksByScope(context.Context, etcd.TaskListScope, etcd.PageRequest) (etcd.Page[etcd.TaskRecord], error)
	ListTaskEvents(context.Context, string, int64) (etcd.TaskEventSnapshot, error)
}

func New(store etcd.Store, logger *slog.Logger, options Options) *Server {
	configureProblemResponses()
	mux := http.NewServeMux()
	config := problemAPIConfig("Groundplane API", version.Value)

	s := &Server{
		Store:                   store,
		Logger:                  logger,
		Mux:                     mux,
		API:                     humago.NewWithPrefix(mux, "/api/v1", config),
		onHTTPReady:             options.OnHTTPReady,
		host:                    options.Host,
		controllerConfig:        options.ControllerConfig,
		controllerUpdates:       options.ControllerUpdates,
		agents:                  options.Agents,
		agentMutations:          options.AgentMutations,
		tenants:                 options.Tenants,
		tenantMutations:         options.TenantMutations,
		tenantChanges:           options.TenantChanges,
		projects:                options.Projects,
		projectMutations:        options.ProjectMutations,
		projectChanges:          options.ProjectChanges,
		backingServices:         options.BackingServices,
		backingServiceMutations: options.BackingServiceMutations,
		components:              options.Components,
		componentMutations:      options.ComponentMutations,
		environments:            options.Environments,
		services:                options.Services,
		serviceMutations:        options.ServiceMutations,
		releaseGroups:           options.ReleaseGroups,
		releaseGroupMutations:   options.ReleaseGroupMutations,
		releases:                options.Releases,
		releaseOperations:       options.ReleaseOperations,
		zones:                   options.Zones,
		zoneMutations:           options.ZoneMutations,
		routeReads:              options.Routes,
		routeMutations:          options.RouteMutations,
		scriptReads:             options.Scripts,
		scriptMutations:         options.ScriptMutations,
		entries:                 options.Entries,
		entryMutations:          options.EntryMutations,
		secrets:                 options.Secrets,
		secretMutations:         options.SecretMutations,
		secretDeletions:         options.SecretDeletions,
		connectors:              options.Connectors,
		connectorMutations:      options.ConnectorMutations,
		connectorDeletions:      options.ConnectorDeletions,
		runners:                 options.Runners,
		runnerProvisioning:      options.RunnerProvisioning,
		runnerMutations:         options.RunnerMutations,
		runnerRemovals:          options.RunnerRemovals,
		backupPolicies:          options.BackupPolicies,
		backupPolicyMutations:   options.BackupPolicyMutations,
		recoveryPoints:          options.RecoveryPoints,
		backupRuns:              options.BackupRuns,
		backupKeyMutations:      options.BackupKeyMutations,
		backupKeyExports:        options.BackupKeyExports,
		volumes:                 options.Volumes,
		volumeMutations:         options.VolumeMutations,
		environmentMutations:    options.EnvironmentMutations,
		environmentChanges:      options.EnvironmentChanges,
		environmentBlueprints:   options.EnvironmentBlueprints,
		hierarchyDeletions:      options.HierarchyDeletions,
		attachMutations:         options.AttachMutations,
		attachFacts:             options.AttachFacts,
		taskMutations:           options.TaskMutations,
		taskAborts:              options.TaskAborts,
		controllerTaskWake:      options.ControllerTaskWake,
		agentTaskWake:           options.AgentTaskWake,
		console:                 options.Console,
		tasks:                   options.Tasks,
		logs:                    options.Logs,
		routePolicies:           make(map[string]routePolicy),
	}
	if options.Tasks != nil {
		s.taskEventStreams = repositoryTaskEventStreamOpener{repository: options.Tasks}
	}
	s.routes()
	s.registerLogOpenAPI()
	s.registerTenants()
	s.registerProjects()
	s.registerBackingServices()
	s.registerControllerConfig()
	s.registerControllerUpdate()
	s.registerComponents()
	s.registerEnvironments()
	registerHierarchyDeletionRoutes(s.API, s.hierarchyDeletions, s.Logger)
	s.registerEnvironmentBlueprints()
	s.registerServices()
	s.registerReleaseGroups()
	s.registerReleases()
	s.registerZones()
	s.registerRoutes()
	s.registerScripts()
	s.registerEntries()
	s.registerSecrets()
	s.registerConnectors()
	s.registerRunners()
	s.registerBackupPolicies()
	s.registerRecoveryPoints()
	s.registerBackupRuns()
	s.registerBackupKeyRoutes()
	s.registerVolumes()
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

	// environment (?project=). Typed list/show/create/rename operations are
	// registered through Huma after the legacy mux surface is assembled.
	// environment singleton sub-resources
	s.jsonRoute("POST /api/v1/environments/{id}/restore", s.acceptTask)
	// router: READ-ONLY projection grouping ingress components — GET only,
	// never PUT (api-cli.md, section 4). Managed entirely through /components.

	// service (?environment=) — deploy/rollback/start/stop/destroy return a task
	// Release Group metadata is registered as typed Huma operations.

	// Zone reads and synchronous creation are typed Huma operations. Task-backed
	// removal remains explicit; Zone fields are immutable and have no PATCH.

	// Route reads, mutations, and task-backed removal are typed Huma operations.

	// Volume reads, protected identity mutations, fixed-revision impact, and
	// confirmed removal are registered as typed Huma operations below.
	// Entry reads, protected mutations, and explicit reveal are typed Huma operations.

	// component (?environment= or ?platform=true) — one resource across both owners

	// backing-service

	// Secret reads, protected create/delete, and explicit reveal are typed
	// Huma operations.

	// Runner list, detail, create, retry, edit, and removal are typed Huma operations.

	// host / agents
	s.registerAgents()
}

func taskResponse(record etcd.TaskRecord, snapshot etcd.TaskEventSnapshot) (apiTypes.Task, error) {
	response, err := taskListResponse(record)
	if err != nil {
		return apiTypes.Task{}, err
	}
	stepStatus := make(map[string]apiTypes.TaskStatus, len(record.Steps))
	defaultStepStatus := apiTypes.TaskPending
	if record.Executor == etcd.TaskExecutorController {
		defaultStepStatus = response.Status
	}
	for _, step := range record.Steps {
		stepStatus[step.ID] = defaultStepStatus
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
	response.Steps = make([]apiTypes.TaskStep, len(record.Steps))
	for index, step := range record.Steps {
		kind, err := taskStepKindResponse(step)
		if err != nil {
			return apiTypes.Task{}, err
		}
		response.Steps[index] = apiTypes.TaskStep{
			Name: step.ID, Status: stepStatus[step.ID], Kind: kind,
			ScriptID: step.ScriptID, ScriptSlug: step.ScriptSlug,
		}
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
