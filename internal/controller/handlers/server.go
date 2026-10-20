// Package handlers owns the Controller's HTTP boundary: request validation,
// public response projection, streaming and Console asset delivery. Capability
// services own the operations invoked by these routes.
package handlers

import (
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/version"
	environmentcapability "github.com/AlanD20/groundplane/internal/controller/environment"
	"github.com/AlanD20/groundplane/internal/controller/hostterminal"
	servicelogs "github.com/AlanD20/groundplane/internal/controller/servicelogs"
	"github.com/AlanD20/groundplane/internal/controller/taskinspection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
)

// Server holds everything a request handler needs. Construct once in
// cmd/controller and pass down; handlers are methods on *Server so they
// share the store/logger without globals.
type Server struct {
	Store             etcdstore.Store
	Logger            *slog.Logger
	Mux               *http.ServeMux
	API               huma.API
	onHTTPReady       func()
	mutationAdmission MutationAdmission
	storageAdmission  StorageAdmission

	host                    HostReader
	hostTerminals           *hostterminal.Manager
	controllerConfig        NativeConfigStore
	etcdConfig              NativeConfigStore
	etcdConfigApplier       EtcdConfigApplier
	controllerUpdates       ControllerUpdater
	softwarePreparer        SoftwarePreparer
	softwareActivator       SoftwareActivator
	softwareReleases        SoftwareReleaseReader
	images                  ImageFetcher
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
	serviceObservations     ServiceObserver
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
	connectorChecks         ConnectorChecker
	runners                 RunnerReader
	runnerProvisioning      RunnerProvisioner
	runnerMutations         RunnerMutator
	runnerRemovals          RunnerRemover
	runnerChecks            RunnerChecker
	backupPolicies          BackupPolicyReader
	backupPolicyMutations   BackupPolicyMutator
	recoveryPoints          RecoveryPointReader
	recoveryPointRemover    RecoveryPointRemover
	backupRuns              BackupRunMutator
	backupRestores          BackupRestorer
	backupRestorePreviews   BackupRestorePreviewer
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
	backingChecks           BackingChecker
	edgeChecks              EdgeChecker
	dnsChecks               DNSChecker
	environmentDNS          EnvironmentDNSService
	taskMutations           TaskRetrier
	taskAborts              TaskAborter
	taskInspection          *taskinspection.Service
	controllerTaskWake      func()
	agentTaskWake           func()
	console                 fs.FS
	logs                    *servicelogs.Service
	routePolicies           map[string]routePolicy
}

type Options struct {
	OnHTTPReady             func()
	MutationAdmission       MutationAdmission
	StorageAdmission        StorageAdmission
	Host                    HostReader
	HostTerminals           *hostterminal.Manager
	ControllerConfig        NativeConfigStore
	EtcdConfig              NativeConfigStore
	EtcdConfigApplier       EtcdConfigApplier
	ControllerUpdates       ControllerUpdater
	SoftwarePreparer        SoftwarePreparer
	SoftwareActivator       SoftwareActivator
	SoftwareReleases        SoftwareReleaseReader
	Images                  ImageFetcher
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
	ServiceObservations     ServiceObserver
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
	ConnectorChecks         ConnectorChecker
	Runners                 RunnerReader
	RunnerProvisioning      RunnerProvisioner
	RunnerMutations         RunnerMutator
	RunnerRemovals          RunnerRemover
	RunnerChecks            RunnerChecker
	BackupPolicies          BackupPolicyReader
	BackupPolicyMutations   BackupPolicyMutator
	RecoveryPoints          RecoveryPointReader
	BackupRuns              BackupRunMutator
	RecoveryPointRemover    RecoveryPointRemover
	BackupRestores          BackupRestorer
	BackupRestorePreviews   BackupRestorePreviewer
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
	BackingChecks           BackingChecker
	EdgeChecks              EdgeChecker
	DNSChecks               DNSChecker
	EnvironmentDNS          EnvironmentDNSService
	TaskMutations           TaskRetrier
	TaskAborts              TaskAborter
	TaskInspection          *taskinspection.Service
	ControllerTaskWake      func()
	AgentTaskWake           func()
	Console                 fs.FS
	Logs                    *servicelogs.Service
}

func New(store etcdstore.Store, logger *slog.Logger, options Options) *Server {
	configureProblemResponses()
	mux := http.NewServeMux()
	config := problemAPIConfig("Groundplane API", version.Value)

	s := &Server{
		Store:                   store,
		Logger:                  logger,
		Mux:                     mux,
		API:                     humago.NewWithPrefix(mux, "/api/v1", config),
		onHTTPReady:             options.OnHTTPReady,
		mutationAdmission:       options.MutationAdmission,
		storageAdmission:        options.StorageAdmission,
		host:                    options.Host,
		hostTerminals:           options.HostTerminals,
		controllerConfig:        options.ControllerConfig,
		etcdConfig:              options.EtcdConfig,
		etcdConfigApplier:       options.EtcdConfigApplier,
		controllerUpdates:       options.ControllerUpdates,
		softwarePreparer:        options.SoftwarePreparer,
		softwareActivator:       options.SoftwareActivator,
		softwareReleases:        options.SoftwareReleases,
		images:                  options.Images,
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
		serviceObservations:     options.ServiceObservations,
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
		connectorChecks:         options.ConnectorChecks,
		runners:                 options.Runners,
		runnerProvisioning:      options.RunnerProvisioning,
		runnerMutations:         options.RunnerMutations,
		runnerRemovals:          options.RunnerRemovals,
		runnerChecks:            options.RunnerChecks,
		backupPolicies:          options.BackupPolicies,
		backupPolicyMutations:   options.BackupPolicyMutations,
		recoveryPoints:          options.RecoveryPoints,
		recoveryPointRemover:    options.RecoveryPointRemover,
		backupRuns:              options.BackupRuns,
		backupRestores:          options.BackupRestores,
		backupRestorePreviews:   options.BackupRestorePreviews,
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
		backingChecks:           options.BackingChecks,
		edgeChecks:              options.EdgeChecks,
		dnsChecks:               options.DNSChecks,
		environmentDNS:          options.EnvironmentDNS,
		taskMutations:           options.TaskMutations,
		taskAborts:              options.TaskAborts,
		taskInspection:          options.TaskInspection,
		controllerTaskWake:      options.ControllerTaskWake,
		agentTaskWake:           options.AgentTaskWake,
		console:                 options.Console,
		logs:                    options.Logs,
		routePolicies:           make(map[string]routePolicy),
	}
	s.routes()
	s.registerLogOpenAPI()
	s.registerTenants()
	s.registerProjects()
	s.registerBackingServices()
	s.registerControllerConfig()
	s.registerEtcdConfig()
	s.registerControllerUpdate()
	s.registerSoftwarePreparation()
	s.registerSoftwareActivation()
	s.registerImageFetch()
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
	s.registerRecoveryPointRemoval()
	s.registerBackupRestore()
	s.registerBackupKeyRoutes()
	s.registerVolumes()
	s.registerAttaches()
	s.registerBackingChecks()
	s.registerEdgeChecks()
	s.registerDNSChecks()
	s.registerEnvironmentDNS()
	s.registerTasks()
	s.registerHost()
	s.registerHostTerminal()
	return s
}

// HTTPHandler returns the complete Controller HTTP surface without opening a
// listener. Release validation uses it to exercise the production dispatcher
// and embedded Console through the same server assembled by application DI.
func (s *Server) HTTPHandler() http.Handler {
	return s.withRequestReference(s.requestHandler())
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
