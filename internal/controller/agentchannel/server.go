package agentchannel

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

// Server terminates the authenticated Controller side of AgentChannel.Connect.
type Server struct {
	agentpb.UnimplementedAgentChannelServer
	auth                   Authenticator
	sessions               *Registry
	tasks                  TaskStore
	plans                  PlanResolver
	materials              MaterializationResolver
	secrets                RestoreIdentityReleaser
	scriptCheckpoints      ScriptCheckpointer
	workIdentities         WorkIdentityResolver
	workStarts             WorkStarter
	workDispatch           WorkDispatcher
	releaseCheckpoints     ReleaseWorkCheckpointer
	environmentProvision   EnvironmentProvisionReporter
	hierarchyReports       HierarchyReporter
	attachReports          AttachReporter
	attachProvisionReports AttachProvisionReporter
	networkReports         NetworkReporter
	componentReports       ComponentReporter
	backingReports         BackingReporter
	resolverReports        ResolverReporter
	entryReports           EntryReporter
	volumeReports          VolumeReporter
	lifecycleReports       LifecycleReporter
	backupWorkReports      BackupWorkReporter
	backupWorkSecrets      BackupWorkSecretResolver
	removalRecoveryReports RemovalRecoveryReporter
	scriptCompletion       ScriptWorkCompleter
	backingHookCheckpoints BackingHookCheckpointer
	scriptArtifacts        ScriptArtifactResolver
	now                    func() time.Time
}

func (s *Server) EnableBackingHookCheckpoints(checkpointer BackingHookCheckpointer) error {
	if s == nil || checkpointer == nil || s.backingHookCheckpoints != nil {
		return errs.New(errs.KindInternal, "backing hook checkpoint service is invalid")
	}
	s.backingHookCheckpoints = checkpointer
	return nil
}

func (s *Server) EnableScriptArtifacts(resolver ScriptArtifactResolver) error {
	if s == nil || resolver == nil {
		return errs.New(errs.KindInternal, "Script artifact resolver is required")
	}
	s.scriptArtifacts = resolver
	return nil
}

// New returns an AgentChannel server backed by the supplied authenticator and
// session registry. A nil registry creates an isolated registry.
func New(auth Authenticator, sessions *Registry, tasks TaskStore, plans PlanResolver) *Server {
	return NewWithMaterializations(auth, sessions, tasks, plans, nil)
}

// NewWithMaterializations returns a server with the transient value resolver
// needed by metadata-only materialization steps.
func NewWithMaterializations(
	auth Authenticator,
	sessions *Registry,
	tasks TaskStore,
	plans PlanResolver,
	materials MaterializationResolver,
) *Server {
	return NewWithPrivateTransfers(auth, sessions, tasks, plans, materials, nil)
}

// NewWithPrivateTransfers returns a server with both transient plaintext
// resolvers used by the sole authenticated stream send loop.
func NewWithPrivateTransfers(
	auth Authenticator,
	sessions *Registry,
	tasks TaskStore,
	plans PlanResolver,
	materials MaterializationResolver,
	secrets RestoreIdentityReleaser,
) *Server {
	return NewWithRuntimeServices(auth, sessions, tasks, plans, materials, secrets)
}

// NewWithRuntimeServices returns a server with all private transfer and
// durable operation-boundary services used by the authenticated stream loop.
func NewWithRuntimeServices(
	auth Authenticator,
	sessions *Registry,
	tasks TaskStore,
	plans PlanResolver,
	materials MaterializationResolver,
	secrets RestoreIdentityReleaser,
) *Server {
	if sessions == nil {
		sessions = NewRegistry()
	}
	return &Server{
		auth:      auth,
		sessions:  sessions,
		tasks:     tasks,
		plans:     plans,
		materials: materials,
		secrets:   secrets,
		now:       time.Now,
	}
}

func NewWithScriptRuntimeServices(
	auth Authenticator,
	sessions *Registry,
	tasks TaskStore,
	plans PlanResolver,
	materials MaterializationResolver,
	secrets RestoreIdentityReleaser,
	scripts ScriptArtifactResolver,
	scriptCheckpoints ScriptCheckpointer,
) *Server {
	server := NewWithRuntimeServices(
		auth, sessions, tasks, plans, materials, secrets,
	)
	server.scriptArtifacts = scripts
	server.scriptCheckpoints = scriptCheckpoints
	return server
}

// Connect authenticates the first message, publishes the authorized config,
// and then owns the live session until disconnect or fencing.
