package agentchannel

import (
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

// Server terminates the authenticated Controller side of AgentChannel.Connect.
type Server struct {
	agentpb.UnimplementedAgentChannelServer
	auth                   Authenticator
	sessions               *Registry
	staging                BackupStagingStore
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
	now                    func() time.Time
}

// New creates the native channel. Every executable operation is configured
// through its typed reporting/dispatch owner, not a generic plan resolver.
func New(auth Authenticator, sessions *Registry, staging BackupStagingStore, secrets RestoreIdentityReleaser, scriptCheckpoints ScriptCheckpointer) *Server {
	if sessions == nil {
		sessions = NewRegistry()
	}
	return &Server{auth: auth, sessions: sessions, staging: staging, secrets: secrets, scriptCheckpoints: scriptCheckpoints, now: time.Now}
}

// Connect authenticates the first message, publishes the authorized config,
// and then owns the live session until disconnect or fencing.
