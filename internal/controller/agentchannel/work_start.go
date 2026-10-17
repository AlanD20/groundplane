package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// WorkIdentityResolver checks the authenticated credential generation and reads
// its durable host binding plus current executor-process generation. Credential
// rotation and process registration have distinct counters. Request fields are
// never used to select the host or either generation.
type WorkIdentityResolver interface {
	ResolveWorkIdentity(context.Context, string, uint64, [16]byte) (agentregistration.WorkAuthorization, error)
	RegisterWorkProcess(context.Context, string, uint64, [16]byte, uint16, []workexecution.Operation) (workexecution.Identity, error)
}

type WorkStarter interface {
	Start(context.Context, agentregistration.WorkAuthorization, *agentpb.WorkStartRequest) (*agentpb.WorkStartAck, error)
	Operations() []workexecution.Operation
}

type ScriptWorkCompleter interface {
	FailScript(context.Context, agentregistration.WorkAuthorization, *agentpb.ScriptWorkFailureRequest) (*agentpb.ScriptWorkFailureAck, error)
	CompleteScriptRecovery(context.Context, agentregistration.WorkAuthorization, *agentpb.ScriptRecoveryCompletionRequest) (*agentpb.ScriptRecoveryCompletionAck, error)
	CompleteScript(context.Context, agentregistration.WorkAuthorization, *agentpb.ScriptWorkCompletionRequest) (*agentpb.ScriptWorkCompletionAck, error)
}

type HierarchyReporter interface {
	FailEnvironmentRemoval(context.Context, agentregistration.WorkAuthorization, *agentpb.EnvironmentRemovalFailureRequest) (*agentpb.EnvironmentRemovalFailureAck, error)
	CheckpointEnvironmentRemoval(context.Context, agentregistration.WorkAuthorization, *agentpb.EnvironmentRemovalCheckpointRequest) (*agentpb.EnvironmentRemovalCheckpointAck, error)
	CompleteEnvironmentRemoval(context.Context, agentregistration.WorkAuthorization, *agentpb.EnvironmentRemovalCompletionRequest) (*agentpb.EnvironmentRemovalCompletionAck, error)
}

type WorkDispatcher interface {
	Ready(context.Context, agentregistration.WorkAuthorization, uint16) ([]workdispatch.Issue, error)
	Dispatch(context.Context, agentregistration.WorkAuthorization, int32, map[string]string, func(*agentpb.ControllerMessage) (bool, error)) ([]workdispatch.Issue, error)
}

type ReleaseWorkCheckpointer interface {
	CheckpointRecovery(context.Context, agentregistration.WorkAuthorization, *agentpb.ReleaseRecoveryCheckpointRequest) (*agentpb.ReleaseRecoveryCheckpointAck, error)
	FailRelease(context.Context, agentregistration.WorkAuthorization, *agentpb.ReleaseWorkFailureRequest) (*agentpb.ReleaseWorkFailureAck, error)
	CompleteRelease(context.Context, agentregistration.WorkAuthorization, *agentpb.ReleaseWorkCompletionRequest) (*agentpb.ReleaseWorkCompletionAck, error)
	LoadReleaseConfiguration(context.Context, agentregistration.WorkAuthorization, *agentpb.ReleaseConfigurationRequest) (*agentpb.ReleaseConfigurationManifest, error)
	LoadReleaseConfigurationFile(context.Context, agentregistration.WorkAuthorization, *agentpb.ReleaseConfigurationFileRequest) (*agentpb.ReleaseConfigurationFileDelivery, error)
	PublishReleaseEffect(context.Context, agentregistration.WorkAuthorization, *agentpb.ReleaseEffectEvidenceRequest) (*agentpb.ReleaseEffectEvidenceAck, error)
	CheckpointRelease(context.Context, agentregistration.WorkAuthorization, *agentpb.ReleaseWorkCheckpointRequest) (*agentpb.ReleaseWorkCheckpointAck, error)
	LoadReleaseHook(context.Context, agentregistration.WorkAuthorization, *agentpb.ReleaseHookRequest) (*agentpb.ReleaseHookDelivery, error)
}

// ConfigureWorkExecution is called during composition, before accepting streams.
// It has no fallback to the old Task assignment or singleton-host inference.
func (server *Server) ConfigureWorkExecution(identities WorkIdentityResolver, starts WorkStarter, releases ReleaseWorkCheckpointer, scripts ScriptWorkCompleter, removals HierarchyReporter, dispatch WorkDispatcher) error {
	if server == nil || identities == nil || starts == nil || releases == nil || scripts == nil || removals == nil || dispatch == nil || server.workIdentities != nil || server.workStarts != nil || server.releaseCheckpoints != nil || server.workDispatch != nil {
		return errs.New(errs.KindInternal, "native Agent work services are missing or already configured")
	}
	server.workIdentities, server.workStarts = identities, starts
	server.releaseCheckpoints = releases
	server.scriptCompletion = scripts
	server.hierarchyReports = removals
	server.workDispatch = dispatch
	return nil
}

// registerWorkProcess precedes normal traffic. Its catalog comes from composed
// operation owners, not names supplied by an Agent. Protocol compatibility is
// checked by session authentication before this call.
func (server *Server) registerWorkProcess(ctx context.Context, session *Session, generation uint64, capacity int32) (*agentpb.WorkExecutorRegistration, error) {
	if server.workIdentities == nil || server.workStarts == nil {
		return nil, errs.New(errs.KindInternal, "native Agent work services are not configured")
	}
	if capacity < 1 || capacity > 256 {
		return nil, errs.New(errs.KindValidationFailed, "Agent execution capacity must be between 1 and 256")
	}
	process, err := session.ProcessAuthentication()
	if err != nil {
		return nil, err
	}
	identity, err := server.workIdentities.RegisterWorkProcess(ctx, session.agentID, generation,
		process.ProcessGeneration, uint16(capacity), server.workStarts.Operations())
	if err != nil {
		return nil, err
	}
	if identity.Validate() != nil || identity.Executor != workexecution.Agent || identity.AgentID != session.agentID {
		return nil, errs.New(errs.KindInternal, "Agent registration returned an invalid executor identity")
	}
	return &agentpb.WorkExecutorRegistration{HostId: identity.HostID, AgentId: identity.AgentID, ExecutorGeneration: identity.Generation,
		ProcessGeneration: append([]byte(nil), process.ProcessGeneration[:]...)}, nil
}

func (server *Server) authenticatedWorkIdentity(ctx context.Context, session *Session, generation uint64) (agentregistration.WorkAuthorization, error) {
	if server.workIdentities == nil {
		return agentregistration.WorkAuthorization{}, errs.New(errs.KindInternal, "native Agent host binding is not configured")
	}
	process, err := session.ProcessAuthentication()
	if err != nil {
		return agentregistration.WorkAuthorization{}, err
	}
	agentID := session.agentID
	authorization, err := server.workIdentities.ResolveWorkIdentity(ctx, agentID, generation, process.ProcessGeneration)
	if err != nil {
		return agentregistration.WorkAuthorization{}, err
	}
	identity := authorization.Identity()
	if identity.Validate() != nil || identity.Executor != workexecution.Agent || identity.AgentID != agentID {
		return agentregistration.WorkAuthorization{}, errs.New(errs.KindStateConflict, "authenticated Agent host binding changed")
	}
	return authorization, nil
}

func (server *Server) startWork(ctx context.Context, session *Session, generation uint64,
	request *agentpb.WorkStartRequest,
) (*agentpb.ControllerMessage, error) {
	if server.workStarts == nil {
		return nil, errs.New(errs.KindInternal, "native Agent start service is not configured")
	}
	identity, err := server.authenticatedWorkIdentity(ctx, session, generation)
	if err != nil {
		return nil, err
	}
	ack, err := server.workStarts.Start(ctx, identity, request)
	if err != nil {
		return nil, err
	}
	return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_WorkStartAck{WorkStartAck: ack}}, nil
}
